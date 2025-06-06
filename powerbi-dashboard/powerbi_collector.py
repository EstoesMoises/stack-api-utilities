import argparse
import aiohttp
import asyncio
import json
import time
import itertools
import threading
import schedule
import signal
import sys
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
import logging
from typing import Dict, List, Optional, Set
from collections import defaultdict

# Global counters and caches
API_V2_CALLS = 0
API_V3_CALLS = 0
USER_CACHE = {}
USER_DETAILS_CACHE = {}
SME_CACHE = {}
ACCEPTED_ANSWERS_CACHE = {}

# Rate limiting configuration according to Teams API v3 Docs.
RATE_LIMIT_REQUESTS = 45  # Stay under 50 to be safe
RATE_LIMIT_WINDOW = 2.0   # 2 seconds
RATE_LIMIT_RETRY_DELAY = 3.0  # Wait 3 seconds on 429 error

# Async semaphore for rate limiting
RATE_LIMITER = None

# Global configuration
CONFIG = {}
VERBOSE = False
RUNNING = True

def setup_logging(verbose: bool = False):
    """Setup logging configuration"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('powerbi_collector.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def signal_handler(signum, frame):
    """Handle graceful shutdown"""
    global RUNNING
    logger.info("Received interrupt signal, shutting down gracefully...")
    RUNNING = False
    sys.exit(0)

def loading_animation(stop_event, message):
    """Show loading animation with spinner"""
    spinner = itertools.cycle(['|', '/', '-', '\\'])
    while not stop_event.is_set():
        print(f"\r{message} {next(spinner)}", end='', flush=True)
        time.sleep(0.2)

def force_api_v3(base_url: str) -> str:
    """Force URL to use API v3"""
    parsed_url = urlparse(base_url.strip())
    return f"{parsed_url.scheme}://{parsed_url.netloc}/api/v3"

def get_api_v2_url(base_url: str) -> str:
    """Get API v2.3 URL for additional data"""
    parsed_url = urlparse(base_url.strip())
    return f"{parsed_url.scheme}://{parsed_url.netloc}/api/2.3"

def get_date_range(time_filter):
    """Generate from/to dates based on the selected time filter"""
    today = datetime.now()
    
    if time_filter == "week":
        # Last week
        from_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    elif time_filter == "month":
        # Last month
        from_date = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    elif time_filter == "quarter":
        # Last quarter (90 days)
        from_date = (today - timedelta(days=90)).strftime("%Y-%m-%d")
    elif time_filter == "year":
        # Last year
        from_date = (today - timedelta(days=365)).strftime("%Y-%m-%d")
    elif time_filter == "custom":
        # Custom range will be handled via explicit from_date/to_date parameters
        return None, None
    else:
        # No filter - return None to indicate no date filtering
        return None, None
        
    to_date = today.strftime("%Y-%m-%d")
    return from_date, to_date

def convert_epoch_to_utc_timestamp(epoch_timestamp):
    """Convert epoch timestamp to UTC timestamp format like 2024-01-03T17:21:01.323"""
    if not epoch_timestamp:
        return None
    
    try:
        # Convert epoch to datetime in UTC
        dt = datetime.fromtimestamp(epoch_timestamp, tz=timezone.utc)
        # Format as requested: YYYY-MM-DDTHH:MM:SS.mmm
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]  # Remove last 3 digits from microseconds to get milliseconds
    except (ValueError, TypeError, OSError) as e:
        log(f"Error converting epoch timestamp {epoch_timestamp}: {str(e)}")
        return None

def log(message: str):
    """Log message if verbose mode is enabled"""
    if VERBOSE:
        logger.debug(message)
    else:
        logger.info(message)

async def make_api_request(session: aiohttp.ClientSession, url: str, params: Dict = None) -> Optional[Dict]:
    """Make async API request with rate limiting and retry logic"""
    global API_V3_CALLS, RATE_LIMITER
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Use semaphore for rate limiting
            async with RATE_LIMITER:
                API_V3_CALLS += 1
                
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 429:
                        # Rate limited - wait and retry
                        retry_after = response.headers.get('Retry-After', str(RATE_LIMIT_RETRY_DELAY))
                        wait_time = float(retry_after)
                        log(f"Rate limited (429), waiting {wait_time} seconds before retry {retry_count + 1}/{max_retries}")
                        await asyncio.sleep(wait_time)
                        retry_count += 1
                        continue
                    
                    if response.status != 200:
                        log(f"API request failed with status {response.status} for {url}")
                        return None
                    
                    return await response.json()
                    
        except asyncio.TimeoutError:
            log(f"Request timeout for {url}, retry {retry_count + 1}/{max_retries}")
            retry_count += 1
            await asyncio.sleep(1)
        except Exception as e:
            if "429" in str(e) or "rate limit" in str(e).lower():
                log(f"Rate limit error: {str(e)}, waiting {RATE_LIMIT_RETRY_DELAY} seconds before retry {retry_count + 1}/{max_retries}")
                await asyncio.sleep(RATE_LIMIT_RETRY_DELAY)
                retry_count += 1
                continue
            else:
                log(f"API request failed for {url}: {str(e)}")
                return None
    
    log(f"Max retries exceeded for {url}")
    return None

async def make_api_v2_request(session: aiohttp.ClientSession, url: str, params: Dict = None) -> Optional[Dict]:
    """Make async API v2.3 request with rate limiting"""
    global API_V2_CALLS, RATE_LIMITER
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            async with RATE_LIMITER:
                API_V2_CALLS += 1
                
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 429:
                        retry_after = response.headers.get('Retry-After', str(RATE_LIMIT_RETRY_DELAY))
                        wait_time = float(retry_after)
                        log(f"Rate limited (429), waiting {wait_time} seconds before retry {retry_count + 1}/{max_retries}")
                        await asyncio.sleep(wait_time)
                        retry_count += 1
                        continue
                    
                    if response.status != 200:
                        log(f"API v2 request failed with status {response.status} for {url}")
                        return None
                    
                    return await response.json()
                    
        except asyncio.TimeoutError:
            log(f"Request timeout for {url}, retry {retry_count + 1}/{max_retries}")
            retry_count += 1
            await asyncio.sleep(1)
        except Exception as e:
            if "429" in str(e) or "rate limit" in str(e).lower():
                log(f"Rate limit error: {str(e)}, waiting {RATE_LIMIT_RETRY_DELAY} seconds before retry {retry_count + 1}/{max_retries}")
                await asyncio.sleep(RATE_LIMIT_RETRY_DELAY)
                retry_count += 1
                continue
            else:
                log(f"API v2 request failed for {url}: {str(e)}")
                return None
    
    log(f"Max retries exceeded for {url}")
    return None

async def get_paginated_data(session: aiohttp.ClientSession, endpoint: str, params: Dict = None) -> List[Dict]:
    """Generic async function to get all paginated data from an endpoint with optional date filtering"""
    all_items = []
    page = 1
    total_pages = None
    
    # Add date filtering to params if configured
    if params is None:
        params = {}
    
    # Add date filter if configured in CONFIG
    if CONFIG.get('from_date') and CONFIG.get('to_date'):
        params.update({
            'from': CONFIG['from_date'],
            'to': CONFIG['to_date']
        })
        log(f"Applying date filter: from {CONFIG['from_date']} to {CONFIG['to_date']}")
    
    filter_message = ""
    if CONFIG.get('from_date') and CONFIG.get('to_date'):
        filter_message = f" with date filter from {CONFIG['from_date']} to {CONFIG['to_date']}"
    
    log(f"Starting to fetch all data from {endpoint}{filter_message}")
    
    while True:
        current_params = params.copy()
        current_params.update({'page': page, 'pageSize': 100})
        
        url = f"{CONFIG['base_url']}/{endpoint}"
        log(f"Fetching page {page}/{total_pages or '?'} from {endpoint}")
        
        data = await make_api_request(session, url, current_params)
        if not data:
            break
            
        items = data.get("items", [])
        all_items.extend(items)
        
        if total_pages is None:
            total_pages = data.get("totalPages", 1)
            log(f"Total pages to fetch: {total_pages}")
        
        log(f"Retrieved {len(items)} items from page {page}/{total_pages}")
        
        if page >= total_pages:
            log(f"All pages fetched. Total items: {len(all_items)}")
            break
        
        page += 1
        
    return all_items

def extract_user_ids_from_questions(questions: List[Dict]) -> Set[int]:
    """Extract unique user IDs from question owners"""
    user_ids = set()
    
    for question in questions:
        owner = question.get('owner', {})
        user_id = owner.get('id')
        if user_id:
            user_ids.add(user_id)
    
    return user_ids

def extract_user_ids_from_accepted_answers(accepted_answers: Dict[int, Dict]) -> Set[int]:
    """Extract unique user IDs from accepted answer owners"""
    user_ids = set()
    
    for answer_data in accepted_answers.values():
        owner = answer_data.get('owner', {})
        user_id = owner.get('id')
        if user_id:
            user_ids.add(user_id)
    
    return user_ids

async def get_users_by_ids(session: aiohttp.ClientSession, user_ids: List[int]) -> List[Dict]:
    """Get user data for specific user IDs using v3 API"""
    if not user_ids:
        return []
    
    users = []
    batch_size = 20  # Conservative batch size for v3 API
    
    for i in range(0, len(user_ids), batch_size):
        batch = user_ids[i:i + batch_size]
        
        # Use individual requests for v3 API (no bulk endpoint)
        tasks = []
        for user_id in batch:
            url = f"{CONFIG['base_url']}/users/{user_id}"
            tasks.append(make_api_request(session, url))
        
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for user_id, result in zip(batch, batch_results):
            if isinstance(result, dict) and not isinstance(result, Exception):
                users.append(result)
            else:
                log(f"Failed to fetch user {user_id}: {result}")
        
        log(f"Fetched user data for batch {i//batch_size + 1}/{(len(user_ids) + batch_size - 1)//batch_size}")
        
        # Small delay between batches
        await asyncio.sleep(0.2)
    
    return users

async def get_accepted_answer_for_question(session: aiohttp.ClientSession, question_id: int) -> Optional[Dict]:
    """Get the accepted answer for a specific question"""
    url = f"{CONFIG['base_url']}/questions/{question_id}/answers"
    
    # Get first page to find accepted answer
    data = await make_api_request(session, url, {'page': 1, 'pageSize': 100})
    if not data or not data.get('items'):
        return None
    
    # Find the accepted answer
    for answer in data['items']:
        if answer.get('isAccepted', False):
            return {
                'id': answer.get('id'),
                'creationDate': answer.get('creationDate'),
                'score': answer.get('score'),
                'owner': answer.get('owner')
            }
    
    # If not found in first page, check remaining pages
    total_pages = data.get("totalPages", 1)
    for page in range(2, total_pages + 1):
        page_data = await make_api_request(session, url, {'page': page, 'pageSize': 100})
        if not page_data or not page_data.get('items'):
            continue
            
        for answer in page_data['items']:
            if answer.get('isAccepted', False):
                return {
                    'id': answer.get('id'),
                    'creationDate': answer.get('creationDate'),
                    'score': answer.get('score'),
                    'owner': answer.get('owner')
                }
    
    return None

async def get_accepted_answers_batch(session: aiohttp.ClientSession, questions_with_accepted: List[Dict]) -> Dict[int, Dict]:
    """Get accepted answers for questions that have them, using async batch processing"""
    accepted_answers = {}
    
    # Create semaphore for concurrent requests (respecting rate limits)
    concurrent_limit = min(20, RATE_LIMIT_REQUESTS // 2)  # Conservative limit
    semaphore = asyncio.Semaphore(concurrent_limit)
    
    async def fetch_accepted_answer(question):
        async with semaphore:
            question_id = question.get('id')
            if not question_id:
                return
            
            try:
                accepted_answer = await get_accepted_answer_for_question(session, question_id)
                if accepted_answer:
                    accepted_answers[question_id] = accepted_answer
                    log(f"Found accepted answer for question {question_id}")
            except Exception as e:
                log(f"Error fetching accepted answer for question {question_id}: {str(e)}")
    
    # Process in batches to avoid overwhelming the API
    batch_size = 50
    for i in range(0, len(questions_with_accepted), batch_size):
        batch = questions_with_accepted[i:i + batch_size]
        log(f"Processing accepted answers batch {i//batch_size + 1}/{(len(questions_with_accepted) + batch_size - 1)//batch_size}")
        
        tasks = [fetch_accepted_answer(question) for question in batch]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Small delay between batches
        await asyncio.sleep(0.5)
    
    log(f"Retrieved {len(accepted_answers)} accepted answers")
    return accepted_answers

async def get_all_questions(session: aiohttp.ClientSession) -> List[Dict]:
    """Get all questions from the API with optional date filtering"""
    return await get_paginated_data(session, "questions")

async def get_unanswered_questions(session: aiohttp.ClientSession) -> List[Dict]:
    """Get all unanswered questions from the API with optional date filtering"""
    return await get_paginated_data(session, "questions", {"isAnswered": "false"})

async def get_questions_with_accepted_answers(session: aiohttp.ClientSession) -> List[Dict]:
    """Get all questions with accepted answers from the API with optional date filtering"""
    return await get_paginated_data(session, "questions", {"hasAcceptedAnswer": "true"})

async def get_all_articles(session: aiohttp.ClientSession) -> List[Dict]:
    """Get all articles from the API with optional date filtering"""
    return await get_paginated_data(session, "articles")

async def get_user_detailed_info_batch(session: aiohttp.ClientSession, user_ids: List[int], batch_size: int = 20) -> Dict[int, Dict]:
    """Get detailed user information from API v2.3 in batches"""
    user_details = {}
    
    # Filter out None values from user_ids
    valid_user_ids = [uid for uid in user_ids if uid is not None]
    
    if not valid_user_ids:
        log("No valid user IDs to fetch detailed info for")
        return user_details
    
    for i in range(0, len(valid_user_ids), batch_size):
        batch = valid_user_ids[i:i + batch_size]
        
        # Convert batch to semicolon-separated string for v2.3 API
        ids_string = ";".join(map(str, batch))
        
        v2_url = f"{CONFIG['api_v2_base']}/users/{ids_string}?order=desc&sort=reputation"
        log(f"Batch fetching detailed info for {len(batch)} users")
        
        user_data = await make_api_v2_request(session, v2_url)
        
        if user_data and 'items' in user_data:
            for user_item in user_data['items']:
                user_id = user_item.get('user_id') if user_item else None
                if user_id:
                    user_details[user_id] = user_item
                    
    return user_details

async def get_sme_data_for_tags(session: aiohttp.ClientSession, tag_ids: List[int]) -> Dict[int, List[int]]:
    """Get SME data for given tag IDs. Returns dict of tag_id -> list of user_ids"""
    sme_data = {}
    
    # Process in batches to avoid overwhelming the API
    batch_size = 10
    for i in range(0, len(tag_ids), batch_size):
        batch = tag_ids[i:i + batch_size]
        tasks = []
        
        for tag_id in batch:
            url = f"{CONFIG['base_url']}/tags/{tag_id}/subject-matter-experts"
            tasks.append(make_api_request(session, url))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for tag_id, result in zip(batch, results):
            if isinstance(result, dict) and 'users' in result:
                user_ids = [user.get('id') for user in result['users'] if user.get('id')]
                sme_data[tag_id] = user_ids
            else:
                sme_data[tag_id] = []
        
        log(f"Processed SME data for batch {i//batch_size + 1}/{(len(tag_ids) + batch_size - 1)//batch_size}")
    
    return sme_data

def calculate_account_longevity(creation_date: int) -> int:
    """Calculate account longevity in days"""
    if not creation_date:
        return 0
    
    created = datetime.fromtimestamp(creation_date)
    now = datetime.now()
    return (now - created).days

def get_user_sme_tags(user_id: int, all_sme_data: Dict[int, List[int]]) -> List[str]:
    """Get list of tag names where user is an SME"""
    sme_tag_names = []
    
    for tag_id, sme_user_ids in all_sme_data.items():
        if user_id in sme_user_ids:
            # Find the tag name from our tag cache or fetch it
            tag_name = SME_CACHE.get(tag_id, f"tag_{tag_id}")
            sme_tag_names.append(tag_name)
    
    return sme_tag_names

def build_user_metrics_from_question_users(question_users: List[Dict], all_questions: List[Dict], 
                                          unanswered_questions: List[Dict], accepted_answer_questions: List[Dict],
                                          all_articles: List[Dict], user_details: Dict[int, Dict], 
                                          all_sme_data: Dict[int, List[int]]) -> Dict[int, Dict]:
    """Build comprehensive user metrics for users found in questions only"""
    user_metrics = {}
    
    # Group questions by owner
    questions_by_user = defaultdict(list)
    for question in all_questions:
        owner = question.get('owner')
        if owner and owner.get('id'):
            questions_by_user[owner.get('id')].append(question)
    
    # Group unanswered questions by owner
    unanswered_by_user = defaultdict(list)
    for question in unanswered_questions:
        owner = question.get('owner')
        if owner and owner.get('id'):
            unanswered_by_user[owner.get('id')].append(question)
    
    # Group accepted answer questions by owner
    accepted_by_user = defaultdict(list)
    for question in accepted_answer_questions:
        owner = question.get('owner')
        if owner and owner.get('id'):
            accepted_by_user[owner.get('id')].append(question)
    
    # Group articles by owner
    articles_by_user = defaultdict(list)
    for article in all_articles:
        owner = article.get('owner')
        if owner and owner.get('id'):
            articles_by_user[owner.get('id')].append(article)
    
    # Build metrics for each user from questions
    for user in question_users:
        user_id = user.get('id')
        if not user_id:
            continue
        
        detailed_info = user_details.get(user_id, {})
        user_questions = questions_by_user.get(user_id, [])
        user_unanswered = unanswered_by_user.get(user_id, [])
        user_accepted = accepted_by_user.get(user_id, [])
        user_articles = articles_by_user.get(user_id, [])
        
        # Calculate account longevity
        creation_date = detailed_info.get('creation_date')
        account_longevity = calculate_account_longevity(creation_date)
        
        # Get SME tags
        sme_tags = get_user_sme_tags(user_id, all_sme_data)
        is_sme = len(sme_tags) > 0
        
        # Convert epoch timestamps to UTC format
        creation_date_utc = convert_epoch_to_utc_timestamp(creation_date)
        last_login_date_utc = convert_epoch_to_utc_timestamp(detailed_info.get('last_access_date'))
        
        user_metrics[user_id] = {
            'DisplayName': user.get('name'),
            'Title': user.get('jobTitle'),
            'Department': user.get('department'),
            'Reputation': user.get('reputation', 0) or (detailed_info.get('reputation', 0) if detailed_info else 0),
            'Account_Longevity': account_longevity,
            'Total_Questions_Asked': len(user_questions),
            'Total_Questions_No_Answers': len(user_unanswered),
            'Answers': 'N/A',  # Cannot calculate without global /answers endpoint
            'Questions_With_Accepted_Answers': len(user_accepted),
            'Articles': len(user_articles),
            'Location': detailed_info.get('location') if detailed_info else None,
            'Account_ID': user.get('accountId'),
            'User_ID': user_id,
            'Creation_Date': creation_date_utc,
            'User_Type': user.get('role'),
            'Is_SME': is_sme,
            'Joined_UTC': creation_date_utc,
            'Last_Login_Date': last_login_date_utc,
            'Tags': sme_tags
        }
    
    return user_metrics

def process_question_data(question: Dict, user_metrics: Dict[int, Dict], accepted_answers: Dict[int, Dict]) -> Dict:
    """Process a single question into question-centric format with user data and accepted answer"""
    try:
        if not question:
            return None
        
        question_id = question.get('id')
        if not question_id:
            return None
        
        # Get question owner info
        owner = question.get('owner', {})
        owner_id = owner.get('id')
        
        # Get user metrics for the question owner
        owner_metrics = user_metrics.get(owner_id, {}) if owner_id else {}
        
        # Extract question tags
        question_tags = []
        for tag in question.get('tags', []):
            if isinstance(tag, dict):
                question_tags.append(tag.get('name', ''))
            else:
                question_tags.append(str(tag))
        
        # Get accepted answer data
        accepted_answer = accepted_answers.get(question_id)
        accepted_answer_data = None
        
        if accepted_answer:
            answer_owner = accepted_answer.get('owner', {})
            answer_owner_id = answer_owner.get('id')
            answer_owner_metrics = user_metrics.get(answer_owner_id, {}) if answer_owner_id else {}
            
            accepted_answer_data = {
                'answer_id': accepted_answer.get('id'),
                'creation_date': accepted_answer.get('creationDate'),
                'score': accepted_answer.get('score'),
                'owner': {
                    'id': answer_owner_id,
                    'display_name': answer_owner.get('name') or answer_owner_metrics.get('DisplayName'),
                    'reputation': answer_owner.get('reputation') or answer_owner_metrics.get('Reputation', 'Unknown'),
                    'account_id': answer_owner.get('accountId') or answer_owner_metrics.get('Account_ID'),
                    'role': answer_owner.get('role') or answer_owner_metrics.get('User_Type'),
                    'title': answer_owner_metrics.get('Title'),
                    'department': answer_owner_metrics.get('Department'),
                    'is_sme': answer_owner_metrics.get('Is_SME', False),
                    'sme_tags': answer_owner_metrics.get('Tags', [])
                }
            }
        
        # Build question-centric data
        question_data = {
            # Question fields
            'Question_ID': question_id,
            'QuestionTitle': question.get('title'),
            'QuestionTags': question_tags,
            
            # User fields (from owner)
            'owner': {
                'DisplayName': owner_metrics.get('DisplayName'),
                'Title': owner_metrics.get('Title'),
                'Department': owner_metrics.get('Department'),
                'Reputation': owner_metrics.get('Reputation', 'Unknown'),
                'Account_Longevity_Days': owner_metrics.get('Account_Longevity', 'Unknown'),
                'Total_Questions_Asked': owner_metrics.get('Total_Questions_Asked', 'Unknown'),
                'Total_Questions_No_Answers': owner_metrics.get('Total_Questions_No_Answers', 'Unknown'),
                'Questions_With_Accepted_Answers': owner_metrics.get('Questions_With_Accepted_Answers', 'Unknown'),
                'Articles': owner_metrics.get('Articles', 'Unknown'),
                'Location': owner_metrics.get('Location'),
                'Account_ID': owner_metrics.get('Account_ID'),
                'User_ID': owner_metrics.get('User_ID'),
                'Creation_Date': owner_metrics.get('Creation_Date'),
                'Joined_UTC': owner_metrics.get('Joined_UTC'), 
                'User_Type': owner_metrics.get('User_Type'),
                'Is_SME': owner_metrics.get('Is_SME', False),
                'Last_Login_Date': owner_metrics.get('Last_Login_Date'),
                'Tags': owner_metrics.get('Tags', []),
            },
            
            # Accepted Answer Data
            'accepted_answer': accepted_answer_data,
            
            # Metadata
            'Question_Creation_Date': question.get('creationDate'),
            'Question_Score': question.get('score', 'Unknown'),
            'Question_View_Count': question.get('viewCount', 'Unknown'),
            'Question_Answer_Count': question.get('answerCount', 'Unknown'),
            'Question_Is_Answered': question.get('isAnswered', False),
            'Last_Updated': datetime.now().isoformat(),
            'Data_Collection_Timestamp': convert_epoch_to_utc_timestamp(datetime.now().timestamp())
        }
        
        return question_data
        
    except Exception as e:
        log(f"Unexpected error in process_question_data for question {question.get('id') if question else 'None'}: {str(e)}")
        return None

async def collect_powerbi_data() -> List[Dict]:
    """Main async function to collect question-centric PowerBI data"""
    global RATE_LIMITER
    
    filter_message = ""
    if CONFIG.get('from_date') and CONFIG.get('to_date'):
        filter_message = f" with date filter from {CONFIG['from_date']} to {CONFIG['to_date']}"
    
    log(f"Starting question-centric PowerBI data collection{filter_message}")
    
    # Initialize rate limiter
    RATE_LIMITER = asyncio.Semaphore(RATE_LIMIT_REQUESTS)
    
    # Create aiohttp session
    headers = CONFIG['headers']
    timeout = aiohttp.ClientTimeout(total=300)  # 5 minute timeout
    
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        # Step 1: Get all questions (with date filtering if configured)
        stop_event = threading.Event()
        loading_message = f"Fetching questions{filter_message}..."
        loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
        
        if not VERBOSE:
            loading_thread.start()
        
        try:
            all_questions = await get_all_questions(session)
        finally:
            stop_event.set()
            if not VERBOSE:
                loading_thread.join()
                print("\rQuestion retrieval complete!        ")
        
        if not all_questions:
            log("No questions found")
            return []
        
        # Step 2: Extract unique user IDs from questions
        question_user_ids = extract_user_ids_from_questions(all_questions)
        log(f"Found {len(question_user_ids)} unique users from questions")
        
        # Step 3: Get unanswered questions (with date filtering if configured)
        stop_event = threading.Event()
        loading_message = f"Fetching unanswered questions{filter_message}..."
        loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
        
        if not VERBOSE:
            loading_thread.start()
        
        try:
            unanswered_questions = await get_unanswered_questions(session)
        finally:
            stop_event.set()
            if not VERBOSE:
                loading_thread.join()
                print("\rUnanswered questions retrieval complete!        ")
        
        # Step 4: Get questions with accepted answers (with date filtering if configured)
        stop_event = threading.Event()
        loading_message = f"Fetching questions with accepted answers{filter_message}..."
        loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
        
        if not VERBOSE:
            loading_thread.start()
        
        try:
            accepted_answer_questions = await get_questions_with_accepted_answers(session)
        finally:
            stop_event.set()
            if not VERBOSE:
                loading_thread.join()
                print("\rAccepted answer questions retrieval complete!        ")
        
        # Step 5: Get accepted answers data
        stop_event = threading.Event()
        loading_message = f"Fetching accepted answers for {len(accepted_answer_questions)} questions..."
        loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
        
        if not VERBOSE:
            loading_thread.start()
        
        try:
            accepted_answers = await get_accepted_answers_batch(session, accepted_answer_questions)
        finally:
            stop_event.set()
            if not VERBOSE:
                loading_thread.join()
                print("\rAccepted answers retrieval complete!        ")
        
        # Step 6: Extract additional user IDs from accepted answers
        answer_user_ids = extract_user_ids_from_accepted_answers(accepted_answers)
        log(f"Found {len(answer_user_ids)} unique users from accepted answers")
        
        # Combine all unique user IDs
        all_user_ids = question_user_ids.union(answer_user_ids)
        log(f"Total unique users to fetch: {len(all_user_ids)}")
        
        # Step 7: Get user data for the identified users only
        stop_event = threading.Event()
        loading_message = f"Fetching user data for {len(all_user_ids)} users..."
        loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
        
        if not VERBOSE:
            loading_thread.start()
        
        try:
            question_users = await get_users_by_ids(session, list(all_user_ids))
        finally:
            stop_event.set()
            if not VERBOSE:
                loading_thread.join()
                print("\rUser data retrieval complete!        ")
        
        # Step 8: Get all articles (with date filtering if configured) - needed for user metrics
        stop_event = threading.Event()
        loading_message = f"Fetching articles{filter_message}..."
        loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
        
        if not VERBOSE:
            loading_thread.start()
        
        try:
            all_articles = await get_all_articles(session)
        finally:
            stop_event.set()
            if not VERBOSE:
                loading_thread.join()
                print("\rArticle retrieval complete!        ")
        
        # Step 9: Get detailed user info for the identified users
        stop_event = threading.Event()
        loading_message = f"Fetching detailed info for {len(all_user_ids)} users..."
        loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
        
        if not VERBOSE:
            loading_thread.start()
        
        try:
            user_details = await get_user_detailed_info_batch(session, list(all_user_ids))
        finally:
            stop_event.set()
            if not VERBOSE:
                loading_thread.join()
                print("\rUser details retrieval complete!        ")
        
        # Step 10: Get SME data for all tags
        all_tag_ids = set()
        for question in all_questions:
            for tag in question.get('tags', []):
                if isinstance(tag, dict) and tag.get('id'):
                    all_tag_ids.add(tag.get('id'))
                    # Cache tag name for later use
                    SME_CACHE[tag.get('id')] = tag.get('name', f"tag_{tag.get('id')}")
        
        stop_event = threading.Event()
        loading_message = f"Fetching SME data for {len(all_tag_ids)} tags..."
        loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
        
        if not VERBOSE:
            loading_thread.start()
        
        try:
            all_sme_data = await get_sme_data_for_tags(session, list(all_tag_ids))
        finally:
            stop_event.set()
            if not VERBOSE:
                loading_thread.join()
                print("\rSME data retrieval complete!        ")
        
        log(f"Content summary: {len(all_questions)} questions, {len(question_users)} users, {len(all_articles)} articles, {len(accepted_answers)} accepted answers")
        
        # Step 11: Build user metrics for question users only
        log("Building comprehensive user metrics for question users...")
        user_metrics = build_user_metrics_from_question_users(
            question_users, all_questions, unanswered_questions, accepted_answer_questions,
            all_articles, user_details, all_sme_data
        )
        
        # Step 12: Process all questions into question-centric format
        log(f"Processing {len(all_questions)} questions")
        
        powerbi_data = []
        for i, question in enumerate(all_questions, 1):
            try:
                question_data = process_question_data(question, user_metrics, accepted_answers)
                if question_data:
                    powerbi_data.append(question_data)
                    
                if i % 100 == 0 or i == len(all_questions):
                    log(f"Processed {i}/{len(all_questions)} questions")
                    
            except Exception as e:
                log(f"Error processing question {question.get('id')}: {str(e)}")
        
        log(f"Collected question-centric data for {len(powerbi_data)} questions")
        return powerbi_data

def save_data_to_json(data: List[Dict], filename: str = None):
    """Save collected data to JSON file"""
    if not filename:

        filename = f"powerbi_questions_data.json"
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        
        log(f"Data saved to {filename}")
        return filename
        
    except Exception as e:
        log(f"Error saving data to JSON: {str(e)}")
        raise

async def export_powerbi_data():
    """Main async export function for cron job"""
    global API_V2_CALLS, API_V3_CALLS
    
    start_time = datetime.now()
    
    # Create filter message for logging
    filter_message = ""
    if CONFIG.get('filter_type'):
        if CONFIG['filter_type'] == "custom":
            filter_message = f" for custom date range (from {CONFIG['from_date']} to {CONFIG['to_date']})"
        else:
            filter_message = f" for the last {CONFIG['filter_type']} (from {CONFIG['from_date']} to {CONFIG['to_date']})"
    
    log(f"Question-centric PowerBI data export started at {start_time}{filter_message}")
    
    # Reset counters
    API_V2_CALLS = 0
    API_V3_CALLS = 0
    
    try:
        # Collect all data efficiently using async
        powerbi_data = await collect_powerbi_data()
        
        if not powerbi_data:
            log("No data collected")
            return
        
        # Save to JSON file
        filename = CONFIG.get('output_file')
        
        save_data_to_json(powerbi_data, filename)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Print summary
        print(f"\n✅ Question-centric PowerBI data export complete!")
        print(f"   Data saved to: {filename}")
        print(f"   Total questions processed{filter_message}: {len(powerbi_data)}")
        print(f"   Total time: {duration}")
        print(f"   Total API v3 calls: {API_V3_CALLS}")
        print(f"   Total API v2.3 calls: {API_V2_CALLS}")
        print(f"   Total API calls: {API_V3_CALLS + API_V2_CALLS}")
        if len(powerbi_data) > 0:
            print(f"   Average time per question: {duration.total_seconds() / len(powerbi_data):.3f}s")
        
        log(f"Export completed successfully in {duration}")
        
    except Exception as e:
        log(f"Export failed: {str(e)}")
        raise

def run_cron_job():
    """Run the scheduled job"""
    if not RUNNING:
        return
        
    log("Running scheduled question-centric PowerBI data collection")
    asyncio.run(export_powerbi_data())

def main():
    global CONFIG, VERBOSE, logger
    
    # Setup argument parser
    parser = argparse.ArgumentParser(
        description="Optimized Async Question-Centric PowerBI Data Collector for Stack Overflow Enterprise with Time Filtering"
    )
    parser.add_argument("--base-url", required=True, 
                       help="Stack Overflow Enterprise Base URL")
    parser.add_argument("--token", required=True, 
                       help="API access token")
    parser.add_argument("--output-file",
                       help="Output JSON filename (auto-generated if not specified)")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose output")
    parser.add_argument("--run-once", action="store_true",
                       help="Run once and exit (no cron job)")
    parser.add_argument("--cron-schedule", default="0 2 * * *",
                       help="Cron schedule (default: daily at 2 AM)")
    
    # Time filtering options
    parser.add_argument("--filter", choices=["week", "month", "quarter", "year", "custom", "none"], 
                       default="quarter", 
                       help="Time filter for data collection (last week, month, quarter, year, custom dates, or none for all data)")
    parser.add_argument("--from-date", 
                       help="Start date for custom filter (format: YYYY-MM-DD)")
    parser.add_argument("--to-date", 
                       help="End date for custom filter (format: YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    # Validate custom filter arguments
    if args.filter == "custom" and (not args.from_date or not args.to_date):
        print("Error: --from-date and --to-date are required when using --filter=custom")
        sys.exit(1)
    
    # Setup logging
    VERBOSE = args.verbose
    logger = setup_logging(VERBOSE)
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Get date range based on filter
    if args.filter == "custom":
        from_date = args.from_date
        to_date = args.to_date
        filter_type = "custom"
    elif args.filter == "none":
        from_date = None
        to_date = None
        filter_type = None
    else:
        from_date, to_date = get_date_range(args.filter)
        filter_type = args.filter
    
    # Setup global configuration
    CONFIG.update({
        'base_url': force_api_v3(args.base_url),
        'api_v2_base': get_api_v2_url(args.base_url),
        'headers': {'Authorization': f'Bearer {args.token}'},
        'output_file': args.output_file,
        'from_date': from_date,
        'to_date': to_date,
        'filter_type': filter_type
    })
    
    # Create filter description for logging
    filter_desc = "all data"
    if filter_type:
        if filter_type == "custom":
            filter_desc = f"custom date range ({from_date} to {to_date})"
        else:
            filter_desc = f"last {filter_type} ({from_date} to {to_date})"
    
    logger.info(f"Optimized Async Question-Centric PowerBI Data Collector starting...")
    logger.info(f"Base URL: {CONFIG['base_url']}")
    logger.info(f"Data collection scope: {filter_desc}")
    logger.info(f"User data collection: Only users from filtered questions and accepted answers")
    if CONFIG.get('output_file'):
        logger.info(f"Output file: {CONFIG['output_file']}")
    else:
        logger.info("Output file: Auto-generated with timestamp and date range")
    
    if args.run_once:
        # Run once and exit
        logger.info("Running data collection once...")
        asyncio.run(export_powerbi_data())
    else:
        # Setup cron job
        logger.info(f"Setting up cron job with schedule: {args.cron_schedule}")
        
        # Parse cron schedule (simplified - assumes format: minute hour day month day_of_week)
        cron_parts = args.cron_schedule.split()
        if len(cron_parts) == 5:
            minute, hour = cron_parts[0], cron_parts[1]
            if minute.isdigit() and hour.isdigit():
                schedule.every().day.at(f"{hour.zfill(2)}:{minute.zfill(2)}").do(run_cron_job)
            else:
                logger.warning("Complex cron schedule not supported, using daily at 02:00")
                schedule.every().day.at("02:00").do(run_cron_job)
        else:
            logger.warning("Invalid cron schedule format, using daily at 02:00")
            schedule.every().day.at("02:00").do(run_cron_job)
        
        # Run once immediately
        logger.info("Running initial data collection...")
        asyncio.run(export_powerbi_data())
        
        # Start scheduler
        logger.info("Starting scheduler...")
        while RUNNING:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    logger.info("Optimized Async Question-Centric PowerBI Data Collector stopped")

if __name__ == "__main__":
    main()