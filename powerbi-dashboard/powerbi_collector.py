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
# Burst throttle: 50 requests in 2 seconds - we stay conservative
BURST_LIMIT_REQUESTS = 45  # Stay under 50 to be safe
BURST_LIMIT_WINDOW = 2.0   # 2 seconds

# Token bucket: 5000 max tokens, 100 tokens per 60 seconds refill
TOKEN_BUCKET_MAX = 5000
TOKEN_BUCKET_REFILL_RATE = 100  # tokens per 60 seconds
TOKEN_BUCKET_REFILL_INTERVAL = 60  # seconds

# Conservative retry delays - we NEVER give up on 429s
MIN_RETRY_DELAY = 5.0    # Minimum wait time on 429
MAX_RETRY_DELAY = 300.0  # Maximum wait time (5 minutes)
BACKOFF_MULTIPLIER = 1.5 # Exponential backoff multiplier

# Rate limiting retry delay
RATE_LIMIT_RETRY_DELAY = 5.0  # Default retry delay for rate limiting

# Async semaphore for rate limiting (burst throttle)
BURST_LIMITER = None

# Token bucket tracking
class TokenBucket:
    def __init__(self):
        self.tokens = TOKEN_BUCKET_MAX
        self.max_tokens = TOKEN_BUCKET_MAX
        self.refill_rate = TOKEN_BUCKET_REFILL_RATE
        self.refill_interval = TOKEN_BUCKET_REFILL_INTERVAL
        self.last_refill = time.time()
        self.lock = asyncio.Lock()
    
    async def wait_for_token(self):
        """Wait until a token is available, with token bucket refill logic"""
        async with self.lock:
            now = time.time()
            # Calculate how many refill cycles have passed
            time_passed = now - self.last_refill
            refill_cycles = time_passed / self.refill_interval
            
            if refill_cycles >= 1:
                # Add tokens based on refill cycles
                tokens_to_add = int(refill_cycles) * self.refill_rate
                self.tokens = min(self.max_tokens, self.tokens + tokens_to_add)
                self.last_refill = now
                log(f"Token bucket refilled: {self.tokens}/{self.max_tokens} tokens available")
            
            # If no tokens available, wait for next refill
            while self.tokens <= 0:
                wait_time = self.refill_interval - (time.time() - self.last_refill)
                if wait_time > 0:
                    log(f"Token bucket empty, waiting {wait_time:.1f} seconds for refill...")
                    await asyncio.sleep(wait_time)
                
                # Refill tokens
                now = time.time()
                time_passed = now - self.last_refill
                if time_passed >= self.refill_interval:
                    self.tokens = min(self.max_tokens, self.tokens + self.refill_rate)
                    self.last_refill = now
                    log(f"Token bucket refilled: {self.tokens}/{self.max_tokens} tokens available")
            
            # Consume a token
            self.tokens -= 1
            return True

# Global token bucket instance
TOKEN_BUCKET = TokenBucket()

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

def detect_instance_type(base_url: str) -> tuple:
    """
    Detect if this is a Teams or Enterprise instance and extract relevant info.
    Returns: (instance_type, team_slug_or_none)
    """
    parsed_url = urlparse(base_url.strip())
    
    # Check if it's a Teams instance
    if 'stackoverflowteams.com' in parsed_url.netloc:
        return 'teams', None
    elif parsed_url.netloc.endswith('.stackenterprise.co') or 'enterprise' in parsed_url.netloc:
        return 'enterprise', None
    else:
        # Could be a custom domain for either type - we'll default to enterprise behavior
        # but allow override via team-slug parameter
        return 'enterprise', None

def build_api_urls(base_url: str, team_slug: str = None) -> tuple:
    """
    Build the appropriate API URLs based on instance type.
    Returns: (api_v3_base, api_v2_base, instance_type)
    """
    parsed_url = urlparse(base_url.strip())
    instance_type, _ = detect_instance_type(base_url)
    
    if instance_type == 'teams' or team_slug:
        # Teams instance
        if not team_slug:
            raise ValueError("Team slug is required for Teams instances. Please provide --team-slug parameter.")
        
        # Teams API URLs
        api_v3_base = f"https://api.stackoverflowteams.com/v3/teams/{team_slug}"
        api_v2_base = f"https://api.stackoverflowteams.com/2.3"
        instance_type = 'teams'
    else:
        # Enterprise instance
        api_v3_base = f"{parsed_url.scheme}://{parsed_url.netloc}/api/v3"
        api_v2_base = f"{parsed_url.scheme}://{parsed_url.netloc}/api/2.3"
        instance_type = 'enterprise'
    
    return api_v3_base, api_v2_base, instance_type

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
    """Make async API request with persistent retry logic - never give up on 429s"""
    global API_V3_CALLS, RATE_LIMITER
    
    # Different retry limits for different error types
    max_non_rate_limit_retries = 3  # For timeouts, server errors, etc.
    non_rate_limit_retry_count = 0
    
    # For rate limits (429), we never give up but use exponential backoff
    rate_limit_retry_count = 0
    current_retry_delay = MIN_RETRY_DELAY
    
    while True:
        try:
            # Use semaphore for rate limiting
            async with RATE_LIMITER:
                API_V3_CALLS += 1
                
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 429:
                        # Rate limited - wait and retry with exponential backoff
                        retry_after = response.headers.get('Retry-After')
                        if retry_after:
                            wait_time = float(retry_after)
                        else:
                            wait_time = min(current_retry_delay, MAX_RETRY_DELAY)
                            current_retry_delay = min(current_retry_delay * BACKOFF_MULTIPLIER, MAX_RETRY_DELAY)
                        
                        rate_limit_retry_count += 1
                        log(f"Rate limited (429), waiting {wait_time:.1f} seconds before retry #{rate_limit_retry_count}")
                        await asyncio.sleep(wait_time)
                        continue  # Never give up on rate limits
                    
                    if response.status == 200:
                        return await response.json()
                    
                    # Other HTTP errors - retry with limit
                    non_rate_limit_retry_count += 1
                    if non_rate_limit_retry_count >= max_non_rate_limit_retries:
                        log(f"API request failed with status {response.status} for {url} after {max_non_rate_limit_retries} retries")
                        return None
                    
                    log(f"API request failed with status {response.status} for {url}, retry {non_rate_limit_retry_count}/{max_non_rate_limit_retries}")
                    await asyncio.sleep(1)
                    continue
                    
        except asyncio.TimeoutError:
            non_rate_limit_retry_count += 1
            if non_rate_limit_retry_count >= max_non_rate_limit_retries:
                log(f"Request timeout for {url} after {max_non_rate_limit_retries} retries")
                return None
            log(f"Request timeout for {url}, retry {non_rate_limit_retry_count}/{max_non_rate_limit_retries}")
            await asyncio.sleep(1)
            continue
            
        except Exception as e:
            if "429" in str(e) or "rate limit" in str(e).lower():
                # Treat as rate limit - never give up
                wait_time = min(current_retry_delay, MAX_RETRY_DELAY)
                current_retry_delay = min(current_retry_delay * BACKOFF_MULTIPLIER, MAX_RETRY_DELAY)
                rate_limit_retry_count += 1
                log(f"Rate limit error: {str(e)}, waiting {wait_time:.1f} seconds before retry #{rate_limit_retry_count}")
                await asyncio.sleep(wait_time)
                continue
            else:
                # Other errors - retry with limit
                non_rate_limit_retry_count += 1
                if non_rate_limit_retry_count >= max_non_rate_limit_retries:
                    log(f"API request failed for {url} after {max_non_rate_limit_retries} retries: {str(e)}")
                    return None
                log(f"API request failed for {url}: {str(e)}, retry {non_rate_limit_retry_count}/{max_non_rate_limit_retries}")
                await asyncio.sleep(1)
                continue

async def make_api_v2_request(session: aiohttp.ClientSession, url: str, params: Dict = None) -> Optional[Dict]:
    """Make async API v2.3 request with persistent retry logic - never give up on 429s"""
    global API_V2_CALLS, RATE_LIMITER
    
    # Different retry limits for different error types
    max_non_rate_limit_retries = 3  # For timeouts, server errors, etc.
    non_rate_limit_retry_count = 0
    
    # For rate limits (429), we never give up but use exponential backoff
    rate_limit_retry_count = 0
    current_retry_delay = MIN_RETRY_DELAY
    
    while True:
        try:
            async with RATE_LIMITER:
                API_V2_CALLS += 1
                
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 429:
                        # Rate limited - wait and retry with exponential backoff
                        retry_after = response.headers.get('Retry-After')
                        if retry_after:
                            wait_time = float(retry_after)
                        else:
                            wait_time = min(current_retry_delay, MAX_RETRY_DELAY)
                            current_retry_delay = min(current_retry_delay * BACKOFF_MULTIPLIER, MAX_RETRY_DELAY)
                        
                        rate_limit_retry_count += 1
                        log(f"Rate limited (429) on v2 API, waiting {wait_time:.1f} seconds before retry #{rate_limit_retry_count}")
                        await asyncio.sleep(wait_time)
                        continue  # Never give up on rate limits
                    
                    if response.status == 200:
                        return await response.json()
                    
                    # Other HTTP errors - retry with limit
                    non_rate_limit_retry_count += 1
                    if non_rate_limit_retry_count >= max_non_rate_limit_retries:
                        log(f"API v2 request failed with status {response.status} for {url} after {max_non_rate_limit_retries} retries")
                        return None
                    
                    log(f"API v2 request failed with status {response.status} for {url}, retry {non_rate_limit_retry_count}/{max_non_rate_limit_retries}")
                    await asyncio.sleep(1)
                    continue
                    
        except asyncio.TimeoutError:
            non_rate_limit_retry_count += 1
            if non_rate_limit_retry_count >= max_non_rate_limit_retries:
                log(f"Request timeout for {url} after {max_non_rate_limit_retries} retries")
                return None
            log(f"Request timeout for {url}, retry {non_rate_limit_retry_count}/{max_non_rate_limit_retries}")
            await asyncio.sleep(1)
            continue
            
        except Exception as e:
            if "429" in str(e) or "rate limit" in str(e).lower():
                # Treat as rate limit - never give up
                wait_time = min(current_retry_delay, MAX_RETRY_DELAY)
                current_retry_delay = min(current_retry_delay * BACKOFF_MULTIPLIER, MAX_RETRY_DELAY)
                rate_limit_retry_count += 1
                log(f"Rate limit error on v2 API: {str(e)}, waiting {wait_time:.1f} seconds before retry #{rate_limit_retry_count}")
                await asyncio.sleep(wait_time)
                continue
            else:
                # Other errors - retry with limit
                non_rate_limit_retry_count += 1
                if non_rate_limit_retry_count >= max_non_rate_limit_retries:
                    log(f"API v2 request failed for {url} after {max_non_rate_limit_retries} retries: {str(e)}")
                    return None
                log(f"API v2 request failed for {url}: {str(e)}, retry {non_rate_limit_retry_count}/{max_non_rate_limit_retries}")
                await asyncio.sleep(1)
                continue

async def get_paginated_data(session: aiohttp.ClientSession, endpoint: str, params: Dict = None) -> List[Dict]:
    """Generic async function to get all paginated data from an endpoint with optional date filtering"""
    all_items = []
    page = 1
    total_pages = None
    
    # Add date filtering to params if configured
    if params is None:
        params = {}
    
    # Add date filter if configured in CONFIG and not getting all users
    if CONFIG.get('from_date') and CONFIG.get('to_date') and endpoint != "users":
        params.update({
            'from': CONFIG['from_date'],
            'to': CONFIG['to_date']
        })
        log(f"Applying date filter: from {CONFIG['from_date']} to {CONFIG['to_date']}")
    
    filter_message = ""
    if CONFIG.get('from_date') and CONFIG.get('to_date') and endpoint != "users":
        filter_message = f" with date filter from {CONFIG['from_date']} to {CONFIG['to_date']}"
    
    log(f"Starting to fetch all data from {endpoint}{filter_message}")
    
    while True:
        current_params = params.copy()
        current_params.update({'page': page, 'pageSize': 100})
        
        url = f"{CONFIG['api_v3_base']}/{endpoint}"
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

async def get_all_users(session: aiohttp.ClientSession) -> List[Dict]:
    """Get ALL users from the instance (no date filtering)"""
    return await get_paginated_data(session, "users")

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

async def get_all_answers_for_questions(session: aiohttp.ClientSession, questions: List[Dict]) -> List[Dict]:
    """Get all answers for the given questions"""
    all_answers = []
    
    # Create semaphore for concurrent requests (respecting rate limits)
    concurrent_limit = min(10, BURST_LIMIT_REQUESTS // 4)  # Conservative limit
    semaphore = asyncio.Semaphore(concurrent_limit)
    
    async def fetch_answers_for_question(question):
        async with semaphore:
            question_id = question.get('id')
            if not question_id:
                return []
            
            try:
                # Get answers for this specific question
                answers = await get_paginated_data_for_question_answers(session, question_id)
                return answers
            except Exception as e:
                log(f"Error fetching answers for question {question_id}: {str(e)}")
                return []
    
    # Process questions in batches to avoid overwhelming the API
    batch_size = 50
    for i in range(0, len(questions), batch_size):
        batch = questions[i:i + batch_size]
        log(f"Processing answers batch {i//batch_size + 1}/{(len(questions) + batch_size - 1)//batch_size}")
        
        tasks = [fetch_answers_for_question(question) for question in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Flatten results and add to all_answers
        for result in results:
            if isinstance(result, list):
                all_answers.extend(result)
        
        # Small delay between batches
        await asyncio.sleep(0.5)
    
    log(f"Retrieved {len(all_answers)} total answers")
    return all_answers

async def get_paginated_data_for_question_answers(session: aiohttp.ClientSession, question_id: int) -> List[Dict]:
    """Get all answers for a specific question with pagination"""
    all_answers = []
    page = 1
    total_pages = None
    
    log(f"Fetching answers for question {question_id}")
    
    while True:
        params = {'page': page, 'pageSize': 100}
        
        # Add date filter if configured in CONFIG
        if CONFIG.get('from_date') and CONFIG.get('to_date'):
            params.update({
                'from': CONFIG['from_date'],
                'to': CONFIG['to_date']
            })
        
        url = f"{CONFIG['api_v3_base']}/questions/{question_id}/answers"
        
        data = await make_api_request(session, url, params)
        if not data:
            break
            
        answers = data.get("items", [])
        all_answers.extend(answers)
        
        if total_pages is None:
            total_pages = data.get("totalPages", 1)
        
        if page >= total_pages:
            break
        
        page += 1
        
    return all_answers

def extract_user_ids_from_questions(questions: List[Dict]) -> Set[int]:
    """Extract unique user IDs from question owners"""
    user_ids = set()
    
    for question in questions:
        owner = question.get('owner', {})
        if owner:
            user_id = owner.get('id')
        if user_id:
            user_ids.add(user_id)
    
    return user_ids

def extract_user_ids_from_answers(answers: List[Dict]) -> Set[int]:
    """Extract unique user IDs from answer owners"""
    user_ids = set()
    
    for answer in answers:
        owner = answer.get('owner', {})
        if owner:
            user_id = owner.get('id')
        if user_id:
            user_ids.add(user_id)
    
    return user_ids

def extract_user_ids_from_articles(articles: List[Dict]) -> Set[int]:
    """Extract unique user IDs from article owners"""
    user_ids = set()
    
    for article in articles:
        owner = article.get('owner', {})
        if owner:
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

async def get_accepted_answer_for_question(session: aiohttp.ClientSession, question_id: int) -> Optional[Dict]:
    """Get the accepted answer for a specific question - now optimized since we already have all answers"""
    # This function is now mainly used as a backup if we need to fetch a specific accepted answer
    # that wasn't captured in the main answers collection
    url = f"{CONFIG['api_v3_base']}/questions/{question_id}/answers"
    
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

def extract_accepted_answers_from_all_answers(all_answers: List[Dict]) -> Dict[int, Dict]:
    """Extract accepted answers from the complete answers collection"""
    accepted_answers = {}
    
    for answer in all_answers:
        if answer.get('isAccepted', False):
            question_id = answer.get('questionId')
            if question_id:
                accepted_answers[question_id] = {
                    'id': answer.get('id'),
                    'creationDate': answer.get('creationDate'),
                    'score': answer.get('score'),
                    'owner': answer.get('owner')
                }
                log(f"Found accepted answer {answer.get('id')} for question {question_id}")
    
    log(f"Extracted {len(accepted_answers)} accepted answers from answers collection")
    return accepted_answers

async def get_accepted_answers_batch(session: aiohttp.ClientSession, questions_with_accepted: List[Dict]) -> Dict[int, Dict]:
    """Get accepted answers for questions that have them, using async batch processing"""
    accepted_answers = {}
    
    # Create semaphore for concurrent requests (respecting rate limits)
    concurrent_limit = min(20, BURST_LIMIT_REQUESTS // 2)  # Conservative limit
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
        ids_string = ";".join(map(str, batch))

        v2_url = f"{CONFIG['api_v2_base']}/users/{ids_string}"
        log(f"Batch fetching detailed info for {len(batch)} users from {v2_url}")

        # Build params dict and exclude None values
        params = {
            "order": "desc",
            "sort": "reputation",
            "key": CONFIG.get("api_key"),
            "access_token": CONFIG.get("token")
        }

        if CONFIG["instance_type"] == "teams" and CONFIG.get("team_slug"):
            params["team"] = CONFIG["team_slug"]

        # Remove any None values from params
        params = {k: v for k, v in params.items() if v is not None}

        user_data = await make_api_v2_request(session, v2_url, params)

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
            url = f"{CONFIG['api_v3_base']}/tags/{tag_id}/subject-matter-experts"
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

def build_user_metrics_from_all_users(all_users: List[Dict], all_questions: List[Dict], 
                                    unanswered_questions: List[Dict], accepted_answer_questions: List[Dict],
                                    all_articles: List[Dict], all_answers: List[Dict], user_details: Dict[int, Dict], 
                                    all_sme_data: Dict[int, List[int]], participated_user_ids: Set[int]) -> Dict[int, Dict]:
    """Build comprehensive user metrics for ALL users with participation flag"""
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
    
    # Group answers by owner
    answers_by_user = defaultdict(list)
    for answer in all_answers:
        owner = answer.get('owner')
        if owner and owner.get('id'):
            answers_by_user[owner.get('id')].append(answer)
    
    # Build metrics for ALL users
    for user in all_users:
        user_id = user.get('id')
        if not user_id:
            continue
        
        detailed_info = user_details.get(user_id, {})
        user_questions = questions_by_user.get(user_id, [])
        user_unanswered = unanswered_by_user.get(user_id, [])
        user_accepted = accepted_by_user.get(user_id, [])
        user_articles = articles_by_user.get(user_id, [])
        user_answers = answers_by_user.get(user_id, [])
        
        # Check if user has participated in the filtered period
        has_participated = user_id in participated_user_ids
        
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
            'Total_Answers': len(user_answers),
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
            'Tags': sme_tags,
            'Has_Participated': has_participated  # New field indicating participation in filtered period
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
                    'sme_tags': answer_owner_metrics.get('Tags', []),
                    'has_participated': answer_owner_metrics.get('Has_Participated', False)
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
                'Total_Answers': owner_metrics.get('Total_Answers', 'Unknown'),
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
                'Has_Participated': owner_metrics.get('Has_Participated', False)
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
    """Main async function to collect question-centric PowerBI data with ALL users"""
    global RATE_LIMITER
    
    filter_message = ""
    if CONFIG.get('from_date') and CONFIG.get('to_date'):
        filter_message = f" with date filter from {CONFIG['from_date']} to {CONFIG['to_date']}"
    
    log(f"Starting question-centric PowerBI data collection with ALL users{filter_message}")
    
    # Initialize rate limiter
    RATE_LIMITER = asyncio.Semaphore(BURST_LIMIT_REQUESTS)
    
    # Create aiohttp session
    headers = CONFIG['headers']
    timeout = aiohttp.ClientTimeout(total=300)  # 5 minute timeout
    
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        # Step 1: Get ALL users from the instance (no date filtering)
        stop_event = threading.Event()
        loading_message = "Fetching ALL users from instance..."
        loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
        
        if not VERBOSE:
            loading_thread.start()
        
        try:
            all_users = await get_all_users(session)
        finally:
            stop_event.set()
            if not VERBOSE:
                loading_thread.join()
                print("\rAll users retrieval complete!        ")
        
        if not all_users:
            log("No users found")
            return []
        
        log(f"Retrieved {len(all_users)} total users from instance")
        
        # Step 2: Get all questions (with date filtering if configured)
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
        
        # Step 3: Get all answers for the questions (with date filtering if configured)
        stop_event = threading.Event()
        loading_message = f"Fetching answers for {len(all_questions)} questions{filter_message}..."
        loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
        
        if not VERBOSE:
            loading_thread.start()
        
        try:
            all_answers = await get_all_answers_for_questions(session, all_questions)
        finally:
            stop_event.set()
            if not VERBOSE:
                loading_thread.join()
                print("\rAnswers retrieval complete!        ")
        
        # Step 4: Get unanswered questions (with date filtering if configured)
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
        
        # Step 5: Get questions with accepted answers (with date filtering if configured)
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
        
        # Step 6: Extract accepted answers from the answers we already collected
        stop_event = threading.Event()
        loading_message = f"Extracting accepted answers from {len(all_answers)} answers..."
        loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
        
        if not VERBOSE:
            loading_thread.start()
        
        try:
            accepted_answers = extract_accepted_answers_from_all_answers(all_answers)
        finally:
            stop_event.set()
            if not VERBOSE:
                loading_thread.join()
                print("\rAccepted answers extraction complete!        ")
        
        # Step 7: Get all articles (with date filtering if configured)
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
        
        # Step 8: Determine which users participated during the filtered period
        participated_user_ids = set()
        
        # Add users who created questions during the period
        participated_user_ids.update(extract_user_ids_from_questions(all_questions))
        
        # Add users who created answers during the period
        participated_user_ids.update(extract_user_ids_from_answers(all_answers))
        
        # Add users who created articles during the period
        participated_user_ids.update(extract_user_ids_from_articles(all_articles))
        
        # Add users who created accepted answers during the period
        participated_user_ids.update(extract_user_ids_from_accepted_answers(accepted_answers))
        
        log(f"Found {len(participated_user_ids)} users who participated during the filtered period")
        
        # Step 9: Get detailed user info for ALL users
        all_user_ids = [user.get('id') for user in all_users if user.get('id')]
        
        stop_event = threading.Event()
        loading_message = f"Fetching detailed info for {len(all_user_ids)} users..."
        loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
        
        if not VERBOSE:
            loading_thread.start()
        
        try:
            user_details = await get_user_detailed_info_batch(session, all_user_ids)
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
        
        log(f"Content summary: {len(all_questions)} questions, {len(all_answers)} answers, {len(all_users)} users, {len(all_articles)} articles, {len(accepted_answers)} accepted answers")
        log(f"Participation summary: {len(participated_user_ids)} users participated during filtered period")
        
        # Step 11: Build user metrics for ALL users with participation flag
        log("Building comprehensive user metrics for ALL users...")
        user_metrics = build_user_metrics_from_all_users(
            all_users, all_questions, unanswered_questions, accepted_answer_questions,
            all_articles, all_answers, user_details, all_sme_data, participated_user_ids
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
        log(f"User metrics available for {len(user_metrics)} users")
        
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
    
    log(f"Question-centric PowerBI data export with ALL users started at {start_time}{filter_message}")
    
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
        print(f"\n✅ Question-centric PowerBI data export with ALL users complete!")
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
        
    log("Running scheduled question-centric PowerBI data collection with ALL users")
    asyncio.run(export_powerbi_data())

def main():
    global CONFIG, VERBOSE, logger
    
    # Setup argument parser
    parser = argparse.ArgumentParser(
        description="Universal Async Question-Centric PowerBI Data Collector for Stack Overflow Enterprise & Teams with Time Filtering and ALL Users"
    )
    parser.add_argument("--base-url", required=True, 
                       help="Stack Overflow Enterprise or Teams Base URL")
    parser.add_argument("--token", required=True, 
                       help="API access token")
    parser.add_argument("--team-slug",
                       help="Team slug (required for Teams instances, auto-detected if not provided)")
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
                       default="none", 
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
    
    # Build API URLs and detect instance type
    try:
        api_v3_base, api_v2_base, instance_type = build_api_urls(args.base_url, args.team_slug)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    # Setup global configuration
    CONFIG.update({
        'api_v3_base': api_v3_base,
        'api_v2_base': api_v2_base,
        'headers': {'Authorization': f'Bearer {args.token}',
                    'User-Agent': 'powerbi_collector / 1.0'},
        'output_file': args.output_file,
        'from_date': from_date,
        'to_date': to_date,
        'filter_type': filter_type,
        'instance_type': instance_type,
        'team_slug': args.team_slug
    })
    
    # Create filter description for logging
    filter_desc = "all data"
    if filter_type:
        if filter_type == "custom":
            filter_desc = f"custom date range ({from_date} to {to_date})"
        else:
            filter_desc = f"last {filter_type} ({from_date} to {to_date})"
    
    logger.info(f"Universal Async Question-Centric PowerBI Data Collector with ALL Users starting...")
    logger.info(f"Instance type: {instance_type.title()}")
    logger.info(f"API v3 Base URL: {CONFIG['api_v3_base']}")
    logger.info(f"API v2.3 Base URL: {CONFIG['api_v2_base']}")
    logger.info(f"Data collection scope: {filter_desc}")
    logger.info(f"User data collection: ALL users from instance with participation flag")
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
    
    logger.info("Universal Async Question-Centric PowerBI Data Collector with ALL Users stopped")

if __name__ == "__main__":
    main()