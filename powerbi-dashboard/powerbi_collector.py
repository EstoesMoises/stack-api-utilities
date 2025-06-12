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

def convert_date_to_epoch(date_string: str) -> int:
    """Convert YYYY-MM-DD date string to epoch timestamp"""
    if not date_string:
        return None
    
    try:
        dt = datetime.strptime(date_string, '%Y-%m-%d')
        # Set to start of day (00:00:00)
        dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return int(dt.timestamp())
    except ValueError as e:
        log(f"Error converting date {date_string} to epoch: {str(e)}")
        return None

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

async def get_users_created_in_timeframe(session: aiohttp.ClientSession) -> List[Dict]:
    """Get users created within the specified timeframe"""
    all_users = []
    page = 1
    total_pages = None
    
    # Convert date strings to epoch timestamps for filtering
    from_epoch = None
    to_epoch = None
    
    if CONFIG.get('from_date') and CONFIG.get('to_date'):
        from_epoch = convert_date_to_epoch(CONFIG['from_date'])
        to_epoch = convert_date_to_epoch(CONFIG['to_date']) + 86400  # Add 24 hours to include end date
        log(f"Filtering users created between {CONFIG['from_date']} and {CONFIG['to_date']}")
        log(f"Epoch range: {from_epoch} to {to_epoch}")
    
    log(f"Starting to fetch users created in specified timeframe")
    
    # If no date filter is specified, use the original logic
    if not from_epoch or not to_epoch:
        log("No date filter specified, fetching all users from API v3")
        while True:
            params = {'page': page, 'pageSize': 100}
            
            url = f"{CONFIG['api_v3_base']}/users"
            log(f"Fetching page {page}/{total_pages or '?'} from users endpoint")
            
            data = await make_api_request(session, url, params)
            if not data:
                break
                
            users = data.get("items", [])
            all_users.extend(users)
            
            if total_pages is None:
                total_pages = data.get("totalPages", 1)
                log(f"Total pages to fetch: {total_pages}")
            
            log(f"Retrieved {len(users)} users from page {page}/{total_pages} (total collected: {len(all_users)})")
            
            if page >= total_pages:
                log(f"All pages fetched. Total users: {len(all_users)}")
                break
            
            page += 1
        
        return all_users
    
    # When date filtering is needed, we need to get users from API v2.3 which has creation_date
    log("Date filter specified, fetching users from API v2.3 with creation_date")
    
    # First, get all user IDs from API v3
    log("Step 1: Getting all user IDs from API v3")
    all_user_ids = []
    v3_page = 1
    v3_total_pages = None
    
    while True:
        params = {'page': v3_page, 'pageSize': 100}
        url = f"{CONFIG['api_v3_base']}/users"
        
        data = await make_api_request(session, url, params)
        if not data:
            break
            
        users = data.get("items", [])
        user_ids = [user.get('id') for user in users if user.get('id')]
        all_user_ids.extend(user_ids)
        
        if v3_total_pages is None:
            v3_total_pages = data.get("totalPages", 1)
        
        log(f"Retrieved {len(user_ids)} user IDs from v3 page {v3_page}/{v3_total_pages}")
        
        if v3_page >= v3_total_pages:
            break
            
        v3_page += 1
    
    log(f"Total user IDs collected from API v3: {len(all_user_ids)}")
    
    # Step 2: Get detailed user info from API v2.3 in batches and filter by creation_date
    log("Step 2: Getting detailed user info from API v2.3 and applying date filter")
    
    filtered_users = []
    batch_size = 20  # API v2.3 batch size limit
    
    for i in range(0, len(all_user_ids), batch_size):
        batch_ids = all_user_ids[i:i + batch_size]
        ids_string = ";".join(map(str, batch_ids))
        
        # Build API v2.3 URL
        v2_url = f"{CONFIG['api_v2_base']}/users/{ids_string}"
        
        # Build params dict and exclude None values
        params = {
            "order": "desc",
            "sort": "creation",
            "key": CONFIG.get("api_key"),
            "access_token": CONFIG.get("token")
        }
        
        if CONFIG["instance_type"] == "teams" and CONFIG.get("team_slug"):
            params["team"] = CONFIG["team_slug"]
        
        # Remove any None values from params
        params = {k: v for k, v in params.items() if v is not None}
        
        log(f"Fetching batch {i//batch_size + 1}/{(len(all_user_ids) + batch_size - 1)//batch_size} from API v2.3")
        
        user_data = await make_api_v2_request(session, v2_url, params)
        
        if user_data and 'items' in user_data:
            for user_item in user_data['items']:
                creation_date = user_item.get('creation_date')
                
                # Apply date filter
                if creation_date and from_epoch <= creation_date <= to_epoch:
                    # Convert v2.3 user format back to v3-like format for consistency
                    v3_formatted_user = {
                        'id': user_item.get('user_id'),
                        'name': user_item.get('display_name'),
                        'accountId': user_item.get('account_id'),
                        'reputation': user_item.get('reputation'),
                        'creationDate': creation_date,  # Add this for consistency
                        'role': user_item.get('user_type'),
                        # Add other fields as needed
                        'location': user_item.get('location'),
                        'jobTitle': None,  # v2.3 doesn't have job title
                        'department': None  # v2.3 doesn't have department
                    }
                    
                    filtered_users.append(v3_formatted_user)
                    log(f"User {user_item.get('user_id')} ({user_item.get('display_name')}) created in timeframe: {convert_epoch_to_utc_timestamp(creation_date)}")
        
        # Small delay between batches to respect rate limits
        await asyncio.sleep(0.1)
    
    log(f"Filtered users in timeframe: {len(filtered_users)}")
    
    # Step 3: For the filtered users, get their complete v3 data to maintain data structure consistency
    if filtered_users:
        log("Step 3: Getting complete v3 data for filtered users")
        
        # Get the v3 user data for consistency with the rest of the pipeline
        complete_users = []
        filtered_user_ids = [user['id'] for user in filtered_users]
        
        # Fetch complete v3 data in smaller batches
        v3_batch_size = 10
        for i in range(0, len(filtered_user_ids), v3_batch_size):
            batch_ids = filtered_user_ids[i:i + v3_batch_size]
            
            # Create tasks to fetch individual user data from v3
            tasks = []
            for user_id in batch_ids:
                url = f"{CONFIG['api_v3_base']}/users/{user_id}"
                tasks.append(make_api_request(session, url))
            
            # Execute batch requests
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for user_id, result in zip(batch_ids, results):
                if isinstance(result, dict) and 'id' in result:
                    # Add the creation date from our v2.3 data
                    filtered_user = next((u for u in filtered_users if u['id'] == user_id), None)
                    if filtered_user:
                        result['creationDate'] = filtered_user['creationDate']
                    complete_users.append(result)
                else:
                    # Fall back to the v2.3 formatted data if v3 fetch fails
                    filtered_user = next((u for u in filtered_users if u['id'] == user_id), None)
                    if filtered_user:
                        complete_users.append(filtered_user)
            
            log(f"Retrieved complete v3 data for batch {i//v3_batch_size + 1}/{(len(filtered_user_ids) + v3_batch_size - 1)//v3_batch_size}")
            
            # Small delay between batches
            await asyncio.sleep(0.1)
        
        all_users = complete_users
    else:
        all_users = filtered_users
    
    log(f"Final user count after filtering: {len(all_users)}")
    return all_users

async def get_questions_for_user(session: aiohttp.ClientSession, user_id: int) -> List[Dict]:
    """Get all questions for a specific user using authorId parameter"""
    all_questions = []
    page = 1
    total_pages = None
    
    log(f"Fetching questions for user {user_id}")
    
    while True:
        params = {'page': page, 'pageSize': 100, 'authorId': user_id}
        
        url = f"{CONFIG['api_v3_base']}/questions"
        
        data = await make_api_request(session, url, params)
        if not data:
            break
            
        questions = data.get("items", [])
        all_questions.extend(questions)
        
        if total_pages is None:
            total_pages = data.get("totalPages", 1)
        
        if page >= total_pages:
            break
        
        page += 1
        
    log(f"Retrieved {len(all_questions)} questions for user {user_id}")
    return all_questions

async def get_articles_for_user(session: aiohttp.ClientSession, user_id: int) -> List[Dict]:
    """Get all articles for a specific user using authorId parameter"""
    all_articles = []
    page = 1
    total_pages = None
    
    log(f"Fetching articles for user {user_id}")
    
    while True:
        params = {'page': page, 'pageSize': 100, 'authorId': user_id}
        
        url = f"{CONFIG['api_v3_base']}/articles"
        
        data = await make_api_request(session, url, params)
        if not data:
            break
            
        articles = data.get("items", [])
        all_articles.extend(articles)
        
        if total_pages is None:
            total_pages = data.get("totalPages", 1)
        
        if page >= total_pages:
            break
        
        page += 1
        
    log(f"Retrieved {len(all_articles)} articles for user {user_id}")
    return all_articles

async def get_answers_for_questions(session: aiohttp.ClientSession, questions: List[Dict]) -> List[Dict]:
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
    
    while True:
        params = {'page': page, 'pageSize': 100}
        
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

def process_answers_data(user_answers: List[Dict]) -> List[Dict]:
    """Process answers data into a clean format"""
    processed_answers = []
    
    for answer in user_answers:
        # Get owner information
        owner = answer.get('owner', {})
        last_editor = answer.get('lastEditor', {})
        last_activity_user = answer.get('lastActivityUser', {})
        
        answer_data = {
            'answer_id': answer.get('id'),
            'question_id': answer.get('questionId'),
            'score': answer.get('score', 0),
            'is_accepted': answer.get('isAccepted', False),
            'is_deleted': answer.get('isDeleted', False),
            'is_bookmarked': answer.get('isBookmarked', False),
            'is_followed': answer.get('isFollowed', False),
            'creation_date': answer.get('creationDate'),
            'locked_date': answer.get('lockedDate'),
            'last_edit_date': answer.get('lastEditDate'),
            'last_activity_date': answer.get('lastActivityDate'),
            'deletion_date': answer.get('deletionDate'),
            'comment_count': answer.get('commentCount', 0),
            'web_url': answer.get('webUrl'),
            'share_link': answer.get('shareLink'),
            'user_can_follow': answer.get('userCanFollow', False),
            'can_be_followed': answer.get('canBeFollowed', False),
            'is_subject_matter_expert': answer.get('isSubjectMatterExpert', False),
            'owner': {
                'id': owner.get('id'),
                'account_id': owner.get('accountId'),
                'name': owner.get('name'),
                'avatar_url': owner.get('avatarUrl'),
                'web_url': owner.get('webUrl'),
                'reputation': owner.get('reputation'),
                'role': owner.get('role')
            },
            'last_editor': {
                'id': last_editor.get('id'),
                'account_id': last_editor.get('accountId'),
                'name': last_editor.get('name'),
                'avatar_url': last_editor.get('avatarUrl'),
                'web_url': last_editor.get('webUrl'),
                'reputation': last_editor.get('reputation'),
                'role': last_editor.get('role')
            } if last_editor else None,
            'last_activity_user': {
                'id': last_activity_user.get('id'),
                'account_id': last_activity_user.get('accountId'),
                'name': last_activity_user.get('name'),
                'avatar_url': last_activity_user.get('avatarUrl'),
                'web_url': last_activity_user.get('webUrl'),
                'reputation': last_activity_user.get('reputation'),
                'role': last_activity_user.get('role')
            } if last_activity_user else None
        }
        processed_answers.append(answer_data)
    
    return processed_answers
def process_articles_data(user_articles: List[Dict]) -> List[Dict]:
    """Process articles data into a clean format"""
    processed_articles = []
    
    for article in user_articles:
        # Get article tags
        article_tags = []
        for tag in article.get('tags', []):
            if isinstance(tag, dict):
                article_tags.append(tag.get('name', ''))
            else:
                article_tags.append(str(tag))
        
        # Get owner information
        owner = article.get('owner', {})
        last_editor = article.get('lastEditor', {})
        
        article_data = {
            'article_id': article.get('id'),
            'type': article.get('type'),
            'title': article.get('title'),
            'tags': article_tags,
            'creation_date': article.get('creationDate'),
            'last_activity_date': article.get('lastActivityDate'),
            'score': article.get('score', 0),
            'view_count': article.get('viewCount', 0),
            'web_url': article.get('webUrl'),
            'share_url': article.get('shareUrl'),
            'is_deleted': article.get('isDeleted', False),
            'is_obsolete': article.get('isObsolete', False),
            'is_closed': article.get('isClosed', False),
            'owner': {
                'id': owner.get('id'),
                'account_id': owner.get('accountId'),
                'name': owner.get('name'),
                'avatar_url': owner.get('avatarUrl'),
                'web_url': owner.get('webUrl'),
                'reputation': owner.get('reputation'),
                'role': owner.get('role')
            },
            'last_editor': {
                'id': last_editor.get('id'),
                'account_id': last_editor.get('accountId'),
                'name': last_editor.get('name'),
                'avatar_url': last_editor.get('avatarUrl'),
                'web_url': last_editor.get('webUrl'),
                'reputation': last_editor.get('reputation'),
                'role': last_editor.get('role')
            } if last_editor else None
        }
        processed_articles.append(article_data)
    
    return processed_articles

def process_user_data(user: Dict, user_details: Dict, user_questions: List[Dict], 
                     user_answers: List[Dict], user_articles: List[Dict], 
                     accepted_answers: Dict[int, Dict], all_sme_data: Dict[int, List[int]]) -> Dict:
    """Process a single user into user-centric format with all their questions, answers, and articles"""
    try:
        if not user:
            return None
        
        user_id = user.get('id')
        if not user_id:
            return None
        
        # Get detailed user info
        detailed_info = user_details.get(user_id, {})
        
        # Calculate account longevity
        creation_date = user.get('creationDate') or detailed_info.get('creation_date')
        account_longevity = calculate_account_longevity(creation_date)
        
        # Get SME tags
        sme_tags = get_user_sme_tags(user_id, all_sme_data)
        is_sme = len(sme_tags) > 0
        
        # Convert epoch timestamps to UTC format
        creation_date_utc = convert_epoch_to_utc_timestamp(creation_date)
        last_login_date_utc = convert_epoch_to_utc_timestamp(detailed_info.get('last_access_date'))
        
        # Process user's questions with their answers
        processed_questions = []
        for question in user_questions:
            question_id = question.get('id')
            
            # Get question tags
            question_tags = []
            for tag in question.get('tags', []):
                if isinstance(tag, dict):
                    question_tags.append(tag.get('name', ''))
                else:
                    question_tags.append(str(tag))
            
            # Get accepted answer data if available
            accepted_answer = accepted_answers.get(question_id)
            accepted_answer_data = None
            
            if accepted_answer:
                answer_owner = accepted_answer.get('owner', {})
                accepted_answer_data = {
                    'answer_id': accepted_answer.get('id'),
                    'creation_date': accepted_answer.get('creationDate'),
                    'score': accepted_answer.get('score'),
                    'owner': {
                        'id': answer_owner.get('id'),
                        'display_name': answer_owner.get('name'),
                        'reputation': answer_owner.get('reputation'),
                        'account_id': answer_owner.get('accountId'),
                        'role': answer_owner.get('role')
                    }
                }
            
            # Get answers for this question (raw format - these are answers BY this user TO other questions)
            question_answers = [answer for answer in user_answers if answer.get('questionId') == question_id]
            
            question_data = {
                'question_id': question_id,
                'title': question.get('title'),
                'tags': question_tags,
                'creation_date': question.get('creationDate'),
                'score': question.get('score', 0),
                'view_count': question.get('viewCount', 0),
                'answer_count': question.get('answerCount', 0),
                'is_answered': question.get('isAnswered', "Not retrieved"),
                'has_accepted_answer': bool(accepted_answer_data),
                'accepted_answer': accepted_answer_data,
                'answers': question_answers  # These are answers TO this question BY this user
            }
            processed_questions.append(question_data)
        
        # Process user's articles
        processed_articles = process_articles_data(user_articles)
        
        # Process user's answers (all answers BY this user)
        processed_answers = process_answers_data(user_answers)
        
        # Calculate user metrics
        questions_with_accepted_answers = len([q for q in user_questions if q.get('hasAcceptedAnswer', False)])
        unanswered_questions = len([q for q in user_questions if not q.get('isAnswered', False)])
        
        # Calculate article metrics
        total_article_views = sum(article.get('view_count', 0) for article in processed_articles)
        total_article_score = sum(article.get('score', 0) for article in processed_articles)
        
        # Calculate answer metrics
        total_answer_score = sum(answer.get('score', 0) for answer in processed_answers)
        accepted_answers_given = len([answer for answer in processed_answers if answer.get('is_accepted', False)])
        
        # Build user-centric data
        user_data = {
            # User Basic Info
            'User_ID': user_id,
            'DisplayName': user.get('name'),
            'Account_ID': user.get('accountId'),
            'Title': user.get('jobTitle'),
            'Department': user.get('department'),
            'Location': detailed_info.get('location') if detailed_info else None,
            'User_Type': user.get('role'),
            
            # User Metrics
            'Reputation': user.get('reputation', 0) or (detailed_info.get('reputation', 0) if detailed_info else 0),
            'Account_Longevity_Days': account_longevity,
            'Creation_Date': creation_date_utc,
            'Joined_UTC': creation_date_utc,
            'Last_Login_Date': last_login_date_utc,
            
            # Activity Metrics
            'Total_Questions_Asked': len(user_questions),
            'Total_Questions_No_Answers': unanswered_questions,
            'Questions_With_Accepted_Answers': questions_with_accepted_answers,
            'Total_Answers_Given': len(processed_answers),
            'Accepted_Answers_Given': accepted_answers_given,
            'Total_Answer_Score': total_answer_score,
            
            # Article Metrics
            'Total_Articles_Written': len(processed_articles),
            'Total_Article_Views': total_article_views,
            'Total_Article_Score': total_article_score,
            
            # SME Info
            'Is_SME': is_sme,
            'SME_Tags': sme_tags,
            
            # Questions Data
            'Questions': processed_questions,
            
            # Articles Data
            'Articles': processed_articles,
            
            # Answers Data (all answers given BY this user)
            'Answers': processed_answers,
            
            # Metadata
            'Last_Updated': datetime.now().isoformat(),
            'Data_Collection_Timestamp': convert_epoch_to_utc_timestamp(datetime.now().timestamp())
        }
        
        return user_data
        
    except Exception as e:
        log(f"Unexpected error in process_user_data for user {user.get('id') if user else 'None'}: {str(e)}")
        return None

async def collect_powerbi_data() -> List[Dict]:
    """Main async function to collect user-centric PowerBI data including articles"""
    global RATE_LIMITER
    
    filter_message = ""
    if CONFIG.get('from_date') and CONFIG.get('to_date'):
        filter_message = f" created between {CONFIG['from_date']} and {CONFIG['to_date']}"
    else:
        filter_message = " (all users - no date filter applied)"
    
    log(f"Starting user-centric PowerBI data collection with articles{filter_message}")
    
    # Initialize rate limiter
    RATE_LIMITER = asyncio.Semaphore(BURST_LIMIT_REQUESTS)
    
    # Create aiohttp session
    headers = CONFIG['headers']
    timeout = aiohttp.ClientTimeout(total=300)  # 5 minute timeout
    
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        # Step 1: Get users created in the specified timeframe
        stop_event = threading.Event()
        loading_message = f"Fetching users{filter_message}..."
        loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
        
        if not VERBOSE:
            loading_thread.start()
        
        try:
            users_in_timeframe = await get_users_created_in_timeframe(session)
        finally:
            stop_event.set()
            if not VERBOSE:
                loading_thread.join()
                print("\rUsers retrieval complete!        ")
        
        if not users_in_timeframe:
            log("No users found in the specified timeframe")
            return []
        
        log(f"Retrieved {len(users_in_timeframe)} users{filter_message}")
        
        # Step 2: Get all questions for each user
        stop_event = threading.Event()
        loading_message = f"Fetching questions for {len(users_in_timeframe)} users..."
        loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
        
        if not VERBOSE:
            loading_thread.start()
        
        try:
            all_user_questions = {}
            all_questions = []
            
            # Create semaphore for concurrent user question requests
            concurrent_limit = min(10, BURST_LIMIT_REQUESTS // 4)
            semaphore = asyncio.Semaphore(concurrent_limit)
            
            async def fetch_questions_for_user(user):
                async with semaphore:
                    user_id = user.get('id')
                    if not user_id:
                        return
                    
                    try:
                        questions = await get_questions_for_user(session, user_id)
                        all_user_questions[user_id] = questions
                        all_questions.extend(questions)
                    except Exception as e:
                        log(f"Error fetching questions for user {user_id}: {str(e)}")
                        all_user_questions[user_id] = []
            
            # Process users in batches
            batch_size = 20
            for i in range(0, len(users_in_timeframe), batch_size):
                batch = users_in_timeframe[i:i + batch_size]
                tasks = [fetch_questions_for_user(user) for user in batch]
                await asyncio.gather(*tasks, return_exceptions=True)
                
                log(f"Processed questions for batch {i//batch_size + 1}/{(len(users_in_timeframe) + batch_size - 1)//batch_size}")
                await asyncio.sleep(0.5)  # Small delay between batches
                
        finally:
            stop_event.set()
            if not VERBOSE:
                loading_thread.join()
                print("\rQuestions retrieval complete!        ")
        
        log(f"Retrieved {len(all_questions)} total questions from {len(users_in_timeframe)} users")
        
        # Step 3: Get all articles for each user
        stop_event = threading.Event()
        loading_message = f"Fetching articles for {len(users_in_timeframe)} users..."
        loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
        
        if not VERBOSE:
            loading_thread.start()
        
        try:
            all_user_articles = {}
            all_articles = []
            
            async def fetch_articles_for_user(user):
                async with semaphore:
                    user_id = user.get('id')
                    if not user_id:
                        return
                    
                    try:
                        articles = await get_articles_for_user(session, user_id)
                        all_user_articles[user_id] = articles
                        all_articles.extend(articles)
                    except Exception as e:
                        log(f"Error fetching articles for user {user_id}: {str(e)}")
                        all_user_articles[user_id] = []
            
            # Process users in batches
            for i in range(0, len(users_in_timeframe), batch_size):
                batch = users_in_timeframe[i:i + batch_size]
                tasks = [fetch_articles_for_user(user) for user in batch]
                await asyncio.gather(*tasks, return_exceptions=True)
                
                log(f"Processed articles for batch {i//batch_size + 1}/{(len(users_in_timeframe) + batch_size - 1)//batch_size}")
                await asyncio.sleep(0.5)  # Small delay between batches
                
        finally:
            stop_event.set()
            if not VERBOSE:
                loading_thread.join()
                print("\rArticles retrieval complete!        ")
        
        log(f"Retrieved {len(all_articles)} total articles from {len(users_in_timeframe)} users")
        
        # Step 4: Get all answers for the questions
        stop_event = threading.Event()
        loading_message = f"Fetching answers for {len(all_questions)} questions..."
        loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
        
        if not VERBOSE:
            loading_thread.start()
        
        try:
            all_answers = await get_answers_for_questions(session, all_questions)
        finally:
            stop_event.set()
            if not VERBOSE:
                loading_thread.join()
                print("\rAnswers retrieval complete!        ")
        
        # Step 5: Extract accepted answers
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
        
        # Step 6: Get detailed user info for all users
        user_ids = [user.get('id') for user in users_in_timeframe if user.get('id')]
        
        stop_event = threading.Event()
        loading_message = f"Fetching detailed info for {len(user_ids)} users..."
        loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
        
        if not VERBOSE:
            loading_thread.start()
        
        try:
            user_details = await get_user_detailed_info_batch(session, user_ids)
        finally:
            stop_event.set()
            if not VERBOSE:
                loading_thread.join()
                print("\rUser details retrieval complete!        ")
        
        # Step 7: Get SME data for all tags (from both questions and articles)
        all_tag_ids = set()
        
        # Get tags from questions
        for questions in all_user_questions.values():
            for question in questions:
                for tag in question.get('tags', []):
                    if isinstance(tag, dict) and tag.get('id'):
                        all_tag_ids.add(tag.get('id'))
                        # Cache tag name for later use
                        SME_CACHE[tag.get('id')] = tag.get('name', f"tag_{tag.get('id')}")
        
        # Get tags from articles
        for articles in all_user_articles.values():
            for article in articles:
                for tag in article.get('tags', []):
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
            all_sme_data = await get_sme_data_for_tags(session, list(all_tag_ids)) if all_tag_ids else {}
        finally:
            stop_event.set()
            if not VERBOSE:
                loading_thread.join()
                print("\rSME data retrieval complete!        ")
        
        # Step 8: Group answers by user
        answers_by_user = defaultdict(list)
        for answer in all_answers:
            owner = answer.get('owner')
            if owner and owner.get('id'):
                answers_by_user[owner.get('id')].append(answer)
        
        log(f"Content summary: {len(users_in_timeframe)} users, {len(all_questions)} questions, {len(all_articles)} articles, {len(all_answers)} answers, {len(accepted_answers)} accepted answers")
        
        # Step 9: Process all users into user-centric format
        log(f"Processing {len(users_in_timeframe)} users")
        
        powerbi_data = []
        for i, user in enumerate(users_in_timeframe, 1):
            try:
                user_id = user.get('id')
                if not user_id:
                    continue
                
                user_questions = all_user_questions.get(user_id, [])
                user_articles = all_user_articles.get(user_id, [])
                user_answers = answers_by_user.get(user_id, [])
                
                user_data = process_user_data(
                    user, user_details, user_questions, user_answers, user_articles,
                    accepted_answers, all_sme_data
                )
                
                if user_data:
                    powerbi_data.append(user_data)
                    
                if i % 50 == 0 or i == len(users_in_timeframe):
                    log(f"Processed {i}/{len(users_in_timeframe)} users")
                    
            except Exception as e:
                log(f"Error processing user {user.get('id')}: {str(e)}")
        
        log(f"Collected user-centric data for {len(powerbi_data)} users")
        
        return powerbi_data

def save_data_to_json(data: List[Dict], filename: str = None):
    """Save collected data to JSON file"""
    if not filename:
        if CONFIG.get('from_date') and CONFIG.get('to_date'):
            filename = f"powerbi_users_with_articles_{CONFIG['from_date']}_to_{CONFIG['to_date']}.json"
        else:
            filename = f"powerbi_users_with_articles_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
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
            filter_message = f" for users created in custom date range (from {CONFIG['from_date']} to {CONFIG['to_date']})"
        elif CONFIG['filter_type'] != "none":
            filter_message = f" for users created in the last {CONFIG['filter_type']} (from {CONFIG['from_date']} to {CONFIG['to_date']})"
        else:
            filter_message = " for all users (no date filter)"
    
    log(f"User-centric PowerBI data export with articles started at {start_time}{filter_message}")
    
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
        filename = save_data_to_json(powerbi_data, filename)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Calculate totals
        total_questions = sum(len(user_data.get('Questions', [])) for user_data in powerbi_data)
        total_articles = sum(len(user_data.get('Articles', [])) for user_data in powerbi_data)
        
        # Print summary
        print(f"\n✅ User-centric PowerBI data export with articles complete!")
        print(f"   Data saved to: {filename}")
        print(f"   Total users processed{filter_message}: {len(powerbi_data)}")
        print(f"   Total questions collected: {total_questions}")
        print(f"   Total articles collected: {total_articles}")
        print(f"   Total time: {duration}")
        print(f"   Total API v3 calls: {API_V3_CALLS}")
        print(f"   Total API v2.3 calls: {API_V2_CALLS}")
        print(f"   Total API calls: {API_V3_CALLS + API_V2_CALLS}")
        if len(powerbi_data) > 0:
            print(f"   Average time per user: {duration.total_seconds() / len(powerbi_data):.3f}s")
        
        log(f"Export completed successfully in {duration}")
        
    except Exception as e:
        log(f"Export failed: {str(e)}")
        raise

def run_cron_job():
    """Run the scheduled job"""
    if not RUNNING:
        return
        
    log("Running scheduled user-centric PowerBI data collection with articles")
    asyncio.run(export_powerbi_data())

def main():
    global CONFIG, VERBOSE, logger
    
    # Setup argument parser
    parser = argparse.ArgumentParser(
        description="Universal Async User-Centric PowerBI Data Collector for Stack Overflow Enterprise & Teams with Articles and Time Filtering"
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
                       help="Time filter for user creation date (last week, month, quarter, year, custom dates, or none for all users)")
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
        filter_type = "none"
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
    filter_desc = "all users"
    if filter_type and filter_type != "none":
        if filter_type == "custom":
            filter_desc = f"users created in custom date range ({from_date} to {to_date})"
        else:
            filter_desc = f"users created in the last {filter_type} ({from_date} to {to_date})"
    
    logger.info(f"Universal Async User-Centric PowerBI Data Collector with Articles starting...")
    logger.info(f"Instance type: {instance_type.title()}")
    logger.info(f"API v3 Base URL: {CONFIG['api_v3_base']}")
    logger.info(f"API v2.3 Base URL: {CONFIG['api_v2_base']}")
    logger.info(f"Data collection scope: {filter_desc}")
    logger.info(f"Data structure: User-centric with all questions and articles per user")
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
    
    logger.info("Universal Async User-Centric PowerBI Data Collector with Articles stopped")

if __name__ == "__main__":
    main()