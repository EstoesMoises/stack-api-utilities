import argparse
import requests
import json
import time
import itertools
import threading
import schedule
import signal
import sys
from datetime import datetime
from urllib.parse import urlparse
import logging
from typing import Dict, List, Optional

# Global counters and caches
API_V2_CALLS = 0
API_V3_CALLS = 0
USER_CACHE = {}
USER_DETAILS_CACHE = {}

# Rate limiting configuration according to Teams API v3 Docs.
RATE_LIMIT_REQUESTS = 45  # Stay under 50 to be safe
RATE_LIMIT_WINDOW = 2.0   # 2 seconds
RATE_LIMIT_RETRY_DELAY = 3.0  # Wait 3 seconds on 429 error

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

def log(message: str):
    """Log message if verbose mode is enabled"""
    if VERBOSE:
        logger.debug(message)
    else:
        logger.info(message)

def make_api_request(url: str, headers: Dict[str, str], params: Dict = None) -> Optional[Dict]:
    """Make API request with simple rate limiting and retry logic"""
    global API_V3_CALLS
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Simple delay to avoid overwhelming the API
            time.sleep(0.1)  # 100ms between requests
            
            API_V3_CALLS += 1
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code == 429:
                # Rate limited - wait and retry
                retry_after = response.headers.get('Retry-After', RATE_LIMIT_RETRY_DELAY)
                wait_time = float(retry_after)
                log(f"Rate limited (429), waiting {wait_time} seconds before retry {retry_count + 1}/{max_retries}")
                time.sleep(wait_time)
                retry_count += 1
                continue
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            if "429" in str(e) or "rate limit" in str(e).lower():
                log(f"Rate limit error: {str(e)}, waiting {RATE_LIMIT_RETRY_DELAY} seconds before retry {retry_count + 1}/{max_retries}")
                time.sleep(RATE_LIMIT_RETRY_DELAY)
                retry_count += 1
                continue
            else:
                log(f"API request failed for {url}: {str(e)}")
                return None
    
    log(f"Max retries exceeded for {url}")
    return None

def make_api_v2_request(url: str, headers: Dict[str, str], params: Dict = None) -> Optional[Dict]:
    """Make API v2.3 request with simple rate limiting"""
    global API_V2_CALLS
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Simple delay to avoid overwhelming the API
            time.sleep(0.1)  # 100ms between requests
            
            API_V2_CALLS += 1
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code == 429:
                retry_after = response.headers.get('Retry-After', RATE_LIMIT_RETRY_DELAY)
                wait_time = float(retry_after)
                log(f"Rate limited (429), waiting {wait_time} seconds before retry {retry_count + 1}/{max_retries}")
                time.sleep(wait_time)
                retry_count += 1
                continue
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            if "429" in str(e) or "rate limit" in str(e).lower():
                log(f"Rate limit error: {str(e)}, waiting {RATE_LIMIT_RETRY_DELAY} seconds before retry {retry_count + 1}/{max_retries}")
                time.sleep(RATE_LIMIT_RETRY_DELAY)
                retry_count += 1
                continue
            else:
                log(f"API v2 request failed for {url}: {str(e)}")
                return None
    
    log(f"Max retries exceeded for {url}")
    return None

def get_all_users() -> List[Dict]:
    """Fetch all users from the API"""
    users = []
    page = 1
    total_pages = None
    
    log("Starting to fetch all users")
    
    while True:
        url = f"{CONFIG['base_url']}/users?page={page}&pageSize=100"
        log(f"Fetching page {page}/{total_pages or '?'} of users")
        
        data = make_api_request(url, CONFIG['headers'])
        if not data:
            break
            
        items = data.get("items", [])
        users.extend(items)
        
        if total_pages is None:
            total_pages = data.get("totalPages", 1)
            log(f"Total pages to fetch: {total_pages}")
        
        log(f"Retrieved {len(items)} users from page {page}/{total_pages}")
        
        if page >= total_pages:
            log(f"All pages fetched. Total users: {len(users)}")
            break
        
        page += 1
        
    return users

def get_all_questions() -> List[Dict]:
    """Get all questions from the API"""
    questions = []
    page = 1
    total_pages = None
    
    log("Starting to fetch all questions")
    
    while True:
        url = f"{CONFIG['base_url']}/questions?page={page}&pageSize=100"
        log(f"Fetching page {page}/{total_pages or '?'} of questions")
        
        data = make_api_request(url, CONFIG['headers'])
        if not data:
            break
            
        items = data.get("items", [])
        questions.extend(items)
        
        if total_pages is None:
            total_pages = data.get("totalPages", 1)
            log(f"Total pages to fetch: {total_pages}")
        
        log(f"Retrieved {len(items)} questions from page {page}/{total_pages}")
        
        if page >= total_pages:
            log(f"All pages fetched. Total questions: {len(questions)}")
            break
        
        page += 1
        
    return questions

def get_all_articles() -> List[Dict]:
    """Get all articles from the API"""
    articles = []
    page = 1
    total_pages = None
    
    log("Starting to fetch all articles")
    
    while True:
        url = f"{CONFIG['base_url']}/articles?page={page}&pageSize=100"
        log(f"Fetching page {page}/{total_pages or '?'} of articles")
        
        data = make_api_request(url, CONFIG['headers'])
        if not data:
            break
            
        items = data.get("items", [])
        articles.extend(items)
        
        if total_pages is None:
            total_pages = data.get("totalPages", 1)
            log(f"Total pages to fetch: {total_pages}")
        
        log(f"Retrieved {len(items)} articles from page {page}/{total_pages}")
        
        if page >= total_pages:
            log(f"All pages fetched. Total articles: {len(articles)}")
            break
        
        page += 1
        
    return articles

def get_user_detailed_info_batch(user_ids: List[int], batch_size: int = 20) -> Dict[int, Dict]:
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
        
        user_data = make_api_v2_request(v2_url, CONFIG['headers'])
        
        if user_data and 'items' in user_data:
            for user_item in user_data['items']:
                user_id = user_item.get('user_id') if user_item else None
                if user_id:
                    user_details[user_id] = user_item
                    
    return user_details

def calculate_account_longevity(creation_date: int) -> int:
    """Calculate account longevity in days"""
    if not creation_date:
        return 0
    
    created = datetime.fromtimestamp(creation_date)
    now = datetime.now()
    return (now - created).days

def process_user_data(user: Dict, all_questions: List[Dict], all_articles: List[Dict], 
                     user_details: Dict[int, Dict]) -> Dict:
    """Process efficient user data for PowerBI"""
    try:
        # Add safety checks for None inputs
        if user is None:
            log(f"Error: user object is None")
            return None
            
        if all_questions is None:
            log(f"Error: all_questions is None")
            return None
            
        if all_articles is None:
            log(f"Error: all_articles is None")
            return None
            
        if user_details is None:
            log(f"Error: user_details is None")
            return None
        
        user_id = user.get('id')
        
        # Skip processing if user_id is None or empty
        if not user_id:
            log(f"Error: user_id is None or empty for user: {user}")
            return None
        
        log(f"Processing user data for user ID: {user_id}")
        
        # Get detailed user info from batch cache
        detailed_info = user_details.get(user_id, {})
        
        # Count user's questions efficiently - only for valid user_ids
        user_questions = []
        for q in all_questions:
            try:
                if q is None:
                    continue
                owner = q.get('owner')
                if owner is None:
                    continue  # Question has no owner, skip
                owner_id = owner.get('id')
                if owner_id == user_id:
                    user_questions.append(q)
            except Exception as e:
                log(f"Error processing question for user {user_id}: {str(e)}")
                continue
        
        total_questions = len(user_questions)
        
        # Count user's articles efficiently - only for valid user_ids
        user_articles = []
        for a in all_articles:
            try:
                if a is None:
                    continue
                owner = a.get('owner')
                if owner is None:
                    continue  # Article has no owner, skip
                owner_id = owner.get('id')
                if owner_id == user_id:
                    user_articles.append(a)
            except Exception as e:
                log(f"Error processing article for user {user_id}: {str(e)}")
                continue
        
        total_articles = len(user_articles)
        
        # Calculate account longevity
        creation_date = detailed_info.get('creation_date') if detailed_info else None
        account_longevity = calculate_account_longevity(creation_date)
        
        # Build efficient user data
        user_data = {
            # Basic user info (from API v3 users endpoint)
            'user_id': user_id,
            'display_name': user.get('name'),
            'title': user.get('jobTitle'),
            'department': user.get('department'),
            'account_id': user.get('accountId'),
            'user_type': user.get('role'),
            
            # Additional info from API v2.3
            'user_reputation': user.get('reputation', 0) or (detailed_info.get('reputation', 0) if detailed_info else 0),
            'location': detailed_info.get('location') if detailed_info else None,
            'creation_date': creation_date,
            'joined_utc': creation_date,
            'last_login_date': detailed_info.get('last_access_date') if detailed_info else None,
            
            # Calculated metrics
            'user_account_longevity_days': account_longevity,
            'total_questions': total_questions,
            'articles': total_articles,
            
            # Metadata
            'last_updated': datetime.now().isoformat(),
            'data_collection_timestamp': datetime.now().timestamp()
        }
        
        return user_data
        
    except Exception as e:
        log(f"Unexpected error in process_user_data for user {user.get('id') if user else 'None'}: {str(e)}")
        return None

def collect_powerbi_data() -> List[Dict]:
    """Main function to collect efficient PowerBI data"""
    log("Starting efficient PowerBI data collection")
    
    # Step 1: Get all users (single paginated API call)
    stop_event = threading.Event()
    loading_message = "Fetching all users..."
    loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
    
    if not VERBOSE:
        loading_thread.start()
    
    try:
        users = get_all_users()
    finally:
        stop_event.set()
        if not VERBOSE:
            loading_thread.join()
            print("\rUser retrieval complete!        ")
    
    if not users:
        log("No users found")
        return []
    
    # Check for users without user IDs and log them
    users_without_ids = []
    users_with_ids = []
    
    for user in users:
        user_id = user.get('id')
        account_id = user.get('accountId')
        
        if not user_id:
            users_without_ids.append(account_id or 'Unknown')
            log(f"⚠️  Account ID '{account_id or 'Unknown'}' has no user ID - skipping from report")
        else:
            users_with_ids.append(user)
    
    # Log summary of users without IDs
    if users_without_ids:
        print(f"\n⚠️  WARNING: Found {len(users_without_ids)} accounts without user IDs:")
        print(f"   Account IDs without user IDs: {', '.join(map(str, users_without_ids))}")
        print(f"   These accounts will be excluded from the PowerBI report")
        log(f"Accounts without user IDs: {users_without_ids}")
    
    log(f"Total users: {len(users)}, Users with valid IDs: {len(users_with_ids)}, Users without IDs: {len(users_without_ids)}")
    
    # Continue processing only with users that have valid IDs
    users = users_with_ids
    
    # Step 2: Get all questions (single paginated API call)
    stop_event = threading.Event()
    loading_message = "Fetching all questions..."
    loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
    
    if not VERBOSE:
        loading_thread.start()
    
    try:
        all_questions = get_all_questions()
    finally:
        stop_event.set()
        if not VERBOSE:
            loading_thread.join()
            print("\rQuestion retrieval complete!        ")
    
    # Step 3: Get all articles (single paginated API call)
    stop_event = threading.Event()
    loading_message = "Fetching all articles..."
    loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
    
    if not VERBOSE:
        loading_thread.start()
    
    try:
        all_articles = get_all_articles()
    finally:
        stop_event.set()
        if not VERBOSE:
            loading_thread.join()
            print("\rArticle retrieval complete!        ")
    
    # Step 4: Get detailed user info in batches (minimal API calls)
    # Only get details for users with valid IDs
    user_ids = [user.get('id') for user in users if user.get('id')]
    
    stop_event = threading.Event()
    loading_message = f"Fetching detailed info for {len(user_ids)} users..."
    loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
    
    if not VERBOSE:
        loading_thread.start()
    
    try:
        user_details = get_user_detailed_info_batch(user_ids)
    finally:
        stop_event.set()
        if not VERBOSE:
            loading_thread.join()
            print("\rUser details retrieval complete!        ")
    
    log(f"Content summary: {len(users)} valid users, {len(all_questions)} questions, {len(all_articles)} articles")
    
    # Step 5: Process all users (no additional API calls)
    log(f"Processing {len(users)} users with valid IDs")
    
    powerbi_data = []
    for i, user in enumerate(users, 1):
        try:
            user_data = process_user_data(user, all_questions, all_articles, user_details)
            if user_data:
                powerbi_data.append(user_data)
                
            if i % 100 == 0 or i == len(users):
                log(f"Processed {i}/{len(users)} users")
                
        except Exception as e:
            log(f"Error processing user {user.get('id')}: {str(e)}")
    
    log(f"Collected data for {len(powerbi_data)} users")
    return powerbi_data

def save_data_to_json(data: List[Dict], filename: str = None):
    """Save collected data to JSON file"""
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"powerbi_data_{timestamp}.json"
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        
        log(f"Data saved to {filename}")
        return filename
        
    except Exception as e:
        log(f"Error saving data to JSON: {str(e)}")
        raise

def export_powerbi_data():
    """Main export function for cron job"""
    global API_V2_CALLS, API_V3_CALLS
    
    start_time = datetime.now()
    log(f"PowerBI data export started at {start_time}")
    
    # Reset counters
    API_V2_CALLS = 0
    API_V3_CALLS = 0
    
    try:
        # Collect all data efficiently
        powerbi_data = collect_powerbi_data()
        
        if not powerbi_data:
            log("No data collected")
            return
        
        # Save to JSON file
        filename = CONFIG.get('output_file', 'powerbi_data.json')
        save_data_to_json(powerbi_data, filename)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Print summary
        print(f"\n✅ Efficient PowerBI data export complete!")
        print(f"   Data saved to: {filename}")
        print(f"   Total users processed: {len(powerbi_data)}")
        print(f"   Total time: {duration}")
        print(f"   Total API v3 calls: {API_V3_CALLS}")
        print(f"   Total API v2.3 calls: {API_V2_CALLS}")
        print(f"   Total API calls: {API_V3_CALLS + API_V2_CALLS}")
        if len(powerbi_data) > 0:
            print(f"   Average time per user: {duration.total_seconds() / len(powerbi_data):.2f}s")
        
        log(f"Export completed successfully in {duration}")
        
    except Exception as e:
        log(f"Export failed: {str(e)}")
        raise

def run_cron_job():
    """Run the scheduled job"""
    if not RUNNING:
        return
        
    log("Running scheduled PowerBI data collection")
    export_powerbi_data()

def main():
    global CONFIG, VERBOSE, logger
    
    # Setup argument parser
    parser = argparse.ArgumentParser(
        description="Efficient PowerBI Data Collector for Stack Overflow Enterprise"
    )
    parser.add_argument("--base-url", required=True, 
                       help="Stack Overflow Enterprise Base URL")
    parser.add_argument("--token", required=True, 
                       help="API access token")
    parser.add_argument("--output-file", default="powerbi_data.json",
                       help="Output JSON filename")
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Enable verbose output")
    parser.add_argument("--run-once", action="store_true",
                       help="Run once and exit (no cron job)")
    parser.add_argument("--cron-schedule", default="0 2 * * *",
                       help="Cron schedule (default: daily at 2 AM)")
    
    args = parser.parse_args()
    
    # Setup logging
    VERBOSE = args.verbose
    logger = setup_logging(VERBOSE)
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Setup global configuration
    CONFIG.update({
        'base_url': force_api_v3(args.base_url),
        'api_v2_base': get_api_v2_url(args.base_url),
        'headers': {'Authorization': f'Bearer {args.token}'},
        'output_file': args.output_file
    })
    
    logger.info(f"Efficient PowerBI Data Collector starting...")
    logger.info(f"Base URL: {CONFIG['base_url']}")
    logger.info(f"Output file: {CONFIG['output_file']}")
    
    if args.run_once:
        # Run once and exit
        logger.info("Running data collection once...")
        export_powerbi_data()
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
        export_powerbi_data()
        
        # Start scheduler
        logger.info("Starting scheduler...")
        while RUNNING:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    logger.info("PowerBI Data Collector stopped")

if __name__ == "__main__":
    main()