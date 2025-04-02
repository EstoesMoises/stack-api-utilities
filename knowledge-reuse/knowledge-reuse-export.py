import argparse
import requests
import csv
import time
import itertools
import threading
from datetime import datetime
from urllib.parse import urlparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

API_V2_CALLS = 0

def loading_animation(stop_event, message):
    spinner = itertools.cycle(['|', '/', '-', '\\'])
    while not stop_event.is_set():
        print(f"\r{message} {next(spinner)}", end='', flush=True)
        time.sleep(0.2)
        
def forceAPIV3(user_input_url):
    parsed_url = urlparse(user_input_url.strip())
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    return f"{base_url}/api/v3"

parser = argparse.ArgumentParser(description="Export questions from Stack Overflow Enterprise API to CSV.")
parser.add_argument("--base-url", required=True, help="Stack Overflow Enterprise Base URL (e.g., https://your-instance.stackoverflow.com)")
parser.add_argument("--token", required=True, help="Access token for authentication")
parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")
parser.add_argument("--threads", "-t", type=int, default=10, help="Number of concurrent threads for API calls")
args = parser.parse_args()

BASE_URL = forceAPIV3(args.base_url)
ACCESS_TOKEN = args.token
HEADERS = { "Authorization": f"Bearer {ACCESS_TOKEN}"}
VERBOSE = args.verbose
MAX_THREADS = args.threads

# Cache for SME data to avoid redundant API calls
TAG_SME_CACHE = {}
USER_SME_CACHE = defaultdict(set)
USER_DATA_CACHE = {}  # Cache for user data (department, jobTitle)
ANSWER_CACHE = {}  # Cache for accepted answers

def log(message):
    """Print log message if verbose mode is enabled"""
    if VERBOSE:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {message}")

def get_questions():
    questions = []
    page = 1
    total_pages = None
    
    log(f"Starting to fetch questions from {BASE_URL}")
    
    while True:
        url = f"{BASE_URL}/questions?page={page}&pageSize=100"
        log(f"Fetching page {page}/{total_pages or '?'} of questions")
        
        try:
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            data = response.json()
            
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
            
        except requests.exceptions.RequestException as e:
            log(f"Error fetching page {page}: {str(e)}")
            raise
        
    return questions

def get_accepted_answer(question_id):
    # Check cache first
    if question_id in ANSWER_CACHE:
        log(f"Using cached answer for question ID: {question_id}")
        return ANSWER_CACHE[question_id]
        
    url = f"{BASE_URL}/questions/{question_id}/answers"
    log(f"Fetching answers for question ID: {question_id}")
    
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        answers = response.json().get("items", [])
        
        log(f"Retrieved {len(answers)} answers for question ID: {question_id}")
        
        for answer in answers:
            if answer.get("isAccepted", False):
                log(f"Found accepted answer ID: {answer.get('id')} for question ID: {question_id}")
                # Store in cache
                ANSWER_CACHE[question_id] = answer
                return answer
        
        log(f"No accepted answer found for question ID: {question_id}")
        # Cache negative result too
        ANSWER_CACHE[question_id] = None
        return None
        
    except requests.exceptions.RequestException as e:
        log(f"Error fetching answers for question {question_id}: {str(e)}")
        ANSWER_CACHE[question_id] = None  # Cache the error case
        return None

def preload_answers(questions):
    """Preload accepted answers for answered questions in parallel"""
    log("Preloading accepted answers for answered questions...")
    
    # Collect all question IDs that are marked as answered
    answered_questions = [q.get("id") for q in questions if q.get("isAnswered")]
    total_answered = len(answered_questions)
    
    log(f"Found {total_answered} answered questions to preload")
    
    stop_event = threading.Event()
    loading_message = f"Preloading accepted answers for {total_answered} questions..."
    loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
    
    if not VERBOSE:
        loading_thread.start()
    
    try:
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            # Submit all question API calls to the thread pool
            futures = {executor.submit(get_accepted_answer, qid): qid for qid in answered_questions}
            
            # Process results as they complete
            completed = 0
            for future in futures:
                try:
                    answer = future.result()  # This ensures any exceptions are raised
                    qid = futures[future]
                    ANSWER_CACHE[qid] = answer  # Store in cache (even if None)
                    completed += 1
                    if VERBOSE and completed % 50 == 0:
                        log(f"Preloaded {completed}/{total_answered} answers")
                except Exception as e:
                    qid = futures[future]
                    log(f"Error preloading answer for question {qid}: {str(e)}")
                    ANSWER_CACHE[qid] = None  # Cache the error case
            
        log(f"Preloaded {len(ANSWER_CACHE)} accepted answers")
        
    finally:
        stop_event.set()
        if not VERBOSE:
            loading_thread.join()
            print("\rAccepted answer preloading complete!        ")

def calculate_user_tenure(joined_date, last_seen_date):
    if joined_date and last_seen_date:
        return (datetime.fromtimestamp(last_seen_date) - datetime.fromtimestamp(joined_date))
    return None

def get_user_data(user_id):
    """Fetch additional user data from API v3 endpoint, including user info but not tenure"""
    # Check cache first
    if user_id in USER_DATA_CACHE:
        log(f"Using cached user data for user ID: {user_id}")
        return USER_DATA_CACHE[user_id]
    
    if not user_id:
        return {"department": None, "jobTitle": None, "tenure": None}
    
    # Call v3 API for department and job title
    v3_url = f"{BASE_URL}/users/{user_id}"
    log(f"Fetching user data from v3 API for user ID: {user_id}")
    
    user_data = {"department": None, "jobTitle": None, "tenure": None}
    
    try:
        v3_response = requests.get(v3_url, headers=HEADERS)
        v3_response.raise_for_status()
        
        v3_data = v3_response.json()
        user_data.update({
            "department": v3_data.get("department"),
            "jobTitle": v3_data.get("jobTitle")
        })
        
        # Tenure data will be fetched separately in batches
        
        # Update cache
        USER_DATA_CACHE[user_id] = user_data
        log(f"Retrieved user data for user ID: {user_id}")
        
        return user_data
        
    except requests.exceptions.RequestException as e:
        log(f"Error fetching user data for user ID {user_id}: {str(e)}")
        # Cache empty result to avoid repeated failed calls
        USER_DATA_CACHE[user_id] = user_data
        return user_data

def get_batch_tenure_data(user_ids, batch_size=10):
    """Fetch tenure data for multiple users in a single API call"""
    global API_V2_CALLS
    
    if not user_ids:
        return
        
    # Parse the base URL to get the domain for v2.3 API
    parsed_url = urlparse(args.base_url.strip())
    base_domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
    
    # Process users in batches
    for i in range(0, len(user_ids), batch_size):
        batch = user_ids[i:i + batch_size]
        
        # Convert batch to semicolon-separated string
        ids_string = ";".join(map(str, batch))
        
        # Call v2.3 API with batched IDs
        v2_url = f"{base_domain}/api/2.3/users/{ids_string}?order=desc&sort=reputation"
        log(f"Batch fetching tenure data from v2.3 API for {len(batch)} users")
        
        # Increment API call counter
        API_V2_CALLS += 1
        
        try:
            v2_response = requests.get(v2_url, headers=HEADERS)
            v2_response.raise_for_status()
            
            v2_data = v2_response.json()
            
            if "items" in v2_data:
                for user_item in v2_data["items"]:
                    user_id = user_item.get("user_id")
                    if not user_id or user_id not in USER_DATA_CACHE:
                        continue
                        
                    creation_date = user_item.get("creation_date")
                    last_access_date = user_item.get("last_access_date")
                    
                    # Calculate user tenure
                    tenure = calculate_user_tenure(creation_date, last_access_date)
                    
                    # Update the existing cache entry with tenure
                    USER_DATA_CACHE[user_id]["tenure"] = tenure
                    
                    log(f"Retrieved tenure data for user ID: {user_id}")
            
        except requests.exceptions.RequestException as e:
            log(f"Error batch fetching tenure data: {str(e)}")
    
def get_smes_for_tag(tag_id):
    # Check cache first
    if tag_id in TAG_SME_CACHE:
        log(f"Using cached SME data for tag ID: {tag_id}")
        return TAG_SME_CACHE[tag_id]
    
    url = f"{BASE_URL}/tags/{tag_id}/subject-matter-experts"
    log(f"Fetching SMEs for tag ID: {tag_id}")
    
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        
        data = response.json()
        sme_users = {user.get('id') for user in data.get('users', [])}
        
        # Update cache
        TAG_SME_CACHE[tag_id] = sme_users
        
        # Update user-tag cache for faster lookups
        for user_id in sme_users:
            USER_SME_CACHE[user_id].add(tag_id)
        
        log(f"Found {len(sme_users)} SMEs for tag ID: {tag_id}")
        return sme_users
        
    except requests.exceptions.RequestException as e:
        log(f"Error fetching SMEs for tag {tag_id}: {str(e)}")
        TAG_SME_CACHE[tag_id] = set()  # Cache empty result to avoid repeated failed calls
        return set()

def check_tag_sme(user_id, tag_id):
    """Check if a user is an SME for a specific tag"""
    # Get SMEs for this tag (will use cache if available)
    smes = get_smes_for_tag(tag_id)
    return user_id in smes

def is_sme(user_id, question_tags):
    if not user_id or not question_tags:
        return False
    
    # Check if we already know this user is an SME for any tag (fast cache check)
    if user_id in USER_SME_CACHE:
        tag_ids = {tag.get('id') for tag in question_tags if 'id' in tag}
        if any(tag_id in USER_SME_CACHE[user_id] for tag_id in tag_ids):
            log(f"User ID {user_id} is an SME (from cache)")
            return True
    
    log(f"Checking if user ID {user_id} is SME for any of {len(question_tags)} tags")
    
    # Get valid tag IDs from question tags
    tag_ids = [tag.get('id') for tag in question_tags if 'id' in tag]
    
    # Check tags in parallel
    with ThreadPoolExecutor(max_workers=min(MAX_THREADS, len(tag_ids))) as executor:
        # Create a future for each tag check
        future_to_tag = {
            executor.submit(check_tag_sme, user_id, tag_id): tag_id 
            for tag_id in tag_ids
        }
        
        # As each future completes, check if the user is an SME
        for future in future_to_tag:
            tag_id = future_to_tag[future]
            try:
                is_sme_for_tag = future.result()
                if is_sme_for_tag:
                    log(f"User ID {user_id} is an SME for tag {tag_id}")
                    return True
            except Exception as e:
                log(f"Error checking SME status for tag {tag_id}: {str(e)}")
    
    log(f"User ID {user_id} is not an SME for these tags")
    return False

def preload_sme_data(questions):
    """Preload SME data for all tags in all questions to avoid repeated API calls"""
    log("Preloading SME data for all tags...")
    
    # Collect all unique tags
    all_tags = set()
    for question in questions:
        tags = question.get("tags", [])
        for tag in tags:
            tag_id = tag.get("id")
            if tag_id:
                all_tags.add(tag_id)
    
    log(f"Found {len(all_tags)} unique tags across all questions")
    
    # Fetch SMEs for each tag using thread pool
    stop_event = threading.Event()
    loading_message = f"Preloading SME data for {len(all_tags)} tags..."
    loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
    
    if not VERBOSE:
        loading_thread.start()
    
    try:
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            # Submit all tag API calls to the thread pool
            futures = [executor.submit(get_smes_for_tag, tag_id) for tag_id in all_tags]
            
            # Process results as they complete
            completed = 0
            for future in futures:
                try:
                    future.result()  # This ensures any exceptions are raised
                    completed += 1
                    if VERBOSE and completed % 10 == 0:
                        log(f"Preloaded {completed}/{len(all_tags)} tags")
                except Exception as e:
                    log(f"Error preloading tag: {str(e)}")
            
        log(f"Preloaded SME data for {len(TAG_SME_CACHE)} tags")
        
    finally:
        stop_event.set()
        if not VERBOSE:
            loading_thread.join()
            print("\rSME data preloading complete!        ")

def preload_user_data(questions):
    """Preload user data (department, job title) for all users in questions and answers"""
    log("Collecting user IDs for data preloading...")
    
    # Collect all unique user IDs
    user_ids = set()
    
    # Add question owners
    for question in questions:
        owner = question.get("owner", {})
        if owner:
            user_id = owner.get("id")
            if user_id:
                user_ids.add(user_id)
    
    # Add answer owners - we'll do this after preloading answers for better efficiency
    
    # First, preload answers for all questions
    preload_answers(questions)
    
    # Now collect answer owner IDs from the preloaded cache
    for question in questions:
        if question.get("isAnswered"):
            qid = question.get("id")
            if qid in ANSWER_CACHE and ANSWER_CACHE[qid]:
                answer_owner = ANSWER_CACHE[qid].get("owner", {})
                user_id = answer_owner.get("id")
                if user_id:
                    user_ids.add(user_id)
    
    log(f"Found {len(user_ids)} unique users")
    
    # First, fetch basic user data from v3 API
    stop_event = threading.Event()
    loading_message = f"Preloading basic user data for {len(user_ids)} users..."
    loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
    
    if not VERBOSE:
        loading_thread.start()
    
    try:
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            # Submit all user API calls to the thread pool for basic data
            futures = [executor.submit(get_user_data, user_id) for user_id in user_ids]
            
            # Process results as they complete
            completed = 0
            for future in futures:
                try:
                    future.result()  # This ensures any exceptions are raised
                    completed += 1
                    if VERBOSE and completed % 50 == 0:
                        log(f"Preloaded basic data for {completed}/{len(user_ids)} users")
                except Exception as e:
                    log(f"Error preloading user data: {str(e)}")
            
        log(f"Preloaded basic user data for {len(USER_DATA_CACHE)} users")
        
    finally:
        stop_event.set()
        if not VERBOSE:
            loading_thread.join()
            print("\rBasic user data preloading complete!        ")
    
    # Now, fetch tenure data in batches
    user_ids_list = list(user_ids)
    batch_size = 10
    total_batches = (len(user_ids_list) + batch_size - 1) // batch_size
    
    stop_event = threading.Event()
    loading_message = f"Preloading tenure data for {len(user_ids)} users in {total_batches} batches..."
    loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
    
    if not VERBOSE:
        loading_thread.start()
    
    try:
        # Process users in batches to get tenure data
        get_batch_tenure_data(user_ids_list, batch_size)
        
        log(f"Preloaded tenure data with {API_V2_CALLS} API calls")
        
    finally:
        stop_event.set()
        if not VERBOSE:
            loading_thread.join()
            print("\rTenure data preloading complete!        ")

def export_to_csv():
    global API_V2_CALLS
    API_V2_CALLS = 0
    
    start_time = datetime.now()
    timestamp = start_time.strftime("%Y%m%d_%H%M%S")
    log(f"Export process started at {start_time}")
    
    stop_event = threading.Event()
    loading_message = "Fetching questions..."
    loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
    
    if not VERBOSE:
        loading_thread.start()
    
    try:
        questions = get_questions()
    finally:
        stop_event.set()
        if not VERBOSE:
            loading_thread.join()
            print("\rQuestion retrieval complete!        ")
    
    # Preload all SME data to improve performance
    preload_sme_data(questions)
    
    # Preload user data (department, job title) to improve performance
    # This now includes preloading accepted answers
    preload_user_data(questions)
    
    csv_filename = f"knowledge_reuse_export_{timestamp}.csv"
    log(f"Writing {len(questions)} questions to {csv_filename}")
    
    stop_event = threading.Event()
    loading_message = f"Writing data to {csv_filename}..."
    loading_thread = threading.Thread(target=loading_animation, args=(stop_event, loading_message))
    
    if not VERBOSE:
        loading_thread.start()
    
    try: 
        with open(csv_filename, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "tags", "owner.account_id", "owner.user_type", "owner.display_name", "is_answered",
                "view_count", "up_vote_count", "creation_date", "question_id", "share_link", "link", "title",
                "is_SME", "status", "department", "job_title", "user_tenure",
                "acc_answer_owner_id", "acc_answer_user_type", "acc_answer_display_name", 
                "acc_answer_up_vote_count", "acc_answer_creation_date", "acc_answer_id",
                "acc_answer_is_SME", "acc_answer_department", "acc_answer_job_title", "acc_answer_user_tenure"
            ])
            
            total_questions = len(questions)
            for i, question in enumerate(questions, 1):
                if i % 10 == 0 or i == 1 or i == total_questions:
                    log(f"Processing question {i}/{total_questions} (ID: {question.get('id')})")
                    # Update loading message with progress percentage
                    if not VERBOSE:
                        progress_percent = int((i / total_questions) * 100)
                        loading_message = f"Writing data to {csv_filename}... {progress_percent}% complete"
                
                owner = question.get("owner", {}) or {}
                owner_id = owner.get("id")
                
                # Get owner user data (department, job title)
                owner_data = {"department": None, "jobTitle": None, "tenure": None}
                if owner_id:
                    owner_data = get_user_data(owner_id)
                
                # Get accepted answer if question is answered (from cache)
                accepted_answer = None
                if question.get("isAnswered"):
                    qid = question.get("id")
                    accepted_answer = ANSWER_CACHE.get(qid)
                
                tags = question.get("tags", [])
                tag_names = [tag["name"] for tag in tags if "name" in tag]
                
                # Check if question owner is SME (should be fast with preloaded data)
                is_owner_sme = False
                if owner_id:
                    is_owner_sme = is_sme(owner_id, tags)
                
                row = [
                    ",".join(tag_names),
                    owner_id,
                    owner.get("role"),
                    owner.get("name"),
                    question.get("isAnswered"),
                    question.get("viewCount"),
                    question.get("score"),
                    question.get("creationDate"),
                    question.get("id"),
                    question.get("shareUrl"),
                    question.get("webUrl"),
                    question.get("title"),
                    is_owner_sme,
                    "Closed" if question.get("isClosed") else "Obsolete" if question.get("isObsolete") else "N/A",
                    owner_data.get("department"),
                    owner_data.get("jobTitle"),
                    owner_data.get("tenure")
                ]
                
                if accepted_answer:
                    answer_owner = accepted_answer.get("owner", {}) or {}
                    answer_owner_id = answer_owner.get("id")
                    
                    # Get answer owner user data (department, job title)
                    answer_owner_data = {"department": None, "jobTitle": None, "tenure": None}
                    if answer_owner_id:
                        answer_owner_data = get_user_data(answer_owner_id)
                    
                    # Check if answer owner is SME (should be fast with preloaded data)
                    is_answer_owner_sme = False
                    if answer_owner_id:
                        is_answer_owner_sme = is_sme(answer_owner_id, tags)
                    
                    row.extend([
                        answer_owner_id,
                        answer_owner.get("role"),
                        answer_owner.get("name"),
                        accepted_answer.get("score"),
                        accepted_answer.get("creationDate"),
                        accepted_answer.get("id"),
                        is_answer_owner_sme,
                        answer_owner_data.get("department"),
                        answer_owner_data.get("jobTitle"),
                        answer_owner_data.get("tenure")
                    ])
                else:
                    row.extend([None] * 10)
                
                writer.writerow(row)
    finally:
        stop_event.set()
        if not VERBOSE:
            loading_thread.join()
            print("\rData export to CSV complete!        ")
        
    end_time = datetime.now()
    duration = end_time - start_time
    log(f"Export process completed at {end_time}. Total duration: {duration}")
    print(f"\n✅ Export complete! Data saved to: {csv_filename}")
    print(f"   Total questions processed: {len(questions)}")
    print(f"   Total time: {duration}")
    print(f"   Total SME API calls: {len(TAG_SME_CACHE)}")
    print(f"   Total user data API calls: {len(USER_DATA_CACHE)}")
    print(f"   Total cached answers: {len(ANSWER_CACHE)}")
    print(f"   Additional API v2.3 calls: {API_V2_CALLS}")
        
if __name__ == "__main__":
    export_to_csv()