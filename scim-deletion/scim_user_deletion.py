'''
This Python script is a working proof of concept example of using Stack Overflow APIs for bulk user deletion. 
If you run into difficulties, please leave feedback in the Github Issues.
'''

# Standard Python libraries
import argparse
import csv
import datetime
import logging
import json
import time
import re
import random
from enum import Enum
from typing import Dict, List, Tuple, Optional

# Local libraries
from so4t_scim import ScimClient


class ErrorType(Enum):
    """Enumeration of different error types for classification"""
    SERVER_ERROR = "server_error"  # 5xx errors
    CLIENT_ERROR = "client_error"  # 4xx errors
    NETWORK_ERROR = "network_error"  # Connection/timeout issues
    AUTHENTICATION_ERROR = "auth_error"  # 401/403 errors
    RATE_LIMIT_ERROR = "rate_limit_error"  # 429 errors
    UNKNOWN_ERROR = "unknown_error"  # Any other errors


class ErrorHandler:
    """Centralized error handling and classification"""
    
    def __init__(self):
        self.error_patterns = {
            ErrorType.SERVER_ERROR: [r'50[0-9]', r'server error', r'internal error'],
            ErrorType.CLIENT_ERROR: [r'40[0-9]', r'client error', r'bad request'],
            ErrorType.AUTHENTICATION_ERROR: [r'401', r'403', r'unauthorized', r'forbidden', r'authentication'],
            ErrorType.RATE_LIMIT_ERROR: [r'429', r'rate limit', r'too many requests'],
            ErrorType.NETWORK_ERROR: [r'connection', r'timeout', r'network', r'dns', r'unreachable']
        }
        
        self.retry_config = {
            ErrorType.SERVER_ERROR: {'max_retries': 3, 'base_delay': 2, 'exponential': True},
            ErrorType.RATE_LIMIT_ERROR: {'max_retries': 5, 'base_delay': 5, 'exponential': True},
            ErrorType.NETWORK_ERROR: {'max_retries': 3, 'base_delay': 1, 'exponential': True},
            ErrorType.CLIENT_ERROR: {'max_retries': 1, 'base_delay': 0, 'exponential': False},
            ErrorType.AUTHENTICATION_ERROR: {'max_retries': 0, 'base_delay': 0, 'exponential': False},
            ErrorType.UNKNOWN_ERROR: {'max_retries': 2, 'base_delay': 1, 'exponential': True}
        }
    
    def classify_error(self, error: Exception) -> ErrorType:
        """Classify an error based on its message and type"""
        error_str = str(error).lower()
        error_type_name = type(error).__name__.lower()
        
        for error_type, patterns in self.error_patterns.items():
            for pattern in patterns:
                if re.search(pattern, error_str) or re.search(pattern, error_type_name):
                    return error_type
        
        return ErrorType.UNKNOWN_ERROR
    
    def should_retry(self, error_type: ErrorType, attempt: int) -> bool:
        """Determine if an error should be retried based on type and attempt number"""
        config = self.retry_config.get(error_type, self.retry_config[ErrorType.UNKNOWN_ERROR])
        return attempt < config['max_retries']
    
    def get_delay(self, error_type: ErrorType, attempt: int) -> float:
        """Calculate delay before retry based on error type and attempt number"""
        config = self.retry_config.get(error_type, self.retry_config[ErrorType.UNKNOWN_ERROR])
        base_delay = config['base_delay']
        
        if config['exponential']:
            return base_delay * (2 ** attempt)
        else:
            return base_delay
    
    def log_error(self, error: Exception, error_type: ErrorType, context: str, attempt: int = 0):
        """Log error with appropriate level and context"""
        error_msg = f"{context} - {error_type.value}: {str(error)}"
        
        if error_type in [ErrorType.SERVER_ERROR, ErrorType.NETWORK_ERROR] and attempt > 0:
            logging.warning(f"Attempt {attempt + 1} - {error_msg}")
        elif error_type == ErrorType.AUTHENTICATION_ERROR:
            logging.error(f"CRITICAL - {error_msg}")
        elif error_type == ErrorType.RATE_LIMIT_ERROR:
            logging.warning(f"RATE LIMITED - {error_msg}")
        else:
            logging.error(error_msg)


def simulate_user_deletion(user: Dict, index: int) -> Dict:
    """
    Simulate a user deletion without making actual API calls.
    Returns a mock deletion result for dry-run mode.
    """
    user_identifier = get_user_identifier(user)
    
    # Simulate different outcomes for realistic dry-run results
    # 90% success rate simulation
    if random.random() < 0.9:
        return {
            'status': 'success',
            'message': f'[DRY-RUN] Would delete user: {user_identifier}',
            'user_id': user['id'],
            'simulated': True
        }
    else:
        # Simulate occasional failures
        mock_errors = [
            'User not found',
            'Permission denied',
            'User has active content'
        ]
        return {
            'status': 'failed',
            'message': f'[DRY-RUN] Simulated failure: {random.choice(mock_errors)}',
            'user_id': user['id'],
            'simulated': True
        }


def preview_users_to_delete(users_to_delete: List, operation_type: str) -> None:
    """
    Show a preview of users that would be deleted in dry-run or before confirmation.
    """
    total_users = len(users_to_delete)
    
    print(f"\n{'='*60}")
    print(f"📋 DELETION PREVIEW - {operation_type.upper()}")
    print(f"{'='*60}")
    print(f"Total users to be deleted: {total_users}")
    print(f"{'='*60}")
    
    # Show first 10 users as preview
    preview_count = min(10, total_users)
    
    for i, user in enumerate(users_to_delete[:preview_count]):
        user_identifier = get_user_identifier(user)
        active_status = "Active" if user.get("active", True) else "Inactive"
        print(f"{i+1:3d}. {user_identifier} ({active_status})")
    
    if total_users > preview_count:
        print(f"     ... and {total_users - preview_count} more users")
    
    print(f"{'='*60}\n")


def get_user_confirmation(users_to_delete: List, operation_type: str, dry_run: bool) -> bool:
    """
    Get user confirmation before proceeding with deletions.
    Returns True if user confirms, False otherwise.
    """
    if dry_run:
        return True  # No need for confirmation in dry-run mode
    
    preview_users_to_delete(users_to_delete, operation_type)
    
    print("⚠️  WARNING: User deletion is IRREVERSIBLE!")
    print("⚠️  Make sure you have backups and have verified this list!")
    print()
    
    while True:
        response = input("Are you sure you want to delete these users? (yes/no): ").lower().strip()
        
        if response in ['yes', 'y']:
            # Double confirmation for large operations
            if len(users_to_delete) > 50:
                print(f"\n🚨 You are about to delete {len(users_to_delete)} users!")
                double_confirm = input("Type 'DELETE' to confirm this large operation: ").strip()
                if double_confirm == 'DELETE':
                    return True
                else:
                    print("❌ Operation cancelled.")
                    return False
            return True
        elif response in ['no', 'n']:
            print("❌ Operation cancelled.")
            return False
        else:
            print("Please enter 'yes' or 'no'")


def main():
    # Configure comprehensive logging
    configure_logging()
    
    args = get_args()
    client = ScimClient(args.token, args.url)
    error_handler = ErrorHandler()

    # Check for dry-run mode
    if args.dry_run:
        logging.info("🔍 Running in DRY-RUN mode - no actual deletions will be performed")

    # Get all users via SCIM API and write to a JSON file with error handling
    all_users = get_all_users_with_retry(client, error_handler)
    if not all_users:
        logging.error("Failed to retrieve users. Exiting.")
        return
    
    write_json(all_users, 'all_users')

    failed_deletions = []
    skipped_indices = []
    error_summary = {}
    
    if args.deactivated and args.csv:
        logging.info("Please provide only one argument for which users to delete.")
        logging.info("Use --deactivated to delete deactivated users.")
        logging.info("Use --csv to delete users from a CSV file.")
        logging.info("Use --dry-run to simulate the operation without actual deletions.")
        logging.info("See README for more information.")
        return

    elif args.deactivated:
        users_to_delete = [user for user in all_users if not user["active"]]
        logging.info(f"Found {len(users_to_delete)} deactivated users to delete")
        
        # Get confirmation unless in dry-run mode
        if not get_user_confirmation(users_to_delete, "deactivated users", args.dry_run):
            return
        
        failed_deletions, skipped_indices, error_summary = delete_users_with_comprehensive_retry(
            client, users_to_delete, error_handler, dry_run=args.dry_run
        )

    elif args.csv:
        csv_users_to_delete = get_users_from_csv(args.csv)
        logging.info(f"Processing {len(csv_users_to_delete)} users from CSV file")
        users_to_delete = []
        
        for user_email in csv_users_to_delete:
            scim_user = scim_user_lookup(all_users, email=user_email)
            if scim_user is None:
                deletion_result = {
                    'email': user_email,
                    'status': 'failed',
                    'message': 'User email address not found via SCIM API',
                    'error_type': 'lookup_failed'
                }
                failed_deletions.append(deletion_result)
                logging.warning(f"User not found: {user_email}")
            else:
                users_to_delete.append(scim_user)
        
        if users_to_delete:
            # Get confirmation unless in dry-run mode
            if not get_user_confirmation(users_to_delete, "CSV users", args.dry_run):
                return
                
            csv_failed_deletions, csv_skipped_indices, csv_error_summary = delete_users_with_comprehensive_retry(
                client, users_to_delete, error_handler, include_email=True, dry_run=args.dry_run
            )
            failed_deletions.extend(csv_failed_deletions)
            skipped_indices.extend(csv_skipped_indices)
            
            # Merge error summaries
            for error_type, count in csv_error_summary.items():
                error_summary[error_type] = error_summary.get(error_type, 0) + count

    else:
        logging.info("Please provide an argument for which users to delete.")
        logging.info("Use --deactivated to delete deactivated users.")
        logging.info("Use --csv to delete users from a CSV file.")
        logging.info("Use --dry-run to simulate the operation without actual deletions.")
        logging.info("See README for more information.")
        return
    
    # Generate comprehensive report
    generate_final_report(failed_deletions, skipped_indices, error_summary, dry_run=args.dry_run)


def configure_logging():
    """Configure comprehensive logging with file and console output"""
    log_format = '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    
    # Create formatters
    formatter = logging.Formatter(log_format)
    
    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    # Console handler (INFO and above)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler (DEBUG and above)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_handler = logging.FileHandler(f'bulk_deletion_{timestamp}.log')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    logging.info("Logging configured - detailed logs written to file")


def get_all_users_with_retry(client, error_handler: ErrorHandler, max_attempts: int = 3) -> Optional[List]:
    """Get all users with comprehensive retry logic"""
    
    for attempt in range(max_attempts):
        try:
            logging.info(f"Attempting to fetch all users (attempt {attempt + 1}/{max_attempts})")
            users = client.get_all_users()
            logging.info(f"Successfully retrieved {len(users)} users")
            return users
            
        except Exception as e:
            error_type = error_handler.classify_error(e)
            error_handler.log_error(e, error_type, "Fetching all users", attempt)
            
            if error_handler.should_retry(error_type, attempt) and attempt < max_attempts - 1:
                delay = error_handler.get_delay(error_type, attempt)
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logging.error(f"Failed to fetch users after {max_attempts} attempts")
                return None
    
    return None


def delete_users_with_comprehensive_retry(
    client, 
    users_to_delete: List, 
    error_handler: ErrorHandler, 
    include_email: bool = False,
    dry_run: bool = False
) -> Tuple[List, List, Dict]:
    """Delete users with comprehensive error handling and retry logic"""
    
    failed_deletions = []
    skipped_indices = []
    error_summary = {}
    
    total_users = len(users_to_delete)
    
    if dry_run:
        logging.info(f"🔍 DRY-RUN MODE: Simulating deletion process for {total_users} users")
        logging.info("⚠️  No actual deletions will be performed")
    else:
        logging.info(f"🗑️  Starting deletion process for {total_users} users")
    
    for index, user in enumerate(users_to_delete):
        user_id = user["id"]
        user_identifier = get_user_identifier(user)
        
        # Progress logging
        if (index + 1) % 10 == 0 or index == 0:
            status_prefix = "[DRY-RUN] " if dry_run else ""
            logging.info(f"{status_prefix}Processing user {index + 1}/{total_users}: {user_identifier}")
        
        if dry_run:
            # Simulate the deletion process
            deletion_result = simulate_user_deletion(user, index)
            
            if include_email and user.get("emails"):
                deletion_result['email'] = user["emails"][0]["value"]
            
            if deletion_result['status'] != 'success':
                deletion_result['index'] = index
                deletion_result['user_identifier'] = user_identifier
                deletion_result['error_type'] = 'simulated_failure'
                failed_deletions.append(deletion_result)
                logging.warning(f"[DRY-RUN] Simulated failure for user {user_identifier}: {deletion_result.get('message', 'No message')}")
            else:
                logging.debug(f"[DRY-RUN] Would successfully delete user at index {index}: {user_identifier}")
        
        else:
            # Original deletion logic
            deletion_successful = False
            final_error = None
            error_history = []
            
            attempt = 0
            while not deletion_successful:
                try:
                    logging.debug(f"Attempting deletion for user {user_identifier} (attempt {attempt + 1})")
                    deletion_result = client.delete_user(user_id)
                    
                    if include_email and user.get("emails"):
                        deletion_result['email'] = user["emails"][0]["value"]
                    
                    if deletion_result['status'] != 'success':
                        deletion_result['index'] = index
                        deletion_result['user_identifier'] = user_identifier
                        deletion_result['error_type'] = 'api_failure'
                        failed_deletions.append(deletion_result)
                        logging.warning(f"API reported failure for user {user_identifier}: {deletion_result.get('message', 'No message')}")
                    else:
                        logging.debug(f"Successfully deleted user at index {index}: {user_identifier}")
                    
                    deletion_successful = True
                    
                except Exception as e:
                    error_type = error_handler.classify_error(e)
                    error_history.append({
                        'attempt': attempt + 1,
                        'error_type': error_type.value,
                        'error_message': str(e),
                        'timestamp': datetime.datetime.now().isoformat()
                    })
                    
                    error_summary[error_type.value] = error_summary.get(error_type.value, 0) + 1
                    error_handler.log_error(e, error_type, f"User {user_identifier} (index {index})", attempt)
                    final_error = e
                    
                    if error_handler.should_retry(error_type, attempt):
                        delay = error_handler.get_delay(error_type, attempt)
                        logging.debug(f"Retrying user {user_identifier} in {delay} seconds...")
                        time.sleep(delay)
                        attempt += 1
                    else:
                        skip_info = {
                            'index': index,
                            'user_id': user_id,
                            'user_identifier': user_identifier,
                            'final_error_type': error_type.value,
                            'final_error_message': str(e),
                            'total_attempts': attempt + 1,
                            'error_history': error_history,
                            'timestamp': datetime.datetime.now().isoformat()
                        }
                        
                        if error_type == ErrorType.AUTHENTICATION_ERROR:
                            logging.error(f"CRITICAL: Authentication error for user {user_identifier}. This may affect all subsequent deletions.")
                        
                        skipped_indices.append(skip_info)
                        logging.error(f"Skipping user at index {index} ({user_identifier}) - {error_type.value} after {attempt + 1} attempts")
                        deletion_successful = True
    
    # Log summary
    successful_deletions = total_users - len(failed_deletions) - len(skipped_indices)
    mode_prefix = "[DRY-RUN] " if dry_run else ""
    
    logging.info(f"{mode_prefix}Deletion process completed:")
    logging.info(f"  Successful: {successful_deletions}")
    logging.info(f"  Failed (API): {len(failed_deletions)}")
    if not dry_run:
        logging.info(f"  Skipped (Errors): {len(skipped_indices)}")
    
    return failed_deletions, skipped_indices, error_summary


def get_user_identifier(user: Dict) -> str:
    """Get a human-readable identifier for a user"""
    if user.get("emails") and len(user["emails"]) > 0:
        return user["emails"][0].get("value", user["id"])
    elif user.get("userName"):
        return user["userName"]
    else:
        return user["id"]


def generate_final_report(failed_deletions: List, skipped_indices: List, error_summary: Dict, dry_run: bool = False):
    """Generate comprehensive final report with error analysis"""
    
    report_date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_prefix = "dry_run_" if dry_run else ""
    
    # Log summary with dry-run indication
    mode_text = " (DRY-RUN)" if dry_run else ""
    
    if skipped_indices:
        logging.warning(f"Skipped {len(skipped_indices)} user deletions due to errors{mode_text}:")
        
        # Group by error type for summary
        error_type_counts = {}
        for skip_info in skipped_indices:
            error_type = skip_info['final_error_type']
            error_type_counts[error_type] = error_type_counts.get(error_type, 0) + 1
        
        for error_type, count in error_type_counts.items():
            logging.warning(f"  {error_type}: {count} users")
            
        # Log individual skipped users at debug level
        for skip_info in skipped_indices:
            logging.debug(f"  Index {skip_info['index']}: {skip_info['final_error_type']} - {skip_info['user_identifier']}")

    # Generate error analysis
    error_analysis = analyze_errors(skipped_indices, error_summary)
    
    if len(failed_deletions) == 0 and len(skipped_indices) == 0:
        success_msg = f"All users {'would be ' if dry_run else ''}deleted successfully{mode_text}."
        logging.info(success_msg)
        status_summary = {
            'status': 'complete_success',
            'message': success_msg,
            'dry_run': dry_run
        }
    elif len(skipped_indices) == 0:
        partial_msg = f"Some users {'would ' if dry_run else ''}not {'be ' if dry_run else ''}deleted successfully (API failures){mode_text}."
        logging.warning(partial_msg)
        status_summary = {
            'status': 'partial_success',
            'message': partial_msg,
            'dry_run': dry_run
        }
    else:
        failure_msg = f"Some users {'would ' if dry_run else ''}not {'be ' if dry_run else ''}deleted due to errors{mode_text}."
        logging.warning(failure_msg)
        status_summary = {
            'status': 'partial_failure',
            'message': failure_msg,
            'dry_run': dry_run
        }

    # Create comprehensive report
    deletion_report = {
        'summary': {
            'timestamp': report_date,
            'dry_run_mode': dry_run,
            'total_processed': len(failed_deletions) + len(skipped_indices),
            'api_failures': len(failed_deletions),
            'error_skipped': len(skipped_indices),
            'status': status_summary
        },
        'error_analysis': error_analysis,
        'failed_deletions': failed_deletions,
        'skipped_indices': skipped_indices,
        'raw_error_summary': error_summary,
        'recommendations': generate_recommendations(error_analysis)
    }

    report_filename = f"{report_prefix}deletion_report_{report_date}"
    write_json(deletion_report, report_filename)
    
    report_type = "DRY-RUN report" if dry_run else "Comprehensive report"
    logging.info(f"{report_type} written to {report_filename}.json")

    # Dry-run specific messaging
    if dry_run:
        print(f"\n{'='*60}")
        print("🔍 DRY-RUN COMPLETED")
        print(f"{'='*60}")
        print("✅ No actual deletions were performed")
        print(f"📊 Report saved to: {report_filename}.json")
        print("💡 Review the results and run without --dry-run to execute")
        print(f"{'='*60}\n")


def analyze_errors(skipped_indices: List, error_summary: Dict) -> Dict:
    """Analyze error patterns and provide insights"""
    
    if not skipped_indices:
        return {'message': 'No errors to analyze'}
    
    analysis = {
        'most_common_errors': {},
        'error_patterns': {},
        'critical_issues': [],
        'retry_effectiveness': {}
    }
    
    # Analyze most common errors
    for error_type, count in error_summary.items():
        analysis['most_common_errors'][error_type] = count
    
    # Analyze retry patterns
    total_attempts = 0
    successful_retries = 0
    
    for skip_info in skipped_indices:
        attempts = skip_info.get('total_attempts', 1)
        total_attempts += attempts
        
        if attempts > 1:
            successful_retries += (attempts - 1)  # All attempts except the last were "successful" retries
    
    if total_attempts > len(skipped_indices):
        analysis['retry_effectiveness']['average_attempts_per_failure'] = total_attempts / len(skipped_indices)
        analysis['retry_effectiveness']['total_retry_attempts'] = successful_retries
    
    # Identify critical issues
    auth_errors = sum(1 for skip in skipped_indices if skip['final_error_type'] == 'auth_error')
    if auth_errors > 0:
        analysis['critical_issues'].append(f"Authentication errors detected ({auth_errors} users) - check credentials")
    
    rate_limit_errors = sum(1 for skip in skipped_indices if skip['final_error_type'] == 'rate_limit_error')
    if rate_limit_errors > len(skipped_indices) * 0.3:  # More than 30% rate limited
        analysis['critical_issues'].append("High rate limiting detected - consider slower processing")
    
    return analysis


def generate_recommendations(error_analysis: Dict) -> List[str]:
    """Generate recommendations based on error analysis"""
    
    recommendations = []
    
    if not error_analysis or error_analysis.get('message') == 'No errors to analyze':
        return ["No specific recommendations - all deletions completed successfully"]
    
    most_common = error_analysis.get('most_common_errors', {})
    critical_issues = error_analysis.get('critical_issues', [])
    
    # Authentication recommendations
    if 'auth_error' in most_common:
        recommendations.append("Verify SCIM token validity and permissions")
        recommendations.append("Check if token has expired or been revoked")
    
    # Rate limiting recommendations
    if 'rate_limit_error' in most_common:
        recommendations.append("Implement longer delays between requests")
        recommendations.append("Consider processing users in smaller batches")
    
    # Server error recommendations
    if 'server_error' in most_common:
        recommendations.append("Server errors detected - consider running during off-peak hours")
        recommendations.append("Monitor Stack Overflow status page for ongoing issues")
    
    # Network recommendations
    if 'network_error' in most_common:
        recommendations.append("Check network connectivity and DNS resolution")
        recommendations.append("Consider running from a more stable network connection")
    
    # Add critical issue recommendations
    recommendations.extend(critical_issues)
    
    if not recommendations:
        recommendations.append("Review detailed error logs for specific issues")
        recommendations.append("Consider contacting Stack Overflow support if errors persist")
    
    return recommendations


def get_args():
    parser = argparse.ArgumentParser(
        description="Delete users from Stack Overflow for Teams with comprehensive error handling."
    )

    parser.add_argument(
        "--token",
        type=str,
        required=True,
        help="The SCIM token for your Stack Overflow for Teams site."
    )

    parser.add_argument(
        "--url",
        type=str,
        required=True,
        help="The base URL for your Stack Overflow for Teams site."
    )

    parser.add_argument(
        "--csv",
        type=str,
        help="A CSV file with a list of users to delete."
    )

    parser.add_argument(
        "--deactivated",
        action="store_true",
        help="Delete deactivated users."
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate the deletion process without actually deleting users. Shows what would be deleted."
    )

    return parser.parse_args()


def get_users_from_csv(csv_file: str) -> List[str]:
    users_to_delete = []
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            for line_num, line in enumerate(csv_reader, 1):
                if line and line[0].strip():  # Skip empty lines
                    users_to_delete.append(line[0].strip())
                else:
                    logging.warning(f"Skipping empty line {line_num} in CSV file")
        
        logging.info(f"Loaded {len(users_to_delete)} users from CSV file: {csv_file}")
        
    except FileNotFoundError:
        logging.error(f"CSV file not found: {csv_file}")
        raise
    except Exception as e:
        logging.error(f"Error reading CSV file {csv_file}: {e}")
        raise
    
    return users_to_delete


def scim_user_lookup(users: List, email: str) -> Optional[Dict]:
    logging.debug(f"Finding account ID for user with email {email}...")
    
    for user in users:
        try:
            if user.get("emails") and len(user["emails"]) > 0:
                if user["emails"][0]["value"].lower() == email.lower():
                    logging.debug(f"Account ID is {user['id']}")
                    return user
        except (KeyError, IndexError, TypeError):
            # Skip users with malformed email data
            continue
    
    logging.debug(f"User not found: {email}")
    return None


def write_json(data, file_name: str):
    file_path = f"{file_name}.json"
    
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logging.debug(f"Successfully wrote data to {file_path}")
    except Exception as e:
        logging.error(f"Failed to write JSON file {file_path}: {e}")
        raise


if __name__ == "__main__":
    main()