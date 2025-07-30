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
from typing import Dict, List, Tuple, Optional

# Local libraries
from so4t_scim import ScimClient, ErrorHandler


def simulate_user_deletion(user: Dict, index: int) -> Dict:
    """
    Simulate a user deletion without making actual API calls.
    Shows what would happen without executing the deletion.
    """
    user_identifier = get_user_identifier(user)
    
    # True dry-run: show what would be attempted
    return {
        'status': 'success',
        'message': f'[DRY-RUN] Would delete user: {user_identifier}',
        'account_id': user['id'],
        'dry_run': True
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
    
    # Create error handler for customization if needed
    error_handler = ErrorHandler()

    client = ScimClient(args.token, args.url, error_handler=error_handler)

    # Check for dry-run mode
    if args.dry_run:
        logging.info("🔍 Running in DRY-RUN mode - no actual deletions will be performed")

    try:
        all_users = client.get_all_users()
        logging.info(f"Successfully retrieved {len(all_users)} users")
        write_json(all_users, 'all_users')
    except Exception as e:
        logging.error(f"Failed to retrieve users after retries: {e}")
        return

    failed_deletions = []
    
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
        
        failed_deletions = delete_users_simplified(
            client, users_to_delete, dry_run=args.dry_run
        )
        generate_final_report(failed_deletions, len(users_to_delete), dry_run=args.dry_run)

    elif args.csv:
        csv_users_to_delete = get_users_from_csv(args.csv)
        logging.info(f"Processing {len(csv_users_to_delete)} users from CSV file")
        users_to_delete = []
        
        for user_email in csv_users_to_delete:
            scim_user = scim_user_lookup(all_users, email=user_email)
            if scim_user is None:
                deletion_result = {
                    'email': user_email,
                    'status': 'error',
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
                
            csv_failed_deletions = delete_users_simplified(
                client, users_to_delete, include_email=True, dry_run=args.dry_run
            )
            failed_deletions.extend(csv_failed_deletions)
            
        # Generate report with total count including CSV lookup failures
        total_attempted = len(csv_users_to_delete)  # Total from CSV file
        generate_final_report(failed_deletions, total_attempted, dry_run=args.dry_run)

    else:
        logging.info("Please provide an argument for which users to delete.")
        logging.info("Use --deactivated to delete deactivated users.")
        logging.info("Use --csv to delete users from a CSV file.")
        logging.info("Use --dry-run to simulate the operation without actual deletions.")
        logging.info("See README for more information.")
        return


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


def delete_users_simplified(
    client: ScimClient, 
    users_to_delete: List, 
    include_email: bool = False,
    dry_run: bool = False
) -> List:
    """Simplified user deletion - retry logic now handled by client"""
    
    failed_deletions = []
    total_users = len(users_to_delete)
    
    if dry_run:
        logging.info(f"🔍 DRY-RUN MODE: Simulating deletion process for {total_users} users")
        logging.info("⚠️  No actual deletions will be performed")
    else:
        logging.info(f"🗑️  Starting deletion process for {total_users} users")
    
    successful_deletions = 0
    
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
                failed_deletions.append(deletion_result)
                logging.warning(f"[DRY-RUN] Would attempt to delete user {user_identifier}")
            else:
                successful_deletions += 1
                logging.debug(f"[DRY-RUN] Would successfully delete user at index {index}: {user_identifier}")
        
        else:
            try:
                deletion_result = client.delete_user(user_id)
                
                if include_email and user.get("emails"):
                    deletion_result['email'] = user["emails"][0]["value"]
                
                if deletion_result['status'] != 'success':
                    deletion_result['index'] = index
                    deletion_result['user_identifier'] = user_identifier
                    failed_deletions.append(deletion_result)
                    logging.warning(f"API reported failure for user {user_identifier}: {deletion_result.get('message', 'No message')}")
                else:
                    successful_deletions += 1
                    logging.debug(f"Successfully deleted user at index {index}: {user_identifier}")
                    
            except Exception as e:
                # This should rarely happen now since client handles retries
                # Only unrecoverable errors should reach here
                deletion_result = {
                    'index': index,
                    'account_id': user_id,
                    'user_identifier': user_identifier,
                    'status': 'error',
                    'message': f'Unrecoverable error after retries: {str(e)}',
                    'error_type': 'unrecoverable_error'
                }
                
                if include_email and user.get("emails"):
                    deletion_result['email'] = user["emails"][0]["value"]
                
                failed_deletions.append(deletion_result)
                logging.error(f"Unrecoverable error for user {user_identifier}: {e}")
    
    # Log summary
    mode_prefix = "[DRY-RUN] " if dry_run else ""
    
    logging.info(f"{mode_prefix}Deletion process completed:")
    logging.info(f"  Successful: {successful_deletions}")
    logging.info(f"  Failed: {len(failed_deletions)}")
    
    return failed_deletions


def get_user_identifier(user: Dict) -> str:
    """Get a human-readable identifier for a user"""
    if user.get("emails") and len(user["emails"]) > 0:
        return user["emails"][0].get("value", user["id"])
    elif user.get("userName"):
        return user["userName"]
    else:
        return user["id"]


def generate_final_report(failed_deletions: List, total_processed: int = 0, dry_run: bool = False):
    """Generate simplified final report"""
    
    report_date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_prefix = "dry_run_" if dry_run else ""
    
    # Log summary with dry-run indication
    mode_text = " (DRY-RUN)" if dry_run else ""
    
    if total_processed == 0:
        no_users_msg = f"No users {'would be ' if dry_run else 'were '}processed - no users found matching criteria{mode_text}."
        logging.info(no_users_msg)
        status_summary = {
            'status': 'no_users_found',
            'message': no_users_msg,
            'dry_run': dry_run
        }
    elif len(failed_deletions) == 0:
        success_msg = f"All {total_processed} users {'would be ' if dry_run else 'were '}deleted successfully{mode_text}."
        logging.info(success_msg)
        status_summary = {
            'status': 'complete_success', 
            'message': success_msg,
            'dry_run': dry_run
        }
    else:
        successful_count = total_processed - len(failed_deletions)
        failure_msg = f"{successful_count}/{total_processed} users {'would be ' if dry_run else 'were '}deleted successfully. {len(failed_deletions)} failed{mode_text}."
        logging.warning(failure_msg)
        status_summary = {
            'status': 'partial_failure',
            'message': failure_msg,
            'dry_run': dry_run
        }

    # Analyze failed deletions
    error_analysis = analyze_failed_deletions(failed_deletions)

    # Create comprehensive report
    deletion_report = {
        'summary': {
            'timestamp': report_date,
            'dry_run_mode': dry_run,
            'total_processed': total_processed,
            'total_failures': len(failed_deletions),
            'status': status_summary
        },
        'error_analysis': error_analysis,
        'failed_deletions': failed_deletions,
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


def analyze_failed_deletions(failed_deletions: List) -> Dict:
    """Analyze failure patterns and provide insights"""
    
    if not failed_deletions:
        return {'message': 'No failures to analyze'}
    
    analysis = {
        'total_failures': len(failed_deletions),
        'failure_types': {},
        'common_issues': []
    }
    
    # Count failure types
    for failure in failed_deletions:
        error_type = failure.get('error_type', 'unknown')
        analysis['failure_types'][error_type] = analysis['failure_types'].get(error_type, 0) + 1
    
    # Identify common issues
    if 'lookup_failed' in analysis['failure_types']:
        analysis['common_issues'].append(f"User lookup failures: {analysis['failure_types']['lookup_failed']} users not found in SCIM")
    
    if 'simulated_failure' in analysis['failure_types']:
        analysis['common_issues'].append(f"Simulated failures (dry-run): {analysis['failure_types']['simulated_failure']} users")
    
    if 'api_failure' in analysis['failure_types']:
        analysis['common_issues'].append(f"API failures: {analysis['failure_types']['api_failure']} users could not be deleted")
    
    return analysis


def generate_recommendations(error_analysis: Dict) -> List[str]:
    """Generate recommendations based on error analysis"""
    
    recommendations = []
    
    if not error_analysis or error_analysis.get('message') == 'No failures to analyze':
        return ["No specific recommendations - all deletions completed successfully"]
    
    failure_types = error_analysis.get('failure_types', {})
    
    # Lookup failure recommendations
    if 'lookup_failed' in failure_types:
        recommendations.append("Some users were not found in SCIM - verify email addresses in CSV")
        recommendations.append("Check if users have been already deleted or never existed")
    
    # API failure recommendations
    if 'api_failure' in failure_types:
        recommendations.append("API failures detected - check user permissions and SCIM settings")
        recommendations.append("Review deletion_report.json for specific error messages")
    
    # Unrecoverable error recommendations
    if 'unrecoverable_error' in failure_types:
        recommendations.append("Unrecoverable errors occurred - check network connectivity and API status")
        recommendations.append("Consider contacting Stack Overflow support if errors persist")
    
    if not recommendations:
        recommendations.append("Review detailed error logs for specific issues")
    
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