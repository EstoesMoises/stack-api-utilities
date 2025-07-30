# Stack Overflow Teams Bulk User Deletion Tool

## ⚠️ CRITICAL WARNINGS ⚠️

### **USER DELETION IS IRREVERSIBLE**
- **Once users are deleted, they CANNOT be recovered**
- **All user data, posts, comments, and activity will be permanently lost**
- **Test with the `--dry-run` flag first**

### **This is a Destructive Operation**
This script performs **PERMANENT USER DELETION** from Stack Overflow for Teams. Deleted users and their associated data cannot be restored. Use with extreme caution.

## Overview

This Python script provides a comprehensive solution for bulk user deletion from Stack Overflow for Teams using the SCIM API. It includes robust error handling, retry logic, detailed logging, and safety features to help administrators manage user lifecycle operations.

## Features

- **Bulk deletion of deactivated users**
- **CSV-based user deletion**
- **Dry-run mode for safe testing**
- **Comprehensive error handling and retry logic**
- **Detailed logging and reporting**
- **User confirmation prompts**
- **Progress tracking**
- **Error classification and analysis**

## Prerequisites

### Required Dependencies
```bash
pip install requests  # For the so4t_scim module
```

### Required Files
- `so4t_scim.py` - The SCIM client module (must be in the same directory)

### Required Permissions
- Valid SCIM token with user deletion permissions
- Administrative access to your Stack Overflow for Teams instance

## Installation

1. Clone or download the script files
2. Ensure `so4t_scim.py` is in the same directory
3. Install required dependencies
4. Set up your SCIM token and base URL

## Usage

### Basic Syntax
```bash
python scim_user_deletion.py --token <SCIM_TOKEN> --url <BASE_URL> [OPTIONS]
```

### Required Arguments
- `--token`: Your SCIM API token
- `--url`: Base URL for your Stack Overflow for Teams site

### Operation Modes

#### 1. Delete Deactivated Users
```bash
python scim_user_deletion.py --token YOUR_TOKEN --url https://your-site.stackenterprise.co --deactivated
```

#### 2. Delete Users from CSV File
```bash
python scim_user_deletion.py --token YOUR_TOKEN --url https://your-site.stackenterprise.co --csv users_to_delete.csv
```

#### 3. Dry Run Mode (RECOMMENDED FIRST STEP)
```bash
# Test deactivated user deletion
python scim_user_deletion.py --token YOUR_TOKEN --url https://your-site.stackenterprise.co --deactivated --dry-run

# Test CSV-based deletion
python scim_user_deletion.py --token YOUR_TOKEN --url https://your-site.stackenterprise.co --csv users_to_delete.csv --dry-run
```

## Safety Features

### 1. Dry Run Mode (`--dry-run`)
- **Always use this first** to preview what would be deleted
- Simulates the entire deletion process without making changes
- Shows which users would be affected
- Generates a report with simulated outcomes
- No confirmation required in dry-run mode

### 2. User Confirmation
- Interactive confirmation required before actual deletions
- Shows preview of users to be deleted
- Double confirmation for large operations (>50 users)
- Type "DELETE" to confirm operations with many users

### 3. Comprehensive Logging
- Detailed logs written to timestamped files
- Console output for real-time monitoring
- Error classification and tracking
- Operation summaries and reports

## CSV File Format

Create a CSV file with email addresses (one per line):
```csv
user1@example.com
user2@example.com
user3@example.com
```

**Important Notes:**
- Only the first column is read
- Empty lines are automatically skipped
- Email addresses are case-insensitive
- Users not found in SCIM will be reported as failures

## Error Handling

The script includes sophisticated error handling:

### Error Types
- **Server Errors (5xx)**: Retried with exponential backoff
- **Client Errors (4xx)**: Limited retries
- **Authentication Errors**: No retries (requires immediate attention)
- **Rate Limiting (429)**: Automatic retry with delays
- **Network Errors**: Retried with backoff

### Retry Logic
- Automatic retries based on error type
- Exponential backoff for server and network errors
- Configurable retry limits
- Detailed retry attempt logging

## Output Files

### Generated Files
1. **`all_users.json`** - Complete user data from SCIM API
2. **`bulk_deletion_TIMESTAMP.log`** - Detailed operation logs
3. **`deletion_report_TIMESTAMP.json`** - Comprehensive operation report
4. **`dry_run_deletion_report_TIMESTAMP.json`** - Dry-run simulation report

### Report Contents
- Operation summary and statistics
- Error analysis and patterns
- Failed deletion details
- Recommendations for issues
- Individual user processing results

## Security Considerations

### Token Security
- **Never commit SCIM tokens to version control**
- Use environment variables or secure configuration files
- Rotate tokens regularly
- Limit token permissions to minimum required

### Access Control
- Run script from secure, controlled environments
- Limit access to administrators only
- Audit script execution
- Monitor for unauthorized usage

### Data Protection
- Test on non-production environments first
- Document all deletion operations
- Maintain audit trails

## Troubleshooting

### Common Issues

#### Authentication Failures
```
CRITICAL - auth_error: 401 Unauthorized
```
**Solution**: Verify SCIM token validity and permissions

#### Rate Limiting
```
RATE LIMITED - rate_limit_error: 429 Too Many Requests
```
**Solution**: Script automatically handles this, but consider slower processing for large operations

#### Network Issues
```
WARNING - network_error: Connection timeout
```
**Solution**: Check network connectivity and consider running from a stable connection

#### Server Errors
```
WARNING - server_error: 500 Internal Server Error
```
**Solution**: Wait and retry; check Stack Overflow status page

### Debug Mode
For verbose debugging, check the detailed log files generated in the same directory.

## Best Practices

### Before Running
1. **Always run with `--dry-run` first**
3. **Test on a small subset of users**
4. **Verify SCIM token permissions**
5. **Review user list carefully**

### During Operation
1. **Monitor console output for errors**
2. **Don't interrupt the process once started**
3. **Have support contacts ready**
4. **Document the operation**

### After Operation
1. **Review the generated reports**
2. **Verify expected users were deleted**
3. **Check for any failed deletions**
4. **Update documentation**
5. **Clean up temporary files if desired**

## Script Architecture

### Key Components
- **ErrorHandler**: Classifies and manages different error types
- **ScimClient**: Handles API communication (from `so4t_scim` module)
- **Retry Logic**: Implements intelligent retry strategies
- **Logging System**: Provides comprehensive operation tracking
- **Safety Checks**: User confirmation and dry-run capabilities

### Error Classification System
The script automatically classifies errors and applies appropriate retry strategies:
- Authentication errors: No retry (requires manual intervention)
- Rate limiting: Aggressive retry with exponential backoff
- Server errors: Limited retry with delays
- Network errors: Retry with moderate delays
- Client errors: Minimal retry attempts

## Version History

This script is provided as a proof of concept example. For production use:
- Test thoroughly in non-production environments
- Consider additional safety measures
- Implement organization-specific approval workflows
- Add additional logging and monitoring as needed

---

**Remember: User deletion is permanent and irreversible. Always exercise extreme caution when using this tool.**