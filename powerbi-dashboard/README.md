# Async User-Centric PowerBI Data Collector

A comprehensive Python tool for collecting user-centric data from Stack Overflow Enterprise and Teams instances, designed for PowerBI analytics and reporting.

## Features

- **Universal Compatibility**: Works with both Stack Overflow Enterprise and Teams instances
- **User-Centric Data Model**: Organizes all data around users with their questions, answers, and articles
- **Async Processing**: High-performance concurrent data collection with intelligent rate limiting
- **Time Filtering**: Flexible date-based filtering for targeted data collection
- **Comprehensive Data**: Collects users, questions, answers, articles, SME data, and accepted answers
- **Smart Rate Limiting**: Implements both burst throttling and token bucket algorithms
- **Persistent Retry Logic**: Never gives up on rate-limited requests (429 errors)
- **Scheduled Collection**: Built-in cron job functionality for automated data collection

## Comprehensive User Metrics Collected

The collector captures detailed analytics for each user across multiple dimensions:

### User Profile Information
- **User_ID**: Unique user identifier
- **DisplayName**: User's display name
- **Account_ID**: Account identifier
- **Title**: Job title/position
- **Department**: User's department
- **Location**: Geographic location
- **User_Type**: User role (registered, moderator, etc.)

### Account & Engagement Metrics
- **Reputation**: Current reputation score
- **Account_Longevity_Days**: Days since account creation
- **Creation_Date**: Account creation timestamp (UTC)
- **Joined_UTC**: Account join date in UTC format
- **Last_Login_Date**: Most recent login timestamp

### Question Activity Analytics
- **Total_Questions_Asked**: Number of questions posted
- **Total_Questions_Score**: Cumulative score across all questions
- **Total_Questions_No_Answers**: Count of unanswered questions
- **Questions_With_Accepted_Answers**: Questions that received accepted answers

### Answer Activity Analytics
- **Total_Answers_Given**: Number of answers provided
- **Accepted_Answers_Given**: Count of answers marked as accepted
- **Total_Answer_Score**: Cumulative score across all answers

### Article/Content Analytics
- **Total_Articles_Written**: Number of articles authored
- **Total_Article_Views**: Total views across all articles
- **Total_Article_Score**: Cumulative score across all articles

### Subject Matter Expertise
- **Is_SME**: Boolean indicating SME status
- **SME_Tags**: List of tags where user is recognized as SME

### Detailed Content Arrays
- **Questions**: Complete question data with metadata
- **Articles**: Full article information and statistics
- **Answers**: Comprehensive answer data and engagement metrics

### System Metadata
- **Last_Updated**: Data collection completion timestamp
- **Data_Collection_Timestamp**: UTC timestamp of collection run

## Data Structure

The collector generates user-centric JSON data with the following comprehensive structure:

```json
{
  // User Basic Information
  "User_ID": 123,
  "DisplayName": "John Doe",
  "Account_ID": 456,
  "Title": "Senior Developer",
  "Department": "Engineering",
  "Location": "New York, NY",
  "User_Type": "registered",
  
  // User Metrics & Reputation
  "Reputation": 1250,
  "Account_Longevity_Days": 365,
  "Creation_Date": "2023-01-15T09:30:15.123",
  "Joined_UTC": "2023-01-15T09:30:15.123",
  "Last_Login_Date": "2024-12-03T14:22:33.456",
  
  // Question Activity Metrics
  "Total_Questions_Asked": 15,
  "Total_Questions_Score": 87,
  "Total_Questions_No_Answers": 3,
  "Questions_With_Accepted_Answers": 8,
  
  // Answer Activity Metrics
  "Total_Answers_Given": 32,
  "Accepted_Answers_Given": 12,
  "Total_Answer_Score": 156,
  
  // Article Activity Metrics
  "Total_Articles_Written": 5,
  "Total_Article_Views": 1234,
  "Total_Article_Score": 89,
  
  // Subject Matter Expert Information
  "Is_SME": true,
  "SME_Tags": ["python", "javascript", "react"],
  
  // Detailed Questions Data
  "Questions": [
    {
      "question_id": 789,
      "title": "How to implement async in Python?",
      "tags": ["python", "async"],
      "creation_date": 1672531200,
      "score": 5,
      "view_count": 123,
      "answer_count": 3,
      "is_answered": true,
      "has_accepted_answer": true,
      "accepted_answer": {
        "answer_id": 890,
        "creation_date": 1672534800,
        "score": 8,
        "owner": {
          "id": 456,
          "display_name": "Jane Smith",
          "reputation": 2500,
          "account_id": 789,
          "role": "moderator"
        }
      }
    }
  ],
  
  // Detailed Articles Data
  "Articles": [
    {
      "article_id": 101,
      "type": "knowledge-article",
      "title": "Best Practices for Code Review",
      "tags": ["code-review", "best-practices"],
      "creation_date": 1672617600,
      "last_activity_date": 1672704000,
      "score": 12,
      "view_count": 234,
      "web_url": "https://...",
      "share_url": "https://...",
      "is_deleted": false,
      "is_obsolete": false,
      "is_closed": false,
      "owner": { /* owner details */ },
      "last_editor": { /* editor details */ }
    }
  ],
  
  // Detailed Answers Data
  "Answers": [
    {
      "answer_id": 555,
      "question_id": 333,
      "score": 8,
      "is_accepted": true,
      "is_deleted": false,
      "is_bookmarked": false,
      "is_followed": false,
      "creation_date": 1672704000,
      "locked_date": null,
      "last_edit_date": 1672790400,
      "last_activity_date": 1672790400,
      "deletion_date": null,
      "comment_count": 2,
      "web_url": "https://...",
      "share_link": "https://...",
      "user_can_follow": true,
      "can_be_followed": true,
      "is_subject_matter_expert": false,
      "owner": { /* owner details */ },
      "last_editor": { /* editor details */ },
      "last_activity_user": { /* user details */ }
    }
  ],
  
  // Metadata
  "Last_Updated": "2024-12-03T15:30:45.123456",
  "Data_Collection_Timestamp": "2024-12-03T15:30:45.123"
}
```

## Installation

### Prerequisites

- Python 3.7+
- Required Python packages:

```bash
pip install aiohttp asyncio argparse schedule
```

### Setup

1. Clone or download the script
2. Install dependencies
3. Obtain API credentials from your Stack Overflow instance

## Usage

### Basic Usage

```bash
python powerbi_collector.py --base-url https://your-instance.stackenterprise.co --token YOUR_API_TOKEN
```

### Teams Instance

```bash
python powerbi_collector.py --base-url https://stackoverflowteams.com --token YOUR_API_TOKEN --team-slug your-team-name
```

### Time Filtering Options

#### Collect users from the last week
```bash
python powerbi_collector.py --base-url https://your-instance.com --token TOKEN --filter week
```

#### Collect users from the last month
```bash
python powerbi_collector.py --base-url https://your-instance.com --token TOKEN --filter month
```

#### Custom date range
```bash
python powerbi_collector.py --base-url https://your-instance.com --token TOKEN --filter custom --from-date 2024-01-01 --to-date 2024-03-31
```

#### All users (no filter)
```bash
python powerbi_collector.py --base-url https://your-instance.com --token TOKEN --filter none
```

### Scheduled Collection

#### Run as cron job (default: daily at 2 AM)
```bash
python powerbi_collector.py --base-url https://your-instance.com --token TOKEN
```

#### Custom schedule
```bash
python powerbi_collector.py --base-url https://your-instance.com --token TOKEN --cron-schedule "0 6 * * *"
```

#### Run once and exit
```bash
python powerbi_collector.py --base-url https://your-instance.com --token TOKEN --run-once
```

## Command Line Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--base-url` | Yes | Stack Overflow Enterprise or Teams Base URL |
| `--token` | Yes | API access token |
| `--team-slug` | Conditional | Team slug (required for Teams instances) |
| `--filter` | No | Time filter: `week`, `month`, `quarter`, `year`, `custom`, `none` (default: `none`) |
| `--from-date` | Conditional | Start date for custom filter (YYYY-MM-DD) |
| `--to-date` | Conditional | End date for custom filter (YYYY-MM-DD) |
| `--output-file` | No | Output JSON filename (auto-generated if not specified) |
| `--verbose` | No | Enable verbose logging |
| `--run-once` | No | Run once and exit (no cron job) |
| `--cron-schedule` | No | Cron schedule (default: "0 2 * * *") |

## Rate Limiting

The collector implements sophisticated rate limiting to respect API limits:

### Burst Throttle
- **Limit**: 45 requests per 2 seconds (conservative under 50 limit)
- **Implementation**: Async semaphore

### Token Bucket
- **Capacity**: 5000 tokens
- **Refill Rate**: 100 tokens per 60 seconds
- **Behavior**: Waits for token refill when bucket is empty

### Retry Logic
- **Rate Limits (429)**: Never gives up, uses exponential backoff
- **Other Errors**: Retries up to 3 times with 1-second delays
- **Timeouts**: 30-second request timeout with retries

## Output Files

Output files are automatically named based on collection scope:

- **With date filter**: `powerbi_users_with_articles_2024-01-01_to_2024-03-31.json`
- **Without filter**: `powerbi_users_with_articles_all_20241203_143022.json`
- **Custom name**: Use `--output-file` parameter

## Logging

- **Console**: Real-time progress and status updates
- **File**: Detailed logs saved to `powerbi_collector.log`
- **Verbose Mode**: Use `--verbose` for detailed debugging information

## Data Collection Process

1. **User Discovery**: Fetches users based on time filter (if specified)
2. **Questions Collection**: Retrieves all questions for each user
3. **Articles Collection**: Retrieves all articles for each user
4. **Answers Collection**: Fetches all answers for the collected questions
5. **User Details**: Gets detailed user information from API v2.3
6. **SME Data**: Collects Subject Matter Expert information for all tags
7. **Data Processing**: Combines all data into user-centric format
8. **Output**: Saves structured JSON file

## API Compatibility

### Stack Overflow Enterprise
- API v3: Primary data collection
- API v2.3: User details and creation dates

### Stack Overflow Teams
- Requires team slug parameter
- Same API structure with team-specific endpoints

## Performance Characteristics

- **Concurrent Processing**: Up to 10 concurrent requests (configurable)
- **Memory Efficient**: Streams data without loading everything into memory
- **Progress Tracking**: Real-time progress indicators (when not in verbose mode)
- **Graceful Shutdown**: Handles SIGINT/SIGTERM for clean exits

## Error Handling

- **Network Issues**: Automatic retries with exponential backoff
- **Rate Limiting**: Persistent retry with respect for API limits
- **Data Validation**: Handles missing or malformed API responses
- **Graceful Degradation**: Continues processing even if some data fails

## Monitoring and Observability

The collector provides comprehensive metrics:

```
✅ User-centric PowerBI data export with articles complete!
   Data saved to: powerbi_users_with_articles_2024-01-01_to_2024-03-31.json
   Total users processed: 150
   Total questions collected: 1,245
   Total articles collected: 87
   Total time: 0:05:23
   Total API v3 calls: 2,456
   Total API v2.3 calls: 234
   Average time per user: 2.154s
```

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify API token is valid
   - Check token permissions for your instance

2. **Rate Limiting**
   - Collector handles this automatically
   - Monitor logs for retry patterns

3. **Team Slug Issues**
   - Ensure correct team slug for Teams instances
   - Check URL format and team access

4. **Date Filter Problems**
   - Use YYYY-MM-DD format
   - Ensure from_date < to_date

### Debug Mode

Use `--verbose` flag for detailed debugging:

```bash
python powerbi_collector.py --base-url https://your-instance.com --token TOKEN --verbose
```

## License

This script is provided as-is for Stack Overflow Enterprise and Teams data collection purposes.

## Support

For issues related to:
- API access: Contact your Stack Overflow administrator
- Script functionality: Check logs and use verbose mode for debugging
- Rate limiting: Monitor the built-in retry logic and timing