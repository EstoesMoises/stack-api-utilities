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

## Data Structure

The collector generates user-centric JSON data with the following structure:

```json
{
  "User_ID": 123,
  "DisplayName": "John Doe",
  "Account_ID": 456,
  "Reputation": 1250,
  "Total_Questions_Asked": 15,
  "Total_Answers_Given": 32,
  "Total_Articles_Written": 5,
  "Is_SME": true,
  "SME_Tags": ["python", "javascript"],
  "Questions": [
    {
      "question_id": 789,
      "title": "How to implement async in Python?",
      "tags": ["python", "async"],
      "score": 5,
      "has_accepted_answer": true,
      "accepted_answer": { /* answer details */ }
    }
  ],
  "Articles": [
    {
      "article_id": 101,
      "title": "Best Practices for Code Review",
      "type": "knowledge-article",
      "score": 12,
      "view_count": 234
    }
  ],
  "Answers": [
    {
      "answer_id": 555,
      "question_id": 333,
      "score": 8,
      "is_accepted": true
    }
  ]
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