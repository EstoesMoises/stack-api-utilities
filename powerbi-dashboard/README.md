# PowerBI Data Collector for Stack Overflow Enterprise

NOT PRODUCTION READY. THIS IS FOR DEMONSTRATION PURPOSES. SOFTWARE IS PROVIDED "AS IS"

An optimized async Python script that collects question-centric data from Stack Overflow Enterprise for PowerBI reporting and analytics.

## Features

- **Question-Centric Data Collection**: Focuses on questions and enriches them with user metrics and accepted answer data
- **Async Processing**: High-performance concurrent API requests with intelligent rate limiting
- **Time Filtering**: Flexible date range filtering (last week, month, quarter, year, or custom ranges)
- **Comprehensive User Metrics**: Detailed user information including SME status, reputation, and activity metrics
- **Accepted Answer Tracking**: Links questions with their accepted answers and answerer details
- **Scheduled Execution**: Built-in cron job support for automated data collection
- **Rate Limiting**: Respects API limits with automatic retry and backoff logic
- **Robust Error Handling**: Graceful handling of API errors and timeouts

## Requirements

- Python 3.7+
- Stack Overflow Enterprise instance with API access
- Valid API access token

### Dependencies

```bash
pip install aiohttp asyncio schedule
```

## Installation

1. Clone or download the script
2. Install dependencies:
   ```bash
   pip install aiohttp schedule
   ```
3. Ensure you have a valid Stack Overflow Enterprise API token

## Usage

### Basic Usage

```bash
python powerbi_collector.py --base-url https://your-enterprise.stackoverflowteams.com --token YOUR_API_TOKEN
```

### Command Line Options

| Option | Description | Required |
|--------|-------------|----------|
| `--base-url` | Stack Overflow Enterprise Base URL | Yes |
| `--token` | API access token | Yes |
| `--output-file` | Output JSON filename (auto-generated if not specified) | No |
| `--verbose`, `-v` | Enable verbose output | No |
| `--run-once` | Run once and exit (no cron job) | No |
| `--cron-schedule` | Cron schedule (default: daily at 2 AM) | No |
| `--filter` | Time filter: `week`, `month`, `quarter`, `year`, `custom`, `none` | No |
| `--from-date` | Start date for custom filter (YYYY-MM-DD) | No* |
| `--to-date` | End date for custom filter (YYYY-MM-DD) | No* |

*Required when using `--filter=custom`

### Examples

#### Collect All Data (Run Once)
```bash
python powerbi_collector.py \
  --base-url https://your-enterprise.stackoverflowteams.com \
  --token YOUR_API_TOKEN \
  --run-once
```

#### Collect Last Month's Data
```bash
python powerbi_collector.py \
  --base-url https://your-enterprise.stackoverflowteams.com \
  --token YOUR_API_TOKEN \
  --filter month \
  --run-once
```

#### Custom Date Range
```bash
python powerbi_collector.py \
  --base-url https://your-enterprise.stackoverflowteams.com \
  --token YOUR_API_TOKEN \
  --filter custom \
  --from-date 2024-01-01 \
  --to-date 2024-03-31 \
  --run-once
```

#### Scheduled Collection (Daily at 3 AM)
```bash
python powerbi_collector.py \
  --base-url https://your-enterprise.stackoverflowteams.com \
  --token YOUR_API_TOKEN \
  --cron-schedule "0 3 * * *" \
  --filter week
```

#### Verbose Output
```bash
python powerbi_collector.py \
  --base-url https://your-enterprise.stackoverflowteams.com \
  --token YOUR_API_TOKEN \
  --verbose \
  --run-once
```

## Output Format

The script generates a JSON file containing an array of question objects. Each question includes:

### Question Data
- Question ID, title, tags
- Creation date, score, view count, answer count
- Whether the question is answered

### Question Owner Data
- Display name, title, department
- Reputation, account longevity
- Question/answer statistics
- SME status and tags
- Account creation and last login dates

### Accepted Answer Data (if available)
- Answer ID, creation date, score
- Answer owner details (name, reputation, SME status, etc.)

### Example Output Structure
```json
[
  {
    "Question_ID": 12345,
    "QuestionTitle": "How to implement async processing?",
    "QuestionTags": ["python", "async", "processing"],
    "owner": {
      "DisplayName": "John Doe",
      "Title": "Senior Developer",
      "Department": "Engineering",
      "Reputation": 1500,
      "Account_Longevity_Days": 365,
      "Is_SME": true,
      "Tags": ["python", "javascript"]
    },
    "accepted_answer": {
      "answer_id": 67890,
      "creation_date": "2024-01-15T10:30:00.000",
      "score": 5,
      "owner": {
        "display_name": "Jane Smith",
        "reputation": 2500,
        "is_sme": true
      }
    },
    "Question_Creation_Date": "2024-01-10T14:20:00.000",
    "Question_Score": 3,
    "Question_View_Count": 150,
    "Question_Is_Answered": true
  }
]
```

## Performance Features

### Rate Limiting
- Respects Stack Overflow Enterprise API limits (45 requests per 2 seconds)
- Automatic retry with exponential backoff on rate limit errors
- Concurrent request processing with semaphore-based throttling

### Efficient Data Collection
- Async processing for improved performance
- Batch processing for user data retrieval
- Intelligent caching to avoid duplicate API calls
- Minimal data collection (only users from filtered questions)

### Memory Optimization
- Streaming JSON output for large datasets
- Efficient data structures to minimize memory usage
- Garbage collection-friendly processing patterns

## Time Filtering

The script supports flexible time filtering to collect data for specific periods:

- **`week`**: Last 7 days
- **`month`**: Last 30 days  
- **`quarter`**: Last 90 days (default)
- **`year`**: Last 365 days
- **`custom`**: Specify exact date range with `--from-date` and `--to-date`
- **`none`**: Collect all available data 

Date filtering applies to questions, and the script automatically includes associated users and accepted answers.

## Logging

The script creates two types of logs:

1. **Console Output**: Real-time progress and status updates
2. **Log File**: Detailed logging saved to `powerbi_collector.log`

Use `--verbose` for detailed debug output.

## Error Handling

The script includes robust error handling for:
- API rate limiting (429 errors)
- Network timeouts and connectivity issues
- Invalid API responses
- Missing or malformed data
- Graceful shutdown on interrupt signals

## Scheduling

### Built-in Cron Support
The script includes built-in scheduling using cron syntax:
```bash
# Daily at 2 AM (default)
--cron-schedule "0 2 * * *"

# Every 6 hours
--cron-schedule "0 */6 * * *"

# Weekdays at 9 AM
--cron-schedule "0 9 * * 1-5"
```

### System Cron Alternative
You can also use system cron for scheduling:
```bash
# Add to crontab
0 2 * * * /usr/bin/python3 /path/to/powerbi_collector.py --base-url https://your-enterprise.stackoverflowteams.com --token YOUR_TOKEN --run-once --filter week
```

## API Usage

The script uses both Stack Overflow Enterprise API v3 and v2.3:
- **API v3**: Primary data collection (questions, users, SME data)
- **API v2.3**: Additional user details and bulk operations

### API Call Optimization
- Minimizes API calls through intelligent batching
- Focuses data collection on relevant users only
- Uses pagination efficiently
- Implements connection pooling for performance

## Troubleshooting

### Common Issues

**Rate Limiting Errors**
- The script automatically handles rate limiting
- Reduce `RATE_LIMIT_REQUESTS` if issues persist
- Check your API token permissions

**Timeout Errors**
- Increase timeout values in the code if needed
- Check network connectivity to your Enterprise instance
- Verify the base URL is correct

**Authentication Errors**
- Verify your API token is valid and has proper permissions
- Ensure the token has read access to questions, users, and answers

**Memory Issues**
- Use time filtering to reduce dataset size
- Consider running on a machine with more RAM for large datasets

### Debug Mode
Run with `--verbose` to see detailed API calls and processing steps:
```bash
python powerbi_collector.py --verbose --base-url YOUR_URL --token YOUR_TOKEN --run-once
```

## Security

- API tokens are passed via command line (consider using environment variables)
- No sensitive data is logged
- HTTPS is enforced for all API communications
- Consider using a service account token for automated collection

## Contributing

To modify or extend the script:

1. The main collection logic is in `collect_powerbi_data()`
2. Question processing is handled by `process_question_data()`
3. User metrics are built in `build_user_metrics_from_question_users()`
4. Rate limiting is managed by the `RATE_LIMITER` semaphore

