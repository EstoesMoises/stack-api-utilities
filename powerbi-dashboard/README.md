# PowerBI Data Collector for Stack Overflow Enterprise

A Python script that collects user activity data from Stack Overflow Enterprise API v3 and exports it to JSON format for PowerBI dashboard consumption. The script supports both one-time execution and automated cron job scheduling.

## Features

- 🔄 **Automated Data Collection**: Scheduled cron jobs for regular data updates
- 🚀 **High Performance**: Multi-threaded processing with intelligent caching
- 📊 **PowerBI Ready**: Structured JSON output optimized for PowerBI dashboards
- 🛡️ **Robust Error Handling**: Comprehensive logging and graceful failure recovery
- ⚡ **API Optimization**: Minimizes API calls through smart caching strategies
- 🎯 **Comprehensive Metrics**: Collects 20+ data points per user

## Data Collected

The script collects comprehensive user activity data including:

### User Information
- User ID and Display Name
- Job Title and Department
- User Reputation and Account ID
- User Type and Location
- Creation Date and Last Login

### Activity Metrics
- Total Questions (with breakdown of unanswered)
- Total Answers (with acceptance rate)
- Median Answer Time (in hours)
- Articles and Comments count
- Total Upvotes across all content

### Content References
- Associated Tags
- Question IDs and Titles
- Answer IDs
- Content timestamps

## Installation

### Prerequisites
- Python 3.7 or higher
- Stack Overflow Enterprise API access token
- Network access to your Stack Overflow Enterprise instance

### Setup
```bash
# Clone or download the script
git clone <repository-url>
cd powerbi-data-collector

# Install dependencies
pip install -r requirements.txt

# Make script executable (Linux/Mac)
chmod +x powerbi_collector.py
```

## Configuration

### Environment Variables (Optional)
```bash
export SOE_BASE_URL="https://your-site.stackoverflow.com"
export SOE_API_TOKEN="your-api-token"
export POWERBI_OUTPUT_FILE="powerbi_data.json"
```

### Command Line Arguments
| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--base-url` | Yes | - | Stack Overflow Enterprise base URL |
| `--token` | Yes | - | API access token |
| `--output-file` | No | `powerbi_data.json` | Output JSON filename |
| `--verbose` | No | False | Enable detailed logging |
| `--threads` | No | 5 | Number of concurrent threads |
| `--run-once` | No | False | Run once and exit (no cron) |
| `--cron-schedule` | No | `0 2 * * *` | Cron schedule format |

## Usage

### One-Time Execution
```bash
# Basic usage
python powerbi_collector.py \
  --base-url https://your-site.stackoverflow.com \
  --token YOUR_API_TOKEN \
  --run-once

# With custom output file and verbose logging
python powerbi_collector.py \
  --base-url https://your-site.stackoverflow.com \
  --token YOUR_API_TOKEN \
  --output-file monthly_data.json \
  --verbose \
  --run-once
```

### Scheduled Execution (Cron Job)
```bash
# Daily at 2 AM (default)
python powerbi_collector.py \
  --base-url https://your-site.stackoverflow.com \
  --token YOUR_API_TOKEN

# Custom schedule - Daily at 6 AM
python powerbi_collector.py \
  --base-url https://your-site.stackoverflow.com \
  --token YOUR_API_TOKEN \
  --cron-schedule "0 6 * * *"

# High-performance mode with more threads
python powerbi_collector.py \
  --base-url https://your-site.stackoverflow.com \
  --token YOUR_API_TOKEN \
  --threads 10 \
  --verbose
```

### Running as a Service (Linux)
Create a systemd service file:

```bash
sudo nano /etc/systemd/system/powerbi-collector.service
```

```ini
[Unit]
Description=PowerBI Data Collector for Stack Overflow Enterprise
After=network.target

[Service]
Type=simple
User=your-username
WorkingDirectory=/path/to/powerbi-collector
ExecStart=/usr/bin/python3 /path/to/powerbi-collector/powerbi_collector.py \
  --base-url https://your-site.stackoverflow.com \
  --token YOUR_API_TOKEN \
  --output-file /data/powerbi_data.json
Restart=always
RestartSec=60

[Install]
WantedBy=multi-user.target
```

Enable and start the service:
```bash
sudo systemctl enable powerbi-collector.service
sudo systemctl start powerbi-collector.service
sudo systemctl status powerbi-collector.service
```

## Output Format

The script generates a JSON file with the following structure:

```json
[
  {
    "user_id": 123,
    "display_name": "John Doe",
    "user_job_title": "Senior Developer",
    "user_job_department": "Engineering",
    "user_reputation": 2500,
    "total_questions": 45,
    "total_questions_no_answer": 3,
    "answers": 120,
    "answers_accepted": 85,
    "median_answer_time_hours": 4.5,
    "articles": 12,
    "comments": 200,
    "total_upvotes": 1200,
    "location": "New York, NY",
    "account_id": 456,
    "creation_date": 1609459200,
    "user_type": "registered",
    "joined_utc": 1609459200,
    "last_login_date": 1704067200,
    "tags": ["python", "javascript", "api"],
    "question_ids": [1001, 1002, 1003],
    "answer_ids": [2001, 2002, 2003],
    "question_titles": ["How to optimize API calls?", "Best practices for error handling"],
    "last_updated": "2024-01-01T12:00:00",
    "data_collection_timestamp": 1704110400.0
  }
]
```

## PowerBI Integration

### Connecting to PowerBI
1. **Get Data** → **JSON** → Select your output file
2. **Transform Data** to expand nested arrays (tags, question_ids, etc.)
3. **Create relationships** between user data and activity metrics
4. **Set up automatic refresh** to reload updated JSON files

### Recommended Visualizations
- **User Activity Dashboard**: Questions vs Answers ratio
- **Department Performance**: Activity by department/job title
- **Response Time Analysis**: Median answer time trends
- **Tag Cloud**: Most active technology areas
- **Reputation vs Activity**: Correlation analysis
- **Geographic Distribution**: User activity by location

### Sample DAX Measures
```dax
# Average Response Time
Average Response Time = AVERAGE('UserData'[median_answer_time_hours])

# Question Answer Ratio
Answer Ratio = DIVIDE(SUM('UserData'[answers]), SUM('UserData'[total_questions]), 0)

# Activity Score
Activity Score = 
  ('UserData'[total_questions] * 2) + 
  ('UserData'[answers] * 3) + 
  ('UserData'[articles] * 5) + 
  ('UserData'[comments] * 1)
```

## Monitoring and Logging

### Log Files
- **powerbi_collector.log**: Detailed execution logs
- **Console Output**: Real-time progress and summary statistics

### Log Levels
- **INFO**: General execution progress
- **DEBUG**: Detailed API calls and caching info (with `--verbose`)
- **ERROR**: Failed operations and exceptions

### Performance Metrics
The script tracks and reports:
- Total execution time
- API call counts (v2.3 and v3)
- Cache hit rates
- Users processed per minute
- Data collection timestamps

## Troubleshooting

### Common Issues

**1. Authentication Errors**
```
Error: HTTP 401 - Unauthorized
```
- Verify your API token is valid and has necessary permissions
- Check if token has expired
- Ensure you're using the correct base URL

**2. Rate Limiting**
```
Error: HTTP 429 - Too Many Requests
```
- Reduce the number of threads with `--threads 3`
- Implement delays between API calls
- Contact your Stack Overflow Enterprise admin about rate limits

**3. Memory Issues**
```
MemoryError: Unable to allocate array
```
- Reduce thread count
- Process data in smaller batches
- Consider running on a machine with more RAM

**4. Network Timeouts**
```
requests.exceptions.ConnectTimeout
```
- Check network connectivity to your Stack Overflow Enterprise instance
- Increase timeout values in the script
- Verify firewall settings

### Performance Optimization

**For Large Instances (1000+ users):**
- Use `--threads 3` to reduce server load
- Run during off-peak hours
- Consider incremental data collection
- Monitor API quota usage

**For Better PowerBI Performance:**
- Split large datasets by date ranges
- Use compressed JSON format
- Implement data archiving strategy
- Consider database storage for very large datasets

## API Usage and Limits

### API Endpoints Used
- **API v3**: Primary data collection
  - `/users` - User listings
  - `/users/{id}/questions` - User questions
  - `/users/{id}/answers` - User answers
  - `/users/{id}/comments` - User comments
  - `/users/{id}/articles` - User articles

- **API v2.3**: Detailed user information
  - `/users/{id}` - Reputation, creation dates, etc.

### Estimated API Calls
For N users:
- **Minimum**: N + (N × 5) calls
- **Maximum**: N + (N × 20) calls (depending on user activity)
- **Caching reduces subsequent runs by 60-80%**

## Contributing

### Development Setup
```bash
# Create virtual environment
python -m venv powerbi-env
source powerbi-env/bin/activate  # Linux/Mac
# or
powerbi-env\Scripts\activate  # Windows

# Install development dependencies
pip install -r requirements.txt
pip install pytest black flake8
```

### Running Tests
```bash
# Unit tests
pytest tests/

# Linting
flake8 powerbi_collector.py
black powerbi_collector.py
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Review log files for detailed error information
3. Create an issue with:
   - Complete error message
   - Command used
   - Log file excerpt
   - Environment details (Python version, OS, etc.)

## Changelog

### v1.0.0
- Initial release
- API v3 support with bearer token authentication
- Multi-threaded data collection
- Comprehensive user activity metrics
- Cron job scheduling
- PowerBI-optimized JSON output
- Intelligent caching system