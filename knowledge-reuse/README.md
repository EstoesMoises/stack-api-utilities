# **Script Usage Instructions**

## **Prerequisites**

- Python 3.6 or higher.
- Run `pip install requests` as that's the only dependency needed for this script that's not included in Python by default.

## **Command Line Arguments**

```bash
python knowledge-reuse-export.py --base-url "https://[your-site].stackenterprise.co" --token "your-api-token" [options]
```

### Required Arguments:
- `--base-url`: URL of your Stack Overflow Enterprise instance
- `--token`: Access token for authentication

### Optional Arguments:
- `--filter`: you can choose between `month`, `quarter`, `year` or `custom` to get information about the question for that give time period to date (defaults to the last `quarter`).
- `--from-date`: If you chose `custom` as the filter, provide the start date in `YYYY-MM-DD` format.
- `--to-date`: If you chose `custom` as the filter, provide the end date in `YYYY-MM-DD` format.
- `--verbose` or `-v`: Enable detailed logging output for troubleshooting
- `--threads` or `-t`: Number of concurrent threads for API calls (default: 10) (using too many threads might result in errors due to throttling)

## **Example Usage**

```bash
python knowledge-reuse-export.py --base-url "https://[your-site].stackenterprise.co" --token "abc123xyz" -v -t 20 --filter "quarter"
```

# **Output Files**

The script generates a timestamped CSV file with the naming convention:
```
knowledge_reuse_export_{YYYY-MM-DD}_to_{YYYY-MM-DD}.csv
```


# **Exported CSV Fields**

## **Question Data**
| Field                     | Type     | Description |
|---------------------------|----------|-------------|
| `tags`                    | string   | Comma-separated string of tags associated with the question. |
| `owner.id`                | integer  | The user ID of the question owner. |
| `owner.user_type`         | string   | Type of user (e.g., registered, moderator, etc.). |
| `owner.display_name`      | string   | Display name of the question owner. |
| `is_answered`             | boolean  | Whether the question has an accepted answer (True/False). |
| `view_count`              | integer  | The number of views the question has received. |
| `up_vote_count`           | integer  | The number of upvotes the question received (score in v3 API). |
| `creation_date`           | datetime | The date and time when the question was created. |
| `question_id`             | integer  | Unique identifier of the question. |
| `share_link`              | string   | Shortened URL to the question on Stack Overflow. |
| `link`                    | string   | URL to the question on Stack Overflow. |
| `title`                   | string   | Title of the question. |
| `is_SME`                  | boolean  | Whether the answer owner is considered a Subject Matter Expert (SME) of any of the tags involved. |
| `status`                  | string   | Indicates if the question is closed/obsolete/N/A. |
| `department`              | string   | Department of the question owner (if applicable). |
| `job_title`               | string   | Job title of the question owner. |
| `user_tenure`             | integer  | Time since the user joined, calculated as `joined_date - last_seen_date`. |

## **Accepted Answer Data**
| Field                     | Type     | Description |
|---------------------------|----------|-------------|
| `answer.owner_id`         | integer  | User ID of the answer owner. |
| `answer.user_type`        | string   | Type of user who posted the answer. |
| `answer.display_name`     | string   | Display name of the answer owner. |
| `answer.up_vote_count`    | integer  | Number of upvotes the accepted answer received. |
| `answer.is_accepted`      | boolean  | Whether the answer is the accepted answer (always True). |
| `answer.creation_date`    | datetime | Date and time when the answer was created. |
| `answer.answer_id`        | integer  | Unique identifier of the answer. |
| `answer.question_id`      | integer  | ID of the related question. |
| `answer.is_SME`           | boolean  | Whether the answer owner is considered a Subject Matter Expert of any of the tags involved. |
| `answer.department`       | string   | Department of the answer owner (if applicable). |
| `answer.job_title`        | string   | Job title of the answer owner. |
| `answer.user_tenure`      | integer  | Time since the user joined, calculated as `joined_date - last_seen_date`. |

## **Notes**
- The `is_SME` field is determined if the user in question is an SME of any of tags that have been added to the question.
- `user_tenure` is calculated as the difference between the user's join date and last seen date at the moment of export. API v2.3 had to be leveraged for this as API v3 doesn't return the required data.
- The script ensures that only the accepted answer (if available) is included in the output.

## **Troubleshooting**

If you encounter issues:

1. Run with the `-v` flag to see detailed logs
2. Check your API token permissions
3. Verify your Stack Overflow Enterprise instance URL
4. Ensure network connectivity to your Stack Overflow Enterprise instance

## **API Endpoints Used**

The script uses multiple Stack Overflow Enterprise API endpoints:
- API v3: For most data retrieval (questions, answers, tags, users, SME status)
- API v2.3: Specifically for user tenure calculation, as creation dates aren't available in v3