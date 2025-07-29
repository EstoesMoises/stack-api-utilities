'''
This Python script is a labor of love and has no formal support from Stack Overflow. 
If you run into difficulties, open an issue here: 
https://github.com/jklick-so/so4t_scim_user_deletion/issues
'''

# Standard Python libraries
import logging
import time
import re
from enum import Enum
from typing import Optional, Callable, Any

# Open source libraries
import requests


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
    
    def classify_error(self, error: Exception, status_code: Optional[int] = None) -> ErrorType:
        """Classify an error based on its message, type, and HTTP status code"""
        error_str = str(error).lower()
        error_type_name = type(error).__name__.lower()
        
        # Check HTTP status code first if available
        if status_code:
            if status_code == 429:
                return ErrorType.RATE_LIMIT_ERROR
            elif status_code in [401, 403]:
                return ErrorType.AUTHENTICATION_ERROR
            elif 400 <= status_code < 500:
                return ErrorType.CLIENT_ERROR
            elif 500 <= status_code < 600:
                return ErrorType.SERVER_ERROR
        
        # Check error patterns
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


class ScimClient:
    VALID_ROLES = ["Registered", "Moderator", "Admin"]

    def __init__(self, token, url, proxy=None, error_handler=None):
        self.session = requests.Session()
        self.base_url = url
        self.token = token
        self.error_handler = error_handler or ErrorHandler()
        
        self.headers = {
            'Authorization': f"Bearer {self.token}",
            'User-Agent': 'so4t_scim_user_deletion/1.0 (http://your-app-url.com; your-contact@email.com)'
        }
        self.proxies = {'https': proxy} if proxy else {'https': None}
        
        if "stackoverflowteams.com" in self.base_url: # For Basic and Business tiers
            self.soe = False
            self.scim_url = f"{self.base_url}/auth/scim/v2/users"
        else: # For Enterprise tier
            self.soe = True
            self.scim_url = f"{self.base_url}/api/scim/v2/users"

        self.ssl_verify = self.test_connection()

    def _retry_request(self, request_func: Callable, context: str) -> Any:
        """Generic retry wrapper for all SCIM operations"""
        attempt = 0
        
        while True:
            try:
                return request_func()
            except requests.exceptions.RequestException as e:
                # Get status code if available
                status_code = getattr(e.response, 'status_code', None) if hasattr(e, 'response') and e.response else None
                
                error_type = self.error_handler.classify_error(e, status_code)
                self.error_handler.log_error(e, error_type, context, attempt)
                
                if self.error_handler.should_retry(error_type, attempt):
                    delay = self.error_handler.get_delay(error_type, attempt)
                    logging.info(f"Retrying {context} in {delay} seconds...")
                    time.sleep(delay)
                    attempt += 1
                else:
                    raise  # Re-raise the exception after exhausting retries
            except Exception as e:
                # Handle non-request exceptions
                error_type = self.error_handler.classify_error(e)
                self.error_handler.log_error(e, error_type, context, attempt)
                
                if self.error_handler.should_retry(error_type, attempt):
                    delay = self.error_handler.get_delay(error_type, attempt)
                    logging.info(f"Retrying {context} in {delay} seconds...")
                    time.sleep(delay)
                    attempt += 1
                else:
                    raise

    def test_connection(self):
        ssl_verify = True

        logging.info("Testing SCIM connection...")
        
        def _test_connection_request():
            return self.session.get(
                self.scim_url, 
                headers=self.headers, 
                proxies=self.proxies,
                verify=ssl_verify
            )
        
        try:
            response = _test_connection_request()
        except requests.exceptions.SSLError:
            logging.warning(f"Received SSL error when connecting to {self.base_url}.")
            logging.warning("If you're sure the URL is correct (and trusted), you can proceed without SSL "
                          "verification.")
            proceed = input("Proceed without SSL verification? (y/n) ")
            if proceed.lower() == "y":
                requests.packages.urllib3.disable_warnings(
                    requests.packages.urllib3.exceptions.InsecureRequestWarning)
                ssl_verify = False
                response = self.session.get(self.scim_url, headers=self.headers, 
                                        verify=ssl_verify, proxies=self.proxies)
            else:
                logging.info("Exiting...")
                raise SystemExit

        if response.status_code == 200:
            logging.info(f"SCIM connection was successful.")
            return ssl_verify
        else:
            logging.error(f"SCIM connection failed. Please check your token and URL.")
            logging.error(f"Status code: {response.status_code}")
            logging.error(f"Response from server: {response.text}")
            logging.error("Exiting...")
            raise SystemExit

    def get_user(self, account_id):
        """Get a single user with retry logic"""
        def _get_user_request():
            scim_user_url = f"{self.scim_url}/{account_id}"
            response = self.session.get(scim_user_url, headers=self.headers)

            if response.status_code == 404:
                logging.info(f"User with account ID {account_id} not found.")
                return None
            elif response.status_code != 200:
                # Raise an exception to trigger retry logic
                response.raise_for_status()
            else:
                logging.info(f"Retrieved user with account ID {account_id}")
                return response.json()
        
        try:
            return self._retry_request(
                _get_user_request,
                context=f"Getting user {account_id}"
            )
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to get user {account_id} after retries: {e}")
            return None

    def get_all_users(self):
        """Fetch all users with page-level retry"""
        items = []
        start_index = 1
        count = 100
        
        while True:
            def _get_users_page():
                params = {
                    "count": count,
                    "startIndex": start_index,
                }
                
                logging.info(f"Getting {count} users from {self.scim_url} with startIndex of {start_index}")
                response = self.session.get(
                    self.scim_url, 
                    headers=self.headers, 
                    params=params,
                    proxies=self.proxies, 
                    verify=self.ssl_verify
                )
                
                if response.status_code != 200:
                    response.raise_for_status()
                
                return response.json()
            
            try:
                response_data = self._retry_request(
                    _get_users_page,
                    context=f"Fetching users page starting at index {start_index}"
                )
                
                items_data = response_data.get('Resources', [])
                items += items_data
                
                total_results = response_data.get('totalResults', 0)
                logging.info(f"Retrieved {len(items_data)} users from this page. Total collected so far: {len(items)}")

                start_index += count
                if start_index > total_results:
                    logging.info(f"Reached end of results. Total users collected: {len(items)}")
                    break
                    
            except requests.exceptions.RequestException as e:
                logging.warning(f"Failed to fetch page starting at index {start_index}: {e}")
                logging.warning("Skipping this page and continuing to next page...")
                start_index += count
                continue

        return items

    def update_user(self, account_id, active=None, role=None):
        """Update a user's active status or role with retry logic"""
        def _update_user_request():
            scim_url = f"{self.scim_url}/{account_id}"
            
            # Build the PATCH payload according to SCIM 2.0 specification
            operations = []
            
            if active is not None:
                operations.append({
                    "op": "replace",
                    "path": "active",
                    "value": active
                })
                
            if role is not None:
                if role in self.VALID_ROLES:
                    operations.append({
                        "op": "replace", 
                        "path": "userType",
                        "value": role
                    })
                else:
                    raise ValueError(f"Invalid role: {role}. Valid roles are: {self.VALID_ROLES}")

            payload = {
                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                "Operations": operations
            }

            # Add User-Agent to headers
            headers = self.headers.copy()
            headers['Content-Type'] = 'application/scim+json'
            
            response = self.session.patch(
                scim_url, 
                headers=headers, 
                json=payload, 
                proxies=self.proxies,
                verify=self.ssl_verify
            )

            if response.status_code == 404:
                logging.warning(f"User with account ID {account_id} not found.")
                return None
            elif response.status_code != 200:
                response.raise_for_status()
            
            return response.json()
        
        try:
            result = self._retry_request(
                _update_user_request,
                context=f"Updating user {account_id}"
            )
            
            if result and role is not None:
                # Verify role update
                response_json = result
                try:
                    user_role = response_json['userType']
                except KeyError: # If user is not a moderator/admin, the 'userType' key will not exist
                    user_role = "Registered"

                if user_role != role:
                    logging.warning(f"Failed to update user with account ID {account_id} to role: {role}")
                    logging.warning("Please check that SCIM settings in the Stack Overflow admin "
                                  "panel to make sure the ability to change user pemissions is enabled "
                                  "(i.e check the boxes).")
                else:
                    logging.info(f"Updated user with account ID {account_id} to role: {role}")
            
            return result
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to update user {account_id} after retries: {e}")
            return None

    def delete_user(self, account_id):
        """Delete a user with comprehensive retry and error handling"""
        deletion_result = {
            'account_id': account_id,
            'account_url': f'{self.base_url}/accounts/{account_id}',
            'status': 'success',
            'message': 'User deleted successfully.'
        }

        def _delete_user_request():
            scim_user_url = f"{self.scim_url}/{account_id}"
            logging.info(f"Sending DELETE request to {scim_user_url}")
            
            response = self.session.delete(
                scim_user_url, 
                headers=self.headers, 
                proxies=self.proxies,
                verify=self.ssl_verify
            )
            
            # Handle specific error cases that shouldn't be retried
            if response.status_code == 400:
                deletion_result['status'] = 'error'
                deletion_result['message'] = response.json().get('ErrorMessage', 'Bad request')
                return deletion_result

            elif response.status_code == 404:
                logging.error(f"Delete request for user with account ID {account_id} returned 404.")
                logging.error("This could mean that user deletion for SCIM is not enabled for your site "
                            "or that the user does not exist.")
                logging.error("To enable user deletion for SCIM, open a support ticket with Stack Overflow.")
                deletion_result['status'] = 'error'
                deletion_result['message'] = "User not found or deletion not enabled for SCIM."
                return deletion_result

            elif response.status_code == 500:
                error_message = response.json().get('ErrorMessage', 'Internal server error')

                if "Adjust role to User" in error_message:
                    logging.warning(f"User with account ID {account_id} cannot be deleted because they're "
                                    "a moderator or admin.")
                    logging.warning("Attempting to reduce their role to Registered...")
                    
                    # Try to update user role first
                    update_result = self.update_user(account_id, role="Registered")
                    if update_result:
                        logging.warning("Role updated, retrying delete...")
                        # Retry the delete by raising an exception to trigger retry logic
                        response.raise_for_status()
                    else:
                        deletion_result['status'] = 'error'
                        deletion_result['message'] = "Failed to update user role before deletion."
                        return deletion_result
                
                elif "FK_CommunityMemberships_CreationUser" in error_message:
                    deletion_result['status'] = 'error'
                    deletion_result['message'] = "User cannot be deleted because they are the creator of a community."
                    return deletion_result
                else:
                    # Other 500 errors should be retried
                    response.raise_for_status()

            elif response.status_code != 204: # any unexpected status code
                response.raise_for_status()

            else:
                logging.info(f"Deleted user with account ID {account_id}")
                return deletion_result
        
        try:
            return self._retry_request(
                _delete_user_request,
                context=f"Deleting user {account_id}"
            )
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to delete user {account_id} after retries: {e}")
            deletion_result['status'] = 'error'
            deletion_result['message'] = f"Failed to delete user after retries: {str(e)}"
            return deletion_result


# Set up logging
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)