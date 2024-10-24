import requests
import yaml
import urllib3
import logging
import pandas as pd
from pandas import json_normalize
from collections import defaultdict

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class APIClient:
    def __init__(self, config_file="config.yaml", debug=False):
        """
        Initializes the APIClient with configuration, logging, and authorization headers.
        
        Parameters:
            config_file (str): Path to the YAML configuration file.
            debug (bool): Flag to enable debug-level logging.
        """
        # Load configuration from the YAML file
        self.config = self._load_config(config_file)
        
        # Get the domain or IP address from the configuration
        self.domain = self.config['domain']
        
        # Determine if SSL is enabled based on the configuration, default is True (HTTPS)
        self.is_ssl = self.config.get('is_ssl', True)
        
        # Dynamically construct the base URL based on whether SSL is enabled
        if self.is_ssl:
            self.base_url = f"https://{self.domain}"  # Use default HTTPS (port 443)
        else:
            self.base_url = f"http://{self.domain}:30845"  # Use non-SSL and specify port 30845
        
        # Extract the API token for authorization
        self.token = self.config['token']
        
        # Set up HTTP headers, including the Authorization Bearer token
        self.headers = {'Authorization': f'Bearer {self.token}', 'Content-Type': 'application/json'}
        
        # Set log level to DEBUG if debug is True, otherwise INFO
        log_level = logging.DEBUG if debug else logging.INFO
        
        # Initialize the logger
        self.logger = self._get_logger("APIClient", "sisensepy.log", log_level)

    def _load_config(self, config_file):
        """
        Loads the configuration file in YAML format.
        
        Parameters:
            config_file (str): Path to the YAML configuration file.
        
        Returns:
            dict: Parsed YAML configuration as a dictionary.
        """
        # Open and parse the YAML file
        with open(config_file, 'r') as stream:
            return yaml.load(stream, Loader=yaml.FullLoader)

    def _get_logger(self, name, log_filename, log_level):
        """
        Sets up and configures a logger for the APIClient.
        
        Parameters:
            name (str): Name of the logger.
            log_filename (str): File path where logs will be saved.
            log_level (int): Logging level (DEBUG, INFO, etc.)
        
        Returns:
            logging.Logger: Configured logger instance.
        """
        # Get or create a logger with the specified name
        logger = logging.getLogger(name)
        
        # Check if the logger already has handlers to avoid duplicates
        if not logger.handlers:
            # Create a file handler for the logger
            handler = logging.FileHandler(log_filename)
            
            # Define the format for log messages
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            
            # Add the file handler to the logger
            logger.addHandler(handler)
        
        # Set the log level (DEBUG, INFO, etc.)
        logger.setLevel(log_level)
        
        return logger

    def get(self, endpoint, params=None):
        """
        Performs a GET request to the specified API endpoint.
        
        Parameters:
            endpoint (str): API endpoint (relative to the base URL).
            params (dict): Optional query parameters.
        
        Returns:
            dict: JSON response from the server or None if the request fails.
        """
        return self._make_request('GET', endpoint, params=params)

    def post(self, endpoint, data=None):
        """
        Performs a POST request to the specified API endpoint.
        
        Parameters:
            endpoint (str): API endpoint (relative to the base URL).
            data (dict): Optional JSON data payload for the POST request.
        
        Returns:
            dict: JSON response from the server or None if the request fails.
        """
        return self._make_request('POST', endpoint, data=data)
    
    def put(self, endpoint, data=None):
        """
        Performs a PUT request to the specified API endpoint.
        
        Parameters:
            endpoint (str): API endpoint (relative to the base URL).
            data (dict): Optional JSON data payload for the PUT request.
        
        Returns:
            dict: JSON response from the server or None if the request fails.
        """
        return self._make_request('PUT', endpoint, data=data)

    def patch(self, endpoint, data=None):
        """
        Performs a PATCH request to the specified API endpoint.
        
        Parameters:
            endpoint (str): API endpoint (relative to the base URL).
            data (dict): Optional JSON data payload for the PATCH request.
        
        Returns:
            dict: JSON response from the server or None if the request fails.
        """
        return self._make_request('PATCH', endpoint, data=data)

    def delete(self, endpoint):
        """
        Performs a DELETE request to the specified API endpoint.
        
        Parameters:
            endpoint (str): API endpoint (relative to the base URL).
        
        Returns:
            dict: JSON response from the server or None if the request fails.
        """
        return self._make_request('DELETE', endpoint)


    def _make_request(self, method, endpoint, params=None, data=None):
        """
        Makes an HTTP request to the API based on the specified method.
        
        Parameters:
            method (str): The HTTP method ('GET', 'POST', 'PUT', 'PATCH', 'DELETE').
            endpoint (str): The API endpoint (relative to the base URL).
            params (dict): Optional query parameters (for GET requests).
            data (dict): Optional JSON data payload (for POST, PUT, PATCH requests).
        
        Returns:
            Response: The full response object, or None if the request fails.
        """
        # Construct the full URL for the API request
        url = f"{self.base_url}{endpoint}"
        
        # Log the request details (method, URL, params, and data)
        self.logger.debug(f"Making {method} request to {url} with data: {data} and params: {params}")
        
        try:
            # Perform the appropriate HTTP request based on the method
            if method == 'GET':
                response = requests.get(url, headers=self.headers, params=params, verify=False)
            elif method == 'POST':
                response = requests.post(url, headers=self.headers, json=data, verify=False)
            elif method == 'PUT':
                response = requests.put(url, headers=self.headers, json=data, verify=False)
            elif method == 'PATCH':
                response = requests.patch(url, headers=self.headers, json=data, verify=False)
            elif method == 'DELETE':
                response = requests.delete(url, headers=self.headers, verify=False)
            else:
                # Raise an error for unsupported HTTP methods
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            # Handle known response codes
            if response.status_code == 200:
                self.logger.info(f"{method} request to {url} succeeded with status code 200")
            elif response.status_code == 201:
                self.logger.info(f"POST request to {url} succeeded with status code 201 (Created)")
            elif response.status_code in [400, 404, 500]:
                # Log the error response text if available
                try:
                    error_message = response.json()
                except ValueError:
                    error_message = response.text  # If the response is not JSON, use raw text
                self.logger.error(f"{method} request to {url} failed with status code {response.status_code}: {error_message}")
                print(f"Error: {response.status_code} - {error_message}")  # Print the error for end-users
            else:
                self.logger.warning(f"{method} request to {url} returned unexpected status code {response.status_code}")
            
            # Always return the full response object
            return response

        except requests.exceptions.RequestException as e:
            # Log and print the error for end-users
            error_message = f"{method} request to {url} failed: {e}"
            self.logger.error(error_message)
            print(f"Request failed: {error_message}")
            return None





        
    def to_dataframe(self, data):
        """
        Converts a list of dictionaries, a single dictionary, or a simple list to a pandas DataFrame.
        Automatically handles flat and nested data.
        
        Parameters:
            data: dict, list of dicts, or a simple list
        
        Returns:
            DataFrame: A pandas DataFrame with the data flattened as much as possible.
        """
        try:
            # Case 1: Data is a single dictionary
            if isinstance(data, dict):
                # Flatten the single dictionary using json_normalize to the deepest level
                df = json_normalize(data)
            
            # Case 2: Data is a list of dictionaries
            elif isinstance(data, list):
                if all(isinstance(item, dict) for item in data):
                    # Check if the dictionaries have nested dictionaries
                    if any(any(isinstance(value, dict) for value in item.values()) for item in data):
                        # Use json_normalize to flatten nested data in lists to the deepest level
                        df = json_normalize(data)
                    else:
                        # Use plain DataFrame if no nested dictionaries
                        df = pd.DataFrame(data)
                elif all(not isinstance(item, dict) for item in data):
                    # Case 3: Plain list (non-dict)
                    # Treat the list as a single column DataFrame
                    df = pd.DataFrame(data, columns=['Column_A'])
                else:
                    raise ValueError("Data contains mixed types. Expected either a list of dictionaries or a simple list.")
            
            # Raise an error if data is neither a dictionary nor a list
            else:
                raise ValueError("Data must be a dictionary, list of dictionaries, or a plain list.")
            
            return df
        
        except ValueError as e:
            error_message = f"Data conversion failed: {e}"
            self.logger.error(error_message)
            print(error_message)
            return None


    def export_to_csv(self, data, file_name="export.csv"):
        """
        Converts data to a DataFrame (handling nested structures) and exports it to a CSV file.
        
        Parameters:
            data: dict, list of dicts, or a simple list
            file_name: str, name of the file to export the CSV to
        """
        try:
            # First, convert the data to DataFrame
            df = self.to_dataframe(data)

            # Check if DataFrame conversion was successful
            if df is not None:
                # Export the DataFrame to CSV
                df.to_csv(file_name, index=False)
                print(f"Data successfully exported to {file_name}")
            else:
                print("Failed to export data due to invalid input format.")
            
        except ValueError as e:
            error_message = f"Data export to CSV failed: {e}"
            self.logger.error(error_message)
            print(error_message)

