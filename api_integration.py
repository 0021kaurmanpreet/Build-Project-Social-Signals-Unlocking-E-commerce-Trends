import requests
import pandas as pd
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    filename='opendota_api_log.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Base API URL for OpenDota
BASE_URL = "https://api.opendota.com/api"

# Function to fetch data from OpenDota API
def get_data(endpoint):
    """
    Fetch data from the OpenDota API for a given endpoint.

    Args:
        endpoint (str): The API endpoint to fetch data from.

    Returns:
        dict: The JSON response data if successful, else None.
    """
    url = f"{BASE_URL}/{endpoint}"
    try:
        # Send GET request to OpenDota API
        response = requests.get(url)

        # Check if the response status is OK (200)
        if response.status_code == 200:
            logging.info(f"Successfully fetched data from {endpoint}")
            return response.json()
        else:
            logging.error(f"Failed to fetch data from {endpoint}. Status code: {response.status_code}")
            return None

    except requests.exceptions.RequestException as e:
        logging.error(f"Error occurred while fetching {endpoint}: {e}")
        return None

def save_to_csv(data, filename):
    """
    Save the fetched data to a CSV file in the specified directory.

    Args:
        data (list): The list of data to save.
        filename (str): The name of the file to save the data to.
    """
    if data:
        try:
            # Specify the full path to save the CSV in the desired directory
            folder_path = r'C:\Users\kaur6\Downloads\BuildProject-ECommerce\Datasets\\'
            file_path = folder_path + filename  # Combine the folder path and filename

            # Convert the data to a pandas DataFrame
            df = pd.DataFrame(data)

            # Save the DataFrame as a CSV file
            df.to_csv(file_path, index=False, encoding='utf-8')
            logging.info(f"Saved {len(df)} records to {file_path}")
        except Exception as e:
            logging.error(f"Error occurred while saving data to {filename}: {e}")
    else:
        logging.warning(f"No data to save for {filename}")

# Dictionary of endpoints and their corresponding CSV filenames
endpoints = {
    "proMatches": "proMatches.csv",
    "publicMatches": "publicMatches.csv",
    "heroStats": "heroStats.csv",
    "teams": "teams.csv",
    "leagues": "leagues.csv"
}

# Main function to fetch data from all endpoints and save it to CSV
def main():
    """
    Main function to fetch data from multiple OpenDota API endpoints
    and save the data to corresponding CSV files.
    """
    for endpoint, filename in endpoints.items():
        logging.info(f"Fetching data from {endpoint}...")

        # Fetch data from the API
        data = get_data(endpoint)

        # Save the fetched data to a CSV file
        save_to_csv(data, filename)

# Run the main function
if __name__ == "__main__":
    logging.info("Script execution started.")
    main()
    logging.info("Script execution completed.")

