import requests
import yaml
import urllib3
import pandas as pd
import logging
from datetime import datetime

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Setup logging
def setup_logging():
    log_filename = f"widget_type_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(filename=log_filename, level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Adding console output for logging
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)

# Load configuration from YAML file
def load_config():
    try:
        with open("config.yaml", 'r') as stream:
            parsed_yaml_file = yaml.load(stream, Loader=yaml.SafeLoader)
            token = parsed_yaml_file['source_token']
            url = parsed_yaml_file['domain']
            head = {'Authorization': f'Bearer {token}', 'Content-type': 'application/json'}
        return token, url, head
    except FileNotFoundError:
        logging.error("config.yaml file not found.")
        raise
    except KeyError as e:
        logging.error(f"Missing key in YAML file: {e}")
        raise

# Retrieve dashboards from the source environment
def get_dashboards(url, headers):
    limit = 50
    skip = 0
    dashboards = []

    while True:
        payload = {
            "queryParams": {
                "ownershipType": "allRoot",
                "search": "",
                "ownerInfo": True,
                "asObject": True
            },
            "queryOptions": {
                "sort": {
                    "title": 1
                },
                "limit": limit,
                "skip": skip
            }
        }
        
        try:
            response = requests.post(f'https://{url}/api/v1/dashboards/searches', json=payload, verify=False, headers=headers)
            
            # Check if the response is successful
            if response.status_code != 200:
                logging.error(f"Failed to retrieve dashboards. Status code: {response.status_code}")
                break

            # Parse the response as JSON
            dashboard_response = response.json()

            # If there are no items, break the loop
            if not dashboard_response or len(dashboard_response.get("items", [])) == 0:
                break
            
            # Append retrieved dashboards to the list
            dashboards.extend(dashboard_response["items"])
            skip += limit

        except requests.exceptions.RequestException as e:
            logging.error(f"An error occurred during dashboard retrieval: {e}")
            break

    if not dashboards:
        logging.error("Failed to retrieve dashboards from the source environment. Please check the logs for more details.")
    else:
        logging.info(f"Successfully retrieved {len(dashboards)} dashboards.")

    return dashboards

# Process dashboards and retrieve widget information
def process_dashboards(url, headers, dashboards):
    output = []
    for dashboard in dashboards:
        # Get the dashboard ID, name, and widget count
        dashboard_id = dashboard.get("oid")
        dashboard_name = dashboard.get("title")
        total_widget_count = dashboard.get("widgetCount", 0)
        if total_widget_count == 0:
            print(dashboard)

        logging.info(f"Processing dashboard: {dashboard_name} (Total Widgets: {total_widget_count})")

        # Get the dashboard metadata
        try:
            response = requests.get(f'https://{url}/api/dashboards/{dashboard_id}?adminAccess=true', verify=False, headers=headers)
            dashboard_data = response.json()

            # If the response does not contain widgets, log and continue
            if dashboard_data is None or "widgets" not in dashboard_data:
                logging.warning(f"Failed to retrieve data for dashboard OID: {dashboard_id} (Title: {dashboard_name})")
                continue
            
            # Process each widget in the dashboard
            processed_widget_count = 0
            for widget in dashboard_data["widgets"]:
                processed_widget_count += 1
                data = {
                    "dashboard_name": dashboard_name,
                    "widget_type": widget["type"],
                }
                output.append(data)

            # Log comparison of widget counts
            logging.info(f"Processed {processed_widget_count} widgets for dashboard: {dashboard_name}")
            if processed_widget_count != total_widget_count:
                logging.warning(f"Widget count mismatch for {dashboard_name}: Expected {total_widget_count}, Processed {processed_widget_count}")

        except requests.exceptions.RequestException as e:
            logging.error(f"An error occurred while processing dashboard {dashboard_name}: {e}")
            continue

    return output

# Save widget data to CSV
def save_to_csv(output):
    df = pd.DataFrame(output)
    df.to_csv("widget_type.csv", index=False)
    logging.info("Widget type data has been exported to widget_type.csv")
    return df

# Main function to orchestrate the workflow
def main():
    setup_logging()
    logging.info("Starting dashboard processing script.")

    # Load configuration
    token, url, headers = load_config()

    # Retrieve dashboards
    dashboards = get_dashboards(url, headers)

    # Process dashboards
    if dashboards:
        output = process_dashboards(url, headers, dashboards)

        # Save to CSV
        df = save_to_csv(output)
        logging.info(f"Data processing completed. Here's a sample:\n{df.head()}")
    else:
        logging.error("No dashboards retrieved. Exiting.")

# Execute the script
if __name__ == "__main__":
    main()
