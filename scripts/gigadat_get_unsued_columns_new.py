import requests
import yaml
import urllib3
import csv
import logging
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Setup logging
log_filename = f"datamodel_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(filename=log_filename, level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

# Import token and data models from YAML file
with open("config.yaml", 'r') as stream:
    parsed_yaml_file = yaml.load(stream, Loader=yaml.BaseLoader)
    token = parsed_yaml_file['source_token']
    url = parsed_yaml_file['domain']
    datamodels = parsed_yaml_file['datamodels']
    head = {'Authorization': f'Bearer {token}', 'Content-type': 'application/json'}

def get_unused_columns(datamodel_name):
    """
    Function to identify unused columns in a DataModel by comparing the columns 
    in the DataModel against the columns used in its associated Dashboards.

    Parameters:
        datamodel_name (str): The name of the DataModel to analyze for unused columns.

    Returns:
        list: A list of dictionaries containing unused column details with a "used" field set to True or False.
    """
    
    source_url = f'https://{url}'
    all_columns = []

    # Get the DataModel ID
    schema_url = f"{source_url}/api/v2/datamodels/schema"
    response = requests.get(schema_url, headers=head, verify=False)
    output = response.json()
    datamodel_id = next(x["oid"] for x in output if x["title"] == datamodel_name)
    logging.info(f"DataModel ID - {datamodel_id}")

    # Get the DataSet IDs
    dataset_url = f"{source_url}/api/v2/datamodels/{datamodel_id}/schema/datasets"
    response = requests.get(dataset_url, headers=head, verify=False)
    dataset_ids = [x["oid"] for x in response.json()]
    logging.info(f"DataSet IDs - {dataset_ids}")

    # Loop through datasets and tables
    for dataset_id in dataset_ids:
        table_url = f"{dataset_url}/{dataset_id}/tables"
        response = requests.get(table_url, headers=head, verify=False)
        for table in response.json():
            table_name = table["name"]
            for column in table["columns"]:
                all_columns.append({
                    "datamodel_name": datamodel_name,
                    "table": table_name,
                    "column": column["name"]
                })

    logging.info(f"All columns from DataModel: {[entry['column'] for entry in all_columns]}")

    ## Unused Columns in Dashboard
    dashboards_id = []
    dashboard_url = f"{source_url}/api/v1/dashboards?datasourceTitle={datamodel_name}"
    response = requests.get(dashboard_url, headers=head, verify=False)
    dashboards = response.json()
    for dash in dashboards:
        dashboards_id.append(dash["oid"])

    logging.info(f"Dashboards - {dashboards_id}")

    ## Get columns from Dashboard
    dashboard_columns = []
    for id in dashboards_id:
        filter_count = 0
        columns_from_filters_count = 0
        columns_from_filters = []
        widget_count = 0
        columns_from_widgets_count = 0
        columns_from_widgets = []

        dashboard_url = f"{source_url}/api/v1/dashboards/export?dashboardIds={id}"
        response = requests.get(dashboard_url, headers=head, verify=False)
        dashboard = response.json()

        # Access the first dictionary in the list
        dash = dashboard[0]
        logging.info(f"Processing Dashboard: {dash['title']}")

        # Get Columns from Filters
        if dash.get('filters'):
            for filter in dash["filters"]:
                filter_count += 1
                if 'levels' in filter:
                    for level in filter["levels"]:
                        columns_from_filters_count += 1
                        columns_from_filters.append(level["column"])
                        dashboard_columns.append({
                            "datamodel_name": datamodel_name,
                            "dashboard_name": dash["title"],
                            "source": "dependent_filter",
                            "widget_id": "N/A",
                            "table": level["table"],
                            "column": level["column"]
                        })

                elif 'jaql' in filter:
                    columns_from_filters_count += 1
                    columns_from_filters.append(filter["jaql"]["column"])
                    dashboard_columns.append({
                        "datamodel_name": datamodel_name,
                        "dashboard_name": dash["title"],
                        "source": "filter",
                        "widget_id": "N/A",
                        "table": filter["jaql"]["table"],
                        "column": filter["jaql"]["column"]
                    })
        else:
            logging.info(f"No Filters in Dashboard - {dash['title']}")

        # Get Columns from Widgets
        for widget in dash["widgets"]:
            widget_count += 1
            for panel in widget["metadata"]["panels"]:
                for item in panel["items"]:
                    # Check if 'context' exists in the item
                    if 'context' in item["jaql"]:
                        context = item["jaql"]["context"]
                        for key, value in context.items():
                            table_name = value.get("table", "Unknown Table")
                            column_name = value.get("column", "Unknown Column")
                            columns_from_widgets_count += 1
                            columns_from_widgets.append(column_name)
                            dashboard_columns.append({
                                "datamodel_name": datamodel_name,
                                "dashboard_name": dash["title"],
                                "source": "widget",
                                "widget_id": widget["oid"],
                                "table": table_name,
                                "column": column_name
                            })
                    else:
                        table_name = item["jaql"].get("table", "Unknown Table")
                        column_name = item["jaql"].get("column", "Unknown Column")
                        columns_from_widgets_count += 1
                        columns_from_widgets.append(column_name)
                        dashboard_columns.append({
                            "datamodel_name": datamodel_name,
                            "dashboard_name": dash["title"],
                            "source": "widget",
                            "widget_id": widget["oid"],
                            "table": table_name,
                            "column": column_name
                        })

        logging.info(f"For Dashboard - {dash['title']} we have {filter_count} filters with {columns_from_filters_count} columns and {widget_count} widgets with {columns_from_widgets_count} columns.")

    logging.info(f"All Columns from all dashboards for {datamodel_name} - {set([entry['column'] for entry in dashboard_columns])}")

    # Step 6: Identify used and unused columns
    dashboard_columns_set = set((entry['table'], entry['column']) for entry in dashboard_columns)

    # Add 'used' field as True if used in a dashboard, otherwise False
    for entry in all_columns:
        table_column_pair = (entry['table'], entry['column'])
        entry['used'] = True if table_column_pair in dashboard_columns_set else False

    return all_columns


def export_to_csv(data, filename="unused_columns_report.csv"):
    """
    Function to export data to a CSV file.

    Parameters:
        data (list): List of dictionaries containing column details.
        filename (str): Name of the CSV file to export the data.
    """
    with open(filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["datamodel_name", "table", "column", "used"])
        writer.writeheader()
        writer.writerows(data)
    logging.info(f"Data exported to {filename}")


if __name__ == "__main__":
    for datamodel_name in datamodels:
        logging.info(f"Processing DataModel: {datamodel_name}")
        unused_columns = get_unused_columns(datamodel_name)
        export_to_csv(unused_columns, filename=f"unused_columns_report_{datamodel_name}.csv")

        for column in unused_columns:
            logging.info(f"Table: {column['table']}, Column: {column['column']}, Used: {column['used']}")
