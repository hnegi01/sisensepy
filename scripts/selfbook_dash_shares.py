
import requests
import os
import yaml
import pandas as pd
from pandas import json_normalize
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# # Import token and domain from YAML file
# stream = open("/Users/himanshunegi_1/Desktop/Sisense/sisensep/config.yaml", 'r')
# parsed_yaml_file = yaml.load(stream, Loader=yaml.BaseLoader)
# token = parsed_yaml_file['source_token']
# url = parsed_yaml_file['domain']

# Import token and domain from YAML file
script_dir = os.path.dirname(__file__)
config_path = os.path.join(script_dir, '..', 'config.yaml')
with open(config_path, 'r') as stream:
    parsed_yaml_file = yaml.load(stream, Loader=yaml.BaseLoader)
    token = parsed_yaml_file['source_token']
    url = parsed_yaml_file['domain']


head = {'Authorization': f'Bearer {token}', 'Content-type': 'application/json'}
source_url = f'https://{url}'

def get_dashboard_shares():
    """Part-1: Get Dashboard Shares Information"""
    limit = 50
    skip = 0
    dashboards = []
    
    # Fetch all dashboards in batches
    while True: 
        dashboard_response = requests.post(f'{source_url}/api/v1/dashboards/searches', 
                json={
                    "queryParams": {
                        "ownershipType": "allRoot", 
                        "search": "", 
                        "ownerInfo": True, 
                        "asObject": True
                    },
                    "queryOptions": {
                        "sort": {"title": 1}, 
                        "limit": limit, 
                        "skip": skip
                    }
                },
                verify=False, 
                headers=head
            ).json()
        # print("Dashboard Response: ", dashboard_response)

        if not dashboard_response or len(dashboard_response.get("items", [])) == 0:
            break
        else:
            dashboards.extend(dashboard_response["items"])
            skip += limit

    # Fetch all users
    users_detail = []
    users = requests.get(f'{source_url}/api/v1/users', headers=head, verify=False).json()

    for user in users:
        users_detail.append({
            "id": user["_id"],
            "email": user["email"]
        })

    # Fetch all groups
    groups_detail = []
    groups = requests.get(f'{source_url}/api/v1/groups', headers=head, verify=False).json()

    for group in groups:
        groups_detail.append({
            "id": group["_id"],
            "name": group["name"]
        })

    # Parse the dashboards and extract shares information
    shared_list = []
    for dashboard in dashboards:
        shares = []
        if dashboard.get("shares"):
            for share in dashboard["shares"]:
                if share["type"] == "user":
                    user = next((user for user in users_detail if user["id"] == share["shareId"]), None)
                    if user:
                        shares.append({
                            "type": "user",
                            "name": user["email"]
                        })
                elif share["type"] == "group":
                    group = next((group for group in groups_detail if group["id"] == share["shareId"]), None)
                    if group:
                        shares.append({
                            "type": "group",
                            "name": group["name"]
                        })
            shared_list.append({
                "dashboard": dashboard["title"],
                "shares": shares
            })
        else:
            shared_list.append({
                "dashboard": dashboard["title"],
                "share_id": None,
                "type": None
            })

    # print("Shared Dashboard Information: ", shared_list)

    # Flatten the nested structure and create a DataFrame
    df = json_normalize(shared_list, 'shares', ['dashboard'])
    
    # Reorder columns for clarity
    df = df[['dashboard', 'type', 'name']]
    
    print("Flattened Dashboard Share Data:")
    print(df)

    # Save to CSV
    df.to_csv("dashboard_shares.csv", index=False)
    print("Dashboard shares saved to 'dashboard_shares.csv'.")


def share_dashboards_with_users_and_groups():
    """Part-2: Share the list of dashboards with the list of groups/users and append to existing shares"""

    # Dashboard, users, and groups to share with
    dashboard_to_share = ["Governance Users Info"]  # List of dashboards
    user_to_share = ["source@sisense.com"]  # List of users (can be empty)
    user_role = "edit"
    group_to_share = ["groupa", "groupb"]  # List of groups (can be empty)
    group_role = "edit"

    # Step 1: Fetch all dashboards to find matching dashboard IDs
    limit = 50
    skip = 0
    all_dashboards = []
    
    while True:
        dashboard_response = requests.post(
            f'{source_url}/api/v1/dashboards/searches',
            json={
                "queryParams": {
                    "ownershipType": "allRoot",
                    "search": "",
                    "ownerInfo": True,
                    "asObject": True
                },
                "queryOptions": {
                    "sort": {"title": 1},
                    "limit": limit,
                    "skip": skip
                }
            },
            verify=False,
            headers=head
        ).json()
        
        if not dashboard_response or len(dashboard_response.get("items", [])) == 0:
            break
        else:
            all_dashboards.extend(dashboard_response["items"])
            skip += limit

    # Step 2: Find matching dashboard IDs
    dashboard_ids_to_share = []
    matching_dashboards = []
    for dashboard_name in dashboard_to_share:
        matching_dashboard = next((dash for dash in all_dashboards if dash["title"] == dashboard_name), None)
        print(f"Matching dashboard: {matching_dashboard}")
        if matching_dashboard:
            dashboard_ids_to_share.append(matching_dashboard["oid"])
            matching_dashboards.append(matching_dashboard)
        else:
            print(f"Dashboard with title '{dashboard_name}' not found.")

    if not dashboard_ids_to_share:
        print("No valid dashboards found to share.")
        return

    # Step 3: Prepare payload for users and groups
    new_shares = []  # Initialize the new shares for users and groups

    # Check if we need to share with users
    if user_to_share:
        # Get user IDs
        users = requests.get(f'{source_url}/api/v1/users', headers=head, verify=False).json()
        users_ids = [user["_id"] for user in users if user["email"] in user_to_share]

        # Add users to the new shares list
        for user_id in users_ids:
            new_shares.append({
                "shareId": user_id,
                "type": "user",
                "rule": user_role,
                "subscribe": False
            })

    # Check if we need to share with groups
    if group_to_share:
        # Get group IDs
        groups = requests.get(f'{source_url}/api/v1/groups', headers=head, verify=False).json()
        groups_ids = [group["_id"] for group in groups if group["name"] in group_to_share]

        # Add groups to the new shares list
        for group_id in groups_ids:
            new_shares.append({
                "shareId": group_id,
                "type": "group",
                "rule": group_role,
                "subscribe": False
            })

    if not new_shares:
        print("No users or groups provided to share with.")
        return

    # Step 4: Append the new shares to the existing shares for each dashboard
    for dashboard in matching_dashboards:
        dashboard_id = dashboard["oid"]
        existing_shares = dashboard.get("shares", [])  # Get existing shares from the dashboard
        updated_shares = existing_shares + new_shares  # Append new shares to the existing ones

        print(f"Updating shares for dashboard '{dashboard['title']}' (ID: {dashboard_id})")
        print({"sharesTo": updated_shares})
        # Post the updated share request
        response = requests.post(
            f'{source_url}/api/shares/dashboard/{dashboard_id}?adminAccess=true',
            json={"sharesTo": updated_shares},
            headers=head,
            verify=False
        )

        if response.status_code == 200:
            print(f"Dashboard '{dashboard['title']}' successfully shared with users {user_to_share} and groups {group_to_share}.")
        else:
            print(f"Failed to share dashboard '{dashboard['title']}'. Status code: {response.status_code}, Response: {response.text}")


# Execute both parts
if __name__ == "__main__":
    #get_dashboard_shares()
    share_dashboards_with_users_and_groups()




