import requests
import json
import yaml
import pandas as pd
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from sisensepy.access_management import AccessManagement
from sisensepy.api_client import APIClient

# Initialize the API client using the config file
api_client = APIClient(config_file="config.yaml")

# Initialize the AccessManagement class with the API client
access_mgmt = AccessManagement(api_client)

# Import token from YAML file
stream = open("config.yaml", 'r')
parsed_yaml_file = yaml.load(stream, Loader=yaml.BaseLoader)
token = parsed_yaml_file['source_token']
url = parsed_yaml_file['domain']
head = {'Authorization': f'Bearer {token}', 'Content-type': 'application/json'}


"""
1. If the folder doesnt have dashboards, owner cannot be changed unless current folder owner is runnging the tool as dahsbaord search api will never have this folder's id
2. First we check if the user running the tool have access to the folder. If the user have access to the folder, we will get all folder ids and dashboard ids
3. If the user doesnt have access to the folder =>
    a. We will get all folders id in the system using dashboard search api as sets
    b. We will get all folders id of the user running the tool using folder api as sets
    c. We will get the difference between the two sets
    d. Now we will share dashbaord whose folder parent id matches with the folder id in difference set - This will allows access to grand parent folder also if the parent folder id was a sub folder
4. Repeat step 2 to capture Folder Ids and Dashboard Ids
5. Change the owner of the folder and dashboards
"""

"""
Test:
1. User running the tool has access - Pass
2. User running the tool doesnt have access - Pass
3. New onwer is already the owner - Pass
4. New owner doesn't have access - Pass
5. New owner has access - Pass
6. User running the tool is the same as the current owner - 
7. User running the tool is the same as the new owner - Pass
8. Folder doesn't have dashboards - Pass
9. Folder is parent folder have access - Pass
10. Folder is parent folder doesn't have access - Pass
11. Folder is subfolder folder have access, so should give access to parent folder and every folder and dashboard in the subfolder and cousin folder and dashboard  - Pass
12. Folder is subfolder folder doesn't have access, so should give access to parent folder and every folder and dashboard in the subfolder and cousin folder and dashboard  - Pass
13. Folder is subfolder folder have partial access, so should give access to parent folder and every folder and dashboard in the subfolder and cousin folder and dashboard  - Pass
14. Need parameter by deafult change all dashbaord ownership when folder ownership is changed or change only folder owner no dashboards - Done
15. Need parameter to change old owner rule to view or edit with edit as default - Done
11. Need parameter to change folder owner and only dashboard owned by old owner - Not added
"""
def change_folder_and_dashboard_ownership(
    user_name, 
    folder_name, 
    new_owner_name, 
    original_owner_rule='edit',  # Default value is 'edit'
    change_dashboard_ownership=True  # Default value is True
):
    """
    Function to change the ownership of folders and optionally dashboards. 

    This function changes the ownership of a target folder and the entire tree structure 
    surrounding it, including:
    - The specified folder itself.
    - All subfolders under the target folder (children, grandchildren, etc.).
    - Sibling folders that share the same parent.
    - Parent folders up the tree (root folder, grandparents, etc.).

    Optionally, it will also change the ownership of dashboards associated with these folders.
    
    Arguments:
    - user_name (str): The user running the tool. This is necessary because we don't have admin-access for the folder API endpoints. To change ownership this user first need to have access to the folders.
    - folder_name (str): The target folder whose ownership needs to be changed.
    - new_owner_name (str): The new owner to whom the folder (and optionally dashboards) ownership will be transferred.
    - original_owner_rule (str, optional): Specifies the ownership rule ('edit' or 'view'). Default is 'edit'.
    - change_dashboard_ownership (bool, optional): Specifies whether to also change the ownership of dashboards in the folder tree. Default is True.
    """

    
    folder_details = set()  # Set to store folder IDs and names
    dashboard_details = set()  # Set to store dashboard IDs and names
    target_folder_found = False  # Variable to track if we found the target folder

    # Recursively traverse folders and subfolders, avoiding duplicates
    def traverse_folder(folder):
        if (folder['oid'], folder['name']) not in folder_details:
            folder_details.add((folder['oid'], folder['name']))
            print(f"Folder found: {folder['name']} (ID: {folder['oid']})")

            if 'dashboards' in folder and folder["dashboards"]:
                for dash in folder["dashboards"]:
                    if (dash['oid'], dash['title']) not in dashboard_details:
                        dashboard_details.add((dash['oid'], dash['title']))
                        print(f"Dashboard found: {dash['title']} (ID: {dash['oid']})")

            if "folders" in folder:
                for subfolder in folder["folders"]:
                    traverse_folder(subfolder)
        else:
            print(f'No subfolders in folder - {folder["name"]}')

    # Search for the target folder and traverse siblings, subfolders, and parent folders
    def search_for_target_and_traverse_tree(folders, target_folder_name):
        nonlocal target_folder_found
        parent_folder = None

        for folder in folders:
            if folder['name'] == target_folder_name:
                print(f"Found target folder: {target_folder_name}")
                traverse_folder(folder)
                target_folder_found = True
                return folder

            if 'folders' in folder:
                found_folder = search_for_target_and_traverse_tree(folder['folders'], target_folder_name)
                if found_folder:
                    parent_folder = folder

        return parent_folder

    # Traverse the parent's siblings and parent folder
    def traverse_parents_and_siblings(parent_folder):
        if parent_folder:
            print(f"Parent folder: {parent_folder['name']} (ID: {parent_folder['oid']})")
            traverse_folder(parent_folder)

            if "folders" in parent_folder:
                for sibling in parent_folder["folders"]:
                    traverse_folder(sibling)

    # Fetch folder details and traverse relevant tree structure
    def get_folder_details():
        response = requests.get(f'https://{url}/api/v1/navver', headers=head, verify=False).json()

        if 'folders' in response:
            print(f"Searching for folder '{folder_name}'...")
            parent_folder = search_for_target_and_traverse_tree(response['folders'], folder_name)

            if target_folder_found:
                print(f"Folder '{folder_name}' found and traversed successfully.")
                traverse_parents_and_siblings(parent_folder)
                return True
            else:
                print(f"Folder '{folder_name}' not found in the response.")
                return False
        else:
            print("No folders found in the API response.")
            return False

    print("Starting folder and dashboard traversal...")
    folder_found = get_folder_details()

    if folder_found:
        print("Collected Folder Details:")
        for folder_id, folder_name in folder_details:
            print(f"Folder: {folder_name} (ID: {folder_id})")

        print("Collected Dashboard Details:")
        for dash_id, dash_name in dashboard_details:
            print(f"Dashboard: {dash_name} (ID: {dash_id})")
    else:
        print("Folder not found, moving to search dashboards and grant access step...")
        # Code to handle sharing dashboards to gain access (re-implementation of your dashboard search logic)
        limit = 10
        skip = 0
        dashboards = []
        while True: 
            dashboard_url = url + '/api/v1/dashboards/searches'
            dash_payload = {
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
            dashboard_request = requests.post(f'http://{dashboard_url}', headers=head, data=json.dumps(dash_payload), verify=False)
            dashboard_response = dashboard_request.json()

            if len(dashboard_response["items"]) == 0:
                break
            else:
                dashboards.extend(dashboard_response["items"])
                skip += limit

        all_folder_ids = set()
        for dic in dashboards:
            if "parentFolder" in dic and dic["parentFolder"] is not None:
                all_folder_ids.add(dic["parentFolder"])

        print(all_folder_ids)

        folder_request = requests.get(f'https://{url}/api/v1/folders', headers=head, verify=False).json()

        user_folder_id = set()
        for folder in folder_request:
            if "oid" in folder and folder["oid"] is not None:
                user_folder_id.add(folder["oid"])

        print(user_folder_id)

        # Difference between the two sets
        diff = all_folder_ids - user_folder_id
        print(diff)

        for dash in dashboards:
            if 'parentFolder' in dash and dash["parentFolder"] is not None and dash["parentFolder"] in diff:
                user_id = access_mgmt.get_user(user_name)
                payload = dash["shares"]
                payload.append({
                    "shareId": user_id["USER_ID"],
                    "type": "user",
                    "rule": "edit",
                    "subscribe": False
                })
                new_payload = {"sharesTo": payload}
                response = requests.post(f'https://{url}/api/shares/dashboard/{dash["oid"]}?adminAccess=true', headers=head, data=json.dumps(new_payload), verify=False)
                if response.status_code == 200:
                    print(f"Dashboard {dash['title']} shared with {user_name}")
                else:
                    print(response.json())

        print("Retrying folder and dashboard traversal after granting access...")
        folder_found = get_folder_details()

        if folder_found:
            print("Collected Folder Details after granting access:")
            for folder_id, folder_name in folder_details:
                print(f"Folder: {folder_name} (ID: {folder_id})")

            print("Collected Dashboard Details after granting access:")
            for dash_id, dash_name in dashboard_details:
                print(f"Dashboard: {dash_name} (ID: {dash_id})")
        else:
            print(f"Folder '{folder_name}' not found after attempting to grant access. Exiting...")
            exit()

    # Change ownership logic
    if folder_details or (change_dashboard_ownership and dashboard_details):
        new_owner = access_mgmt.get_user(new_owner_name)
        new_owner_id = new_owner["USER_ID"]

        print("Changing folder and dashboard owners...")

        # Change folder owners
        for folder_id, folder_name in folder_details:
            current_folder = requests.get(f'https://{url}/api/v1/folders/{folder_id}', headers=head, verify=False).json()
            current_owner_id = current_folder.get("owner")

            if current_owner_id == new_owner_id:
                print(f"Folder '{folder_name}' is already owned by {new_owner_name}, no action needed.")
            else:
                data = {"owner": new_owner_id}
                response = requests.patch(f'https://{url}/api/v1/folders/{folder_id}', headers=head, data=json.dumps(data), verify=False)
                if response.status_code == 200:
                    print(f"Folder '{folder_name}' owner changed to {new_owner_name}")
                else:
                    print(response.json())

        # Change dashboard owners if enabled
        if change_dashboard_ownership:
            for dash_id, dash_name in dashboard_details:
                current_dashboard = requests.get(f'https://{url}/api/v1/dashboards/{dash_id}', headers=head, verify=False).json()
                current_owner_id = current_dashboard.get("owner")

                if current_owner_id == new_owner_id:
                    print(f"Dashboard '{dash_name}' is already owned by {new_owner_name}, no action needed.")
                else:
                    if current_owner_id == access_mgmt.get_user(user_name)["USER_ID"]:
                        data = {"ownerId": new_owner_id, "originalOwnerRule": original_owner_rule}
                        response = requests.post(f'https://{url}/api/v1/dashboards/{dash_id}/change_owner', headers=head, data=json.dumps(data), verify=False)
                    else:
                        data = {"ownerId": new_owner_id, "originalOwnerRule": original_owner_rule}
                        response = requests.post(f'https://{url}/api/v1/dashboards/{dash_id}/change_owner?adminAccess=true', headers=head, data=json.dumps(data), verify=False)

                    if response.status_code == 200:
                        print(f"Dashboard '{dash_name}' owner changed to {new_owner_name}")
                    else:
                        print(response.json())
    else:
        print("No folders or dashboards to change ownership. Exiting the tool.")

# Example usage
# change_folder_and_dashboard_ownership(
#     user_name='himanshu.negi@sisense.com',
#     folder_name='level4b',
#     new_owner_name='mike.jones@example.com'
# )

change_folder_and_dashboard_ownership(
    user_name='himanshu.negi@sisense.com',
    folder_name='level4b',
    new_owner_name='mike.jones@example.com',
    original_owner_rule='view',
    change_dashboard_ownership=False
)


# mike.jones@example.com

# himanshu.negi@sisense.com