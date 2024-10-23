from sisensepy.api_client import APIClient

class AccessManagement:
    def __init__(self, api_client=None, config_file="config.yaml", debug=False):
        """
        Initializes the AccessManagement class.

        If no API client is provided, it will create an APIClient internally using the provided config file.

        Parameters:
            api_client (APIClient, optional): An existing APIClient instance. If None, a new APIClient is created.
            config_file (str): Path to the YAML configuration file. Default is 'config.yaml'.
        """
        # If an API client is provided, use it. Otherwise, initialize a new APIClient with the config file.
        if api_client:
            self.api_client = api_client
        else:
            self.api_client = APIClient(config_file=config_file, debug=debug)

        # Use the logger from the APIClient instance
        self.logger = self.api_client.logger


    def get_user(self, user_name):
        """
        Retrieves user details by their email (username) and expands the response to include group and role information.

        Parameters:
            user_name (str): The email or username of the user to be retrieved.

        Returns:
            dict: A dictionary containing user details, or an empty dictionary if no user matches the provided username.
        """
        self.logger.debug(f"Getting user with username: {user_name}")

        # Query parameters to expand the response with group and role information
        params = {'expand': 'groups,role'}

        # Fetch all users from the API with the specified query parameters
        response = self.api_client.get("/api/v1/users", params=params)

        # Check if the API request failed
        if response is None:
            self.logger.error(f"Failed to retrieve users from API for username: {user_name}")
            print("Failed to retrieve users from API. Please check the logs for more details.")
            return {}

        # Mapping role names
        ROLE_MAPPING = {
            'consumer': 'viewer',
            'super': 'sysAdmin',
            'contributor': 'dashboardDesigner'
        }

        # Iterate over each user in the response to find the one matching the given username
        for user in response:
            try:
                self.logger.debug(f"Checking user: {user['email']}")

                if user["email"] == user_name:
                    self.logger.info(f"Found user: {user['email']}")

                    # Prepare the user's data including role and group information
                    user_data = {
                        'USER_ID': user["_id"],
                        'USER_NAME': user["userName"],
                        'FIRST_NAME': user["firstName"],
                        'LAST_NAME': user.get('lastName', ''),
                        'EMAIL': user["email"],
                        'IS_ACTIVE': user["active"],
                        'ROLE_ID': user["role"]["_id"],
                        'ROLE_NAME': ROLE_MAPPING.get(user["role"]["name"], user["role"]["name"]),
                        'GROUPS': [group["name"] for group in user.get("groups", [])]  # Extract group names
                    }

                    #print(f"User found: {user_data}")
                    return user_data

            except Exception as e:
                self.logger.error(f"Error processing user {user['email']}: {e}")
                print(f"Error processing user {user['email']}. Check logs for details.")

        # If no matching user is found, log an error and return an empty dictionary
        self.logger.error(f"User with username {user_name} not found")
        print(f"User with username {user_name} not found. Please check the logs for more details.")
        return {}



    def get_users_all(self):
        """
        Retrieves user details along with tenant, group, and role information.

        Removes the "Everyone" group from users if they belong to other groups,
        but keeps the "Everyone" group if it's the only group the user belongs to.

        Parameters:
            None

        Returns:
            list: A list of dictionaries where each dictionary contains user details and group information.
        """
        self.logger.debug("Getting all users")

        # Query parameters to expand the response with group and role information
        params = {'expand': 'groups,role'}

        # Fetch user data from the API with the specified query parameters
        response = self.api_client.get("/api/v1/users", params=params)
        
        # Check if the API request failed
        if response is None:
            self.logger.error("Failed to retrieve users from API")
            print("Failed to retrieve users from API. Please check the logs for more details.")
            return None

        # Initialize list to store user information
        data_list = []

        # Mapping role names
        ROLE_MAPPING = {
            'consumer': 'viewer',
            'super': 'sysAdmin',
            'contributor': 'dashboardDesigner'
        }

        # Process the API response to build data_list
        for user in response:
            try:
                self.logger.debug(f"Processing user: {user['email']}")

                # Base data that applies to each user
                base_data = {
                    'USER_ID': user["_id"],
                    'USER_NAME': user["userName"],
                    'FIRST_NAME': user["firstName"],
                    'LAST_NAME': user.get('lastName', ''),
                    'EMAIL': user["email"],
                    'IS_ACTIVE': user["active"],
                    'ROLE_ID': user["role"]["_id"],
                    'ROLE_NAME': ROLE_MAPPING.get(user["role"]["name"], user["role"]["name"]),
                    'GROUPS': []
                }

                # Add all group names to the 'GROUPS' list
                if 'groups' in user and user["groups"]:
                    base_data['GROUPS'] = [group["name"] for group in user["groups"]]

                # Process users with multiple groups
                if len(base_data['GROUPS']) > 1 and 'Everyone' in base_data['GROUPS']:
                    # Remove "Everyone" from the list if the user belongs to other groups
                    base_data['GROUPS'].remove('Everyone')

                # Add the processed user to the data_list
                data_list.append(base_data)

                self.logger.debug(f"Successfully processed user: {user['email']}")

            except Exception as e:
                self.logger.error(f"Error processing user {user['email']}: {e}")
                print(f"Error processing user {user['email']}. Check logs for details.")

        # Log the result and return the final data list
        if data_list:
            self.logger.info(f"Found {len(data_list)} users")
            print(f"Found {len(data_list)} users.")
        else:
            self.logger.error("No users found")
            print("No users found. Please check the logs for more details.")
            
        return data_list


    def create_user(self, user_data):
        """
        Creates a new user by processing the provided user data to replace role names and group names
        with their corresponding IDs, then sends a POST request to create the user.

        Parameters:
            user_data (dict): A dictionary containing user details such as email, firstName, lastName,
                            role (role name), groups (list of group names), and preferences.

        Returns:
            dict or None: The response from the API if successful, None otherwise.
        """
        self.logger.debug(f"Creating user with data: {user_data}")

        # Custom role mapping
        role_alias_mapping = {
            "VIEWER": "CONSUMER",
            "DESIGNER": "CONTRIBUTOR"
        }

        # Convert the role name in the user_data to uppercase for case-insensitive matching
        user_role = user_data["role"].upper()
        mapped_role = role_alias_mapping.get(user_role, user_role)  # Replace with mapped role if it exists

        # Fetch roles from the API and map them, converting names to uppercase
        role_response = self.api_client.get('/api/roles')
        if not role_response:
            self.logger.error("Failed to fetch roles from API")
            print("Failed to fetch roles from API. Please check the logs for more details.")
            return None

        roles_mapping = [{"id": role["_id"], "name": role["name"].upper()} for role in role_response]
        self.logger.debug(f"Roles mapping: {roles_mapping}")

        # Replace the role name with the corresponding ID (case-insensitive)
        for role in roles_mapping:
            if role["name"] == mapped_role:
                user_data["roleId"] = role["id"]  # Use roleId for the API call
                break
        else:
            self.logger.error(f"Role '{user_data['role']}' not found in roles_mapping")
            raise ValueError(f"Role '{user_data['role']}' not found in roles_mapping")

        # Remove the 'role' key from user_data since we're using 'roleId'
        user_data.pop("role", None)

        # Handle group mapping only if groups are provided
        if user_data.get("groups"):
            # Convert the group names in the user_data to uppercase for case-insensitive matching
            user_data["groups"] = [group_name.upper() for group_name in user_data["groups"]]

            # Fetch groups from the API and map them, converting names to uppercase
            group_response = self.api_client.get('/api/v1/groups')
            if not group_response:
                self.logger.error("Failed to fetch groups from API")
                print("Failed to fetch groups from API. Please check the logs for more details.")
                return None

            groups_mapping = [{"id": group["_id"], "name": group["name"].upper()} for group in group_response]
            self.logger.debug(f"Groups mapping: {groups_mapping}")

            # Replace each group name with the corresponding ID (case-insensitive)
            updated_groups = []
            for group_name in user_data["groups"]:
                for group in groups_mapping:
                    if group["name"] == group_name:
                        updated_groups.append(group["id"])
                        break
                else:
                    self.logger.error(f"Group '{group_name}' not found in groups_mapping")
                    raise ValueError(f"Group '{group_name}' not found in groups_mapping")

            # Update the user_data with the list of group IDs
            user_data["groups"] = updated_groups
        else:
            # If no groups are provided, ensure the groups key remains an empty list
            user_data["groups"] = []

        # Log the final payload before sending the request
        self.logger.debug(f"Final user data for API call: {user_data}")

        # Send the POST request to create the user
        response = self.api_client.post("/api/v1/users", data=user_data)
        if response:
            self.logger.info(f"User created successfully: {response}")
            print(f"User has been created successfully. Response: {response}")
        else:
            self.logger.error("Failed to create user")
            print("Failed to create user. Please check the logs for more details.")

        return response


    def update_user(self, user_name, user_data):
        """
        Updates a user by their User Name.

        Parameters:
            user_name (str): The email or username of the user to be updated.
            user_data (dict): A dictionary containing user details to update, such as role, groups, etc.

        Returns:
            dict or None: The response from the API if successful, None otherwise.
        """
        self.logger.debug(f"Updating user with username: {user_name}")
        
        # Reuse the get_user method to fetch user details
        user = self.get_user(user_name)

        # If user is not found, return None
        if not user:
            return None

        # Custom role mapping
        role_alias_mapping = {
            "VIEWER": "CONSUMER",
            "DESIGNER": "CONTRIBUTOR"
        }

        # Convert the role name in the user_data to uppercase for case-insensitive matching
        if "role" in user_data:
            user_role = user_data["role"].upper()
            mapped_role = role_alias_mapping.get(user_role, user_role)  # Replace with mapped role if it exists

            # Fetch roles from the API and map them, converting names to uppercase
            role_response = self.api_client.get('/api/roles')
            roles_mapping = [{"id": role["_id"], "name": role["name"].upper()} for role in role_response]
            self.logger.debug(f"Roles mapping: {roles_mapping}")

            # Replace the role name with the corresponding ID (case-insensitive)
            for role in roles_mapping:
                if role["name"] == mapped_role:
                    user_data["roleId"] = role["id"]
                    break
            else:
                self.logger.error(f"Role '{user_data['role']}' not found in roles_mapping")
                raise ValueError(f"Role '{user_data['role']}' not found in roles_mapping")

            # Remove the 'role' key from user_data since we're using 'roleId'
            user_data.pop("role", None)

        # Handle group mapping only if groups are provided
        if user_data.get("groups"):
            # Convert the group names in the user_data to uppercase for case-insensitive matching
            user_data["groups"] = [group_name.upper() for group_name in user_data["groups"]]

            # Fetch groups from the API and map them, converting names to uppercase
            group_response = self.api_client.get('/api/v1/groups')
            groups_mapping = [{"id": group["_id"], "name": group["name"].upper()} for group in group_response]
            self.logger.debug(f"Groups mapping: {groups_mapping}")

            # Replace each group name with the corresponding ID (case-insensitive)
            updated_groups = []
            for group_name in user_data["groups"]:
                for group in groups_mapping:
                    if group["name"] == group_name:
                        updated_groups.append(group["id"])
                        break
                else:
                    self.logger.error(f"Group '{group_name}' not found in groups_mapping")
                    raise ValueError(f"Group '{group_name}' not found in groups_mapping")

            # Update the user_data with the list of group IDs
            user_data["groups"] = updated_groups
        else:
            # If no groups are provided, ensure the groups key remains an empty list
            user_data["groups"] = []

        # Log the final payload before sending the request
        self.logger.debug(f"Final updated user data for API call: {user_data}")

        # Send the PATCH request to update the user
        response = self.api_client.patch(f"/api/v1/users/{user['_id']}", data=user_data)
        if response:
            self.logger.info(f"User updated successfully: {response}")
            print(f"User has been updated successfully. Response: {response}")
        else:
            self.logger.error("Failed to update user")
            print("Failed to update user. Please check the logs for more details.")

        return response



    def delete_user(self, user_name):
        """
        Deletes a user by their email (username).

        Parameters:
            user_name (str): The email or username of the user to be deleted.

        Returns:
            dict or None: The response from the API if successful, None otherwise.
        """
        self.logger.debug(f"Starting 'delete_user' method for username: {user_name}")
        
        # Reuse the get_user method to fetch user details
        self.logger.debug(f"Fetching user details for '{user_name}' using 'get_user' method.")
        user = self.get_user(user_name)

        # If user is not found, log and return None
        if not user:
            self.logger.error(f"User with username '{user_name}' not found. Cannot proceed with deletion.")
            print(f"User with username '{user_name}' not found. Please check the logs for more details.")
            return None

        self.logger.debug(f"User '{user_name}' found. Proceeding to delete user with ID: {user['_id']}")

        # Send the DELETE request to delete the user
        response = self.api_client.delete(f"/api/v1/users/{user['_id']}")
        
        if response:
            self.logger.info(f"User '{user_name}' (ID: {user['_id']}) deleted successfully. Response: {response}")
            print(f"User has been deleted successfully. Response: {response}")
        else:
            self.logger.error(f"Failed to delete user with username '{user_name}' (ID: {user['_id']}).")
            print("Failed to delete user. Please check the logs for more details.")

        self.logger.debug(f"Completed 'delete_user' method for username: {user_name}")
        return response



    def users_per_group(self, group_name):
        """
        Retrieves usernames of all users belonging to a specific group.

        Parameters:
            group_name (str): The name of the group to filter users by.

        Returns:
            dict: A dictionary with 'group' as the group name and 'username' as a list of usernames in that group.
        """
        self.logger.debug(f"Starting 'users_per_group' method for group: {group_name}")
        
        # Initialize dictionary to store the group and the corresponding usernames
        users_in_group = {"group": group_name, "username": []}
        
        # Log the initialization
        self.logger.debug(f"Initialized group dictionary: {users_in_group}")

        # Fetch all users and filter by the specified group using get_users_all
        all_users = self.get_users_all()
        if not all_users:
            self.logger.error(f"No users returned from 'get_users_all'. Unable to filter users for group: {group_name}")
            return users_in_group

        self.logger.debug(f"Retrieved {len(all_users)} users. Now filtering for group '{group_name}'.")

        # Filter users based on the group
        for user in all_users:
            if 'GROUPS' in user:
                for group in user["GROUPS"]:
                    if group.upper() == group_name.upper():
                        self.logger.debug(f"User '{user['USER_NAME']}' found in group '{group_name}'.")
                        users_in_group["username"].append(user["USER_NAME"])

        # Log the result and return the filtered list of users
        if users_in_group["username"]:
            self.logger.info(f"Found {len(users_in_group['username'])} users in the group '{group_name}'")
            print(f"Found {len(users_in_group['username'])} users in the group '{group_name}'.")
        else:
            self.logger.error(f"No users found in the group '{group_name}'")
            print(f"No users found in the group '{group_name}'. Please check the logs for more details.")

        self.logger.debug(f"Final result for group '{group_name}': {users_in_group}")
        return users_in_group


    def users_per_group_all(self):
        """
        Retrieves all groups and maps them with the users belonging to those groups.
        Groups like 'Everyone' and 'All users in system' are excluded.
        Users with roles like 'admin', 'dataAdmin', and 'sysAdmin' are mapped to the existing 'Admins' group.
        Groups with no users are also included in the final result.

        Returns:
            list: A list of dictionaries, where each dictionary contains a group name and the list of usernames in that group.
        """
        # Define the groups to exclude
        EXCLUDED_GROUPS = {"Everyone", "All users in system"}

        self.logger.debug("Starting to retrieve all groups and their users.")

        # Fetch all groups from the API
        group_response = self.api_client.get("/api/v1/groups")
        if not group_response:
            self.logger.error("Failed to retrieve groups from API")
            print("Failed to retrieve groups. Please check the logs for more details.")
            return []

        # Fetch all users from get_users_all method
        all_users = self.get_users_all()
        if not all_users:
            self.logger.error(f"No users returned from 'get_users_all' method.")
            return []

        # Step 1: Build a dictionary for groups with no users yet, excluding the unwanted groups
        self.logger.debug("Building group dictionary.")
        groups_dict = {group['name']: [] for group in group_response if group['name'] not in EXCLUDED_GROUPS}

        # Step 2: Populate the users in each group from the user data
        self.logger.debug("Populating groups with users.")
        for user in all_users:
            # Check if 'GROUPS' key exists for this user
            if "GROUPS" in user:
                for group in user["GROUPS"]:
                    group_name = group
                    if group_name not in EXCLUDED_GROUPS:
                        groups_dict[group_name].append(user["USER_NAME"])
                        self.logger.debug(f"Added user {user['USER_NAME']} to group {group_name}")

        # Step 3: Populate users for the 'Admins' group based on their roles
        self.logger.debug("Populating Admin group based on role names.")            
        # Check users for admin roles and add them to the 'Admins' group
        for user in all_users:
            if user.get("ROLE_NAME") in ["sysAdmin", "dataAdmin", "admin"]:
                groups_dict["Admins"].append(user["USER_NAME"])
                self.logger.debug(f"Added user {user['USER_NAME']} to Admins group based on role")

        # Step 4: Format the result as a list of dictionaries
        result = [{"group": group_name, "username": usernames} for group_name, usernames in groups_dict.items()]

        # Log and return the result
        if result:
            self.logger.info(f"Found {len(result)} groups with users.")
            print(f"Found {len(result)} groups.")
        else:
            self.logger.error("No groups or users found.")
            print("No groups or users found. Please check the logs for more details.")

        return result
    
    def change_folder_and_dashboard_ownership(self, user_name, folder_name, new_owner_name, original_owner_rule='edit', change_dashboard_ownership=True):
        """
        Method to change the ownership of folders and optionally dashboards.

        This method changes the ownership of a target folder and the entire tree structure
        surrounding it, including subfolders, sibling folders, and parent folders.

        Optionally, it will also change the ownership of dashboards associated with these folders.

        Parameters:
            user_name (str): The user running the tool. This is necessary for API access checks.
            folder_name (str): The target folder whose ownership needs to be changed.
            new_owner_name (str): The new owner to whom the folder (and optionally dashboards) ownership will be transferred.
            original_owner_rule (str, optional): Specifies the ownership rule ('edit' or 'view'). Default is 'edit'.
            change_dashboard_ownership (bool, optional): Specifies whether to also change the ownership of dashboards in the folder tree. Default is True.
        """

        folder_details = set()  # Set to store folder IDs and names
        dashboard_details = set()  # Set to store dashboard IDs and names
        target_folder_found = False  # Variable to track if we found the target folder
        total_folders_changed = 0  # Counter for folders whose ownership has been changed
        total_dashboards_changed = 0  # Counter for dashboards whose ownership has been changed

        self.logger.info("Starting folder and dashboard traversal...")
        self.logger.debug(f"Looking for folder '{folder_name}' to change ownership to '{new_owner_name}'")

        # Recursively traverse folders and subfolders, avoiding duplicates
        def traverse_folder(folder):
            if (folder['oid'], folder['name']) not in folder_details:
                folder_details.add((folder['oid'], folder['name']))
                self.logger.info(f"Folder found: {folder['name']} (ID: {folder['oid']})")

                if 'dashboards' in folder and folder["dashboards"]:
                    for dash in folder["dashboards"]:
                        if (dash['oid'], dash['title']) not in dashboard_details:
                            dashboard_details.add((dash['oid'], dash['title']))
                            self.logger.info(f"Dashboard found: {dash['title']} (ID: {dash['oid']})")

                if "folders" in folder:
                    for subfolder in folder["folders"]:
                        traverse_folder(subfolder)
            else:
                if not folder.get("folders"):  # Only log if no subfolders exist
                    self.logger.debug(f'No subfolders in folder - {folder["name"]}')

        # Search for the target folder and traverse siblings, subfolders, and parent folders
        def search_for_target_and_traverse_tree(folders, target_folder_name):
            nonlocal target_folder_found
            parent_folder = None

            for folder in folders:
                self.logger.debug(f"Checking folder '{folder['name']}' (ID: {folder['oid']})")
                if folder['name'] == target_folder_name:
                    self.logger.info(f"Found target folder: {target_folder_name}")
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
                self.logger.info(f"Parent folder: {parent_folder['name']} (ID: {parent_folder['oid']})")
                traverse_folder(parent_folder)

                if "folders" in parent_folder:
                    for sibling in parent_folder["folders"]:
                        traverse_folder(sibling)

        # Fetch folder details and traverse relevant tree structure
        def get_folder_details():
            self.logger.debug("Fetching all folders from API")
            response = self.api_client.get('/api/v1/navver')

            if not response or 'folders' not in response:
                self.logger.error("No folders found in the API response or invalid response.")
                return False

            self.logger.info(f"Searching for folder '{folder_name}'...")
            parent_folder = search_for_target_and_traverse_tree(response['folders'], folder_name)

            if target_folder_found:
                self.logger.info(f"Folder '{folder_name}' found and traversed successfully.")
                traverse_parents_and_siblings(parent_folder)
                return True
            else:
                self.logger.warning(f"Folder '{folder_name}' not found in the response.")
                return False

        folder_found = get_folder_details()

        if folder_found:
            self.logger.info("Collected Folder Details:")
            for folder_id, folder_name in folder_details:
                self.logger.info(f"Folder: {folder_name} (ID: {folder_id})")

            self.logger.info("Collected Dashboard Details:")
            for dash_id, dash_name in dashboard_details:
                self.logger.info(f"Dashboard: {dash_name} (ID: {dash_id})")
        else:
            self.logger.warning("Folder not found, moving to search dashboards and grant access step...")
            limit = 50
            skip = 0
            dashboards = []
            while True: 
                self.logger.debug(f"Fetching dashboards (limit={limit}, skip={skip})")
                dashboard_response = self.api_client.post('/api/dashboards/searches', data={
                    "queryParams": {"ownershipType": "allRoot", "search": "", "ownerInfo": True, "asObject": True},
                    "queryOptions": {"sort": {"title": 1}, "limit": limit, "skip": skip}
                })

                if not dashboard_response or len(dashboard_response.get("items", [])) == 0:
                    self.logger.debug("No more dashboards found.")
                    break
                else:
                    dashboards.extend(dashboard_response["items"])
                    skip += limit

            all_folder_ids = {dic["parentFolder"] for dic in dashboards if "parentFolder" in dic and dic["parentFolder"]}
            self.logger.debug(f"Collected parent folder IDs from dashboards: {all_folder_ids}")

            folder_response = self.api_client.get('/api/v1/folders')
            user_folder_ids = {folder["oid"] for folder in folder_response if "oid" in folder}
            self.logger.debug(f"Collected user-accessible folder IDs: {user_folder_ids}")

            diff = all_folder_ids - user_folder_ids
            self.logger.info(f"Folders the user does not have access to: {diff}")

            for dash in dashboards:
                if 'parentFolder' in dash and dash["parentFolder"] in diff:
                    user_id = self.get_user(user_name)["USER_ID"]
                    payload = dash["shares"]
                    payload.append({
                        "shareId": user_id,
                        "type": "user",
                        "rule": "edit",
                        "subscribe": False
                    })
                    self.logger.debug(f"Sharing dashboard {dash['title']} (ID: {dash['oid']}) with {user_name}")
                    share_response = self.api_client.post(f'/api/shares/dashboard/{dash["oid"]}?adminAccess=true', data={"sharesTo": payload})
                    if share_response:
                        self.logger.info(f"Dashboard '{dash['title']}' shared with {user_name}")
                    else:
                        self.logger.error(f"Failed to share dashboard '{dash['title']}': {share_response}")

            self.logger.info("Retrying folder and dashboard traversal after granting access...")
            folder_found = get_folder_details()

            if folder_found:
                self.logger.info("Collected Folder Details after granting access:")
                for folder_id, folder_name in folder_details:
                    self.logger.info(f"Folder: {folder_name} (ID: {folder_id})")

                self.logger.info("Collected Dashboard Details after granting access:")
                for dash_id, dash_name in dashboard_details:
                    self.logger.info(f"Dashboard: {dash_name} (ID: {dash_id})")
            else:
                self.logger.warning(f"Folder '{folder_name}' not found after attempting to grant access. Exiting...")
                return

        # Change ownership logic
        if folder_details or (change_dashboard_ownership and dashboard_details):
            new_owner = self.get_user(new_owner_name)
            new_owner_id = new_owner["USER_ID"]

            self.logger.info("Changing folder and dashboard owners...")

            # Change folder owners
            for folder_id, folder_name in folder_details:
                data = {"owner": new_owner_id}
                self.logger.debug(f"Changing owner for folder {folder_name} (ID: {folder_id}) with data: {data}")
                
                response = self.api_client.patch(f'/api/v1/folders/{folder_id}', data=data)
                
                # Log response
                self.logger.debug(f"API response for folder change: {response}")
                
                if response and response.get("owner") == new_owner_id:
                    self.logger.info(f"Folder '{folder_name}' owner changed to {new_owner_name}")
                    print(f"Folder '{folder_name}' owner changed to {new_owner_name}")
                    total_folders_changed += 1
                else:
                    self.logger.error(f"Failed to change folder owner for '{folder_name}'.")


            # Change dashboard owners if enabled
            if change_dashboard_ownership:
                for dash_id, dash_name in dashboard_details:
                    current_dashboard = self.api_client.get(f'/api/v1/dashboards/{dash_id}')
                    if not current_dashboard:
                        self.logger.error(f"Dashboard with ID '{dash_id}' not found. Skipping.")
                        continue

                    current_owner_id = current_dashboard.get("owner")

                    if current_owner_id == new_owner_id:
                        self.logger.info(f"Dashboard '{dash_name}' is already owned by {new_owner_name}, no action needed.")
                        print(f"Dashboard '{dash_name}' is already owned by {new_owner_name}, no action needed.")
                    else:
                        if current_owner_id == self.get_user(user_name)["USER_ID"]:
                            data = {"ownerId": new_owner_id, "originalOwnerRule": original_owner_rule}
                            response = self.api_client.post(f'/api/v1/dashboards/{dash_id}/change_owner', data=data)
                        else:
                            data = {"ownerId": new_owner_id, "originalOwnerRule": original_owner_rule}
                            response = self.api_client.post(f'/api/v1/dashboards/{dash_id}/change_owner?adminAccess=true', data=data)

                        if response:
                            self.logger.info(f"Dashboard '{dash_name}' owner changed to {new_owner_name}")
                            print(f"Dashboard '{dash_name}' owner changed to {new_owner_name}")
                            total_dashboards_changed += 1
                        else:
                            self.logger.error(f"Failed to change dashboard owner for '{dash_name}'.")
                            print(f"Failed to change dashboard owner for '{dash_name}'.")

            # Log total changes
            self.logger.info(f"Ownership changed for {total_folders_changed} folders and {total_dashboards_changed} dashboards.")
            return {"total_folders_changed": total_folders_changed, "total_dashboards_changed": total_dashboards_changed}
        else:
            self.logger.info("No folders or dashboards to change ownership. Exiting.")
            return None
 
    def get_datamodel_columns(self, datamodel_name):
        """
        Method to retrieve columns from a DataModel by collecting them from the DataModel's datasets and tables.

        Parameters:
            datamodel_name (str): The name of the DataModel from which to extract columns.

        Returns:
            list: A list of dictionaries where each dictionary contains DataModel ID, DataModel name, table name, and column name.
        """
        all_columns = []
        
        self.logger.info(f"Fetching columns for DataModel: {datamodel_name}")

        # Step 1: Get DataModel ID
        self.logger.debug(f"Fetching DataModel ID for '{datamodel_name}'")
        schema_url = "/api/v2/datamodels/schema"
        response = self.api_client.get(schema_url)
        if not response:
            self.logger.error(f"Failed to fetch DataModel schema for '{datamodel_name}'")
            return []

        datamodel_id = next((x["oid"] for x in response if x["title"] == datamodel_name), None)
        if not datamodel_id:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return []

        self.logger.info(f"DataModel ID for '{datamodel_name}' is {datamodel_id}")

        # Step 2: Get DataSet IDs
        self.logger.debug(f"Fetching DataSet IDs for DataModel ID '{datamodel_id}'")
        dataset_url = f"/api/v2/datamodels/{datamodel_id}/schema/datasets"
        response = self.api_client.get(dataset_url)
        if not response:
            self.logger.error(f"Failed to fetch DataSet schema for DataModel ID '{datamodel_id}'")
            return []

        dataset_ids = [x["oid"] for x in response]
        self.logger.info(f"DataSet IDs for DataModel '{datamodel_name}': {dataset_ids}")

        # Step 3: Loop through datasets and collect columns from tables
        for dataset_id in dataset_ids:
            self.logger.debug(f"Fetching tables for DataSet ID '{dataset_id}'")
            table_url = f"{dataset_url}/{dataset_id}/tables"
            response = self.api_client.get(table_url)
            if not response:
                self.logger.error(f"Failed to fetch tables for DataSet ID '{dataset_id}'")
                continue

            for table in response:
                table_name = table["name"]
                for column in table["columns"]:
                    all_columns.append({
                        "datamodel_id": datamodel_id,
                        "datamodel_name": datamodel_name,
                        "table": table_name,
                        "column": column["name"]
                    })

        self.logger.info(f"Collected columns from DataModel '{datamodel_name}': {len(all_columns)} columns.")
        return all_columns
    
    def get_dashboard_columns(self, dashboard_name):
        """
        Method to retrieve columns from a specific dashboard.

        This method uses pagination to search for a dashboard by its name, 
        then retrieves all the columns used in that dashboard from filters and widgets.

        Parameters:
            dashboard_name (str): The name of the dashboard to retrieve columns from.

        Returns:
            list: A list of dictionaries containing distinct table and column information from the dashboard.
        """
        limit = 50
        skip = 0
        dashboards = []
        dashboard_columns = []

        self.logger.info(f"Starting to retrieve columns from dashboard: {dashboard_name}")
        
        # Step 1: Fetch all dashboards and search for the matching title
        while True:
            self.logger.debug(f"Fetching dashboards with limit={limit}, skip={skip}")
            
            dashboard_response = self.api_client.post(
                '/api/v1/dashboards/searches',
                data={
                    "queryParams": {
                        "ownershipType": "allRoot", 
                        "search": dashboard_name, 
                        "ownerInfo": True, 
                        "asObject": True
                    },
                    "queryOptions": {
                        "sort": {"title": 1}, 
                        "limit": limit, 
                        "skip": skip
                    }
                }
            )
            
            if not dashboard_response or len(dashboard_response.get("items", [])) == 0:
                self.logger.info("No more dashboards found.")
                break
            else:
                dashboards.extend(dashboard_response["items"])
                skip += limit
                self.logger.debug(f"Retrieved {len(dashboard_response['items'])} dashboards, total so far: {len(dashboards)}")
        
        # Step 2: Match the dashboard name and fetch the columns
        matching_dashboard = next((dash for dash in dashboards if dash["title"] == dashboard_name), None)
        
        if not matching_dashboard:
            self.logger.error(f"Dashboard '{dashboard_name}' not found.")
            return []

        self.logger.info(f"Found dashboard '{dashboard_name}' with ID: {matching_dashboard['oid']}")

        # Step 3: Fetch columns from the dashboard's filters and widgets
        dashboard_url = f"/api/v1/dashboards/export?dashboardIds={matching_dashboard['oid']}&adminAccess=true"
        dashboard_data = self.api_client.get(dashboard_url)
        
        if not dashboard_data:
            self.logger.error(f"Failed to export dashboard with ID '{matching_dashboard['oid']}'")
            return []

        dashboard = dashboard_data[0]
        self.logger.debug(f"Analyzing Dashboard '{dashboard['title']}' (ID: {matching_dashboard['oid']})")

        # Extract columns from filters
        if dashboard.get('filters'):
            for filter in dashboard["filters"]:
                if 'levels' in filter:
                    for level in filter["levels"]:
                        dashboard_columns.append({
                            "dashboard_name": dashboard_name,
                            "source": "filter",
                            "widget_id": "N/A",
                            "table": level.get("table", "Unknown Table"),
                            "column": level.get("column", "Unknown Column")
                        })
                elif 'jaql' in filter:
                    dashboard_columns.append({
                        "dashboard_name": dashboard_name,
                        "source": "filter",
                        "widget_id": "N/A",
                        "table": filter["jaql"].get("table", "Unknown Table"),
                        "column": filter["jaql"].get("column", "Unknown Column")
                    })

        # Extract columns from widgets
        for widget in dashboard["widgets"]:
            for panel in widget["metadata"]["panels"]:
                for item in panel["items"]:
                    if 'context' in item["jaql"]:
                        for key, value in item["jaql"]["context"].items():
                            dashboard_columns.append({
                                "dashboard_name": dashboard_name,
                                "source": "widget",
                                "widget_id": widget["oid"],
                                "table": value.get("table", "Unknown Table"),
                                "column": value.get("column", "Unknown Column")
                            })
                    else:
                        dashboard_columns.append({
                            "dashboard_name": dashboard_name,
                            "source": "widget",
                            "widget_id": widget["oid"],
                            "table": item["jaql"].get("table", "Unknown Table"),
                            "column": item["jaql"].get("column", "Unknown Column")
                        })

        self.logger.debug(f"Full column data from dashboard '{dashboard_name}': {dashboard_columns}")

        # Step 4: Deduplicate based on 'table' and 'column'
        distinct_columns_set = set()
        distinct_dashboard_columns = []
        
        for entry in dashboard_columns:
            key = (entry["table"], entry["column"])  # Define key based on table and column
            if key not in distinct_columns_set:
                distinct_dashboard_columns.append(entry)  # Add only distinct ones
                distinct_columns_set.add(key)

        self.logger.info(f"Retrieved {len(distinct_dashboard_columns)} distinct columns from dashboard '{dashboard_name}'")
        
        return distinct_dashboard_columns


    def get_unused_columns(self, datamodel_name):
        """
        Method to identify unused columns in a DataModel by comparing the columns 
        in the DataModel against the columns used in its associated Dashboards.

        **Covers the following:**
        - **Filters**: Dashboard-level filters, Widget filters, Dependent Filters, and Measured Filters.
        - **Widget Panels**: Row, Values, Column panels, and other widget panels.

        **Order of Capture:**
        1. **Columns from Dashboard Level**: Captures columns used in Dashboard Filters, including Dependent Filters.
        2. **Columns from Widget Level**: 
            - Captures columns used in Widget Panels, including Row, Values, Column panels.
            - Captures columns from Measured Filters.
            - Captures columns from Widget Filters.

        Parameters:
            datamodel_name (str): The name of the DataModel to analyze for unused columns.

        Returns:
            list: A list of dictionaries containing unused column details with a "used" field set to True or False.

        Limitation:
            - The method assumes that the all Dashboards under this datamodel are shared with user.
        """
        all_columns = []
        dashboard_columns = []
        
        self.logger.info(f"Starting analysis for unused columns in DataModel: {datamodel_name}")
        
        # Step 1: Get DataModel ID
        self.logger.debug(f"Fetching DataModel ID for '{datamodel_name}'")
        schema_url = "/api/v2/datamodels/schema"
        response = self.api_client.get(schema_url)
        if not response:
            self.logger.error(f"Failed to fetch DataModel schema for '{datamodel_name}'")
            return []

        datamodel_id = next((x["oid"] for x in response if x["title"] == datamodel_name), None)
        if not datamodel_id:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return []
        
        self.logger.info(f"DataModel ID for '{datamodel_name}' is {datamodel_id}")

        # Step 2: Get DataSet IDs
        self.logger.debug(f"Fetching DataSet IDs for DataModel ID '{datamodel_id}'")
        dataset_url = f"/api/v2/datamodels/{datamodel_id}/schema/datasets"
        response = self.api_client.get(dataset_url)
        if not response:
            self.logger.error(f"Failed to fetch DataSet schema for DataModel ID '{datamodel_id}'")
            return []

        dataset_ids = [x["oid"] for x in response]
        self.logger.info(f"DataSet IDs for DataModel '{datamodel_name}': {dataset_ids}")

        # Step 3: Loop through datasets and collect columns from tables
        for dataset_id in dataset_ids:
            self.logger.debug(f"Fetching tables for DataSet ID '{dataset_id}'")
            table_url = f"{dataset_url}/{dataset_id}/tables"
            response = self.api_client.get(table_url)
            if not response:
                self.logger.error(f"Failed to fetch tables for DataSet ID '{dataset_id}'")
                continue

            for table in response:
                table_name = table["name"]
                for column in table["columns"]:
                    all_columns.append({
                        "datamodel_id": datamodel_id,
                        "datamodel_name": datamodel_name,
                        "table": table_name,
                        "column": column["name"]
                    })
        
        self.logger.info(f"Collected columns from DataModel '{datamodel_name}': {len(all_columns)} columns.")
        self.logger.debug(f"Collected columns from DataModel '{datamodel_name}': {all_columns}")

        # Step 4: Fetch dashboards using the DataModel
        self.logger.info(f"Fetching Dashboards for DataModel '{datamodel_name}'")
        dashboard_url = f"/api/v1/dashboards?datasourceTitle={datamodel_name}"
        response = self.api_client.get(dashboard_url)
        if not response:
            self.logger.error(f"Failed to fetch Dashboards for DataModel '{datamodel_name}'")
            return []

        dashboards = response
        dashboard_ids = [dash["oid"] for dash in dashboards]
        self.logger.info(f"Total number of Dashboards associated with DataModel '{datamodel_name}': {len(dashboard_ids)}")
        self.logger.debug(f"Dashboards associated with DataModel '{datamodel_name}': {dashboard_ids}")

        # Step 5: Extract columns used in Dashboards
        for dashboard_id in dashboard_ids:
            # Reset counts for each dashboard
            filter_count = 0
            columns_from_filters_count = 0
            columns_from_filters = []
            widget_count = 0
            columns_from_widgets_count = 0
            columns_from_widgets = []
            dashboard_url = f"/api/v1/dashboards/export?dashboardIds={dashboard_id}&adminAccess=true"
            response = self.api_client.get(dashboard_url)
            if not response:
                self.logger.error(f"Failed to export dashboard with ID '{dashboard_id}'")
                continue

            dashboard = response[0]
            self.logger.debug(f"Analyzing Dashboard '{dashboard['title']}' (ID: {dashboard_id})")

            # Extract columns from filters
            if dashboard.get('filters'):
                for filter in dashboard["filters"]:
                    filter_count += 1
                    if 'levels' in filter:
                        for level in filter["levels"]:
                            columns_from_filters_count += 1
                            columns_from_filters.append(level["column"])
                            dashboard_columns.append({
                                "datamodel_name": datamodel_name,
                                "dashboard_name": dashboard["title"],
                                "source": "filter",
                                "widget_id": "N/A",
                                "table": level.get("table", "Unknown Table"),
                                "column": level.get("column", "Unknown Column")
                            })
                    elif 'jaql' in filter:
                        columns_from_filters_count += 1
                        columns_from_filters.append(filter["jaql"]["column"])
                        dashboard_columns.append({
                            "datamodel_name": datamodel_name,
                            "dashboard_name": dashboard["title"],
                            "source": "filter",
                            "widget_id": "N/A",
                            "table": filter["jaql"].get("table", "Unknown Table"),
                            "column": filter["jaql"].get("column", "Unknown Column")
                        })

            # Extract columns from widgets
            for widget in dashboard["widgets"]:
                widget_count += 1
                for panel in widget["metadata"]["panels"]:
                    for item in panel["items"]:
                        if 'context' in item["jaql"]:
                            for key, value in item["jaql"]["context"].items():
                                columns_from_widgets_count += 1
                                columns_from_widgets.append(value.get("column", "Unknown Column"))
                                dashboard_columns.append({
                                    "datamodel_name": datamodel_name,
                                    "dashboard_name": dashboard["title"],
                                    "source": "widget",
                                    "widget_id": widget["oid"],
                                    "table": value.get("table", "Unknown Table"),
                                    "column": value.get("column", "Unknown Column")
                                })
                        else:
                            columns_from_widgets_count += 1
                            columns_from_widgets.append(item["jaql"].get("column", "Unknown Column"))
                            dashboard_columns.append({
                                "datamodel_name": datamodel_name,
                                "dashboard_name": dashboard["title"],
                                "source": "widget",
                                "widget_id": widget["oid"],
                                "table": item["jaql"].get("table", "Unknown Table"),
                                "column": item["jaql"].get("column", "Unknown Column")
                            })

            self.logger.debug(f"For Dashboard '{dashboard['title']}', found {filter_count} filters with {columns_from_filters_count} columns and {widget_count} widgets with {columns_from_widgets_count} columns.")

        self.logger.info(f"Collected dashboard columns from DataModel '{datamodel_name}': {len(dashboard_columns)} columns.")

        # Step 6: Identify used and unused columns
        dashboard_columns_set = set((entry['table'], entry['column']) for entry in dashboard_columns)

        # Add 'used' field as True if used in a dashboard, otherwise False
        for entry in all_columns:
            table_column_pair = (entry['table'], entry['column'])
            entry['used'] = True if table_column_pair in dashboard_columns_set else False

        # Log the total counts of used and unused columns
        used_columns_count = sum(1 for entry in all_columns if entry['used'])
        unused_columns_count = len(all_columns) - used_columns_count

        self.logger.info(f"Total used columns: {used_columns_count}")
        self.logger.info(f"Total unused columns: {unused_columns_count}")

        return all_columns

    def get_all_dashboard_shares(self):
        """
        Method to retrieve all dashboard shares, including user and group details for each shared dashboard.

        This method uses pagination to retrieve all dashboards and their share information, and it collects 
        corresponding user and group details for each share.

        Returns:
            list: A list of dictionaries containing the dashboard title, share type (user or group), and share name (email or group name).
        """
        limit = 50
        skip = 0
        dashboards = []
        
        self.logger.info("Starting to retrieve dashboard shares...")
        
        while True:
            self.logger.debug(f"Fetching dashboards with limit={limit}, skip={skip}")
            
            dashboard_response = self.api_client.post(
                '/api/v1/dashboards/searches', 
                data={
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
                }
            )
            
            if not dashboard_response or len(dashboard_response.get("items", [])) == 0:
                self.logger.info("No more dashboards found.")
                break
            else:
                dashboards.extend(dashboard_response["items"])
                skip += limit
                self.logger.debug(f"Retrieved {len(dashboard_response['items'])} dashboards, total so far: {len(dashboards)}")

        # Get all users
        self.logger.info("Fetching all users.")
        users = self.api_client.get('/api/v1/users')
        users_detail = [{"id": user["_id"], "email": user["email"]} for user in users]

        # Get all groups
        self.logger.info("Fetching all groups.")
        groups = self.api_client.get('/api/v1/groups')
        groups_detail = [{"id": group["_id"], "name": group["name"]} for group in groups]
        
        shared_list = []
        
        # Parse the dashboards to find shared users and groups
        self.logger.debug(f"Parsing {len(dashboards)} dashboards for shared users and groups.")
        for dashboard in dashboards:
            if dashboard.get("shares"):
                for share in dashboard["shares"]:
                    share_info = {"dashboard": dashboard["title"], "type": None, "name": None}
                    
                    if share["type"] == "user":
                        user = next((user for user in users_detail if user["id"] == share["shareId"]), None)
                        if user:
                            share_info["type"] = "user"
                            share_info["name"] = user["email"]
                    elif share["type"] == "group":
                        group = next((group for group in groups_detail if group["id"] == share["shareId"]), None)
                        if group:
                            share_info["type"] = "group"
                            share_info["name"] = group["name"]
                    
                    shared_list.append(share_info)
            else:
                # Add placeholder if there are no shares for the dashboard
                shared_list.append({
                    "dashboard": dashboard["title"],
                    "type": None,
                    "name": None
                })

        self.logger.info(f"Parsed {len(shared_list)} shared dashboards.")
        
        # Return the result as a list of dictionaries
        return shared_list


    def schedule_build():
        """
        Method to schedule a build for a dashboard.
        """
        pass
    









