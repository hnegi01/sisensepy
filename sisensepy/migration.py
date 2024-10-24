from sisensepy.api_client import APIClient
from sisensepy.access_management import AccessManagement

class Migration:
    def __init__(self, source_yaml, target_yaml, debug=False):
        """
        Initializes the Migration class with API clients and Access Management for both source and target environments.

        Parameters:
            source_yaml (str): Path to the YAML file for source environment configuration.
            target_yaml (str): Path to the YAML file for target environment configuration.
        """
        # Initialize API clients for both source and target environments
        self.source_client = APIClient(config_file=source_yaml, debug=debug)
        self.target_client = APIClient(config_file=target_yaml, debug=debug)

        # Initialize AccessManagement using the source client
        self.access_mgmt = AccessManagement(self.source_client, debug=debug)

        # Use the logger from the source client for consistency
        self.logger = self.source_client.logger


    def migrate_groups(self, group_name_list):
        """
        Migrates specific groups from the source environment to the target environment using the bulk endpoint.

        Parameters:
            group_name_list (list): A list of group names to migrate.

        Returns:
            list: A list of group migration results, including any errors encountered during the process.
        """
        self.logger.info("Starting group migration from source to target.")

        # Step 1: Get all groups from the source environment
        source_groups = self.source_client.get("/api/v1/groups")
        if not source_groups:
            self.logger.error("Failed to retrieve groups from the source environment.")
            print("Failed to retrieve groups from the source environment. Please check the logs for more details.")
            return []

        self.logger.info(f"Retrieved {len(source_groups)} groups from the source environment.")

        # Step 2: Filter the groups to migrate
        bulk_group_data = []
        for group in source_groups:
            if group["name"] in group_name_list:
                # Prepare group data excluding unnecessary fields
                group_data = {key: value for key, value in group.items() if key not in ["created", "lastUpdated", "tenantId", "_id"]}
                bulk_group_data.append(group_data)
                self.logger.debug(f"Prepared data for group: {group['name']}")

        # Step 3: Make the bulk POST request with the group data
        if bulk_group_data:
            self.logger.info(f"Sending bulk migration request for {len(bulk_group_data)} groups")
            response = self.target_client.post("/api/v1/groups/bulk", data=bulk_group_data)

            # Step 4: Handle the response from the bulk API call
            migration_results = []
            if response is not None:
                # Check for errors in the response
                if 'error' in response and response['error']['status'] == 400:
                    error_body = response['error']
                    error_code = error_body.get('code')

                    # Log and print the entire error response for better visibility
                    self.logger.error(f"Migration failed with error: {error_body}")
                    print(f"Error: {error_body}")

                    # Handle case where the groups already exist (code 4004)
                    if error_code == 4004:
                        existing_groups = error_body.get('moreInfo', {}).get('existingGroups', [])
                        for existing_group in existing_groups:
                            self.logger.warning(f"Group '{existing_group}' already exists in the target environment.")
                            migration_results.append({"name": existing_group, "status": "Already Exists"})
                            print(f"Warning: Group '{existing_group}' already exists.")
                else:
                    # Handle successful migration
                    for group_data in bulk_group_data:
                        self.logger.info(f"Successfully migrated group: {group_data['name']}")
                        migration_results.append({"name": group_data["name"], "status": "Success"})
                        print(f"Successfully migrated group: {group_data['name']}")
            else:
                self.logger.error("Bulk group migration failed. No response received.")
                migration_results = [{"name": group_data["name"], "status": "Failed"} for group_data in bulk_group_data]

            self.logger.info(f"Finished migrating groups. Successfully migrated {len([r for r in migration_results if r['status'] == 'Success'])} out of {len(group_name_list)} groups.")
            return migration_results
        else:
            self.logger.warning("No groups to migrate.")
            return []


    def migrate_all_groups(self):
        """
        Migrates all groups from the source environment to the target environment using the bulk endpoint.

        Returns:
            list: A list of group migration results, including any errors encountered during the process.
        """
        self.logger.info("Starting group migration from source to target.")

        # Step 1: Get all groups from the source environment
        source_groups = self.source_client.get("/api/v1/groups")
        if not source_groups:
            self.logger.error("Failed to retrieve groups from the source environment.")
            print("Failed to retrieve groups from the source environment. Please check the logs for more details.")
            return []

        self.logger.info(f"Retrieved {len(source_groups)} groups from the source environment.")

        # Step 2: Filter out specific groups
        bulk_group_data = []
        for group in source_groups:
            if group["name"] not in ["Admins", "All users in system", "Everyone"]:
                # Prepare group data excluding unnecessary fields
                group_data = {key: value for key, value in group.items() if key not in ["created", "lastUpdated", "tenantId", "_id"]}
                bulk_group_data.append(group_data)
                self.logger.debug(f"Prepared data for group: {group['name']}")

        # Step 3: Make the bulk POST request with the group data
        if bulk_group_data:
            self.logger.info(f"Sending bulk migration request for {len(bulk_group_data)} groups")
            response = self.target_client.post("/api/v1/groups/bulk", data=bulk_group_data)

            # Step 4: Handle the response from the bulk API call
            migration_results = []
            if response is not None:
                # Check for errors in the response
                if 'error' in response and response['error']['status'] == 400:
                    error_body = response['error']
                    error_code = error_body.get('code')

                    # Log and print the entire error response for better visibility
                    self.logger.error(f"Migration failed with error: {error_body}")
                    print(f"Error: {error_body}")

                    # Handle case where the groups already exist (code 4004)
                    if error_code == 4004:
                        existing_groups = error_body.get('moreInfo', {}).get('existingGroups', [])
                        for existing_group in existing_groups:
                            self.logger.warning(f"Group '{existing_group}' already exists in the target environment.")
                            migration_results.append({"name": existing_group, "status": "Already Exists"})
                            print(f"Warning: Group '{existing_group}' already exists.")
                else:
                    # Handle successful migration
                    for group_data in bulk_group_data:
                        self.logger.info(f"Successfully migrated group: {group_data['name']}")
                        migration_results.append({"name": group_data["name"], "status": "Success"})
                        print(f"Successfully migrated group: {group_data['name']}")
            else:
                self.logger.error("Bulk group migration failed. No response received.")
                migration_results = [{"name": group_data["name"], "status": "Failed"} for group_data in bulk_group_data]

            self.logger.info(f"Finished migrating groups. Successfully migrated {len([r for r in migration_results if r['status'] == 'Success'])} out of {len(source_groups)} groups.")
            return migration_results
        else:
            self.logger.warning("No groups to migrate.")
            return []


    def migrate_users(self, user_name_list):
        """
        Migrates specific users from the source environment to the target environment.

        Parameters:
            user_name_list (list): A list of user names to migrate.

        Returns:
            list: A list of user migration results, including any errors encountered during the process.
        """
        self.logger.info("Starting user migration from source to target.")

        # Query parameters to expand the response with group and role information
        params = {'expand': 'groups,role'}

        # Step 1: Get all users from the source environment
        source_users = self.source_client.get("/api/v1/users", params=params)
        if not source_users:
            self.logger.error("Failed to retrieve users from the source environment.")
            print("Failed to retrieve users from the source environment. Please check the logs for more details.")
            return []

        self.logger.info(f"Retrieved {len(source_users)} users from the source environment.")

        # Step 2: Get roles and groups information from the target environment to match and get IDs
        target_roles = self.target_client.get("/api/roles")
        target_groups = self.target_client.get("/api/v1/groups")

        EXCLUDED_GROUPS = {"Everyone", "All users in system"}
        single_user_data = []  # List to hold a single user data for migration

        # Step 3: Find and process the user based on the input list
        for user in source_users:
            if user["email"] in user_name_list:  # Match users by email
                # Construct the required payload for the user
                user_data = {
                    "email": user["email"],
                    "firstName": user["firstName"],
                    "lastName": user.get("lastName", ""),  # Optional field, ensure it's included even if missing
                    "roleId": next((role["_id"] for role in target_roles if role["name"] == user["role"]["name"]), None),  # Map role name to role ID
                    "groups": [group["_id"] for group in target_groups if group["name"] in [g["name"] for g in user["groups"]] and group["name"] not in EXCLUDED_GROUPS],  # Map group names to group IDs
                    "preferences": user.get("preferences", {"localeId": "en-US"})  # Default to English language preference if not provided
                }

                # Append user data to the list (even though it's a single user, for consistency with bulk migration)
                single_user_data.append(user_data)
                self.logger.debug(f"Prepared data for user: {user['email']}")
                break  # No need to continue if we already found the user

        # Step 4: Make the POST request with the single user data
        if single_user_data:
            self.logger.info(f"Sending migration request for user: {single_user_data[0]['email']}")
            response = self.target_client.post("/api/v1/users", data=single_user_data[0])  # Send only the single user

            # Step 5: Handle the response from the API call
            migration_results = []
            if response is not None:
                # Check if we have any errors with the status code 400 (Bad Request)
                if 'error' in response and response['error']['status'] == 400:
                    error_body = response['error']
                    error_code = error_body.get('code')

                    # Log and print the entire error response for better visibility
                    self.logger.error(f"Migration failed with error: {error_body}")
                    print(f"Error: {error_body}")

                    # Handle case where the user already exists (code 2004)
                    if error_code == 2004:
                        existing_users = error_body.get('moreInfo', {}).get('existingUsers', [])
                        for existing_user in existing_users:
                            self.logger.warning(f"User '{existing_user['email']}' already exists in the target environment.")
                            migration_results.append({"name": existing_user["email"], "status": "Already Exists"})
                            print(f"Warning: User '{existing_user['email']}' already exists.")
                else:
                    # Handle successful migration case
                    self.logger.info(f"Successfully migrated user: {single_user_data[0]['email']}")
                    migration_results.append({"name": single_user_data[0]["email"], "status": "Success"})
                    print(f"Successfully migrated user: {single_user_data[0]['email']}")
            else:
                self.logger.error(f"Failed to migrate user: {single_user_data[0]['email']}. No response received.")
                print(f"Failed to migrate user: {single_user_data[0]['email']}. No response received.")
                migration_results.append({"name": single_user_data[0]['email'], "status": "Failed"})

            return migration_results
        else:
            self.logger.warning("No users to migrate.")
            return []


    def migrate_all_users(self):
        """
        Migrates all users from the source environment to the target environment using the bulk endpoint.

        Returns:
            list: A list of user migration results, including any errors encountered during the process.
        """
        self.logger.info("Starting full user migration from source to target.")

        # Query parameters to expand the response with group and role information
        params = {'expand': 'groups,role'}

        # Step 1: Get all users from the source environment
        source_users = self.source_client.get("/api/v1/users", params=params)
        if not source_users:
            self.logger.error("Failed to retrieve users from the source environment.")
            print("Failed to retrieve users from the source environment. Please check the logs for more details.")
            return []

        self.logger.info(f"Retrieved {len(source_users)} users from the source environment.")

        # Step 2: Get roles and groups information from the target environment to match and get IDs
        self.logger.info("Retrieving roles and groups from the target environment.")
        target_roles = self.target_client.get("/api/roles")
        target_groups = self.target_client.get("/api/v1/groups")

        EXCLUDED_GROUPS = {"Everyone", "All users in system"}
        bulk_user_data = []  # List to hold the user data for bulk upload

        # Step 3: Process each user and prepare the payload for the bulk endpoint
        for user in source_users:
            # Construct the required payload for each user
            user_data = {
                "email": user["email"],
                "firstName": user["firstName"],
                "lastName": user.get("lastName", ""),  # Optional field, ensure it's included even if missing
                "roleId": next((role["_id"] for role in target_roles if role["name"] == user["role"]["name"]), None),  # Map role name to role ID
                "groups": [group["_id"] for group in target_groups if group["name"] in [g["name"] for g in user["groups"]] and group["name"] not in EXCLUDED_GROUPS],  # Map group names to group IDs
                "preferences": user.get("preferences", {"localeId": "en-US"})  # Default to English language preference if not provided
            }

            # Append user data to the bulk list
            bulk_user_data.append(user_data)
            self.logger.debug(f"Prepared data for user: {user['email']}")

        # Step 4: Make the bulk POST request with the user data
        if bulk_user_data:
            self.logger.info(f"Sending bulk migration request for {len(bulk_user_data)} users")
            response = self.target_client.post("/api/v1/users/bulk", data=bulk_user_data)

            # Step 5: Handle the response from the bulk API call
            migration_results = []
            if response is not None:
                # Check if we have any errors with the status code 400 (Bad Request)
                if 'error' in response and response['error']['status'] == 400:
                    error_body = response['error']
                    error_code = error_body.get('code')
                    existing_users = error_body.get('moreInfo', {}).get('existingUsers', [])

                    # Log and print the entire error response for better visibility
                    self.logger.error(f"Bulk migration failed with error: {error_body}")
                    print(f"Error: {error_body}")

                    # Handle case where one or more users already exist (code 2004)
                    if error_code == 2004 and existing_users:
                        for existing_user in existing_users:
                            self.logger.warning(f"User '{existing_user['email']}' already exists in the target environment.")
                            migration_results.append({"name": existing_user["email"], "status": "Already Exists"})
                            print(f"Warning: User '{existing_user['email']}' already exists.")

                else:
                    # Handle successful migration cases
                    for user_data in bulk_user_data:
                        self.logger.info(f"Successfully migrated user: {user_data['email']}")
                        migration_results.append({"name": user_data["email"], "status": "Success"})
                        print(f"Successfully migrated user: {user_data['email']}")
            else:
                self.logger.error("Bulk migration failed. No response received.")
                print("Bulk migration failed. No response received.")
                migration_results = [{"name": user_data["email"], "status": "Failed"} for user_data in bulk_user_data]

            self.logger.info(f"Finished migrating users. Successfully migrated {len([r for r in migration_results if r['status'] == 'Success'])} out of {len(source_users)} users.")
            return migration_results
        else:
            self.logger.warning("No users to migrate.")
            return []

    def migrate_dashboards(self, dashboard_ids=None, dashboard_names=None, action=None, republish=False, shares=False):
        """
        Migrates specific dashboards from the source environment to the target environment using the bulk endpoint.

        Parameters:
            dashboard_ids (list, optional): A list of dashboard IDs to migrate. Either `dashboard_ids` or `dashboard_names` must be provided.
            dashboard_names (list, optional): A list of dashboard names to migrate. Either `dashboard_ids` or `dashboard_names` must be provided.
            action (str, optional): Determines if the existing dashboard should be overwritten.
                                    Options: 'skip', 'overwrite', 'duplicate'.
                                    Default: None (no action passed to API).
            republish (bool, optional): Whether to republish dashboards after migration.
                                        Default: False.
            shares (bool, optional): Whether to also migrate the dashboard's shares. Default is False.

        Returns:
            dict: A summary of the migration results with lists of succeeded, skipped, and failed dashboards,
                and a count of successfully migrated and failed shares if `shares=True`.
        """
        shares_flag = shares

        if dashboard_ids and dashboard_names:
            raise ValueError("Please provide either 'dashboard_ids' or 'dashboard_names', not both.")
        
        self.logger.info("Starting dashboard migration from source to target.")

        # Step 1: Fetch dashboards based on provided parameters (IDs or names)
        bulk_dashboard_data = []
        if dashboard_ids:
            self.logger.info(f"Processing dashboard migration by IDs: {dashboard_ids}")
            for dashboard_id in dashboard_ids:
                # Make a request to export the dashboard by ID
                source_dashboard_response = self.source_client.get(f"/api/dashboards/{dashboard_id}/export?adminAccess=true")
                if source_dashboard_response.status_code == 200:
                    self.logger.debug(f"Dashboard with ID: {dashboard_id} retrieved successfully.")
                    bulk_dashboard_data.append(source_dashboard_response.json())
                else:
                    self.logger.error(f"Failed to export dashboard with ID: {dashboard_id}. Status Code: {source_dashboard_response.status_code}")

        elif dashboard_names:
            self.logger.debug("Fetching dashboards from the source environment using searches endpoint.")
            limit = 50
            skip = 0
            dashboards = []
            while True:
                self.logger.debug(f"Fetching dashboards (limit={limit}, skip={skip})")
                dashboard_response = self.source_client.post('/api/v1/dashboards/searches', data={
                    "queryParams": {"ownershipType": "allRoot", "search": "", "ownerInfo": True, "asObject": True},
                    "queryOptions": {"sort": {"title": 1}, "limit": limit, "skip": skip}
                })

                if dashboard_response.status_code != 200 or len(dashboard_response.json().get("items", [])) == 0:
                    self.logger.debug("No more dashboards found or failed to retrieve.")
                    break
                else:
                    dashboards.extend(dashboard_response.json()["items"])
                    skip += limit
            
            if not dashboards:
                self.logger.error("Failed to retrieve dashboards from the source environment.")
                return []

            self.logger.info(f"Retrieved {len(dashboards)} dashboards from the source environment.")

            # Step 2: Filter the dashboards to migrate
            self.logger.info("Filtering dashboards to migrate.")
            for dashboard in dashboards:
                if dashboard["title"] in dashboard_names:
                    # Make a request to export the dashboard by name
                    source_dashboard_response = self.source_client.get(f"/api/dashboards/{dashboard['oid']}/export?adminAccess=true")
                    if source_dashboard_response.status_code == 200:
                        self.logger.debug(f"Dashboard: {dashboard['title']} retrieved successfully.")
                        bulk_dashboard_data.append(source_dashboard_response.json())
                    else:
                        self.logger.error(f"Failed to export dashboard: {dashboard['title']} (ID: {dashboard['oid']}). Status Code: {source_dashboard_response.status_code}")
                else:
                    self.logger.debug(f"Skipping dashboard: {dashboard['title']}")

        # Step 3: Make the bulk POST request with the dashboard data
        if bulk_dashboard_data:
            url = f"/api/v1/dashboards/import/bulk?republish={str(republish).lower()}"
            if action:
                url += f"&action={action}"

            self.logger.info(f"Sending bulk migration request for {len(bulk_dashboard_data)} dashboards.")
            response = self.target_client.post(url, data=bulk_dashboard_data)

            # Step 3.1: Handle the migration results
            migration_summary = {'succeeded': [], 'skipped': [], 'failed': []}

            if response.status_code == 200:
                response_data = response.json()

                # Handle succeeded dashboards
                if 'succeeded' in response_data:
                    for dash in response_data['succeeded']:
                        migration_summary['succeeded'].append(dash['title'])
                        self.logger.info(f"Successfully migrated dashboard: {dash['title']}")

                # Handle skipped dashboards
                if 'skipped' in response_data:
                    for dash in response_data['skipped']:
                        migration_summary['skipped'].append(dash['title'])
                        self.logger.info(f"Skipped dashboard: {dash['title']}")

                # Handle failed dashboards
                if 'failed' in response_data:
                    for error in response_data['failed'].get('CannotOverwriteDashboard', []):
                        migration_summary['failed'].append(error['title'])
                        self.logger.warning(f"Failed to migrate dashboard: {error['title']} - {error['error']['message']}")
            else:
                self.logger.error(f"Bulk migration failed. Status Code: {response.status_code}")
                migration_summary['failed'].extend([dash['title'] for dash in bulk_dashboard_data])

        # Step 4: Handle shares if flag is set
        if shares_flag:
            self.logger.info("Processing shares for the migrated dashboards.")
            
            # Initialize counters for share success and failures
            share_success_count = 0
            share_fail_count = 0
            
            # Fetch source and target users/groups
            self.logger.debug("Fetching userIds from source system")
            source_user_ids = self.source_client.get("/api/v1/users")
            if source_user_ids.status_code == 200:
                source_user_ids = {user["email"]: user["_id"] for user in source_user_ids.json()}
            else:
                self.logger.error("Failed to retrieve user IDs from the source environment.")
                source_user_ids = {}

            self.logger.debug("Fetching userIds from target system")
            target_user_ids = self.target_client.get("/api/v1/users")
            if target_user_ids.status_code == 200:
                target_user_ids = {user["email"]: user["_id"] for user in target_user_ids.json()}
            else:
                self.logger.error("Failed to retrieve user IDs from the target environment.")
                target_user_ids = {}

            user_mapping = {source_user_ids[key]: target_user_ids.get(key, None) for key in source_user_ids}

            self.logger.debug("Fetching groups from source system")
            source_group_ids = self.source_client.get("/api/v1/groups")
            if source_group_ids.status_code == 200:
                source_group_ids = {group["name"]: group["_id"] for group in source_group_ids.json() if group["name"] not in ["Everyone", "All users in system"]}
            else:
                self.logger.error("Failed to retrieve group IDs from the source environment.")
                source_group_ids = {}

            self.logger.debug("Fetching groups from target system")
            target_group_ids = self.target_client.get("/api/v1/groups")
            if target_group_ids.status_code == 200:
                target_group_ids = {group["name"]: group["_id"] for group in target_group_ids.json() if group["name"] not in ["Everyone", "All users in system"]}
            else:
                self.logger.error("Failed to retrieve group IDs from the target environment.")
                target_group_ids = {}

            group_mapping = {source_group_ids[key]: target_group_ids.get(key, None) for key in source_group_ids}

            # Process shares for each migrated dashboard
            for dashboard in bulk_dashboard_data:
                self.logger.debug(f"Processing shares for dashboard: {dashboard['title']}")
                dashboard_id = dashboard['oid']
                dashboard_shares_response = self.source_client.get(f"/api/shares/dashboard/{dashboard_id}?adminAccess=true")
                if dashboard_shares_response.status_code == 200:
                    dashboard_shares = dashboard_shares_response.json()
                    new_shares = []
                    for share in dashboard_shares["sharesTo"]:
                        if share["type"] == "user":
                            new_share_user_id = user_mapping.get(share["shareId"], None)
                            if new_share_user_id:
                                new_shares.append({
                                    "shareId": new_share_user_id,
                                    "type": "user",
                                    "rule": share.get("rule", "owner"),
                                    "subscribe": share["subscribe"]
                                })
                        elif share["type"] == "group":
                            new_share_group_id = group_mapping.get(share["shareId"], None)
                            if new_share_group_id:
                                new_shares.append({
                                    "shareId": new_share_group_id,
                                    "type": "group",
                                    "rule": share["rule"],
                                    "subscribe": share["subscribe"]
                                })
                    
                    # Step 4.1: Post the new shares to the target dashboard
                    if new_shares:
                        response = self.target_client.post(f"/api/shares/dashboard/{dashboard_id}", data={"sharesTo": new_shares})
                        if response.status_code in [200, 201]:
                            self.logger.info(f"Dashboard '{dashboard['title']}' shares migrated successfully.")
                            share_success_count += 1
                        else:
                            self.logger.error(f"Failed to migrate shares for dashboard: {dashboard['title']}. Status Code: {response.status_code}")
                            share_fail_count += 1
                    else:
                        self.logger.warning(f"No valid shares found for dashboard: {dashboard['title']}. Make sure the users/groups exist in the target environment.")
                else:
                    self.logger.warning(f"No shares found for dashboard: {dashboard['title']}")

            # Log the final share migration summary
            self.logger.info(f"Shares migration completed. Success: {share_success_count}, Failed: {share_fail_count}")
            migration_summary['share_success_count'] = share_success_count
            migration_summary['share_fail_count'] = share_fail_count

        else:
            self.logger.info("Skipping shares migration since shares flag is set to False.")
        
        self.logger.info("Finished dashboard migration.")
        self.logger.info(migration_summary)
        
        return migration_summary


    def migrate_all_dashboards(self, action=None, republish=False, shares=False):
        """
        Migrates all dashboards from the source environment to the target environment using the bulk endpoint.

        Parameters:
            action (str, optional): Determines if the existing dashboard should be overwritten.
                                    Options: 'skip', 'overwrite', 'duplicate'.
                                    Default: None (no action passed to API).
            republish (bool, optional): Republish dashboards on the target server after copying 
                                        (only affects overwritten dashboards that were previously shared on the target server).
                                        Default: False.
            shares (bool, optional): Whether to also migrate the dashboard's shares. Default is False.

        Returns:
            dict: A summary of the migration results with lists of succeeded, skipped, and failed dashboards,
                and a count of successfully migrated and failed shares if `shares=True`.
        """
        self.logger.info("Starting full dashboard migration from source to target.")

        # Step 1: Fetch all dashboards from the source environment using searches endpoint
        self.logger.debug("Fetching all dashboards from the source environment.")
        limit = 50
        skip = 0
        dashboards = []
        
        while True:
            self.logger.debug(f"Fetching dashboards (limit={limit}, skip={skip})")
            dashboard_response = self.source_client.post('/api/v1/dashboards/searches', data={
                "queryParams": {"ownershipType": "allRoot", "search": "", "ownerInfo": True, "asObject": True},
                "queryOptions": {"sort": {"title": 1}, "limit": limit, "skip": skip}
            })

            if dashboard_response.status_code != 200 or len(dashboard_response.json().get("items", [])) == 0:
                self.logger.debug("No more dashboards found or failed to retrieve.")
                break
            else:
                dashboards.extend(dashboard_response.json()["items"])
                skip += limit

        if not dashboards:
            self.logger.error("Failed to retrieve dashboards from the source environment.")
            return {"succeeded": [], "skipped": [], "failed": []}
        
        self.logger.info(f"Retrieved {len(dashboards)} dashboards from the source environment.")

        # Step 2: Export dashboards for migration
        bulk_dashboard_data = []
        for dashboard in dashboards:
            source_dashboard_response = self.source_client.get(f"/api/dashboards/{dashboard['oid']}/export?adminAccess=true")
            if source_dashboard_response.status_code == 200:
                self.logger.debug(f"Dashboard: {dashboard['title']} retrieved successfully.")
                bulk_dashboard_data.append(source_dashboard_response.json())
            else:
                self.logger.error(f"Failed to export dashboard: {dashboard['title']} (ID: {dashboard['oid']}). Status Code: {source_dashboard_response.status_code if source_dashboard_response else 'No response'}")

        if not bulk_dashboard_data:
            self.logger.warning("No dashboards were successfully retrieved for migration.")
            return {"succeeded": [], "skipped": [], "failed": []}

        # Step 3: Make the bulk POST request with the dashboard data
        url = f"/api/v1/dashboards/import/bulk?republish={str(republish).lower()}"
        if action:
            url += f"&action={action}"

        self.logger.info(f"Sending bulk migration request for {len(bulk_dashboard_data)} dashboards.")
        response = self.target_client.post(url, data=bulk_dashboard_data)

        # Step 3.1: Handle the migration results
        migration_summary = {'succeeded': [], 'skipped': [], 'failed': []}

        if response.status_code == 200:
            response_data = response.json()

            # Handle succeeded dashboards
            if 'succeeded' in response_data:
                for dash in response_data['succeeded']:
                    migration_summary['succeeded'].append(dash['title'])
                    self.logger.info(f"Successfully migrated dashboard: {dash['title']}")

            # Handle skipped dashboards
            if 'skipped' in response_data:
                for dash in response_data['skipped']:
                    migration_summary['skipped'].append(dash['title'])
                    self.logger.info(f"Skipped dashboard: {dash['title']}")

            # Handle failed dashboards
            if 'failed' in response_data:
                for error in response_data['failed'].get('CannotOverwriteDashboard', []):
                    migration_summary['failed'].append(error['title'])
                    self.logger.warning(f"Failed to migrate dashboard: {error['title']} - {error['error']['message']}")
        else:
            self.logger.error(f"Bulk migration failed. Status Code: {response.status_code if response else 'No response received'}")
            migration_summary['failed'].extend([dash['title'] for dash in dashboards])

        # Step 4: Handle shares if flag is set
        if shares:
            self.logger.info("Processing shares for the migrated dashboards.")

            # Initialize counters for share success and failures
            share_success_count = 0
            share_fail_count = 0

            # Fetch source and target users/groups
            self.logger.debug("Fetching userIds from source system")
            source_user_ids = self.source_client.get("/api/v1/users")
            if source_user_ids.status_code == 200:
                source_user_ids = {user["email"]: user["_id"] for user in source_user_ids.json()}
            else:
                self.logger.error("Failed to retrieve user IDs from the source environment.")
                source_user_ids = {}

            self.logger.debug("Fetching userIds from target system")
            target_user_ids = self.target_client.get("/api/v1/users")
            if target_user_ids.status_code == 200:
                target_user_ids = {user["email"]: user["_id"] for user in target_user_ids.json()}
            else:
                self.logger.error("Failed to retrieve user IDs from the target environment.")
                target_user_ids = {}

            user_mapping = {source_user_ids[key]: target_user_ids.get(key, None) for key in source_user_ids}

            self.logger.debug("Fetching groups from source system")
            source_group_ids = self.source_client.get("/api/v1/groups")
            if source_group_ids.status_code == 200:
                source_group_ids = {group["name"]: group["_id"] for group in source_group_ids.json() if group["name"] not in ["Everyone", "All users in system"]}
            else:
                self.logger.error("Failed to retrieve group IDs from the source environment.")
                source_group_ids = {}

            self.logger.debug("Fetching groups from target system")
            target_group_ids = self.target_client.get("/api/v1/groups")
            if target_group_ids.status_code == 200:
                target_group_ids = {group["name"]: group["_id"] for group in target_group_ids.json() if group["name"] not in ["Everyone", "All users in system"]}
            else:
                self.logger.error("Failed to retrieve group IDs from the target environment.")
                target_group_ids = {}

            group_mapping = {source_group_ids[key]: target_group_ids.get(key, None) for key in source_group_ids}

            # Process shares for each migrated dashboard
            for dashboard in bulk_dashboard_data:
                self.logger.debug(f"Processing shares for dashboard: {dashboard['title']}")
                dashboard_id = dashboard['oid']
                dashboard_shares_response = self.source_client.get(f"/api/shares/dashboard/{dashboard_id}?adminAccess=true")
                if dashboard_shares_response.status_code == 200:
                    dashboard_shares = dashboard_shares_response.json()
                    new_shares = []
                    for share in dashboard_shares["sharesTo"]:
                        if share["type"] == "user":
                            new_share_user_id = user_mapping.get(share["shareId"], None)
                            if new_share_user_id:
                                new_shares.append({
                                    "shareId": new_share_user_id,
                                    "type": "user",
                                    "rule": share.get("rule", "owner"),
                                    "subscribe": share["subscribe"]
                                })
                        elif share["type"] == "group":
                            new_share_group_id = group_mapping.get(share["shareId"], None)
                            if new_share_group_id:
                                new_shares.append({
                                    "shareId": new_share_group_id,
                                    "type": "group",
                                    "rule": share["rule"],
                                    "subscribe": share["subscribe"]
                                })
                    
                    # Step 4.1: Post the new shares to the target dashboard
                    if new_shares:
                        response = self.target_client.post(f"/api/shares/dashboard/{dashboard_id}", data={"sharesTo": new_shares})
                        if response.status_code in [200, 201]:
                            self.logger.info(f"Dashboard '{dashboard['title']}' shares migrated successfully.")
                            share_success_count += 1
                        else:
                            self.logger.error(f"Failed to migrate shares for dashboard: {dashboard['title']}. Status Code: {response.status_code}")
                            share_fail_count += 1
                    else:
                        self.logger.warning(f"No valid shares found for dashboard: {dashboard['title']}. Make sure the users/groups exist in the target environment.")
                else:
                    self.logger.warning(f"No shares found for dashboard: {dashboard['title']}")

            # Log the final share migration summary
            self.logger.info(f"Shares migration completed. Success: {share_success_count}, Failed: {share_fail_count}")
            migration_summary['share_success_count'] = share_success_count
            migration_summary['share_fail_count'] = share_fail_count

        else:
            self.logger.info("Skipping shares migration since shares flag is set to False.")

        self.logger.info("Finished full dashboard migration.")
        self.logger.info(migration_summary)

        return migration_summary


    def migrate_datamodels(self, datamodel_ids=None, datamodel_names=None, dependencies=None, shares=False):
        """
        Migrates specific data models from the source environment to the target environment.

        Parameters:
            datamodel_ids (list, optional): A list of data model IDs to migrate. Either `datamodel_ids` or `datamodel_names` must be provided.
            datamodel_names (list, optional): A list of data model names to migrate. Either `datamodel_ids` or `datamodel_names` must be provided.
            dependencies (list, optional): A list of dependencies to include in the migration. If not provided or if 'all' is passed, all dependencies are selected by default. 
                                        Possible values for `dependencies` are:
                                        - "dataSecurity" (includes both Data Security and Scope Configuration)
                                        - "formulas" (for Formulas)
                                        - "hierarchies" (for Drill Hierarchies)
                                        - "perspectives" (for Perspectives)
                                        If left blank or set to "all", all dependencies are included by default.
            shares (bool, optional): Whether to also migrate the data model's shares. Default is False.

        Returns:
            dict: A summary of the migration results with lists of succeeded, skipped, and failed data models.
        """
        # Mapping user-friendly terms to API parameters
        dependency_mapping = {
            "dataSecurity": ["dataContext", "scopeConfiguration"],
            "formulas": ["formulaManagement"],
            "hierarchies": ["drillHierarchies"],
            "perspectives": ["perspectives"]
        }

        # Handle if 'all' is passed or dependencies are not provided (None)
        if dependencies is None or dependencies == "all":
            dependencies = ["dataSecurity", "formulas", "hierarchies", "perspectives"]

        # Convert user-friendly dependencies to API-compatible parameters
        api_dependencies = []
        for dep in dependencies:
            if dep in dependency_mapping:
                api_dependencies.extend(dependency_mapping[dep])

        # Ensure unique values and format for the API call
        api_dependencies = list(set(api_dependencies))  # Remove duplicates

        # Validate input parameters
        if datamodel_ids and datamodel_names:
            raise ValueError("Please provide either 'datamodel_ids' or 'datamodel_names', not both.")

        self.logger.info("Starting data model migration from source to target.")

        # Initialize migration summary
        migration_summary = {'succeeded': [], 'failed': []}
        success_count = 0
        fail_count = 0

        # Fetch data models based on provided parameters (IDs or names)
        all_datamodel_data = []
        if datamodel_ids:
            self.logger.info(f"Processing data model migration by IDs: {datamodel_ids}")
            for datamodel_id in datamodel_ids:
                source_datamodel_response = self.source_client.get(
                    f"/api/v2/datamodel-exports/schema?datamodelId={datamodel_id}&type=schema-latest&dependenciesIdsToInclude={','.join(api_dependencies)}"
                )
                if source_datamodel_response.status_code == 200:
                    self.logger.debug(f"Data model with ID: {datamodel_id} retrieved successfully.")
                    all_datamodel_data.append(source_datamodel_response.json())
                else:
                    self.logger.error(f"Failed to export data model with ID: {datamodel_id}")
        elif datamodel_names:
            self.logger.debug("Fetching data models from the source environment using searches endpoint.")
            datamodel_response = self.source_client.get("/api/v2/datamodels/schema")
            if datamodel_response.status_code != 200:
                self.logger.error("Failed to retrieve data models from the source environment.")
                return migration_summary
            self.logger.info(f"Retrieved {len(datamodel_response.json())} data models from the source environment.")

            # Filter the data models to migrate
            for datamodel in datamodel_response.json():
                if datamodel["title"] in datamodel_names:
                    source_datamodel_response = self.source_client.get(
                        f"/api/v2/datamodel-exports/schema?datamodelId={datamodel['oid']}&type=schema-latest&dependenciesIdsToInclude={','.join(api_dependencies)}"
                    )
                    if source_datamodel_response.status_code == 200:
                        self.logger.debug(f"Data model: {datamodel['title']} retrieved successfully.")
                        all_datamodel_data.append(source_datamodel_response.json())
                    else:
                        self.logger.error(f"Failed to export data model: {datamodel['title']} (ID: {datamodel['oid']})")

        # Migrate each data model one by one
        if all_datamodel_data:
            self.logger.info(f"Migrating '{len(all_datamodel_data)}' datamodels one by one to the target environment.")
            successfully_migrated_datamodels = []
            for data_model in all_datamodel_data:
                response = self.target_client.post(f"/api/v2/datamodel-imports/schema", data=data_model)
                if response.status_code == 201:  # Successful response
                    self.logger.info(f"Successfully migrated data model: {data_model['title']}")
                    migration_summary['succeeded'].append(data_model['title'])
                    successfully_migrated_datamodels.append(data_model)
                    success_count += 1
                else:
                    error_message = response.json().get("detail", "Unknown error")
                    self.logger.error(f"Failed to migrate data model: {data_model['title']}. Error: {error_message}")
                    migration_summary['failed'].append(data_model['title'])
                    fail_count += 1
        else:
            self.logger.warning("No data models were successfully retrieved for migration.")
            return migration_summary

        # Final logging for data model migration success and failure counts
        self.logger.info(f"Data model migration completed. Success: {success_count}, Failed: {fail_count}")

        # Handle shares if the flag is set
        if shares:
            self.logger.info("Processing shares for the migrated datamodels.")

            # Fetch source and target users/groups
            self.logger.debug("Fetching userIds from source system")
            source_user_ids = self.source_client.get("/api/v1/users")
            if source_user_ids.status_code == 200:
                source_user_ids = {user["email"]: user["_id"] for user in source_user_ids.json()}
            else:
                self.logger.error("Failed to retrieve user IDs from the source environment.")
                source_user_ids = {}

            self.logger.debug("Fetching userIds from target system")
            target_user_ids = self.target_client.get("/api/v1/users")
            if target_user_ids.status_code == 200:
                target_user_ids = {user["email"]: user["_id"] for user in target_user_ids.json()}
            else:
                self.logger.error("Failed to retrieve user IDs from the target environment.")
                target_user_ids = {}

            user_mapping = {source_user_ids[key]: target_user_ids.get(key, None) for key in source_user_ids}

            self.logger.debug("Fetching groups from source system")
            source_group_ids = self.source_client.get("/api/v1/groups")
            if source_group_ids.status_code == 200:
                source_group_ids = {group["name"]: group["_id"] for group in source_group_ids.json() if group["name"] not in ["Everyone", "All users in system"]}
            else:
                self.logger.error("Failed to retrieve group IDs from the source environment.")
                source_group_ids = {}

            self.logger.debug("Fetching groups from target system")
            target_group_ids = self.target_client.get("/api/v1/groups")
            if target_group_ids.status_code == 200:
                target_group_ids = {group["name"]: group["_id"] for group in target_group_ids.json() if group["name"] not in ["Everyone", "All users in system"]}
            else:
                self.logger.error("Failed to retrieve group IDs from the target environment.")
                target_group_ids = {}

            group_mapping = {source_group_ids[key]: target_group_ids.get(key, None) for key in source_group_ids}

            # Proceed with share logic for successfully migrated datamodels
            share_success_count = 0
            share_fail_count = 0
            if successfully_migrated_datamodels:
                for datamodel in successfully_migrated_datamodels:
                    datamodel_id = datamodel['oid']
                    if datamodel["type"] == "extract":
                        datamodel_shares_response = self.source_client.get(f"/api/elasticubes/localhost/{datamodel['title']}/permissions")
                        datamodel_shares = datamodel_shares_response.json().get("shares", []) if datamodel_shares_response.status_code == 200 else []
                    elif datamodel["type"] == "live":
                        datamodel_shares_response = self.source_client.get(f"/api/v1/elasticubes/live/{datamodel_id}/permissions")
                        datamodel_shares = datamodel_shares_response.json() if datamodel_shares_response.status_code == 200 else []
                    else:
                        self.logger.warning(f"Unknown datamodel type for: {datamodel['title']}")
                        continue

                    # Process the shares if they exist
                    if datamodel_shares:
                        new_shares = []
                        for share in datamodel_shares:
                            if share["type"] == "user":
                                new_share_user_id = user_mapping.get(share["partyId"], None)
                                if new_share_user_id:
                                    new_shares.append({
                                        "partyId": new_share_user_id,
                                        "type": "user",
                                        "permission": share.get("permission", "a"),
                                    })
                            elif share["type"] == "group":
                                new_share_group_id = group_mapping.get(share["partyId"], None)
                                if new_share_group_id:
                                    new_shares.append({
                                        "partyId": new_share_group_id,
                                        "type": "group",
                                        "permission": share.get("permission", "a"),
                                    })

                        # Post the new shares to the target datamodel
                        if new_shares:
                            if datamodel["type"] == "extract":
                                response = self.target_client.put(f"/api/elasticubes/localhost/{datamodel['title']}/permissions", data=new_shares)
                            elif datamodel["type"] == "live":
                                response = self.target_client.patch(f"/api/v1/elasticubes/live/{datamodel_id}/permissions", data=new_shares)

                            if response.status_code in [200, 201]:
                                self.logger.info(f"Datamodel '{datamodel['title']}' shares migrated successfully.")
                                share_success_count += 1
                            else:
                                self.logger.error(f"Failed to migrate shares for datamodel: {datamodel['title']}. Error: {response.json() if response else 'No response received.'}")
                                share_fail_count += 1
                        else:
                            self.logger.warning(f"No valid shares found for datamodel: {datamodel['title']}.")

            # Log the final share migration summary
            self.logger.info(f"Shares migration completed. Success: {share_success_count}, Failed: {share_fail_count}")
            migration_summary['share_success_count'] = share_success_count
            migration_summary['share_fail_count'] = share_fail_count
        else:
            self.logger.info("Skipping shares migration since shares flag is set to False.")

        # Final log for the entire migration process
        self.logger.info("Finished data model migration.")
        self.logger.info(migration_summary)

        return migration_summary


    def migrate_all_datamodels(self, dependencies=None, shares=False):
        """
        Migrates all data models from the source environment to the target environment.

        Parameters:
            dependencies (list, optional): A list of dependencies to include in the migration. If not provided or if 'all' is passed, all dependencies are selected by default.
                                        Possible values for `dependencies` are:
                                        - "dataSecurity" (includes both Data Security and Scope Configuration)
                                        - "formulas" (for Formulas)
                                        - "hierarchies" (for Drill Hierarchies)
                                        - "perspectives" (for Perspectives)
                                        If left blank or set to "all", all dependencies are included by default.
            shares (bool, optional): Whether to also migrate the data model's shares. Default is False.

        Returns:
            dict: A summary of the migration results with lists of succeeded, skipped, and failed data models.
        """
        # Mapping user-friendly terms to API parameters
        dependency_mapping = {
            "dataSecurity": ["dataContext", "scopeConfiguration"],
            "formulas": ["formulaManagement"],
            "hierarchies": ["drillHierarchies"],
            "perspectives": ["perspectives"]
        }

        # Handle if 'all' is passed or dependencies are not provided (None)
        if dependencies is None or dependencies == "all":
            dependencies = ["dataSecurity", "formulas", "hierarchies", "perspectives"]

        # Convert user-friendly dependencies to API-compatible parameters
        api_dependencies = []
        for dep in dependencies:
            if dep in dependency_mapping:
                api_dependencies.extend(dependency_mapping[dep])

        # Ensure unique values and format for the API call
        api_dependencies = list(set(api_dependencies))  # Remove duplicates

        self.logger.info("Starting full data model migration from source to target.")

        # Initialize migration summary
        migration_summary = {'succeeded': [], 'failed': []}
        success_count = 0
        fail_count = 0

        # Step 1: Fetch all data models from the source environment
        self.logger.debug("Fetching all data models from the source environment.")
        datamodel_response = self.source_client.get("/api/v2/datamodels/schema")
        if datamodel_response.status_code != 200:
            self.logger.error("Failed to retrieve data models from the source environment.")
            return migration_summary

        datamodels = datamodel_response.json()
        self.logger.info(f"Retrieved {len(datamodels)} data models from the source environment.")

        # Step 2: Export data models for migration
        all_datamodel_data = []
        for datamodel in datamodels:
            source_datamodel_response = self.source_client.get(
                f"/api/v2/datamodel-exports/schema?datamodelId={datamodel['oid']}&type=schema-latest&dependenciesIdsToInclude={','.join(api_dependencies)}"
            )
            if source_datamodel_response.status_code == 200:
                self.logger.debug(f"Data model: {datamodel['title']} retrieved successfully.")
                all_datamodel_data.append(source_datamodel_response.json())
            else:
                self.logger.error(f"Failed to export data model: {datamodel['title']} (ID: {datamodel['oid']})")

        # Step 3: Migrate each data model one by one
        if all_datamodel_data:
            self.logger.info(f"Migrating '{len(all_datamodel_data)}' datamodels one by one to the target environment.")
            successfully_migrated_datamodels = []
            for data_model in all_datamodel_data:
                response = self.target_client.post(f"/api/v2/datamodel-imports/schema", data=data_model)
                if response.status_code == 201:  # Successful response
                    self.logger.info(f"Successfully migrated data model: {data_model['title']}")
                    migration_summary['succeeded'].append(data_model['title'])
                    successfully_migrated_datamodels.append(data_model)
                    success_count += 1
                else:
                    error_message = response.json().get("detail", "Unknown error")
                    self.logger.error(f"Failed to migrate data model: {data_model['title']}. Error: {error_message}")
                    migration_summary['failed'].append(data_model['title'])
                    fail_count += 1
        else:
            self.logger.warning("No data models were successfully retrieved for migration.")
            return migration_summary

        # Step 4: Handle shares if the flag is set
        if shares:
            self.logger.info("Processing shares for the migrated datamodels.")

            # Fetch source and target users/groups
            self.logger.debug("Fetching userIds from source system")
            source_user_ids = self.source_client.get("/api/v1/users")
            if source_user_ids.status_code == 200:
                source_user_ids = {user["email"]: user["_id"] for user in source_user_ids.json()}
            else:
                self.logger.error("Failed to retrieve user IDs from the source environment.")
                source_user_ids = {}

            self.logger.debug("Fetching userIds from target system")
            target_user_ids = self.target_client.get("/api/v1/users")
            if target_user_ids.status_code == 200:
                target_user_ids = {user["email"]: user["_id"] for user in target_user_ids.json()}
            else:
                self.logger.error("Failed to retrieve user IDs from the target environment.")
                target_user_ids = {}

            user_mapping = {source_user_ids[key]: target_user_ids.get(key, None) for key in source_user_ids}

            self.logger.debug("Fetching groups from source system")
            source_group_ids = self.source_client.get("/api/v1/groups")
            if source_group_ids.status_code == 200:
                source_group_ids = {group["name"]: group["_id"] for group in source_group_ids.json() if group["name"] not in ["Everyone", "All users in system"]}
            else:
                self.logger.error("Failed to retrieve group IDs from the source environment.")
                source_group_ids = {}

            self.logger.debug("Fetching groups from target system")
            target_group_ids = self.target_client.get("/api/v1/groups")
            if target_group_ids.status_code == 200:
                target_group_ids = {group["name"]: group["_id"] for group in target_group_ids.json() if group["name"] not in ["Everyone", "All users in system"]}
            else:
                self.logger.error("Failed to retrieve group IDs from the target environment.")
                target_group_ids = {}

            group_mapping = {source_group_ids[key]: target_group_ids.get(key, None) for key in source_group_ids}

            # Proceed with share logic for successfully migrated datamodels
            share_success_count = 0
            share_fail_count = 0
            if successfully_migrated_datamodels:
                for datamodel in successfully_migrated_datamodels:
                    datamodel_id = datamodel['oid']
                    if datamodel["type"] == "extract":
                        datamodel_shares_response = self.source_client.get(f"/api/elasticubes/localhost/{datamodel['title']}/permissions")
                        datamodel_shares = datamodel_shares_response.json().get("shares", []) if datamodel_shares_response.status_code == 200 else []
                    elif datamodel["type"] == "live":
                        datamodel_shares_response = self.source_client.get(f"/api/v1/elasticubes/live/{datamodel_id}/permissions")
                        datamodel_shares = datamodel_shares_response.json() if datamodel_shares_response.status_code == 200 else []
                    else:
                        self.logger.warning(f"Unknown datamodel type for: {datamodel['title']}")
                        continue

                    # Process the shares if they exist
                    if datamodel_shares:
                        new_shares = []
                        for share in datamodel_shares:
                            if share["type"] == "user":
                                new_share_user_id = user_mapping.get(share["partyId"], None)
                                if new_share_user_id:
                                    new_shares.append({
                                        "partyId": new_share_user_id,
                                        "type": "user",
                                        "permission": share.get("permission", "a"),
                                    })
                            elif share["type"] == "group":
                                new_share_group_id = group_mapping.get(share["partyId"], None)
                                if new_share_group_id:
                                    new_shares.append({
                                        "partyId": new_share_group_id,
                                        "type": "group",
                                        "permission": share.get("permission", "a"),
                                    })

                        # Post the new shares to the target datamodel
                        if new_shares:
                            if datamodel["type"] == "extract":
                                response = self.target_client.put(f"/api/elasticubes/localhost/{datamodel['title']}/permissions", data=new_shares)
                            elif datamodel["type"] == "live":
                                response = self.target_client.patch(f"/api/v1/elasticubes/live/{datamodel_id}/permissions", data=new_shares)

                            if response.status_code in [200, 201]:
                                self.logger.info(f"Datamodel '{datamodel['title']}' shares migrated successfully.")
                                share_success_count += 1
                            else:
                                self.logger.error(f"Failed to migrate shares for datamodel: {datamodel['title']}. Error: {response.json() if response else 'No response received.'}")
                                share_fail_count += 1
                        else:
                            self.logger.warning(f"No valid shares found for datamodel: {datamodel['title']}.")

            # Log the final share migration summary
            self.logger.info(f"Shares migration completed. Success: {share_success_count}, Failed: {share_fail_count}")
            migration_summary['share_success_count'] = share_success_count
            migration_summary['share_fail_count'] = share_fail_count
        else:
            self.logger.info("Skipping shares migration since shares flag is set to False.")

        # Final log for the entire migration process
        self.logger.info("Finished full data model migration.")
        self.logger.info(migration_summary)

        return migration_summary
