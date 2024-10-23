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

    def migrate_dashboards(self, dashboard_name_list, action=None, republish=False, shares=False):
        """
        Migrates specific dashboards from the source environment to the target environment using the bulk endpoint.

        Parameters:
            dashboard_name_list (list): A list of dashboard names to migrate.
            action (str, optional): Determines if the existing dashboard should be overwritten.
                                    Options: 'skip', 'overwrite', 'duplicate'.
                                    Default: None (no action passed to API).
            republish (bool, optional): Whether to republish dashboards after migration.
                                        Default: False.
            shares (bool): Whether to also migrate the dashboard's shares. Default is False.

        Returns:
            dict: A summary of the migration results with lists of succeeded, skipped, and failed dashboards.
        """
        self.logger.info("Starting dashboard migration from source to target.")
        
        # Step 1: Get all dashboards from the source environment
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

            if not dashboard_response or len(dashboard_response.get("items", [])) == 0:
                self.logger.debug("No more dashboards found.")
                break
            else:
                dashboards.extend(dashboard_response["items"])
                skip += limit
        
        if not dashboards:
            self.logger.error("Failed to retrieve dashboards from the source environment.")
            print("Failed to retrieve dashboards from the source environment. Please check the logs for more details.")
            return []
        
        self.logger.info(f"Retrieved {len(dashboards)} dashboards from the source environment.")

        # Step 2: Filter the dashboards to migrate
        self.logger.info("Filtering dashboards to migrate.")
        bulk_dashboard_data = []
        for dashboard in dashboards:
            if dashboard["title"] in dashboard_name_list:
                # Make a request to export the dashboard
                source_dashboard_response = self.source_client.get(f"/api/dashboards/{dashboard['oid']}/export?adminAccess=true")
                
                # Check if the response is valid
                if source_dashboard_response:
                    self.logger.debug(f"Dashboard: {dashboard['title']} retrieved successfully.")
                    bulk_dashboard_data.append(source_dashboard_response)
                else:
                    self.logger.error(f"Failed to export dashboard: {dashboard['title']} (ID: {dashboard['oid']})")
            else:
                self.logger.debug(f"Skipping dashboard: {dashboard['title']}")

        self.logger.info(f"Prepared {len(bulk_dashboard_data)} dashboards for migration.")

        # Step 3: Share the migrated dashboards with the same users as the source dashboards
        if shares:
            self.logger.info("Processing shares from the source dashboards.")
            self.logger.debug("Fetching userIds from source system")
            source_user_ids = self.source_client.get("/api/v1/users")
            if source_user_ids:
                source_user_ids = {user["email"]: user["_id"] for user in source_user_ids}
            else:
                self.logger.error("Failed to retrieve user IDs from the source environment.")
                source_user_ids = {}
            #print(source_user_ids)
            self.logger.debug("Fetching userIds from target system")
            target_user_ids = self.target_client.get("/api/v1/users")
            if target_user_ids:
                target_user_ids = {user["email"]: user["_id"] for user in target_user_ids}
            else:
                self.logger.error("Failed to retrieve user IDs from the target environment.")
                target_user_ids = {}
            #print(target_user_ids)
            user_mapping = {source_user_ids[key]: target_user_ids.get(key, None) for key in source_user_ids}
            #print(user_mapping)
            self.logger.debug("Fetching groups from source system")
            source_group_ids = self.source_client.get("/api/v1/groups")
            if source_group_ids:
                source_group_ids = {group["name"]: group["_id"] for group in source_group_ids if group["name"] not in ["Everyone", "All users in system"]}
            else:
                self.logger.error("Failed to retrieve group IDs from the source environment.")
                source_group_ids = {}
            #print(source_group_ids)
            self.logger.debug("Fetching groups from target system")
            target_group_ids = self.target_client.get("/api/v1/groups")
            if target_group_ids:
                target_group_ids = {group["name"]: group["_id"] for group in target_group_ids if group["name"] not in ["Everyone", "All users in system"]}
            else:
                self.logger.error("Failed to retrieve group IDs from the target environment.")
                target_group_ids = {}
            #print(target_group_ids)
            group_mapping = {source_group_ids[key]: target_group_ids.get(key, None) for key in source_group_ids}
            #print(group_mapping)
            for dashboard in bulk_dashboard_data:
                    self.logger.debug(f"Processing shares for dashboard: {dashboard['title']}")
                    dashboard_id = dashboard['oid']
                    shares = self.source_client.get(f"/api/shares/dashboard/{dashboard_id}?adminAccess=true")
                    if shares:
                        print(shares)
                        new_shares = []
                        for share in shares["sharesTo"]:
                            if share["type"] == "user":
                                new_share_user_id = user_mapping.get(share["shareId"], None)
                                if new_share_user_id:  # Only append if a valid mapping exists
                                    share={
                                        "rule": share.get("rule", "owner"), ### NEED TO CHECK ##################
                                        "shareId": new_share_user_id,
                                        "subscribe": share["subscribe"],
                                        "type": share["type"]
                                    }
                                    new_shares.append(share)
                                else:
                                    self.logger.warning(f"User shareId '{share['shareId']}' not found in user_mapping.")
                            elif share["type"] == "group":
                                new_share_group_id = group_mapping.get(share["shareId"], None)
                                if new_share_group_id:  # Only append if a valid mapping exists
                                    share={
                                        "rule": share["rule"],
                                        "shareId": new_share_group_id,
                                        "subscribe": share["subscribe"],
                                        "type": share["type"]
                                    }
                                    new_shares.append(share)
                                else:
                                    self.logger.warning(f"Group shareId '{share['shareId']}' not found in group_mapping.")
                            else:
                                continue  # Skip any other types of shares
                        print(new_shares)
                    break

                            


        #             dashboard_id = dashboard['oid']
        #             shares = self.source_client.get(f"/api/v1/dashboards/{dashboard_id}/shares")
        #             if shares:
        #                 for share in shares:
        #                     share_data = {
        #                         "entityType": share["entityType"],
        #                         "entityId": share["entityId"],
        #                         "permission": share["permission"],
        #                         "shareType": share["shareType"]
        #                     }
        #                     self.target_client.post(f"/api/v1/dashboards/{dashboard_id}/shares", data=share_data)
        #                     self.logger.info(f"Shared dashboard '{dashboard['title']}' with '{share['entityType']}' ID: '{share['entityId']}'")
        #             else:
        #                 self.logger.warning(f"No shares found for dashboard: {dashboard['title']}")
        #     self.logger.info("Finished processing shares.")
        # else:
        #     self.logger.info("Skipping shares migration.")

        # # Step 4: Make the bulk POST request with the dashboard data
        # if bulk_dashboard_data:
        #     url = f"/api/v1/dashboards/import/bulk?republish={str(republish).lower()}"
        #     if action:
        #         url += f"&action={action}"

        #     self.logger.info(f"Sending bulk migration request for {len(bulk_dashboard_data)} dashboards.")
        #     response = self.target_client.post(url, data=bulk_dashboard_data)
            
        #     # Step 3.1: Handle the migration results
        #     migration_summary = {'succeeded': [], 'skipped': [], 'failed': []}

        #     if response:
        #         # Handle succeeded dashboards
        #         if 'succeded' in response:
        #             for dash in response['succeded']:
        #                 migration_summary['succeeded'].append(dash['title'])
        #                 self.logger.info(f"Successfully migrated dashboard: {dash['title']}")

        #         # Handle skipped dashboards
        #         if 'skipped' in response:
        #             for dash in response['skipped']:
        #                 migration_summary['skipped'].append(dash['title'])
        #                 self.logger.info(f"Skipped dashboard: {dash['title']}")

        #         # Handle failed dashboards
        #         if 'failed' in response:
        #             for error in response['failed'].get('CannotOverwriteDashboard', []):
        #                 migration_summary['failed'].append(error['title'])
        #                 self.logger.warning(f"Failed to migrate dashboard: {error['title']} - {error['error']['message']}")
        #                 print(f"Dashboard '{error['title']}' failed to migrate. Error: {error['error']['message']}. "
        #                     f"To resolve this, provide an appropriate action (skip, overwrite, or duplicate) as the 'action' argument in the 'migrate_dashboards' method.")

        #     else:
        #         self.logger.error("Bulk migration failed. No response received.")
        #         migration_summary['failed'].extend([dash['title'] for dash in bulk_dashboard_data])

        #     return migration_summary
        # else:
        #     self.logger.warning("No dashboards to migrate.")
        #     #return {'succeeded': [], 'skipped': [], 'failed': []}

