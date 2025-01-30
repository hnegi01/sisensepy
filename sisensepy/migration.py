"""
The migration.py module handles the migration of users, groups, dashboards, and data models between Sisense environments.

Methods:
- __init__(): Initializes the Migration class with API clients for source and target environments.
- migrate_groups(): Migrates specific groups from the source to the target environment.
- migrate_all_groups(): Migrates all groups from the source to the target environment.
- migrate_users(): Migrates specific users from the source to the target environment.
- migrate_all_users(): Migrates all users from the source to the target environment.
- migrate_dashboard_shares(): Migrates shares for specific dashboards between environments.
- migrate_dashboards(): Migrates specific dashboards with options for republishing, ownership transfer, and share migration.
- migrate_all_dashboards(): Migrates all dashboards in batches with configurable processing options.
- migrate_datamodels(): Migrates specific data models along with dependencies and shares.
- migrate_all_datamodels(): Migrates all data models in batches, supporting dependency selection and share migration.
"""

from sisensepy.api_client import APIClient
from sisensepy.access_management import AccessManagement
import time

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
        self.logger.debug("Fetching groups from the source environment.")
        source_response = self.source_client.get("/api/v1/groups")
        if not source_response or source_response.status_code != 200:
            self.logger.error("Failed to retrieve groups from the source environment.")
            print("Failed to retrieve groups from the source environment. Please check the logs for more details.")
            return []

        # Log the full response at debug level
        self.logger.debug(f"Source environment response status code: {source_response.status_code}")
        self.logger.debug(f"Source environment response body: {source_response.text}")

        source_groups = source_response.json()
        self.logger.info(f"Retrieved {len(source_groups)} groups from the source environment.")

        # Step 2: Filter the groups to migrate
        bulk_group_data = []
        for group in source_groups:
            if group["name"] in group_name_list:
                # Prepare group data excluding unnecessary fields
                group_data = {
                    key: value for key, value in group.items()
                    if key not in ["created", "lastUpdated", "tenantId", "_id"]
                }
                bulk_group_data.append(group_data)
                self.logger.debug(f"Prepared data for group: {group['name']}")

        # If no groups match, log an info message and exit early
        if not bulk_group_data:
            self.logger.info("No matching groups found for migration. Ending process.")
            return [{"message": "No matching groups found for migration. Ending process. Please verify the group names and try again."}]


        # Step 3: Make the bulk POST request with the group data
        self.logger.info(f"Sending bulk migration request for {len(bulk_group_data)} groups")
        self.logger.debug(f"Payload for bulk migration: {bulk_group_data}")
        response = self.target_client.post("/api/v1/groups/bulk", data=bulk_group_data)

        # Log the full response at debug level
        self.logger.debug(f"Target environment response status code: {response.status_code if response else 'No response'}")
        self.logger.debug(f"Target environment response body: {response.text if response else 'No response body'}")

        # If response is empty or the response body is empty
        if not response or not response.text.strip():
            self.logger.error("No response received from the migration API. Please check the logs for details.")
            return [{"message": "No response received from the migration API. Please check the logs for details."}]

        # Step 4: Handle the response from the bulk API call
        migration_results = []
        if response and response.status_code == 201:  # Check for status code 201 only
            try:
                response_data = response.json()  # Parse JSON response
                self.logger.info(f"Bulk migration succeeded. Response: {response_data}")

                # Process the response (list of migrated groups)
                for group in response_data:
                    group_name = group.get("name", "Unknown Group")
                    self.logger.info(f"Successfully migrated group: {group_name}")
                    migration_results.append({"name": group_name, "status": "Success"})
                    print(f"Successfully migrated group: {group_name}")
            except ValueError:
                self.logger.warning("Response is not valid JSON. Assuming migration was successful.")
                # Assume success if status code is correct but response is not JSON
                migration_results = [{"name": group_data["name"], "status": "Success"} for group_data in bulk_group_data]
        else:
            # Log and handle unsuccessful status codes
            self.logger.error(f"Bulk migration failed. Status code: {response.status_code if response else 'No response'}")
            migration_results = [{"name": group_data["name"], "status": "Failed"} for group_data in bulk_group_data]

        self.logger.info(f"Finished migrating groups. Successfully migrated {len([r for r in migration_results if r['status'] == 'Success'])} out of {len(group_name_list)} groups.")
        return migration_results

    def migrate_all_groups(self):
        """
        Migrates all groups from the source environment to the target environment using the bulk endpoint.

        Returns:
            list: A list of group migration results, including any errors encountered during the process.
        """
        self.logger.info("Starting group migration from source to target.")

        # Step 1: Get all groups from the source environment
        self.logger.debug("Fetching groups from the source environment.")
        source_response = self.source_client.get("/api/v1/groups")
        if not source_response or source_response.status_code != 200:
            self.logger.error("Failed to retrieve groups from the source environment.")
            print("Failed to retrieve groups from the source environment. Please check the logs for more details.")
            return [{"message": "Failed to retrieve groups from the source environment. Please check the logs for more details."}]

        # Log the full response at debug level
        self.logger.debug(f"Source environment response status code: {source_response.status_code}")
        self.logger.debug(f"Source environment response body: {source_response.text}")

        source_groups = source_response.json()
        if not source_groups:
            self.logger.info("No groups found in the source environment. Ending process.")
            return [{"message": "No groups found in the source environment. Nothing to migrate."}]

        self.logger.info(f"Retrieved {len(source_groups)} groups from the source environment.")

        # Step 2: Filter out specific groups
        bulk_group_data = []
        for group in source_groups:
            if group["name"] not in ["Admins", "All users in system", "Everyone"]:
                # Prepare group data excluding unnecessary fields
                group_data = {
                    key: value for key, value in group.items()
                    if key not in ["created", "lastUpdated", "tenantId", "_id"]
                }
                bulk_group_data.append(group_data)
                self.logger.debug(f"Prepared data for group: {group['name']}")

        # If no groups to migrate, log and exit early
        if not bulk_group_data:
            self.logger.info("No eligible groups found for migration. Ending process.")
            return [{"message": "No eligible groups found for migration. Please verify the group list and try again."}]

        # Step 3: Make the bulk POST request with the group data
        self.logger.info(f"Sending bulk migration request for {len(bulk_group_data)} groups")
        self.logger.debug(f"Payload for bulk migration: {bulk_group_data}")
        response = self.target_client.post("/api/v1/groups/bulk", data=bulk_group_data)

        # Log the full response at debug level
        self.logger.debug(f"Target environment response status code: {response.status_code if response else 'No response'}")
        self.logger.debug(f"Target environment response body: {response.text if response else 'No response body'}")

        # Step 4: Handle the response from the bulk API call
        migration_results = []
        if response and response.status_code == 201:
            try:
                response_data = response.json()  # Parse JSON response
                self.logger.info(f"Bulk migration succeeded. Response: {response_data}")

                # Process the response (list of migrated groups)
                for group in response_data:
                    group_name = group.get("name", "Unknown Group")
                    self.logger.info(f"Successfully migrated group: {group_name}")
                    migration_results.append({"name": group_name, "status": "Success"})
                    print(f"Successfully migrated group: {group_name}")
            except ValueError:
                self.logger.warning("Response is not valid JSON. Assuming migration was successful.")
                # Assume success if status code is correct but response is not JSON
                migration_results = [{"name": group_data["name"], "status": "Success"} for group_data in bulk_group_data]
        else:
            # Log and handle unsuccessful status codes
            self.logger.error(f"Bulk migration failed. Status code: {response.status_code if response else 'No response'}")
            migration_results = [{"name": group_data["name"], "status": "Failed"} for group_data in bulk_group_data]

        self.logger.info(f"Finished migrating groups. Successfully migrated {len([r for r in migration_results if r['status'] == 'Success'])} out of {len(source_groups)} groups.")
        return migration_results


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
        self.logger.debug("Fetching users from the source environment.")
        source_response = self.source_client.get("/api/v1/users", params=params)
        if not source_response or source_response.status_code != 200:
            self.logger.error("Failed to retrieve users from the source environment.")
            print("Failed to retrieve users from the source environment. Please check the logs for more details.")
            return [{"message": "Failed to retrieve users from the source environment. Please check the logs for more details."}]

        # Log full response for debugging
        self.logger.debug(f"Source environment response status code: {source_response.status_code}")
        self.logger.debug(f"Source environment response body: {source_response.text}")

        source_users = source_response.json()
        if not source_users:
            self.logger.info("No users found in the source environment. Ending process.")
            return [{"message": "No users found in the source environment. Nothing to migrate."}]

        self.logger.info(f"Retrieved {len(source_users)} users from the source environment.")

        # Step 2: Get roles and groups information from the target environment to match and get IDs
        self.logger.debug("Fetching roles and groups from the target environment.")
        target_roles_response = self.target_client.get("/api/roles")
        target_groups_response = self.target_client.get("/api/v1/groups")

        if not target_roles_response or target_roles_response.status_code != 200:
            self.logger.error("Failed to retrieve roles from the target environment.")
            return [{"message": "Failed to retrieve roles from the target environment. Please check the logs for details."}]

        if not target_groups_response or target_groups_response.status_code != 200:
            self.logger.error("Failed to retrieve groups from the target environment.")
            return [{"message": "Failed to retrieve groups from the target environment. Please check the logs for details."}]

        target_roles = target_roles_response.json()
        target_groups = target_groups_response.json()
        self.logger.debug(f"Retrieved {len(target_roles)} roles and {len(target_groups)} groups from the target environment.")

        EXCLUDED_GROUPS = {"Everyone", "All users in system"}
        single_user_data = []  # List to hold a single user data for migration

        # Step 3: Find and process the users based on the input list
        bulk_user_data = []  # List to hold data for all users to be migrated
        for user in source_users:
            if user["email"] in user_name_list:  # Match users by email
                # Construct the required payload for the user
                user_data = {
                    "email": user["email"],
                    "firstName": user["firstName"],
                    "lastName": user.get("lastName", ""),  # Optional field
                    "roleId": next((role["_id"] for role in target_roles if role["name"] == user["role"]["name"]), None),
                    "groups": [
                        group["_id"] for group in target_groups
                        if group["name"] in [g["name"] for g in user["groups"]] and group["name"] not in EXCLUDED_GROUPS
                    ],
                    "preferences": user.get("preferences", {"localeId": "en-US"})  # Default to English language preference
                }

                # Append user data to the bulk list
                bulk_user_data.append(user_data)
                self.logger.debug(f"Prepared data for user: {user['email']}")

        # If no matching users, log and exit
        if not bulk_user_data:
            self.logger.info("No matching users found for migration. Ending process.")
            return [{"message": "No matching users found for migration. Please verify the user list and try again."}]


        # Step 4: Make the POST request with the bulk user data
        self.logger.info(f"Sending bulk migration request for {len(bulk_user_data)} users")
        self.logger.debug(f"Payload for bulk user migration: {bulk_user_data}")
        response = self.target_client.post("/api/v1/users/bulk", data=bulk_user_data)  # Send bulk data as a list

        # Log the full response for debugging
        self.logger.debug(f"Target environment response status code: {response.status_code if response else 'No response'}")
        self.logger.debug(f"Target environment response body: {response.text if response else 'No response body'}")

        # Step 5: Handle the response from the bulk API call
        migration_results = []
        if response and response.status_code == 201:
            try:
                response_data = response.json()
                self.logger.info(f"Bulk migration succeeded. Response: {response_data}")

                # Process the response (list of migrated users)
                for user in response_data:
                    user_name = user.get("email", "Unknown User")
                    self.logger.info(f"Successfully migrated user: {user_name}")
                    migration_results.append({"name": user_name, "status": "Success"})
                    print(f"Successfully migrated user: {user_name}")
            except ValueError:
                self.logger.warning("Response is not valid JSON. Assuming migration was successful.")
                migration_results = [{"name": user_data["email"], "status": "Success"} for user_data in bulk_user_data]
        else:
            # Log and handle unsuccessful status codes
            self.logger.error(f"Bulk user migration failed. Status code: {response.status_code if response else 'No response'}")
            migration_results = [{"name": user_data["email"], "status": "Failed"} for user_data in bulk_user_data]

        return migration_results



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
        self.logger.debug("Fetching users from the source environment.")
        source_response = self.source_client.get("/api/v1/users", params=params)
        if not source_response or source_response.status_code != 200:
            self.logger.error("Failed to retrieve users from the source environment.")
            print("Failed to retrieve users from the source environment. Please check the logs for more details.")
            return [{"message": "Failed to retrieve users from the source environment. Please check the logs for details."}]

        # Log the full response at debug level
        self.logger.debug(f"Source environment response status code: {source_response.status_code}")
        self.logger.debug(f"Source environment response body: {source_response.text}")

        source_users = source_response.json()
        if not source_users:
            self.logger.info("No users found in the source environment. Ending process.")
            return [{"message": "No users found in the source environment. Nothing to migrate."}]

        self.logger.info(f"Retrieved {len(source_users)} users from the source environment.")

        # Step 2: Get roles and groups information from the target environment to match and get IDs
        self.logger.debug("Fetching roles and groups from the target environment.")
        target_roles_response = self.target_client.get("/api/roles")
        target_groups_response = self.target_client.get("/api/v1/groups")

        if not target_roles_response or target_roles_response.status_code != 200:
            self.logger.error("Failed to retrieve roles from the target environment.")
            return [{"message": "Failed to retrieve roles from the target environment. Please check the logs for details."}]

        if not target_groups_response or target_groups_response.status_code != 200:
            self.logger.error("Failed to retrieve groups from the target environment.")
            return [{"message": "Failed to retrieve groups from the target environment. Please check the logs for details."}]

        target_roles = target_roles_response.json()
        target_groups = target_groups_response.json()
        self.logger.debug(f"Retrieved {len(target_roles)} roles and {len(target_groups)} groups from the target environment.")

        EXCLUDED_GROUPS = {"Everyone", "All users in system"}
        bulk_user_data = []  # List to hold the user data for bulk upload

        # Step 3: Process each user and prepare the payload for the bulk endpoint
        for user in source_users:
            user_data = {
                "email": user["email"],
                "firstName": user["firstName"],
                "lastName": user.get("lastName", ""),  # Optional field
                "roleId": next((role["_id"] for role in target_roles if role["name"] == user["role"]["name"]), None),
                "groups": [
                    group["_id"] for group in target_groups
                    if group["name"] in [g["name"] for g in user["groups"]] and group["name"] not in EXCLUDED_GROUPS
                ],
                "preferences": user.get("preferences", {"localeId": "en-US"})  # Default to English language preference
            }

            bulk_user_data.append(user_data)
            self.logger.debug(f"Prepared data for user: {user['email']}")

        # Step 4: Make the bulk POST request with the user data
        if not bulk_user_data:
            self.logger.info("No users to migrate. Ending process.")
            return [{"message": "No users to migrate. Nothing to process."}]

        self.logger.info(f"Sending bulk migration request for {len(bulk_user_data)} users")
        self.logger.debug(f"Payload for bulk user migration: {bulk_user_data}")
        response = self.target_client.post("/api/v1/users/bulk", data=bulk_user_data)

        # Log the full response for debugging
        self.logger.debug(f"Target environment response status code: {response.status_code if response else 'No response'}")
        self.logger.debug(f"Target environment response body: {response.text if response else 'No response body'}")

        # Step 5: Handle the response from the bulk API call
        migration_results = []
        if response and response.status_code == 201:
            try:
                response_data = response.json()
                self.logger.info(f"Bulk migration succeeded. Response: {response_data}")

                # Process the response (list of migrated users)
                for user in response_data:
                    user_email = user.get("email", "Unknown User")
                    self.logger.info(f"Successfully migrated user: {user_email}")
                    migration_results.append({"name": user_email, "status": "Success"})
                    print(f"Successfully migrated user: {user_email}")
            except ValueError:
                self.logger.warning("Response is not valid JSON. Assuming migration was successful.")
                migration_results = [{"name": user_data["email"], "status": "Success"} for user_data in bulk_user_data]
        else:
            self.logger.error(f"Bulk user migration failed. Status code: {response.status_code if response else 'No response'}")
            migration_results = [{"name": user_data["email"], "status": "Failed"} for user_data in bulk_user_data]

        self.logger.info(f"Finished migrating users. Successfully migrated {len([r for r in migration_results if r['status'] == 'Success'])} out of {len(source_users)} users.")
        return migration_results

    def migrate_dashboard_shares(self, source_dashboard_ids, target_dashboard_ids, change_ownership=False):
        """
        Migrates shares for specific dashboards from the source to the target environment.

        Parameters:
            source_dashboard_ids (list): A list of dashboard IDs from the source environment to fetch shares from.
            target_dashboard_ids (list): A list of dashboard IDs from the target environment to apply shares to.
            change_ownership (bool, optional): Whether to change ownership of the target dashboard. Defaults to False.

        Returns:
            dict: A summary of the share migration process with counts of succeeded and failed shares,
                and details of failed dashboards.

        Raises:
            ValueError: If `source_dashboard_ids` or `target_dashboard_ids` are not provided,
                        or if their lengths do not match.
        """
        if not source_dashboard_ids or not target_dashboard_ids:
            raise ValueError("Both 'source_dashboard_ids' and 'target_dashboard_ids' must be provided.")
        if len(source_dashboard_ids) != len(target_dashboard_ids):
            raise ValueError("The lengths of 'source_dashboard_ids' and 'target_dashboard_ids' must match.")

        self.logger.info("Starting share migration for specified dashboards.")
        self.logger.debug(f"Source Dashboard IDs: {source_dashboard_ids}")
        self.logger.debug(f"Target Dashboard IDs: {target_dashboard_ids}")

        share_migration_summary = {'share_success_count': 0, 'share_fail_count': 0, 'failed_dashboards': []}

        # Step 1: Fetch users and groups once
        self.logger.info("Fetching users and groups from source and target environments.")
        try:
            # Fetch source users and groups
            source_users = self.source_client.get("/api/v1/users").json()
            source_user_map = {user["_id"]: user["email"] for user in source_users}
            source_groups = self.source_client.get("/api/v1/groups").json()
            source_group_map = {group["_id"]: group["name"] for group in source_groups}

            # Fetch target users and groups
            target_users = self.target_client.get("/api/v1/users").json()
            target_user_map = {user["email"]: user["_id"] for user in target_users}
            target_groups = self.target_client.get("/api/v1/groups").json()
            target_group_map = {group["name"]: group["_id"] for group in target_groups}

            user_mapping = {source_id: target_user_map.get(email) for source_id, email in source_user_map.items()}
            group_mapping = {source_id: target_group_map.get(name) for source_id, name in source_group_map.items()}
            self.logger.info("User and group mapping created successfully.")
        except Exception as e:
            self.logger.error(f"Failed to fetch users or groups: {e}")
            return share_migration_summary

        # Step 2: Process each dashboard pair
        for source_id, target_id in zip(source_dashboard_ids, target_dashboard_ids):
            self.logger.info(f"Processing shares for dashboard: Source ID {source_id}, Target ID {target_id}")

            # Fetch shares from the source environment
            dashboard_shares_response = self.source_client.get(f"/api/shares/dashboard/{source_id}?adminAccess=true")
            self.logger.debug(f"Response for shares of source dashboard ID {source_id}: {dashboard_shares_response.text if dashboard_shares_response else 'No response'}")
            if not dashboard_shares_response or dashboard_shares_response.status_code != 200:
                self.logger.error(f"Failed to fetch shares for source dashboard ID: {source_id}.")
                share_migration_summary['failed_dashboards'].append({"source_id": source_id, "target_id": target_id})
                continue

            response_json = dashboard_shares_response.json()
            dashboard_shares = response_json.get("sharesTo", [])
            if not dashboard_shares:
                self.logger.warning(f"No shares found for source dashboard ID: {source_id}.")
                continue

            # Identify the potential owner
            owner_field = response_json.get("owner", {})
            source_owner_id = owner_field.get("_id")
            owner_username = owner_field.get("userName", "Unknown User")
            potential_owner_id = user_mapping.get(source_owner_id)
            potential_owner_name = user_mapping.get(owner_username)
            
            if potential_owner_id:
                self.logger.info(f"Potential owner identified: {owner_username} (ID: {potential_owner_id})")
            else:
                self.logger.warning(f"Potential owner {owner_username} not found in the target environment.")

            # Prepare the shares for migration
            self.logger.info(f"Preparing shares for migration to target dashboard ID {target_id}.")
            new_shares = []
            for share in dashboard_shares:
                if share["type"] == "user":
                    new_share_user_id = user_mapping.get(share["shareId"])
                    user_email = source_user_map.get(share["shareId"], "Unknown User")
                    if new_share_user_id:
                        rule = share.get("rule", "edit")
                        new_shares.append({
                            "shareId": new_share_user_id,
                            "type": "user",
                            "rule": rule,
                            "subscribe": share.get("subscribe", False)
                        })
                        self.logger.debug(f"Prepared user share for migration: {user_email} (Rule: {rule})")
                elif share["type"] == "group":
                    new_share_group_id = group_mapping.get(share["shareId"])
                    group_name = source_group_map.get(share["shareId"], "Unknown Group")
                    if new_share_group_id:
                        new_shares.append({
                            "shareId": new_share_group_id,
                            "type": "group",
                            "rule": share.get("rule", "viewer"),
                            "subscribe": share.get("subscribe", False)
                        })
                        self.logger.debug(f"Prepared group share for migration: {group_name} (Rule: {share.get('rule', 'viewer')})")

            # Combine new shares with existing ones
            self.logger.debug(f"Fetching shares for target dashboard ID {target_id} with adminAccess=true.")
            target_dashboard_shares_response = self.target_client.get(f"/api/shares/dashboard/{target_id}?adminAccess=true")

            if target_dashboard_shares_response is not None:
                if target_dashboard_shares_response.status_code == 403:
                    self.logger.warning(f"Access denied for target dashboard ID {target_id} with adminAccess. Retrying without adminAccess.")
                    target_dashboard_shares_response = self.target_client.get(f"/api/shares/dashboard/{target_id}")
                    if target_dashboard_shares_response and target_dashboard_shares_response.status_code == 200:
                        self.logger.debug(f"Successfully fetched shares for target dashboard ID {target_id} without adminAccess.")
                    else:
                        self.logger.error(f"Retry without adminAccess also failed for target dashboard ID {target_id}.")
                        existing_shares = []
                else:
                    self.logger.debug(f"Shares fetched with adminAccess for target dashboard ID {target_id}.")
            else:
                self.logger.error(f"Failed to fetch shares for target dashboard ID {target_id}. Response is None.")
                existing_shares = []

            existing_shares = []
            if target_dashboard_shares_response and target_dashboard_shares_response.status_code == 200:
                existing_shares = target_dashboard_shares_response.json().get("sharesTo", [])

            all_shares = existing_shares + new_shares

            if not all_shares:
                self.logger.warning(f"No valid shares found for source dashboard ID {source_id}. Ensure users and groups exist in the target environment.")
                continue

            # Post the shares to the target environment
            self.logger.info(f"Migrating shares to target dashboard ID {target_id}.")
            post_url = f"/api/shares/dashboard/{target_id}?adminAccess=true"
            self.logger.debug(f"Making POST request to {post_url}.")

            response = self.target_client.post(post_url, data={"sharesTo": all_shares})

            # Check if response is 403 and attempt retry without adminAccess
            if response is not None:
                if response.status_code == 403:
                    self.logger.warning(f"Access denied for POST request to {post_url}. Retrying without adminAccess.")
                    post_url_without_admin = f"/api/shares/dashboard/{target_id}"
                    self.logger.debug(f"Retrying POST request to {post_url_without_admin}.")
                    response = self.target_client.post(post_url_without_admin, data={"sharesTo": all_shares})
                    if response and response.status_code in [200, 201]:
                        self.logger.debug(f"POST request successful without adminAccess for dashboard ID {target_id}.")
                    else:
                        self.logger.error(f"Retry without adminAccess also failed for POST request to dashboard ID {target_id}. "
                                        f"Status Code: {response.status_code if response else 'No response'}")
                elif response.status_code in [200, 201]:
                    self.logger.info(f"Shares migrated successfully to target dashboard ID {target_id}.")
                    share_migration_summary['share_success_count'] += len(new_shares)
                else:
                    self.logger.error(f"Unexpected status code for POST request to {post_url}: {response.status_code}.")
            else:
                self.logger.error(f"POST request to {post_url} failed. No response received.")

            # Handle the response or fallback logic
            if response and response.status_code in [200, 201]:
                self.logger.info(f"Shares migrated successfully to target dashboard ID {target_id}.")
                share_migration_summary['share_success_count'] += len(new_shares)
            else:
                self.logger.error(f"Failed to migrate shares for target dashboard ID {target_id}. "
                                f"Status Code: {response.status_code if response else 'No response'}")
                share_migration_summary['share_fail_count'] += len(new_shares)
                share_migration_summary['failed_dashboards'].append({"source_id": source_id, "target_id": target_id})

            self.logger.debug('Starting ownership change process.')
            # Handle ownership change if required
            if change_ownership and potential_owner_id:
                self.logger.info(f"Changing ownership of target dashboard ID {target_id} to user: {potential_owner_name} (ID: {potential_owner_id}).")
                
                ownership_url = f"/api/v1/dashboards/{target_id}/change_owner?adminAccess=true"
                self.logger.debug(f"Making POST request to {ownership_url} for ownership change.")
                
                owner_change_response = self.target_client.post(ownership_url, data={"ownerId": potential_owner_id, "originalOwnerRule": "edit"})
                
                # Check for 403 and retry without adminAccess
                if owner_change_response is None or owner_change_response.status_code == 403:
                    self.logger.warning(f"Access denied for ownership change at {ownership_url}. Retrying without adminAccess.")
                    ownership_url_without_admin = f"/api/v1/dashboards/{target_id}/change_owner"
                    self.logger.debug(f"Retrying ownership change POST request to {ownership_url_without_admin}.")
                    owner_change_response = self.target_client.post(ownership_url_without_admin, data={"ownerId": potential_owner_id, "originalOwnerRule": "edit"})
                
                # Handle the response after retry logic
                if owner_change_response and owner_change_response.status_code in [200, 201]:
                    self.logger.info(f"Ownership changed successfully for dashboard ID {target_id}.")
                else:
                    self.logger.error(f"Failed to change ownership for dashboard ID {target_id}. "
                                    f"Status Code: {owner_change_response.status_code if owner_change_response else 'No response'}.")


        self.logger.info("Finished share migration.")
        self.logger.info(share_migration_summary)
        return share_migration_summary


    def migrate_dashboards(self, dashboard_ids=None, dashboard_names=None, action=None, republish=False, migrate_share=False, change_ownership=False):
        """
        Migrates specific dashboards from the source to the target environment using the bulk endpoint.

        Parameters:
            dashboard_ids (list, optional): A list of dashboard IDs to migrate. Either `dashboard_ids` or `dashboard_names` must be provided.
            dashboard_names (list, optional): A list of dashboard names to migrate. Either `dashboard_ids` or `dashboard_names` must be provided.
            action (str, optional): Determines how to handle existing dashboards in the target environment.
                                    Options:
                                    - 'skip': Skip existing dashboards in the target; new dashboards are processed normally, including shares and ownership.
                                    - 'overwrite': Overwrite existing dashboards; shares and ownership will not be migrated. If the dashboard already exists, shares will be retained, but the API user will be set as the new owner.
                                    - 'duplicate': Create a duplicate of existing dashboards without migrating shares or ownership.
                                    Default: None. Existing dashboards are skipped, and only new ones are migrated.
                                    **Note:** If an existing dashboard in the target environment has a different owner than the user's token running the SDK, the dashboard will be migrated with a new ID, and its shares and ownership will be migrated from the original source dashboard.
            republish (bool, optional): Whether to republish dashboards after migration. Default: False.
            migrate_share (bool, optional): Whether to migrate shares for the dashboards. If `True`, shares will be migrated, and ownership migration will be controlled by the `change_ownership` parameter. 
                                            If `False`, both shares and ownership migration will be skipped. Default: False.
            change_ownership (bool, optional): Whether to change ownership of the target dashboards. Effective only if `migrate_share` is True. Default: False.

        Returns:
            dict: A summary of the migration results with lists of succeeded, skipped, and failed dashboards.

        Notes:
            - **When `action` is not provided, existing dashboards in the target environment are skipped, and only new dashboards are added.
            - **Best Use Case**: Suitable when migrating dashboards for the first time to a target environment.
            - **Overwrite Action:** When using `overwrite`, shares and ownership will not be migrated. If a dashboard already exists, the target dashboard will be overwritten, retaining its existing shares but setting the API user as the new owner. 
            Subsequent adjustments to shares and ownership will not be supported in this mode.
            - **Duplicate Action**: Creates duplicate dashboards without shares and ownership migration.
            - **Skip Action**: Skips migration for existing dashboards, but new ones are processed normally.
        """
        
        if dashboard_ids and dashboard_names:
            raise ValueError("Please provide either 'dashboard_ids' or 'dashboard_names', not both.")
        

        if not migrate_share and change_ownership:
            raise ValueError("The `change_ownership` parameter requires `migrate_share=True`.")

        self.logger.info("Starting dashboard migration from source to target.")

        # Step 1: Fetch dashboards based on provided IDs or names
        bulk_dashboard_data = []
        if dashboard_ids:
            self.logger.info(f"Processing dashboard migration by IDs: {dashboard_ids}")
            for dashboard_id in dashboard_ids:
                source_dashboard_response = self.source_client.get(f"/api/dashboards/{dashboard_id}/export?adminAccess=true")
                self.logger.debug(f"Response for source dashboard ID {dashboard_id}: {source_dashboard_response.text if source_dashboard_response else 'No response'}")
                if source_dashboard_response and source_dashboard_response.status_code == 200:
                    self.logger.debug(f"Dashboard with ID: {dashboard_id} retrieved successfully.")
                    bulk_dashboard_data.append(source_dashboard_response.json())
                else:
                    self.logger.error(f"Failed to export dashboard with ID: {dashboard_id}. Status Code: {source_dashboard_response.status_code if source_dashboard_response else 'No response'}")

        elif dashboard_names:
            self.logger.info(f"Processing dashboard migration by names: {dashboard_names}")
            limit = 50
            skip = 0
            dashboards = []
            # Fetch dashboards from the source environment
            while True:
                self.logger.debug(f"Fetching dashboards (limit={limit}, skip={skip})")
                dashboard_response = self.source_client.post('/api/v1/dashboards/searches', data={
                    "queryParams": {"ownershipType": "allRoot", "search": "", "ownerInfo": True, "asObject": True},
                    "queryOptions": {"sort": {"title": 1}, "limit": limit, "skip": skip}
                })

                if not dashboard_response or dashboard_response.status_code != 200:
                    self.logger.debug("No more dashboards found or failed to retrieve.")
                    break

                items = dashboard_response.json().get("items", [])
                if not items:
                    self.logger.debug("No more items in response; breaking pagination loop.")
                    break

                self.logger.debug(f"Fetched {len(items)} dashboards in this batch.")
                dashboards.extend(items)
                skip += limit

            # Filter dashboards by name and avoid duplicates
            unique_dashboards = {dash["oid"]: dash for dash in dashboards}
            dashboards = list(unique_dashboards.values())
            self.logger.info(f"Total unique dashboards retrieved: {len(dashboards)}.")
            bulk_dashboard_data = []
            for dashboard in dashboards:
                if dashboard["title"] in dashboard_names:
                    self.logger.debug(f"Matching dashboard: {dashboard['title']}")
                    source_dashboard_response = self.source_client.get(f"/api/dashboards/{dashboard['oid']}/export?adminAccess=true")
                    if source_dashboard_response and source_dashboard_response.status_code == 200:
                        bulk_dashboard_data.append(source_dashboard_response.json())
                        self.logger.debug(f"Dashboard {dashboard['title']} added to migration list.")
                    else:
                        self.logger.error(f"Failed to export dashboard: {dashboard['title']} (ID: {dashboard['oid']}).")
                else:
                    self.logger.debug(f"Dashboard {dashboard['title']} not in the provided names; skipping.")

        # Step 2: Perform bulk migration
        migration_summary = {'succeeded': [], 'skipped': [], 'failed': []}
        source_dash_dict = {dash['oid']: dash['title'] for dash in bulk_dashboard_data}  # Create a map of source OIDs to titles
        migrated_target_dash_dict = {}  # Placeholder for target OIDs and titles after migration
        if bulk_dashboard_data:
            url = f"/api/v1/dashboards/import/bulk?republish={str(republish).lower()}"
            if action:
                url += f"&action={action}"

            self.logger.info(f"Sending bulk migration request for {len(bulk_dashboard_data)} dashboards.")
            response = self.target_client.post(url, data=bulk_dashboard_data)
            self.logger.debug(f"Response for bulk migration: {response.text if response else 'No response'}")

            # Handle the migration results
            if response and response.status_code == 201:
                response_data = response.json()

                # Process succeeded dashboards
                if "succeded" in response_data:  
                    for response_dash in response_data['succeded']:
                        target_oid = response_dash['oid']
                        title = response_dash['title']

                        # Populate the target map dictionary
                        migrated_target_dash_dict[target_oid] = title
                        migration_summary['succeeded'].append(title)

                        self.logger.debug(f"Captured Target OID '{target_oid}' with title '{title}' in migrated_target_map_dict.")

                # Process skipped dashboards
                if "skipped" in response_data:
                    migration_summary['skipped'] = [dash['title'] for dash in response_data['skipped']]
                    for dash_title in migration_summary['skipped']:
                        self.logger.info(f"Skipped dashboard: {dash_title}")

                # Process failed dashboards
                if "failed" in response_data:
                    failed_items = response_data['failed']
                    for category, errors in failed_items.items():
                        for error in errors:
                            migration_summary['failed'].append(error['title'])
                            self.logger.warning(f"Failed to migrate dashboard: {error['title']} - {error['error']['message']}")
            else:
                self.logger.error(f"Bulk migration failed. Status Code: {response.status_code if response else 'No response'}")
                migration_summary['failed'].extend([dash['title'] for dash in bulk_dashboard_data])

        self.logger.info("Dashboard migration completed.")
        self.logger.debug(f"Source Map Dictionary: {source_dash_dict}")
        self.logger.debug(f"Migrated Target Map Dictionary: {migrated_target_dash_dict}")

        # Step 3: Handle shares and ownership migration
        if not migrate_share:
            self.logger.info("Migrate Share is set to False. Skipping shares and ownership migration.")
        elif action in ["duplicate", "overwrite"]:
            self.logger.info(f"Action '{action}' selected. Skipping shares and ownership migration.")
        else:
            self.logger.info("Starting share and ownership migration.")

            # Compare source and target OIDs to identify dashboards to process
            dash_to_process = {}
            problem_dash = []

            for source_oid, source_title in source_dash_dict.items():
                # Match title in the migrated target dictionary
                matching_target = next(
                    (target_oid for target_oid, target_title in migrated_target_dash_dict.items() if target_title == source_title),
                    None
                )
                if matching_target:
                    if source_oid != matching_target:
                        # Log mismatched OIDs with matching titles
                        problem_dash.append({
                            "title": source_title,
                            "source_id": source_oid,
                            "target_id": matching_target
                        })
                        self.logger.warning(
                            f"Title '{source_title}' has mismatched OIDs: "
                            f"Source ID '{source_oid}' and Target ID '{matching_target}'."
                        )
                    # Add to dashboards to process
                    dash_to_process[source_oid] = matching_target
                else:
                    # Log missing target for a source dashboard
                    self.logger.warning(
                        f"Source dashboard '{source_title}' with ID '{source_oid}' was not found in the target environment."
                    )

            self.logger.info(f"Dashboards to process: {dash_to_process}")
            self.logger.info(f"Problematic dashboards: {problem_dash}")


            if not dash_to_process:
                self.logger.info(
                    "No successfully migrated dashboards were captured. Skipping shares and ownership migration. "
                    "This may be due to errors during migration or because the dashboards already existed and were skipped. "
                    "Review the logs for more details."
                )
            else:
                self.logger.info(f"Processing shares and ownership for dashboards: {dash_to_process}")
                self.migrate_dashboard_shares(
                    source_dashboard_ids=list(dash_to_process.keys()),      # Original source OIDs
                    target_dashboard_ids=list(dash_to_process.values()),    # Corresponding target OIDs
                    change_ownership=change_ownership
                )
                self.logger.info("Share and ownership migration completed.")


        self.logger.info("Finished dashboard migration.")
        self.logger.info(f"Total Dashboards Migrated: {len(migration_summary['succeeded'])}")
        self.logger.info(f"Total Dashboards Skipped: {len(migration_summary['skipped'])}")
        self.logger.info(f"Total Dashboards Failed: {len(migration_summary['failed'])}")
        self.logger.info(migration_summary)
        
        return migration_summary


    def migrate_all_dashboards(self, action=None, republish=False, migrate_share=False, change_ownership=False, batch_size=10, sleep_time=10):
        """
        Migrates all dashboards from the source to the target environment in batches.

        Parameters:
            action (str, optional): Determines how to handle existing dashboards in the target environment.
                                    Options:
                                    - 'skip': Skip existing dashboards in the target; new dashboards are processed normally, including shares and ownership.
                                    - 'overwrite': Overwrite existing dashboards; shares and ownership will not be migrated. If the dashboard already exists, shares will be retained, but the API user will be set as the new owner.
                                    - 'duplicate': Create a duplicate of existing dashboards without migrating shares or ownership.
                                    Default: None. Existing dashboards are skipped, and only new ones are migrated. Unless existing dashboards are different owners, shares will be migrated.
                                    **Note:** If an existing dashboard in the target environment has a different owner than the user's token running the SDK, the dashboard will be migrated with a new ID, and its shares and ownership will be migrated from the original source dashboard.
            republish (bool, optional): Whether to republish dashboards after migration. Default: False.
            migrate_share (bool, optional): Whether to migrate shares for the dashboards. If `True`, shares will be migrated, and ownership migration will be controlled by the `change_ownership` parameter. 
                                            If `False`, both shares and ownership migration will be skipped. Default: False.
            change_ownership (bool, optional): Whether to change ownership of the target dashboards. Effective only if `migrate_share` is True. Default: False.
            batch_size (int, optional): Number of dashboards to process in each batch. Default: 10.
            sleep_time (int, optional): Time (in seconds) to sleep between batches. Default: 10 seconds.

        Returns:
            dict: A summary of the migration results for all batches, containing lists of succeeded, skipped, and failed dashboards.

        Notes:
            - **Batch Processing**: Dashboards are processed in batches to avoid overloading the system.
            - **Best Use Case**: This method is suitable when migrating all dashboards from a source to a target environment.
            - **Overwrite Action**: When using `overwrite`, shares and ownership will not be migrated. If a dashboard already exists, the target dashboard will be overwritten, retaining its existing shares but setting the API user as the new owner. Subsequent adjustments to shares and ownership will not be supported in this mode.
            - **Duplicate Action**: Creates duplicate dashboards without shares and ownership migration.
            - **Skip Action**: Skips migration for existing dashboards, but new ones are processed normally.
        """

        self.logger.info("Fetching all dashboards from the source environment.")
        all_dashboard_ids = set()
        
        # Step 1: Fetch all dashboards
        limit = 50
        skip = 0
        while True:
            dashboard_response = self.source_client.post('/api/v1/dashboards/searches', data={
                "queryParams": {"ownershipType": "allRoot", "search": "", "ownerInfo": True, "asObject": True},
                "queryOptions": {"sort": {"title": 1}, "limit": limit, "skip": skip}
            })
            
            if not dashboard_response or dashboard_response.status_code != 200:
                self.logger.debug("No more dashboards found or failed to retrieve.")
                break
            
            items = dashboard_response.json().get("items", [])
            if not items:
                self.logger.debug("No more items in response; breaking pagination loop.")
                break
            
            self.logger.debug(f"Fetched {len(items)} dashboards in this batch.")
            all_dashboard_ids.update([dash["oid"] for dash in items])
            skip += limit

        self.logger.info(f"Total unique dashboards retrieved: {len(all_dashboard_ids)}.")

        # Step 2: Migrate dashboards in batches
        all_dashboard_ids = list(all_dashboard_ids)
        migration_summary = {'succeeded': [], 'skipped': [], 'failed': []}

        for i in range(0, len(all_dashboard_ids), batch_size):
            batch_ids = all_dashboard_ids[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            self.logger.info(f"Processing batch {batch_number} with {len(batch_ids)} dashboards: {batch_ids}")
            
            try:
                batch_summary = self.migrate_dashboards(
                    dashboard_ids=batch_ids,
                    action=action,
                    republish=republish,
                    migrate_share=migrate_share,
                    change_ownership=change_ownership
                )
                self.logger.info(f"Batch {batch_number} migration summary: {batch_summary}")
                
                # Aggregate batch results into the overall summary
                migration_summary['succeeded'].extend(batch_summary['succeeded'])
                migration_summary['skipped'].extend(batch_summary['skipped'])
                migration_summary['failed'].extend(batch_summary['failed'])
            except Exception as e:
                self.logger.error(f"Error occurred in batch {batch_number}: {e}")
                continue  # Continue with the next batch even if an error occurs

            if i + batch_size < len(all_dashboard_ids):  # Avoid sleeping after the last batch
                self.logger.info(f"Sleeping for {sleep_time} seconds before processing the next batch.")
                time.sleep(sleep_time)

        self.logger.info("Finished migrating all dashboards.")
        self.logger.info(f"Total Dashboards Migrated: {len(migration_summary['succeeded'])}")
        self.logger.info(f"Total Dashboards Skipped: {len(migration_summary['skipped'])}")
        self.logger.info(f"Total Dashboards Failed: {len(migration_summary['failed'])}")
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

        # # Handle if 'all' is passed or dependencies are not provided (None)
        # if dependencies is None or dependencies == "all":
        #     dependencies = ["dataSecurity", "formulas", "hierarchies", "perspectives"]

        # # Convert user-friendly dependencies to API-compatible parameters
        # api_dependencies = []
        # for dep in dependencies:
        #     if dep in dependency_mapping:
        #         api_dependencies.extend(dependency_mapping[dep])

        # # Ensure unique values and format for the API call
        # api_dependencies = list(set(api_dependencies))  # Remove duplicates

        # Set default dependencies if none are provided
        if dependencies is None or dependencies == "all":
            dependencies = list(dependency_mapping.keys())

        api_dependencies = list({dep for key in dependencies for dep in dependency_mapping.get(key, [])})


        # Validate input parameters
        if datamodel_ids and datamodel_names:
            raise ValueError("Please provide either 'datamodel_ids' or 'datamodel_names', not both.")

        self.logger.info("Starting data model migration from source to target.")
        self.logger.debug(f"Input Parameters: datamodel_ids={datamodel_ids}, datamodel_names={datamodel_names}, dependencies={dependencies}, shares={shares}")

        # Initialize migration summary
        migration_summary = {'succeeded': [], 'failed': []}
        success_count = 0
        fail_count = 0

        # Fetch data models based on provided parameters (IDs or names)
        all_datamodel_data = []
        if datamodel_ids:
            self.logger.debug(f"Processing data model migration by IDs: {datamodel_ids}")
            for datamodel_id in datamodel_ids:
                response = self.source_client.get(f"/api/v2/datamodel-exports/schema", params={
                    "datamodelId": datamodel_id,
                    "type": "schema-latest",
                    "dependenciesIdsToInclude": ",".join(api_dependencies),
                })
                if response.status_code == 200:
                    self.logger.debug(f"Successfully fetched data model ID {datamodel_id}: {response.json()}")
                    all_datamodel_data.append(response.json())
                else:
                    self.logger.error(f"Failed to fetch data model ID {datamodel_id}. Response: {response.text}")

        elif datamodel_names:
            self.logger.debug("Fetching all data models to filter by names.")
            response = self.source_client.get("/api/v2/datamodels/schema", params={"fields": "oid,title"})
            if response.status_code != 200:
                self.logger.error(f"Failed to fetch data models. Response: {response.text}")
                return migration_summary
            self.logger.info(f"Retrieved {len(response.json())} data models from the source environment.")

            source_datamodels = response.json()
            self.logger.debug(f"Source data models fetched: {source_datamodels}")

            # Filter the data models to migrate
            for datamodel in source_datamodels:
                if datamodel["title"] in datamodel_names:
                    response = self.source_client.get(f"/api/v2/datamodel-exports/schema", params={
                        "datamodelId": datamodel["oid"],
                        "type": "schema-latest",
                        "dependenciesIdsToInclude": ",".join(api_dependencies),
                    })
                    if response.status_code == 200:
                        self.logger.debug(f"Successfully fetched data model '{datamodel['title']}' with ID {datamodel['oid']}.")
                        all_datamodel_data.append(response.json())
                    else:
                        self.logger.error(f"Failed to fetch data model '{datamodel['title']}' (ID: {datamodel['oid']}). Response: {response.text}")

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
                    # Handle failed response
                    if datamodel_shares_response.status_code != 200:
                        self.logger.error(f"Failed to fetch shares for datamodel: '{datamodel['title']}' (ID: {datamodel['oid']}). "
                                        f"Error: {datamodel_shares_response.json()}")
                        share_fail_count += 1
                        migration_summary['failed'].append(datamodel['title'])
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
                                # To update shares Live model needs to be published first
                                self.logger.info(f"Publishing datamodel '{datamodel['title']}' to update shares.")
                                publish_response = self.target_client.post(f"/api/v2/builds", data={"datamodelId": datamodel_id, "buildType": "publish"})
                                if publish_response.status_code == 201:
                                    self.logger.info(f"Datamodel '{datamodel['title']}' published successfully. Now updating shares.")
                                    response = self.target_client.patch(f"/api/v1/elasticubes/live/{datamodel_id}/permissions", data=new_shares)
                                else:
                                    self.logger.error(f"Failed to publish datamodel '{datamodel['title']}'. Error: {publish_response.json() if publish_response else 'No response received.'}")
                                    response = None

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

    def migrate_all_datamodels(self, dependencies=None, shares=False, batch_size=10, sleep_time=5):
        """
        Migrates all data models from the source environment to the target environment in batches.

        Parameters:
            dependencies (list, optional): A list of dependencies to include in the migration. If not provided or if 'all' is passed, all dependencies are selected by default. 
                                        Possible values for `dependencies` are:
                                        - "dataSecurity" (includes both Data Security and Scope Configuration)
                                        - "formulas" (for Formulas)
                                        - "hierarchies" (for Drill Hierarchies)
                                        - "perspectives" (for Perspectives)
                                        If left blank or set to "all", all dependencies are included by default.
            shares (bool, optional): Whether to also migrate the data model's shares. Default is False.
            batch_size (int, optional): Number of data models to migrate in each batch. Default is 10.
            sleep_time (int, optional): Time in seconds to wait between processing batches. Default is 5 seconds.

        Returns:
            dict: A summary of the migration results with lists of succeeded, skipped, and failed data models.
        """
        self.logger.info("Starting migration of all data models from source to target.")
        self.logger.debug(f"Input Parameters: dependencies={dependencies}, shares={shares}, batch_size={batch_size}, sleep_time={sleep_time}")

        # Fetch all data models
        response = self.source_client.get("/api/v2/datamodels/schema", params={"fields": "oid,title"})
        if response.status_code != 200:
            self.logger.error(f"Failed to fetch data models. Response: {response.text}")
            return {"succeeded": [], "skipped": [], "failed": []}

        source_datamodels = response.json()
        all_datamodel_ids = [datamodel["oid"] for datamodel in source_datamodels]
        self.logger.info(f"Retrieved {len(all_datamodel_ids)} data models from the source environment.")

        migration_summary = {"succeeded": [], "skipped": [], "failed": []}

        for i in range(0, len(all_datamodel_ids), batch_size):
            batch_ids = all_datamodel_ids[i:i + batch_size]
            batch_number = (i // batch_size) + 1
            self.logger.info(f"Processing batch {batch_number} with {len(batch_ids)} data models: {batch_ids}")

            try:
                batch_summary = self.migrate_datamodels(
                    datamodel_ids=batch_ids,
                    dependencies=dependencies,
                    shares=shares
                )
                self.logger.info(f"Batch {batch_number} migration summary: {batch_summary}")

                # Aggregate batch results into the overall summary
                migration_summary['succeeded'].extend(batch_summary['succeeded'])
                migration_summary['failed'].extend(batch_summary['failed'])
            except Exception as e:
                self.logger.error(f"Error occurred in batch {batch_number}: {e}")
                continue  # Continue with the next batch even if an error occurs

            if i + batch_size < len(all_datamodel_ids):  # Avoid sleeping after the last batch
                self.logger.info(f"Sleeping for {sleep_time} seconds before processing the next batch.")
                time.sleep(sleep_time)

        self.logger.info("Finished migrating all data models.")
        self.logger.info(f"Total Data Models Migrated: {len(migration_summary['succeeded'])}")
        self.logger.info(f"Total Data Models Failed: {len(migration_summary['failed'])}")
        self.logger.info(migration_summary)

        return migration_summary