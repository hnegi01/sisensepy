# This script serves as a guide for how to interact with the AccessManagement class.
# You can uncomment and run one example at a time to see the results.

# Importing the necessary classes from the sisensepy package
import csv
from sisensepy.access_management import AccessManagement

# Initializes AccessManagement with the default config file
access_mgmt = AccessManagement(debug=True)


# # --- Example 1: Get User Information by Email ---
# user_email = 'himanshu.negi@sisense.com'
# response = access_mgmt.get_user(user_email)
# # Option: Convert the response to a DataFrame and print or export it
# df = access_mgmt.api_client.to_dataframe(response)
# print(df)


# # --- Example 2: Get All Users ---
# print("Example 2: Get All Users")
# response = access_mgmt.get_users_all()

# # Option: Convert the response to a DataFrame and print or export it
# df = access_mgmt.api_client.to_dataframe(response)
# print(df)
# access_mgmt.api_client.export_to_csv(response, file_name="all_users.csv")


# # --- Example 3: Create a New User using role and group name instead of Ids ---

# # Define the user data
# user_data = {
#     "email": "himanshu@sisense.com",  # Required: User's email address
#     "firstName": "Himanshu",  # Optional: User's first name
#     "lastName": "Negi",  # Optional: User's last name
#     "role": "dataDesigner",  # Optional: Remove this field if not needed; if omitted, the user will be assigned the default role of 'viewer'. Cannot be an empty string.
#     "groups": ["groupa", "groupb"],  # Optional: List of group names, can be an empty list if the user is not part of any group
#     "password": "Sisense141!@",  # Optional: Provide a password if needed; if omitted, the user will receive an email to set their password. Cannot be an empty string.
#     "preferences": {  # Optional: User preferences, such as language settings, can be an empty dict if not needed
#         "language": "en-US"
#     }
# }

# # Attempt to create the new user
# response = access_mgmt.create_user(user_data)

# # Create multiple user reading from a csv
# """ Example CSV File:
# email,firstName,lastName,role,groups,password,language
# john.doe@example.com,John,Doe,dataDesigner,"groupa,groupb","Password123!","en-US"
# jane.smith@example.com,Jane,Smith,viewer,"groupa","Password456!","fr-FR"
# mike.jones@example.com,Mike,Jones,Designer,"","Password789!","es-ES"
# """

# # Path to your CSV file
# csv_file_path = 'export.csv'

# # Open and read the CSV file
# with open(csv_file_path, mode='r', newline='', encoding='utf-8') as csvfile:
#     reader = csv.DictReader(csvfile)
    
#     # Iterate over each row in the CSV file
#     for row in reader:
#         # Prepare user data from CSV row
#         user_data = {
#             "email": row['email'],
#             "firstName": row['firstName'],
#             "lastName": row['lastName'],
#             "roleId": row['role'],
#             "groups": row['groups'].split(',') if row['groups'] else [],  # Split groups by comma, handle empty string
#             "password": row['password'],
#             "preferences": {
#                 "language": row['language']
#             }
#         }
        
#         # Attempt to create the new user
#         response = access_mgmt.create_user(user_data)

# # --- Example 4: Update an Existing User ---

# # Define the username (email) of the user to be updated
# user_name = "john.doe@example.com"

# # Define the new data for the user
# user_data = {
#     "firstName": "John",  # Optional: Update user's first name
#     "lastName": "Doe",  # Optional: Update user's last name
#     "role": "designer",  # Optional: Role name to update; will be mapped to corresponding roleId
#     "groups": ["groupA", "groupB"],  # Optional: List of group names to update; will be mapped to corresponding group IDs
#     "preferences": {  # Optional: Update user preferences, such as language settings
#         "language": "fr-FR"
#     }
# }

# # Attempt to update the user with the provided data
# response = access_mgmt.update_user(user_name, user_data)

# # --- Example 5: Delete a User using UserName---

# # Define the username or email of the user you want to delete
# user_name = "john.doe@example.com"

# # Call the delete_user method to delete the user
# response = access_mgmt.delete_user(user_name)

# # Check if the user was successfully deleted
# if response:
#     print(f"User {user_name} has been successfully deleted. Response: {response}")
# else:
#     print(f"Failed to delete user {user_name}. Please check the logs for more details.")


# # --- Example 6: Get All Users in a Specific Group ---

# # Define the group name to search for
# group_name = "Admins"

# # Fetch users belonging to the specified group
# response = access_mgmt.users_per_group(group_name)

# # Print the response
# if response:
#     print("Users found in group:", response)
# else:
#     print(f"No users found in the group '{group_name}'.")

# df = access_mgmt.api_client.to_dataframe(response)
# print(df)

# # --- Example 7: Get All Groups with Users ---

# # Fetch all groups and their associated users
# response = access_mgmt.users_per_group_all()

# # Print the response
# if response:
#     for group_info in response:
#         print(f"Group: {group_info['group']}")
#         print(f"Usernames: {', '.join(group_info['username'])}\n")
# else:
#     print("No groups found with users.")

# df = access_mgmt.api_client.to_dataframe(response)
# print(df)


# # --- Example 8: Change Ownership of Folders and Dashboards ---

# # Define the user running the tool (must have access to the folder)
# user_name = 'himanshu.negi@sisense.com'
# # Define the target folder whose ownership needs to be changed
# folder_name = 'level4b'
# # Define the new owner to whom the folder and dashboard ownership will be transferred
# new_owner_name = 'himanshu.negi@sisense.com'
# # Optionally, specify the original owner rule (either 'edit' or 'view') - Default is 'edit'
# original_owner_rule = 'edit'
# # Optionally, specify whether to change the ownership of dashboards as well - Default is True
# change_dashboard_ownership = True
# # Call the method to change the ownership of folders and dashboards
# response = access_mgmt.change_folder_and_dashboard_ownership(
#     user_name=user_name,
#     folder_name=folder_name,
#     new_owner_name=new_owner_name,
#     original_owner_rule=original_owner_rule,
#     change_dashboard_ownership=change_dashboard_ownership
# )
# if response:
#     print(f"Folders changed: {response['total_folders_changed']}")
#     print(f"Dashboards changed: {response['total_dashboards_changed']}")
# else:
#     print("No changes were made.")

# # --- Example 9: Get columns from a DataModel

# datamodel_name = "Sample Lead Generation"
# all_columns = access_mgmt.get_datamodel_columns(datamodel_name)
# df = access_mgmt.api_client.to_dataframe(all_columns)
# print(df)

# # --- Example 10: Get columns from a Dashboard

# dashboard_id = "samp_lead_gen_2"
# dashboard_columns = access_mgmt.get_dashboard_columns(dashboard_id)
# df = access_mgmt.api_client.to_dataframe(dashboard_columns)
# print(df)

# # --- Example 11: Get Unused Columns in a DataModel

# unused_columns = access_mgmt.get_unused_columns(datamodel_name='Sample Lead Generation')
# # Output the result
# if unused_columns:
#     df = access_mgmt.api_client.to_dataframe(unused_columns)
#     print(df)


# # --- Example 12: Get the dashboard shares info
# dashboard_shares = access_mgmt.get_all_dashboard_shares()

# # Printing or handling the result
# df = access_mgmt.api_client.to_dataframe(dashboard_shares)
# print(df)




