import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from sisensepy.migration import Migration

"""
Migrate folders as well. Flow might be like this:
1. Share each dashboard with the target user which will share the folder automatically
2. Get all folders from source
3. Create folders in target
4. Get all dashboards from source
5. Create dashboards in target using folder info from soucre dashbaord response
"""

# Initialize the Migration class with both clients
migration = Migration(source_yaml='source.yaml', target_yaml='target.yaml',debug=False)


# # --- Example 1: Migrate specific groups from source to target
# group_name_list = ["mig_test", "mig_test_2"]
# migration_results = migration.migrate_groups(group_name_list)
# print("Migration Results:", migration_results)


# # --- Example 2: Migrate all groups from source to target
# migration_results = migration.migrate_all_groups()
# print("Migration Results:", migration_results)


# # --- Example 3: Migrate specific users from source to target
# user_name =  ["himanshu.negi@sisense.com"]
# migration_results = migration.migrate_users(user_name)
# print("Migration Results:", migration_results)

# # --- Example 4: Migrate all users from source to target
# migration_results = migration.migrate_all_users()
# print("Migration Results:", migration_results)


# # --- Example 5: Migrate specific dashboard's share from source to target
# # Define source and target dashboard IDs
# source_dashboard_ids = ["659583469a933c002adc8574"]
# target_dashboard_ids = ["659583469a933c002adc8574"]

# # Migrate dashboard shares
# share_migration_results = migration.migrate_dashboard_shares(
#     source_dashboard_ids=source_dashboard_ids,
#     target_dashboard_ids=target_dashboard_ids,
#     change_ownership=True,
# )

# # Print migration results
# print("Share Migration Results:")
# print(share_migration_results)

# # --- Example 6: Migrate specific dashboards from source to target
# # dashboard_name_list = ["Governance Dashboard", "Export to PDF text widget", "samp_lead_gen_2"]
# dashboard_id_list = ["659583469a933c002adc8574"]
# migration_results = migration.migrate_dashboards(
#     # dashboard_names=dashboard_name_list,
#     dashboard_ids=dashboard_id_list,
#     action="skip",       # Options: "skip", "overwrite", "duplicate".
#     republish=True,             # Republishes dashboards after migration
#     migrate_share=True,         # Migrate shares for the dashboards
#     change_ownership=True       # Change ownership of dashboards (requires migrate_share=True)
# )
# print("Migration Results (by names):", migration_results)

# # --- Example 7: Migrate all dashboards from source to target environment in batches
# migration_results = migration.migrate_all_dashboards(
#     #action="overwrite",      # Options: "skip", "overwrite", "duplicate".
#     republish=True,          # Republishes dashboards after migration
#     migrate_share=True,      # Migrate shares for the dashboards
#     change_ownership=True,   # Change ownership of dashboards (requires migrate_share=True)
#     batch_size=10,           # Process 10 dashboards at a time
#     sleep_time=10            # Wait for 10 seconds between processing each batch
# )
# print("Migration Results (all dashboards):", migration_results)


# # --- Example 8: Migrate specific datamodels from source to target
# # datamodel_ids = ['dddfac9f-86e6-4a9a-af28-f52106dcc55c', 'fb942dff-e2ef-4230-8c3d-4a5e6b39ff63']
# # migration_results = migration.migrate_datamodels(datamodel_ids=datamodel_ids, dependencies=['dataSecurity', 'formulas'], shares=True)
# # print(migration_results)
# # Opitonally, you can migrate daatamodels by their names
# datamodel_names = ['bank_churn', 'bigquery']
# migration_results = migration.migrate_datamodels(datamodel_names=datamodel_names, dependencies='all', shares=True)
# print(migration_results)

# # --- Example 9: Migrate all datamodels from source to target environment in batches
# dependencies = ["dataSecurity", "formulas"]  # Only migrate selected dependencies
# shares = True  # Migrate shares along with the data models
# batch_size = 10  # Process 5 data models per batch
# sleep_time = 10  # Wait 10 seconds between batches

# # Perform the migration of all data models
# migration_summary = migration.migrate_all_datamodels(
#     dependencies=dependencies,
#     shares=shares,
#     batch_size=batch_size,
#     sleep_time=sleep_time
# )
