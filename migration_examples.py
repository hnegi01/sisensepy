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
migration = Migration(source_yaml='source.yaml', target_yaml='target.yaml',debug=True)


# # --- Example 1: Migrate specific groups from source to target
# group_name_list = ["mig_test", "mig_test_2"]
# migration_results = migration.migrate_groups(group_name_list)
# print("Migration Results:", migration_results)


# # --- Example 2: Migrate all groups from source to target
# migration_results = migration.migrate_all_groups()
# print("Migration Results:", migration_results)


# # --- Example 3: Migrate specific users from source to target
# user_name =  ["himanshu.negi@sisense.com","bob@a.com"]
# migration_results = migration.migrate_users(user_name)
# print("Migration Results:", migration_results)

# # --- Example 4: Migrate all users from source to target
# migration_results = migration.migrate_all_users()
# print("Migration Results:", migration_results)

# --- Example 5: Migrate specific dashboards from source to target
dashboard_name_list = ["Governance Dashboard","Export to PDF text widget","samp_lead_gen_2"]
migration_results = migration.migrate_dashboards(dashboard_name_list, shares=True)

print("Migration Results:", migration_results)
