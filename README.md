# 📊 Sisense SDK (`pysisense`)

Sisense SDK (`pysisense`) is a Python library designed for seamless interaction with the **Sisense API**.  
It simplifies API requests, allowing users to manage **assets, users, permissions, dashboards, data models**, and automate **dashboard migrations** between environments.

🚀 **Version 1.0** – More features coming soon!

---

## 📥 Installation

You can install `pysisense` using pip:

```bash
pip install pysisense
```

For local development, install in **editable mode**:

```bash
pip install -e .
```

---

## 🔧 Usage

### **Initialize API Client**
```python
from pysisense.api_client import APIClient

# Create an instance of APIClient
api_client = APIClient(config_file="config.yaml", debug=True)
```

---

## 📌 Features
- ✅ **User & Group Management** – Create, update, and delete users/groups.
- ✅ **Dashboard Management** – Fetch, update, and migrate dashboards between environments.
- ✅ **Permissions & Access Control** – Manage user roles and permissions.
- ✅ **Data Model Management** – Fetch and update data models.
- ✅ **Logging & Error Handling** – Built-in error handling for API responses.
- ✅ **Automated Migration** – Migrate dashboards, users, and data models between environments.

---

## 🛠️ API Methods (Examples)

### **Get Users**
```python
from pysisense.accessmanagement import AccessManagement

access_mgmt = AccessManagement()
users = access_mgmt.get_users()
print(users)
```

### **Create a New User**
```python
new_user = access_mgmt.create_user(email="newuser@example.com", name="New User", role="Viewer")
print(new_user)
```

### **Get Dashboards**
```python
from pysisense.dashboard import Dashboard

dashboard_mgmt = Dashboard()
dashboards = dashboard_mgmt.get_dashboards()
print(dashboards)
```

### **Migrate Dashboards**
```python
from pysisense.migration import Migration

migration = Migration(source_yaml="source.yaml", target_yaml="target.yaml", debug=False)
migration_results = migration.migrate_dashboard("dashboard123")
print("Migration Results:", migration_results)
```

---

## 📌 Roadmap (Upcoming Features)
- 🔹 **Folder & Data Model Management**
- 🔹 **AutoML Integration with Sisense**
- 🔹 **Real-time Prediction Dashboards**
- 🔹 **Enhanced Logging & Debugging**
- 🔹 **Support for More API Endpoints**

---

## 📝 Contributing

We welcome contributions! To contribute:
1. Fork the repository.
2. Create a new feature branch (`feature-name`).
3. Commit your changes.
4. Open a pull request.

---

## 📄 License

This project is licensed under the **MIT License**.

---

## 📧 Contact

For questions or support, reach out at:  
📩 **himanshu.negi.08@gmail.com**  
🔗 [GitHub](https://github.com/hnegi01/pysisense)
