import os
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute

load_dotenv()

w = WorkspaceClient()

print("Connection successful!")
print("Current user:", w.current_user.me().user_name)

print("\Clusters:")
for c in w.clusters.list():
    print(f"- {c.cluster_name} (ID: {c.cluster_id}, State: {c.state})")
