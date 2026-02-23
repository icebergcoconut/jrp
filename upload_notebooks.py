import os
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
import base64

load_dotenv()
w = WorkspaceClient()

files_to_upload = [
    "databricks/01_data_ingestion.py",
    "databricks/02_strategy_evaluation.py",
    "databricks/03_data_export.py"
]

user_folder = f"/Users/{w.current_user.me().user_name}/jrp"

try:
    w.workspace.mkdirs(user_folder)
    print(f"Created/verified folder: {user_folder}")
except Exception as e:
    print(f"Error creating folder: {e}")

for file_path in files_to_upload:
    if os.path.exists(file_path):
        name = os.path.basename(file_path).split('.')[0]
        target_path = f"{user_folder}/{name}"
        
        with open(file_path, "rb") as f:
            content = f.read()
        
        encoded_content = base64.b64encode(content).decode("utf-8")
        
        try:
            w.workspace.import_(
                path=target_path,
                format=workspace.ImportFormat.SOURCE,
                language=workspace.Language.PYTHON,
                content=encoded_content,
                overwrite=True
            )
            print(f"✅ Uploaded {file_path} to {target_path}")
        except Exception as e:
            print(f"❌ Failed to upload {file_path}: {e}")
    else:
        print(f"⚠️  File not found: {file_path}")

print("Upload process complete.")
