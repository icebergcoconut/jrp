import subprocess
import os
import sys
import unittest

class TestDatabricksPipeline(unittest.TestCase):
    def test_pipeline_execution(self):
        scripts = [
            "research-platform-mini/databricks/01_data_ingestion.py",
            "research-platform-mini/databricks/02_signal_calculation.py",
            "research-platform-mini/databricks/03_data_export.py"
        ]
        
        python_exe = os.path.join(".venv", "bin", "python")
        if not os.path.exists(python_exe):
            python_exe = sys.executable
            
        for script in scripts:
            print(f"\\n--- Running {script} ---")
            result = subprocess.run([python_exe, script], capture_output=True, text=True)
            if result.returncode != 0:
                print(f"--- STDOUT ---\n{result.stdout}")
                print(f"--- STDERR ---\n{result.stderr}")
            self.assertEqual(result.returncode, 0, f"Script {script} failed with exit code {result.returncode}")
            self.assertNotIn("NameError", result.stderr)
            
        print("\\n✅ All pipeline scripts executed cleanly!")

if __name__ == '__main__':
    unittest.main()
