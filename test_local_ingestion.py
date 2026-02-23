import subprocess
import os
import sys
import unittest

class TestLocalSparkFallback(unittest.TestCase):
    def test_run_ingestion_script_idempotent(self):
        """
        Tests that running the data ingestion script twice locally 
        does not cause a LOCATION_ALREADY_EXISTS SparkRuntimeException.
        """
        # Determine paths
        script_path = os.path.join("trading-signal-app", "databricks", "01_data_ingestion.py")
        python_exe = os.path.join(".venv", "bin", "python")
        
        if not os.path.exists(python_exe):
            python_exe = sys.executable

        print(f"\n--- [Test] Running {script_path} ... ---")
        result = subprocess.run([python_exe, script_path], capture_output=True, text=True)
        
        # Provide debugging output on failure
        if result.returncode != 0:
            print(f"--- STDOUT ---\n{result.stdout}")
            print(f"--- STDERR ---\n{result.stderr}")
            
        self.assertEqual(result.returncode, 0, f"Script failed with exit code {result.returncode}.")
        self.assertNotIn("LOCATION_ALREADY_EXISTS", result.stderr)
        self.assertNotIn("LOCATION_ALREADY_EXISTS", result.stdout)
        
        print("✅ Test passed! Script executed cleanly without location errors.")

if __name__ == '__main__':
    unittest.main()
