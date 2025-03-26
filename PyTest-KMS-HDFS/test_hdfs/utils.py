#no scope mismatch due to utils
import subprocess

KMS_CONTAINER_NAME = "ranger-kms"             # Replace with your actual KMS container name
KMS_LOG_FILE = "/var/log/ranger/kms/ranger-kms-ranger-kms.example.com-root.log"

def fetch_logs():
    try:
        cmd = f"docker exec {KMS_CONTAINER_NAME} tail -n 100 {KMS_LOG_FILE}"
        logs = subprocess.check_output(cmd, shell=True, text=True)
        error_logs = [line for line in logs.split("\n") if "ERROR" in line or "Exception" in line]
        return "\n".join(error_logs) if error_logs else "No recent errors in logs."
    except Exception as e:
        return f"Failed to fetch logs from container: {str(e)}"
