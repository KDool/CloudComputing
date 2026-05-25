import subprocess
import re

WORKSPACE = "/workspace"


def submit_hadoop_job(request):

    jar_path = f"{WORKSPACE}/jars/{request.jar}"

    cmd = [
        "hadoop",
        "jar",
        jar_path,
        request.main_class
    ] + request.args

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    stdout, stderr = process.communicate()

    application_id = None

    combined_output = stdout + "\n" + stderr

    match = re.search(r"(application_\d+_\d+)", combined_output)

    if match:
        application_id = match.group(1)

    return {
        "status": "submitted",
        "application_id": application_id,
        "stdout": stdout,
        "stderr": stderr
    }