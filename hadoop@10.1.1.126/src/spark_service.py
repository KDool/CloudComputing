import subprocess
import re

WORKSPACE = "/workspace"


def submit_spark_job(request):

    script_path = f"{WORKSPACE}/pyspark/{request.script}"

    cmd = [
        "spark-submit",
        "--master", "yarn",
        "--deploy-mode", request.deploy_mode,
        "--executor-memory", request.executor_memory,
        "--num-executors", str(request.num_executors),
        script_path
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