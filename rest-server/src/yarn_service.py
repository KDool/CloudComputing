import subprocess


def get_application_status(application_id: str):

    cmd = [
        "yarn",
        "application",
        "-status",
        application_id
    ]

    process = subprocess.run(
        cmd,
        capture_output=True,
        text=True
    )

    return {
        "stdout": process.stdout,
        "stderr": process.stderr,
        "returncode": process.returncode
    }