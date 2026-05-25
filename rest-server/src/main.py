from fastapi import FastAPI, UploadFile, File
from models import (
    SparkJobRequest,
    HadoopJobRequest
)

from spark_service import submit_spark_job
from hadoop_service import submit_hadoop_job
from yarn_service import get_application_status

import shutil

app = FastAPI(
    title="Distributed Analytics REST Server",
    version="1.0.0"
)


@app.get("/")
def healthcheck():
    return {"status": "ok"}


# =========================
# Upload APIs
# =========================

@app.post("/upload/jar")
async def upload_jar(file: UploadFile = File(...)):

    path = f"/workspace/jars/{file.filename}"

    with open(path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    return {
        "status": "uploaded",
        "path": path
    }


@app.post("/upload/pyspark")
async def upload_pyspark(file: UploadFile = File(...)):

    path = f"/workspace/pyspark/{file.filename}"

    with open(path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    return {
        "status": "uploaded",
        "path": path
    }


# =========================
# Spark APIs
# =========================

@app.post("/jobs/spark")
async def submit_spark(request: SparkJobRequest):

    result = submit_spark_job(request)

    return result


# =========================
# Hadoop APIs
# =========================

@app.post("/jobs/hadoop")
async def submit_hadoop(request: HadoopJobRequest):

    result = submit_hadoop_job(request)

    return result


# =========================
# YARN APIs
# =========================

@app.get("/jobs/{application_id}")
async def job_status(application_id: str):

    result = get_application_status(application_id)

    return result