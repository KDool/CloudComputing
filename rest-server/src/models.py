from pydantic import BaseModel
from typing import List, Optional


class SparkJobRequest(BaseModel):
    script: str
    args: Optional[List[str]] = []
    executor_memory: str = "512m"
    driver_memory: str = "512m"
    num_executors: int = 2


class HadoopJobRequest(BaseModel):
    jar: str
    main_class: str
    args: Optional[List[str]] = []


class JobResponse(BaseModel):
    status: str
    application_id: Optional[str] = None
    stdout: Optional[str] = None
    stderr: Optional[str] = None
