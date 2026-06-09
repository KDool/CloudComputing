# CloudComputing
This is Cloud Computing project



# How to run Docker REST SERVER
``` 
docker run -d   --name rest-server   --user 1000:1000   
--network host   
-v /etc/passwd:/etc/passwd:ro   
-v /etc/group:/etc/group:ro   
-v /opt/hadoop:/opt/hadoop   
-v /opt/spark:/opt/spark   
-v /data/jobs:/workspace   
-e HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop   
-e YARN_CONF_DIR=/opt/hadoop/etc/hadoop   
rest-server:0.0.2

```

# Access the Swagger UI
Once the REST server is running, open the Swagger UI in your browser at:

- `http://10.1.1.126:8080/docs`

This endpoint is provided by FastAPI and exposes the API documentation for:

- `POST /upload/jar`
- `POST /upload/pyspark`
- `POST /jobs/spark`
- `POST /jobs/hadoop`
- `GET /jobs/{application_id}`

If you are not using host networking, replace `localhost` with the container host IP or hostname and ensure port `8080` is accessible.

# Example job submission

## Submit the Spark RDD example

From the project root, use Spark's `spark-submit` with the example script:

```bash
spark-submit spark/chicago_crimes_spark_rdd.py <input_csv_or_dir> <output_dir>
```

Example:

```bash
spark-submit spark/chicago_crimes_spark_rdd.py archive/Chicago_Crimes_2001_to_2004.csv spark-output
```

The job will write output files under `spark-output` (one part file if the script uses `coalesce(1)`).

## Build and submit the Hadoop MapReduce example

First build the JAR from the `map-reduce` module:

```bash
cd map-reduce
mvn package
```

Then submit it with Hadoop:

```bash
hadoop jar target/map-reduce-1.0-SNAPSHOT.jar it.unipi.cloud.ChicagoCrimesApp <input_csv> <intermediate_output_dir> <final_output_dir>
```

Example:

```bash
cd map-reduce
hadoop jar target/map-reduce-1.0-SNAPSHOT.jar it.unipi.cloud.ChicagoCrimesApp /data/jobs/chicago_crimes.csv /data/jobs/mr-intermediate /data/jobs/mr-final
```

If the output directories already exist, remove them before running the job:

```bash
hdfs dfs -rm -r /data/jobs/mr-intermediate /data/jobs/mr-final
```

> Replace paths with your actual input/output locations. If you are running inside a Docker container, use the mounted host paths visible to the container (for example `/workspace`).
