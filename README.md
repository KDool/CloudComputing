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
