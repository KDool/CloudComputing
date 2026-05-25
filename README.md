# CloudComputing
This is Cloud Computing project



# How to run Docker REST SERVER
``` 
docker run -d \
  --name rest-server \
  --network host \
  -p 8080:8080 \
  -v /etc/hadoop/conf:/etc/hadoop/conf \
  -v /opt/hadoop:/opt/hadoop \
  -v /opt/spark:/opt/spark \
  -v /data/jobs:/workspace \
  rest-server 
  ```