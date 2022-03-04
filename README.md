# Spark with docker

In order to execute the pyspark script, first clone this repo and make sure the files
`docker-compose.yml`, `assignment.py` and `Data` are in the same directory (preferrably the
current directy.)
##### Step-1 
Now the first step spawns the containers. There are two spark-worker services and one 
spark-master service.
```
docker-compose up -d
```
###### Note : There are supposed to be 3 containers working, 1 master and 2 workers. So perform a `docker container ls` to check after step-1, whether they are all working as expected or not.

##### Step-2
Run the python file on the spark-master container directly with docker exec.
```
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit /case-study/assignment.py --input_dir /case-study/Data  --output_dir /case-study/Output
```