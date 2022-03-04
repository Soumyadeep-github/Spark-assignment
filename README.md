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
###### Note : There are supposed to be 3 containers working, 1 master and 2 works. So perform a `docker container ls`.
##### Step-2

```
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit /case-study/assignment.py --input_dir /case-study/Data  --output_dir /case-study/Output
```