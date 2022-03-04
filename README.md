# Spark with docker

##### Step-1
```
docker-compose up -d
```

##### Step-2
```
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit /case-study/assignment.py --input_dir /case-study/Data  --output_dir /case-study/Output
```