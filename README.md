# Spark MongoDB API
Three docker containers, the first contain a hadoop with spark 2.2.1, the second contain a mongodb server and the third contain a api to access mongodb data
## Prerequisites
* Docker
* Docker Compose

## Dockerfiles
If you want to see or modify the dockerfiles, you can find them in the folder with their names

## Execute
First thing, you need to clone this repository
```
git clone https://github.com/guicolla/api-spark-mongodb-docker.git
```
Enter in the directory
```
cd api-spark-mongodb-docker/
```
Next you need run the application you need to do, in the directory of docker-compose.yml:
```
docker-compose up
```
After this you need to enter in the bash of spark container and execute the code to do calculations and insert into mongodb
* Executing the container bash
```
docker exec -it spark bash
```
* Executing the calculations
```
cd /root
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.0 etl_geofusion.py
```
Now the results is in mongodb, for visualize the data you only need to execute in the browser
```
http://localhost:5000
```
