# SABD
This project uses Spark engine to analyze the data taken from the github repository of vaccinations in Italy against Covid 19 ([covid19-opendata-vaccini](https://github.com/italia/covid19-opendata-vaccini)) in order to answer the following queries:

<b>Query1</b> - mean number of vaccinations for a generic center for each region and each month

<b>Query2</b> - for each month and age the top five regions which have been predicted to have the highest number of women vaccinated the first day of that month

<b>Query3</b> - for each region predict the total number of vaccinations for the first day of June and classify them using K-means or Bisecting K-means as clustering algorithms

The code for each of these queries can be found in src/main/java/queries, but you can also find alternative implementations for the second and third queries using SparkSQL in folder src/main/java/sql_queries. 

## Requirements
This project uses docker and docker-compose to instantiate the HDFS, Spark, Nifi and Redis containers.

#### Docker links :
* [docker download](https://www.docker.com/products/docker-desktop)
* [docker compose download](https://docs.docker.com/compose/install/)

#### Frameworks links :
* [<img src="https://uploads-ssl.webflow.com/5abbd6c80ca1b5830c921e17/5ad766e2a1a548ee4fc61cf6_hadoop%20(1).png" width=70px>](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html)
* [<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/f/f3/Apache_Spark_logo.svg/1200px-Apache_Spark_logo.svg.png" width=70px>](https://spark.apache.org/)
* [<img src="https://miro.medium.com/max/400/1*b-i9e82pUCgJbsg3lpdFnA.jpeg" width=70px>](https://nifi.apache.org/)
* [<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/6/6b/Redis_Logo.svg/1200px-Redis_Logo.svg.png" width=70px height=35px>](https://redis.io/)

## Deployment

Worker nodes for both spark and hdfs can be scaled as needed using docker-compose:

    docker-compose up --scale spark-worker=3 --scale datanode=4

e.g. cluster with 3 spark workers and 4 hdfs datanodes.

## Nifi configuration
On the first deployment of the cluster you can import the templates to use in nifi saved in the folder /nifi/templates:

* <b>input.xml</b> - takes data from the github repository and injects them into hdfs
* <b>redis.xml</b> - takes data from hdfs and puts them in redis

## Query submission
Create the jar needed for the submission of the query:

    mvn package    

To submit a query to the spark cluster you can use the scripts in the folder /scripts in the root of the project.

    sh submit_query.sh 1

    sh submit-query.sh 3 0 4

The first parameter specifies which query to submit, while other parameters are necessary only for query 3 and sql query 3 to specify algorithm (0 for k-means and 1 for bisecting k-means) and number of clusters.

## Web UI

* http://localhost:9870 &nbsp;&nbsp;&nbsp; hdfs namenode
* http://localhost:8080 &nbsp;&nbsp;&nbsp; spark master
* http://localhost:4040 &nbsp;&nbsp;&nbsp; spark application
* http://localhost:9090/nifi &nbsp;&nbsp;&nbsp; nifi

## Visualize data
You can visualize the data collected trough the Grafana dashboard using the following link ( shows the ranking for each region and the trend for each category using only the data from Lazio ):

[SABD dashboard](https://lisa9601.grafana.net/dashboard/snapshot/j262Lw4iUP8ucMkrC7MvJtbhqX5878qx)