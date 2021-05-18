# SABD
This project uses the Spark engine to analyze the data taken from the github repository of vaccinations in Italy against Covid 19 ([covid19-opendata-vaccini](https://github.com/italia/covid19-opendata-vaccini)).

## Requirements
This project uses docker and docker-compose to instantiate the HDFS, Spark and Nifi containers.

#### Docker links :
* [docker download](https://www.docker.com/products/docker-desktop)
* [docker compose download](https://docs.docker.com/compose/install/)

#### Frameworks links :
* [<img src="https://uploads-ssl.webflow.com/5abbd6c80ca1b5830c921e17/5ad766e2a1a548ee4fc61cf6_hadoop%20(1).png" width=70px>](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html)
* [<img src="https://upload.wikimedia.org/wikipedia/commons/thumb/f/f3/Apache_Spark_logo.svg/1200px-Apache_Spark_logo.svg.png" width=70px>](https://spark.apache.org/)
* [<img src="https://miro.medium.com/max/400/1*b-i9e82pUCgJbsg3lpdFnA.jpeg" width=70px>](https://nifi.apache.org/)

## Deployment

    docker-compose up --scale spark-worker=3 --scale datanode=4

Using the docker-compose command you can scale worker nodes as you please.

Each of the systems provides its web ui accessible at:
* http://localhost:9870 &nbsp;&nbsp;&nbsp; hdfs namenode
* http://localhost:8080 &nbsp;&nbsp;&nbsp; spark master
* http://localhost:9090 &nbsp;&nbsp;&nbsp; nifi

On the first deployment of the cluster you can import the templates to use in nifi using the web ui:
    
    /nifi/templates

## Submitting queries
To submit a query to the spark cluster you can use the scripts in the folder /scripts in the root of the project.

    sh submit_query.sh 1

The script takes one parameter specifying the query, e.g. 1 submits Query1.