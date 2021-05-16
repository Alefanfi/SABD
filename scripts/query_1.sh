#!/bin/bash -i

docker exec spark-master spark-submit --class "queries.Query1" --master "local" queries/target/SABD-1.0-SNAPSHOT.jar