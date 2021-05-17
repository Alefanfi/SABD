#!/bin/bash

if [[ $1 -eq 1 ]]
then
  echo "Submitting query 1 to spark cluster"
  docker exec spark-master spark-submit --class "queries.Query1" --master "local" queries/SABD-1.0-SNAPSHOT.jar
elif [[ $1 -eq 2 ]]
then
  echo "Submitting query 2 to spark cluster"
  docker exec spark-master spark-submit --class "queries.Query2" --master "local" queries/SABD-1.0-SNAPSHOT.jar
elif [[ $1 -eq 3 ]]
then
  echo "Submitting query 3 to spark cluster"
  docker exec spark-master spark-submit --class "queries.Query3" --master "local" queries/SABD-1.0-SNAPSHOT.jar
else
  echo "Usage: sh query.sh query_number"
fi