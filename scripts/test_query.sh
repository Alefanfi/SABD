#!/bin/bash

if [[ $1 -eq 1 ]]
then
  echo "Testing Query1"
  for i in $(seq 1 $2);
  do
    (docker exec spark-master spark-submit --class "queries.Query1" --master "local" queries/SABD-1.0-SNAPSHOT.jar) 2> /dev/null
    echo $i
  done
elif [[ $1 -eq 2 ]]
then
  echo "Testing Query2"
  for i in $(seq 1 $2);
  do
    (docker exec spark-master spark-submit --class "queries.Query2" --master "local" queries/SABD-1.0-SNAPSHOT.jar) 2> /dev/null
    echo $i
  done
elif [[ $1 -eq 3 ]]
then
  echo "Testing Query3"
  for i in $(seq 1 $2);
  do
    (docker exec spark-master spark-submit --class "queries.Query3" --master "local" queries/SABD-1.0-SNAPSHOT.jar $3 $4) 2> /dev/null
    echo $i
  done
elif [[ $1 -eq 4 ]]
then
  echo "Testing SQL Query2"
  for i in $(seq 1 $2);
  do
    (docker exec spark-master spark-submit --class "sql_queries.Query2" --master "local" queries/SABD-1.0-SNAPSHOT.jar) 2> /dev/null
    echo $i
  done
elif [[ $1 -eq 5 ]]
then
  echo "Testing SQL Query3"
  for i in $(seq 1 $2);
  do
    (docker exec spark-master spark-submit --class "sql_queries.Query3" --master "local" queries/SABD-1.0-SNAPSHOT.jar $3 $4) 2> /dev/null
    echo $i
  done
else
  echo "Usage: sh test_query.sh query_number num_iterations param_if_any"
fi