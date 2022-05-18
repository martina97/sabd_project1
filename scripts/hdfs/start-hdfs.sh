#!/bin/bash

CSV1="https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2021-12.parquet"
CSV2="https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2022-01.parquet"
CSV3="https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2022-02.parquet"

mkdir -m 777 data
cd ./data

wget $CSV1
#wget $CSV2
#wget $CSV3

cd ..

sudo docker cp data hdfs-namenode:/data/
sudo docker exec -it hdfs-namenode hdfs dfs -put /data /data
sudo docker exec -it hdfs-namenode hdfs dfs -mkdir /output
sudo docker exec -it hdfs-namenode hdfs dfs -chmod 0777 /output