# Sistemi e Architetture per Big Data - AA 2021/2011

Lo scopo del progetto è rispondere a 3 query riguardanti il dataset dei dati della città di New York (NYC), utilizzando il framework di data processing Apache Spark.
Per gli scopi di questo progetto si utilizzano i seguenti file, forniti in formato Parquet, relativi, rispettivamente, ai viaggi dei taxi di colore giallo ed ai mesi di dicembre 2021, gennaio 2022 e febbraio 2022:
- yellow tripdata 2021-12.parquet,
- yellow tripdata 2022-01.parquet,
- yellow tripdata 2022-02.parquet,


Disponibili ai seguenti link:
- https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2021-12.parquet
- https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2022-01.parquet
- https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2022-02.parquet

## Deployment
I nodi worker per Spark e HDFS possono essere scalati utilizzando docker-compose, in particolare modificando il file start_docker.sh contenuto nelladirectory docker:
    sudo docker-compose up --scale spark-worker=3 --scale hdfs-datanode=5 -d
