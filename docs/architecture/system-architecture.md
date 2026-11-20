# NileSupplly Golbal_ stastem Achitecture

## 1: Project Overview
    An End to End Data Engineering Platform for a global supply chain company 


## 2: Data Achitecture 

    Postgres (erb) -> Debezium -> Kafka -> python Consumer -> MinIo -> Iceberg -> spark / Trino -> dbt -> Power BI


## 3: components and Responsibilities 

    POstgresql : source Database 
    Debezium : Capture database changes using CDC
    Kafka : Event Streaming 
    python consumer : Validation , Deduplication and custom logic

    MinIo : object storge
    Iceberg : Table Format 

    Nessie: Catalog and versioning 

    Spark : Distribute Data processing
    Trino : distributed sql query 

    dbt : Data Transformation (row based to column based) and modeling

    airflow : workflow orchestration

    power bi : Reports and Dashboards


## 4: Data Ingestion 

    . CDC : capture new database changes using debezium and kafka

    . batch : load historical data using python batch ingestion.

## 5: Storge Architecture

    .MinIO : stores data files 
    .Iceberg : organies data into tables 
    .nessie : manages table catalog and version




