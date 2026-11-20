# Choosing the lakehouse Storge Achitecture 

## Status 
    Accepted

## Context
    NileSupply Global needs a scalable storge architecture to manage approximtely 
        100 million historical records and future data changes 

    the system needs to suuport :
    
    - large scale data storge 
    - SQL querys
    - distributed data processing
    - Schema evolution
    - Reliable table management

    Decision 
    We decides to use :
    
    -MinIO for object storge.
    -Apache Iceberg as the table format 
    -Project Nessie as the catalog



## Architecture 
    MinIO -> Iceberg -> Nessie Catalog