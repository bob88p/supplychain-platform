# NileSupply Global — End-to-End Supply Chain Data Platform
Production-style data engineering platform implementing reliable CDC (PostgreSQL WAL -> Debezium -> Kafka -> Python Consumer -> MinIO Bronze), Lakehouse storage (Apache Iceberg + Nessie), Spark & dbt/Trino transformations, Airflow orchestration, Great Expectations quality gates, Prometheus/Grafana observability, and Power BI analytics.

## Repository Structure
- `infra/`: Service configurations (PostgreSQL, Debezium, Kafka, Registry, MinIO, Nessie, Trino, Airflow, Monitoring)
- `services/`: Python ingestion services (`cdc-consumer`, `batch-ingestion`)
- `contracts/`: Event contracts, schemas, and data dictionaries
- `lakehouse/`: Spark jobs and Iceberg configurations
- `dbt/`: Staging, intermediate, and Gold mart models
- `dags/`: Airflow orchestration DAGs
- `quality/`: Great Expectations suites and checks
- `powerbi/`: Semantic model and report documentation
- `tests/`: Unit, integration, contract, e2e, and load tests
- `docs/`: System architecture, ADRs, runbooks, and data models