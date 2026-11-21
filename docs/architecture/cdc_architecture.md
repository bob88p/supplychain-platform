# DE-101: Reliable CDC Ingestion Architecture (`sales_orders`)

## 1. Purpose & Design Philosophy
The ERP `sales_orders` table contains ~100M historical rows with continuous `INSERT`, `UPDATE`, and `DELETE` mutations. Polling `updated_at` is prohibited because it misses hard deletes, misses updates that do not modify `updated_at`, loses intermediate state transitions, and places heavy read load on the OLTP database.

**Delivery Guarantee:** We design for **At-Least-Once Delivery** from Kafka to MinIO Bronze paired with an **Idempotent Sink**. End-to-end exactly-once delivery across Kafka, MinIO, and Metadata PostgreSQL is explicitly not claimed because they are independent storage systems without a shared distributed transaction coordinator.

## 2. Initial Snapshot & Bootstrap Strategy (100M Rows)
- **Mechanism:** Debezium PostgreSQL connector using `snapshot.mode=initial` (with incremental chunked snapshot capability via signaling table for safe resumptions).
- **Consistency Boundary:** Debezium opens a repeatable-read transaction, acquires a PostgreSQL logical replication slot, records the exact WAL `LSN` cutover point, reads historical rows in bounded chunks (`snapshot.fetch.size=10000`), and seamlessly transitions to streaming WAL events from that exact `LSN` without gaps or duplicates.
- **WAL Retention Risk & Mitigation:** PostgreSQL retains WAL segments for the active replication slot. We configure `max_slot_wal_keep_size` as a safety circuit-breaker against disk exhaustion and monitor `pg_replication_slots` lag continuously.

## 3. Kafka Topic & Partition Design
| Topic Name | Partitions | Replication Factor | Cleanup Policy | Retention | Message Key |
| :--- | :---: | :---: | :---: | :---: | :--- |
| `erp.cdc.sales_orders` | 6 | 1 (local) / 3 (prod) | `delete` | 7 days (168h) | `order_id` |
| `erp.cdc.order_items` | 6 | 1 (local) / 3 (prod) | `delete` | 7 days (168h) | `order_id` |
| `erp.dlq.sales_orders` | 3 | 1 (local) / 3 (prod) | `delete` | 30 days (720h) | `order_id` |
| `erp.schema-changes` | 1 | 1 (local) / 3 (prod) | `compact,delete` | 30 days | `table_name` |

## 4. Event Contract & Deterministic Event Identity
Deterministic `event_id` formula (random consumer UUIDs are prohibited):
`event_id = sha256(f"{source_system}:{database_name}:{schema_name}:{table_name}:{order_id}:{lsn}:{operation}")`

## 5. MinIO Bronze Storage Layout & Metadata Control Plane
- **Object Path:** `s3://supplychain-platform/bronze/cdc/source=erp/table=sales_orders/ingest_date=YYYY-MM-DD/partition={kafka_partition:02d}/{event_id}.json`
- **Metadata PostgreSQL Tables (`supplychain_platform_meta`):** `processed_events` (`event_id` PK) and `bronze_manifests`.

## 6. Safe Write & Offset-Commit Sequence
1. Poll record(s) with `enable.auto.commit=false`.
2. Validate schema & compute deterministic `event_id` (route invalid/poison payloads to `erp.dlq.sales_orders`).
3. Persist immutable object to MinIO at `{event_id}.json`.
4. Insert `event_id` into `processed_events` (`ON CONFLICT DO NOTHING`) and update `bronze_manifests` in Metadata PostgreSQL.
5. Call `consumer.commit(asynchronous=False)` ONLY after Steps 3 and 4 succeed durably.

## 7. Crash-Recovery Matrix
- **Crash before MinIO write:** Kafka redelivers; zero loss, zero duplicates.
- **Crash after MinIO write, before Metadata DB:** Kafka redelivers; MinIO overwrites identical `{event_id}.json` idempotently; Metadata DB inserts; offset commits.
- **Crash after Metadata DB, before Offset commit:** Kafka redelivers; `processed_events` PK detects duplicate, skips re-write, increments `cdc_duplicate_events_total`, commits offset.
- **Out-of-order replay:** Silver Iceberg `MERGE` compares `source_lsn` and ignores stale events.

## 8. Schema Evolution & Dead Letter Queue (DLQ) Policy
Apicurio Registry enforces `BACKWARD` compatibility. Transient errors retry with bounded exponential backoff; permanent schema/payload errors route immediately to `erp.dlq.sales_orders` with diagnostic headers.

## 9. Observability Metrics & Operational Runbook Summary
- **Metrics:** `cdc_events_consumed_total`, `cdc_events_persisted_total`, `cdc_duplicate_events_total`, `cdc_dlq_events_total`, `cdc_minio_write_latency_seconds`, `cdc_consumer_lag`, `cdc_source_freshness_seconds`.
- **Runbook:** Check container health, consumer logs, MinIO `:9000/minio/health/live`, Metadata DB readiness, Kafka consumer lag, and `pg_replication_slots` retained WAL before taking recovery actions.
