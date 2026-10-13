# Phase 1 — Postgres → Debezium → Kafka

Goal: capture row-level changes from a multi-tenant Postgres database and land them in Kafka as CDC events, with no custom code.

By the end of Phase 1 you have an INSERT, UPDATE, and DELETE in Postgres each appearing as a separate event in a Kafka topic, keyed by the row's primary key.

---

## Prerequisites

- Docker Desktop running (tested on macOS, Docker 28.1+).
- Ports available on the host: `5432` (Postgres), `9092` (Kafka), `8083` (Kafka Connect REST).
- `curl` and `psql` optional for host-side verification; both are also available inside the containers.

---

## Architecture

```
Postgres (WAL, wal_level=logical)
   |
   | logical replication slot (pgoutput)
   v
Debezium Postgres Connector (inside Kafka Connect)
   |
   | produces one Kafka message per row change
   v
Kafka (KRaft, single broker)
   |
   | topics: db.public.<table>
   v
Consumer of your choice
```

Three containers, one network:

| Container     | Image                           | Role                       | Host port |
|---------------|---------------------------------|----------------------------|-----------|
| `dm-postgres` | `postgres:16.4`                 | Operational database       | 5432      |
| `dm-kafka`    | `confluentinc/cp-kafka:7.7.1`   | Event transport (KRaft)    | 9092      |
| `dm-connect`  | `debezium/connect:2.7.3.Final`  | Runs the Debezium connector| 8083      |

---

## Files in this phase

```
docker-compose.yml                     # 3 services + 2 volumes
db/schema.sql                          # tables, publication, debezium user
connect/dmv-postgres-connector.json    # connector config (POSTed to Connect REST)
```

---

## Step 1 — Start Postgres

Create `docker-compose.yml` with the `postgres` service and bring it up:

```bash
docker compose up -d postgres
```

Verify logical replication is enabled (Debezium requires it):

```bash
docker exec dm-postgres psql -U dmv -d dmv -c "SHOW wal_level;"
# expected: logical
```

**Why `wal_level=logical`:** Postgres always writes to the WAL, but by default it only logs enough to replay on crash (physical replication). `logical` adds the extra information needed to decode WAL into row-level change events — which is what Debezium consumes.

---

## Step 2 — Create schema, user, publication

Apply `db/schema.sql`:

```bash
docker exec -i dm-postgres psql -U dmv -d dmv < db/schema.sql
```

This creates:

- `vehicle`, `registration`, `payment` — operational tables, composite PKs `(tenant_id, record_id)`.
- `tenant` — lookup table, seeded with California (whale), Nevada, Rhode Island.
- `debezium` role — login + REPLICATION attributes, read-only on CDC tables.
- `dbz_pub` — a Postgres `PUBLICATION` covering only the three CDC tables.

**Why `REPLICA IDENTITY FULL`:** By default, Postgres only logs primary-key columns on UPDATE/DELETE. Composite PKs still get PKs logged, but downstream MERGE logic typically wants the entire previous row (full "before" image). `FULL` logs every column. Trade-off: larger WAL volume. For a PoC this is fine.

**Why a dedicated publication (not `FOR ALL TABLES`):** The publication is Postgres's whitelist of tables whose changes are decoded. Scoping it to the three CDC tables keeps the stream deterministic and prevents accidentally streaming `tenant` or any future table added to the schema. Debezium can auto-create a publication, but creating it ourselves means the connector needs no `CREATE` privilege.

Verify:

```bash
docker exec dm-postgres psql -U dmv -d dmv -c "\dt"
docker exec dm-postgres psql -U dmv -d dmv -c "SELECT * FROM tenant;"
docker exec dm-postgres psql -U dmv -d dmv -c \
  "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname='dbz_pub';"
```

---

## Step 3 — Start Kafka (KRaft, single node)

```bash
docker compose up -d kafka
```

Verify:

```bash
docker exec dm-kafka kafka-broker-api-versions --bootstrap-server localhost:9092
docker exec dm-kafka kafka-topics --bootstrap-server localhost:9092 --list
# expected: no topics listed yet
```

**Key compose settings and why:**

- `KAFKA_PROCESS_ROLES: broker,controller` — single-node KRaft, no ZooKeeper. Simpler for a PoC.
- `CLUSTER_ID`: must be a 22-character base64-url-encoded UUID. We hardcode one so the volume stays interpretable across restarts.
- Two listeners:
  - `PLAINTEXT://kafka:29092` — advertised to other containers on the Docker network.
  - `HOST://localhost:9092` — advertised to clients on your Mac.
  - Advertising the right address per client is a very common source of "connection refused" / "metadata timeout" errors — do not collapse these to one.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` — broker will not auto-create topics on first produce. See step 5 for how we create topics instead.
- All replication factors = 1 — single broker.

---

## Step 4 — Start Kafka Connect (Debezium)

```bash
docker compose up -d connect
```

Verify the REST API is up and the Debezium Postgres plugin is installed:

```bash
curl -s http://localhost:8083/
# {"version":"3.7.0","commit":"...","kafka_cluster_id":"..."}

curl -s http://localhost:8083/connector-plugins | tr ',' '\n' | grep postgres
# includes: "class":"io.debezium.connector.postgresql.PostgresConnector"

curl -s http://localhost:8083/connectors
# []
```

**Key compose settings and why:**

- `BOOTSTRAP_SERVERS: kafka:29092` — the Docker-network listener, not the host one.
- `CONFIG_STORAGE_TOPIC`, `OFFSET_STORAGE_TOPIC`, `STATUS_STORAGE_TOPIC` — Kafka Connect stores its own state in Kafka. These topics are created automatically.
- `KEY_CONVERTER` and `VALUE_CONVERTER` set to JSON with schemas disabled — keeps messages readable in a console consumer. For production, Avro + Schema Registry is more common.
- `depends_on` with `condition: service_healthy` — Connect must not start before Postgres and Kafka are reachable.

---

## Step 5 — Register the Debezium connector

```bash
curl -X POST -H "Content-Type: application/json" \
  http://localhost:8083/connectors \
  --data @connect/dmv-postgres-connector.json

curl -s http://localhost:8083/connectors/dmv-postgres/status
# connector.state and tasks[0].state both: "RUNNING"
```

Verify the logical replication slot was created in Postgres:

```bash
docker exec dm-postgres psql -U dmv -d dmv -c \
  "SELECT slot_name, plugin, slot_type, active FROM pg_replication_slots;"
# debezium_dmv | pgoutput | logical | t
```

**Key connector config settings and why:**

- `plugin.name: pgoutput` — Postgres's built-in logical-decoding output plugin. Shipped with Postgres 10+, no extensions to install. Pairs with the `PUBLICATION` we created.
- `publication.name: dbz_pub` + `publication.autocreate.mode: disabled` — tell Debezium to use our pre-created publication, do not create or modify one.
- `slot.name: debezium_dmv` — named slot persists across connector restarts. If you delete the connector, you should drop the slot too (see Teardown) or Postgres will keep retaining WAL for it indefinitely.
- `snapshot.mode: initial` — on first start, SELECT * from each table in `table.include.list` and emit each existing row as an `op=r` event, then stream from the LSN at the start of the snapshot. On restart with existing committed offsets, skips the snapshot.
- `topic.prefix: db` — prefix for all topic names. With `table.include.list: public.vehicle,...`, topics will be `db.public.vehicle`, `db.public.registration`, `db.public.payment`.
- `topic.creation.default.replication.factor: 1` + `topic.creation.default.partitions: 1` — **required because the broker has auto-create disabled.** These tell the connector itself to create topics on first produce. Without this, the connector's producer loops forever trying to send to a non-existent topic and no events ever land. See "Gotcha" below.

---

## Step 6 — Smoke test: INSERT / UPDATE / DELETE

Generate CDC events:

```bash
docker exec -i dm-postgres psql -U dmv -d dmv <<'EOF'
INSERT INTO vehicle (tenant_id, vehicle_id, vin, make, model)
  VALUES (101, 2, 'JT2BF28K3W0000002', 'Honda', 'Civic');
INSERT INTO registration (tenant_id, registration_id, vehicle_id, status)
  VALUES (101, 1002, 2, 'ACTIVE');
UPDATE registration SET status='SUSPENDED', updated_at=now()
  WHERE tenant_id=101 AND registration_id=1002;
DELETE FROM registration
  WHERE tenant_id=101 AND registration_id=1002;
EOF
```

Confirm the topics appeared and consume them:

```bash
docker exec dm-kafka kafka-topics --bootstrap-server localhost:9092 --list \
  | grep '^db\.'
# db.public.registration
# db.public.vehicle

docker exec dm-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic db.public.registration \
  --from-beginning \
  --timeout-ms 5000 \
  --property print.key=true \
  --property print.topic=true \
  --property key.separator=' | '
```

Expected on `db.public.registration` — four messages for the one row:

| # | key                           | value                                     | notes                                        |
|---|-------------------------------|-------------------------------------------|----------------------------------------------|
| 1 | `{tenant_id:101, registration_id:1002}` | `op:c`, `before:null`, `after:{status:ACTIVE}` | INSERT                                       |
| 2 | same key                      | `op:u`, `before:{status:ACTIVE}`, `after:{status:SUSPENDED}` | full before-image thanks to `REPLICA IDENTITY FULL` |
| 3 | same key                      | `op:d`, `before:{status:SUSPENDED}`, `after:null` | DELETE                                       |
| 4 | same key                      | `null`                                    | **tombstone** — see explanation below        |

### Event envelope reference

Every Debezium event has the same shape:

```json
{
  "before": {...} | null,
  "after":  {...} | null,
  "op":     "c" | "u" | "d" | "r" | "t",
  "source": { "lsn": 26818056, "txId": 762, "ts_ms": ..., "snapshot": "false", ... },
  "ts_ms":  <when Debezium emitted the event>
}
```

- `op`: `c`=create, `u`=update, `d`=delete, `r`=read (snapshot), `t`=truncate.
- `source.lsn` and `source.txId` are the authoritative ordering keys. Use them to deduplicate / order downstream.
- `source.snapshot`: `"true"` / `"first"` / `"last"` for snapshot events, `"false"` for streamed events.

### Tombstones

After every `op=d` event Debezium emits a second message with the same key and a `null` value. This is Kafka's log-compaction signal: when the topic is configured for compaction, the tombstone eventually causes the key to be fully removed from storage.

- If you treat the CDC topic as a current-state table, tombstones are useful.
- If a consumer can't handle `null` values, filter them out, or disable with `tombstones.on.delete: "false"` in the connector config (you lose the compaction signal).

---

## Gotcha hit during development

**Symptom:** connector reports `RUNNING`, Postgres commits are visible, but no CDC events land in Kafka. Connect logs show the producer looping on `UNKNOWN_TOPIC_OR_PARTITION` for `db.public.<table>`.

**Cause:** `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` on the broker, and no `topic.creation.*` settings on the connector. Debezium's producer cannot create the topic itself and the broker won't create it on first produce.

**Fix (used in step 5):** add to the connector config

```json
"topic.creation.default.replication.factor": "1",
"topic.creation.default.partitions": "1"
```

**Secondary gotcha:** `PUT`ing a new config to an already-registered connector restarts it. If no offsets have been successfully committed yet (because of the above issue), the restart re-runs the snapshot. Rows that were INSERTed and then DELETEd before the restart are not in the table any more, so the snapshot misses them and the streaming phase starts past their LSN — those events are lost. If you need to recover cleanly, drop the connector and the slot, re-register, then do the DML.

---

## Teardown

Stop and remove containers (data persists in named volumes):

```bash
docker compose down
```

Nuke everything, including the Postgres and Kafka storage:

```bash
docker compose down -v
```

If you only want to reset the connector but keep Postgres data, you also need to drop the replication slot — otherwise Postgres keeps retaining WAL for it:

```bash
curl -X DELETE http://localhost:8083/connectors/dmv-postgres
docker exec dm-postgres psql -U dmv -d dmv -c \
  "SELECT pg_drop_replication_slot('debezium_dmv');"
```

---

## What Phase 1 does not do

- No Bronze/Silver/Gold Iceberg tables yet (Phase 2+).
- No Spark job consuming the topics.
- No tenant router — all three tenants land in the same `db.public.*` topics for now. Routing by `tenant_id` to separate "whale" vs "shared" topics comes in a later phase.
- Single broker, single-task connector, no HA. Fine for a laptop PoC.

---

## Recreating from a clean checkout

```bash
docker compose up -d postgres
docker exec -i dm-postgres psql -U dmv -d dmv < db/schema.sql

docker compose up -d kafka
docker compose up -d connect

# wait for Connect to be healthy
until curl -fsS http://localhost:8083/ >/dev/null; do sleep 2; done

curl -X POST -H "Content-Type: application/json" \
  http://localhost:8083/connectors \
  --data @connect/dmv-postgres-connector.json

# verify
curl -s http://localhost:8083/connectors/dmv-postgres/status
```

Then run step 6 to see events flow.
