# Demo 3 design — tenant router + whale vs shared isolation

**Status: design doc, not implemented.** This describes the architecture, the key choices, and the demo plan for noisy-neighbor isolation. Nothing in the repo is wired up for it yet.

The companion phase that already exists (Phase 2+3) assumes one shared pipeline for all tenants. This document is about splitting that into two pipelines so a whale tenant can't degrade service for everyone else.

---

## Goal

Prove that a 1000:1 traffic imbalance between tenants does not translate into a 1000:1 latency imbalance.

Concrete success criteria for the demo:

- Generate 100,000 vehicle events for California (`tenant_id = 101`, whale).
- Generate 100 vehicle events for Nevada (`tenant_id = 102`, shared) **during the same window**.
- Measure end-to-end latency (Postgres commit → Silver row visible) for Nevada events.
- Target: Nevada's p99 end-to-end latency during the whale burst stays within 2× of its steady-state latency.
- Non-target: California's latency during the burst — it will degrade, that's expected.

If the shared pipeline cannot meet that bound while a single-pipeline PoC cannot, we've shown the value of isolation.

---

## What noisy-neighbor actually looks like in our current stack

Today everything shares resources:

```
Postgres  →  Debezium (one connector)  →  Kafka (3 topics, 1 partition each)
                                                      │
                                                      ▼
                     kafka_to_bronze.py — one Spark job, one JVM
                                   │
                                   ▼
                     bronze.*_cdc — one shared Iceberg writer
                                   │
                                   ▼
                     bronze_to_silver.py — one Spark job, one JVM
                                   │
                                   ▼
                     silver.* — one shared Iceberg table per source
```

A 100K-row California burst saturates every shared resource in sequence:

| Shared resource | Failure mode under burst |
|---|---|
| **Postgres replication slot** | Debezium backs up on WAL fetch; Nevada's events sit in the slot until the burst is drained |
| **Kafka topic (1 partition)** | Debezium produces to the same topic for all tenants in strict offset order; Nevada's events are physically behind California's in the log |
| **Spark micro-batch** | Each micro-batch reads whatever's in the topic — mostly California. Nevada's events get processed, but each batch is huge and slow |
| **Iceberg MERGE commit** | Each micro-batch's MERGE scans + rewrites files; dominated by California's volume. Commit contention is low (one writer) but duration is high |
| **Checkpoint write** | Grows with batch size |

The latency bound for Nevada is effectively **"wait until California's burst clears."** With a 100K burst and a 5-second micro-batch, this could easily be minutes.

---

## Target architecture

Split the pipeline per **tenant classification** (whale vs shared), not per tenant. Each classification gets its own topics, Spark job, and resource pool.

```
Postgres  →  Debezium (one connector)  →  Kafka source topics (db.public.*)
                                                      │
                                                      ▼
                                     ┌──────────────────────────┐
                                     │     Tenant Router        │
                                     │  (Kafka Streams / Flink  │
                                     │   / Kafka Connect SMT)   │
                                     └──────────┬───────────────┘
                                                │  routes by tenant_id
                              ┌─────────────────┴─────────────────┐
                              ▼                                   ▼
            ┌─────────────────────────────┐       ┌─────────────────────────────┐
            │  whale topics               │       │  shared topics              │
            │  whale.ca.vehicle           │       │  shared.vehicle             │
            │  whale.ca.registration      │       │  shared.registration        │
            │  whale.ca.payment           │       │  shared.payment             │
            │  (more partitions)          │       │  (fewer partitions)         │
            └─────────────┬───────────────┘       └─────────────┬───────────────┘
                          ▼                                      ▼
            ┌─────────────────────────────┐       ┌─────────────────────────────┐
            │  Spark whale job            │       │  Spark shared job           │
            │  dedicated executors        │       │  shared executors           │
            │  own checkpoint             │       │  own checkpoint             │
            └─────────────┬───────────────┘       └─────────────┬───────────────┘
                          ▼                                      ▼
            ┌─────────────────────────────┐       ┌─────────────────────────────┐
            │  bronze.whale_ca.*          │       │  bronze.shared.*            │
            │  silver.whale_ca.*          │       │  silver.shared.*            │
            │  gold.whale_ca.*            │       │  gold.shared.*              │
            └─────────────────────────────┘       └─────────────────────────────┘
```

Three layers of isolation:
1. **Kafka layer** — separate topics mean separate offset streams. Nevada's events are not physically behind California's in any log.
2. **Compute layer** — separate Spark jobs mean separate JVMs, schedulers, memory pools, and GC behaviors.
3. **Storage layer** — separate Iceberg tables (and namespaces) mean separate commit paths, no catalog contention, independent file lifecycles.

---

## Key design decisions

### 1. Where does the router sit?

Four candidates:

| Candidate | How it works | Verdict |
|---|---|---|
| Debezium Single Message Transform (SMT) | Add `transforms` to connector config; `io.debezium.transforms.ByLogicalTableRouter` or custom SMT rewrites the topic name based on `after.tenant_id` | **Simplest; recommended.** No new service. Config-only change. |
| Kafka Streams / Flink router app | Separate microservice consumes `db.public.*`, re-publishes to whale/shared topics | More flexible (can do stateful routing, enrichment); more infra. |
| Debezium content-based routing with `MessageSelector` | Content-based filter + fan-out via multiple connectors | Fragile; multiple connectors mean multiple replication slots and WAL amplification. Avoid. |
| Routing at the Spark reader | One Spark job reads `db.public.*`, routes to different write streams | Defeats the purpose — no resource isolation. |

**Recommended: Debezium SMT.** Config in `connect/dmv-postgres-connector.json`:

```json
"transforms": "route",
"transforms.route.type": "io.debezium.transforms.ContentBasedRouter",
"transforms.route.language": "jsr223.groovy",
"transforms.route.topic.expression":
  "value.get('after').get('tenant_id') == 101 ? 'whale.california.' + topic.substring(10) : 'shared.' + topic.substring(10)"
```

Downside: ContentBasedRouter requires the Groovy JSR-223 jar in the Connect classpath, and SMT execution is a per-record scripted call — small CPU cost per event. For 100K events at demo scale this is fine; at 100M it bears measuring.

### 2. Topic naming convention

```
whale.<tenant_slug>.<table>      e.g. whale.california.vehicle
shared.<table>                   e.g. shared.vehicle
```

- One topic per whale, per table. Whale tenants are few; this scales fine.
- One topic per shared table, with all shared tenants multiplexed in. Partitioning within the topic can use `tenant_id` as the partition key so one shared tenant's traffic lands on a predictable partition.
- Delete handling: tombstones follow the same routing (keyed by the same `tenant_id`).

Alternatives considered:
- `db.<tenant>.<table>` for everyone — simpler, but no classification distinction; harder to reason about resource pools.
- One topic per tenant always — tenant proliferation becomes a Kafka admin burden (topic count, metadata overhead).

### 3. Spark job topology

Two independent `spark-submit` processes:

```
kafka_to_bronze_whale.py    reads whale.california.* → bronze.whale_california.*
kafka_to_bronze_shared.py   reads shared.*          → bronze.shared.*

bronze_to_silver_whale.py   reads bronze.whale_california.* → silver.whale_california.*
bronze_to_silver_shared.py  reads bronze.shared.* → silver.shared.*
```

Each job:
- Its own `--driver-memory`, `--executor-memory`, `--executor-cores`.
- Its own Kafka consumer group → independent offset tracking.
- Its own checkpoint directory on RustFS.
- Its own Iceberg writer identity → independent commit backoff.

**On a laptop PoC** these are both `--master local[*]` but with distinct JVMs. In production you'd target them at separate YARN/K8s pools, or separate Spark clusters.

### 4. Downstream namespace / table layout

```
bronze.whale_california.vehicle
bronze.whale_california.registration
bronze.whale_california.payment
bronze.shared.vehicle          (partitioned by tenant_id; multiple shared tenants)
bronze.shared.registration     (partitioned by tenant_id)
bronze.shared.payment          (partitioned by tenant_id)

silver.whale_california.*      (same pattern)
silver.shared.*                (partitioned by tenant_id)

gold.whale_california.*        (per-whale aggregations, rebuilt independently)
gold.shared.*                  (shared aggregations, rebuilt once)
```

Grants align with namespaces (ties into [[security-design]]): `whale_california` principal gets `GRANT ALL ON NAMESPACE whale_california`; shared principals get scoped views on `silver.shared.*`.

### 5. Scaling knobs that actually matter

| Knob | Whale | Shared |
|---|---|---|
| Kafka topic partitions | High (8–32) — can parallelize consumer | Lower (1–4) |
| Spark executor count | Dedicated pool, provisioned for burst | Shared pool |
| Micro-batch trigger | Short (1–3s) for low latency | Longer (10–30s) acceptable |
| Iceberg data file target size | Larger (128–256 MB) for scan efficiency | Smaller (64 MB) given lower volume |
| `expire_snapshots` cadence | More frequent (hourly) — more churn | Daily |

### 6. Promotion and demotion

A tenant classified "shared" today might outgrow it. Promotion to whale requires:

1. Create the whale namespace and empty tables.
2. Pause the shared stream for the target tenant (adjust the router filter).
3. Copy historical data: `INSERT INTO whale_X.vehicle SELECT * FROM shared.vehicle WHERE tenant_id = X`.
4. Verify counts match.
5. Update router config: tenant X now routes to whale topics.
6. Delete tenant X from shared tables.

Demotion is the mirror image. Both are planned events with explicit cutovers.

---

## Scaling knob interactions worth understanding

### Why more Kafka partitions help whales but not shared

Spark Structured Streaming with Kafka source reads partitions in parallel — one Kafka partition = one Spark task. A whale with 16 partitions can be consumed by 16 parallel executors; a shared topic with 4 partitions caps at 4. For throughput-bound workloads this is linear scaling.

But: **ordering within a key is preserved only within a partition.** If you need per-key ordering for a tenant (we do — the LSN guard assumes it), the partition key must be stable per tenant. For a whale topic that only carries one tenant, any partition key works. For a shared topic, partition on `tenant_id` so one tenant's events stay on one partition and remain in order.

### Why the Iceberg catalog is still a single point of coordination

Every Iceberg commit is a `UPDATE iceberg_tables SET metadata_location = ...` in Postgres. The REST catalog serializes commits per table. If whale and shared both commit to the same table, they contend; if they commit to different tables (our design), no contention.

But: the catalog's Postgres backend is itself shared infrastructure. A whale producing 1000 commits/minute on its tables doesn't directly block shared commits (different rows in `iceberg_tables`), but if the Postgres connection pool is exhausted, every connecting caller waits. Catalog connection pool sizing becomes a shared resource worth monitoring.

### Why "share nothing" at the Spark layer matters more than "share nothing at Kafka"

Kafka can handle enormous partition counts and consumers cheaply. The real contention is at the compute layer — one JVM running MERGE for 100K rows while another is trying to process 100 rows will starve the small job of CPU/memory because the GC and task scheduler don't know (or care) about tenant classification.

Running two separate Spark JVMs sidesteps this entirely at the cost of memory overhead (two drivers, two sets of executors). For a laptop PoC it's a few hundred extra MB.

---

## Demo plan (what to actually run)

**Step 0.** Build the router (Debezium SMT or standalone), build the whale and shared Spark jobs. Verify basic routing with a few manual INSERTs per tenant.

**Step 1.** Baseline the shared pipeline (current single-pipeline). Generate 100K rows for California + 100 for Nevada concurrently. Measure Nevada's p50 / p99 end-to-end latency. Expect it to be bad — probably minutes.

**Step 2.** Switch to the routed pipeline. Repeat the same load. Measure Nevada's latency.

**Step 3.** Compare. Nevada should be within target (2× steady state).

**Step 4.** Repeat with a 10× heavier whale load. See where the shared pipeline degrades, if at all.

### Load generator design

A simple Python script:

```python
# Generate California burst
for i in range(100_000):
    cur.execute("INSERT INTO vehicle (tenant_id, vehicle_id, ...) VALUES (101, %s, ...)", (i,))
    if i % 1000 == 0: conn.commit()

# Concurrently, Nevada steady-state (threading / asyncio)
for i in range(100):
    cur.execute("INSERT INTO vehicle (tenant_id, vehicle_id, ...) VALUES (102, %s, ...)", (i,))
    conn.commit()
    time.sleep(1)   # one event per second, 100 seconds total
```

Timing instrumentation: tag each INSERT with `inserted_at = now()`; measure `silver_updated_at - source_ts_ms` in Silver after the fact.

---

## Pragmatic sequencing (what to build first)

If this becomes a real phase:

1. **Router proof-of-concept** — Debezium SMT alone, routing to just two topics (`whale.california.vehicle`, `shared.vehicle`). No Spark changes yet. Verify the routing is correct.
2. **Dedicated whale Spark job** — `kafka_to_bronze_whale.py`, `bronze_to_silver_whale.py`, writing to `bronze.whale_california.*` and `silver.whale_california.*`. Keep the shared pipeline running against the original topics (which will now be empty of California).
3. **Load test** — the actual noisy-neighbor demo.
4. **Promotion scripts** — a reusable script to move a tenant between classifications. Even if we don't use it, writing it exposes the pain points.
5. **Operational dashboards** — Kafka lag per topic, Spark micro-batch duration per job, Iceberg commit rate per table.

Each step is independently valuable. Steps 1–3 are enough to answer "does the design work?" The rest is production readiness.

---

## Tradeoffs honest to the design

### What this gets you

- Hard isolation on Kafka and Spark layers. Nevada's latency is bounded by shared-pipeline load, not whale-pipeline load.
- Independent failure domains. A whale Spark OOM doesn't stop shared processing.
- Independent scaling. Whales can get big resources without over-provisioning for everyone.
- Independent lifecycles. Whale table compaction / expiration cadence can differ from shared.
- Natural alignment with per-tenant deletion (whale → `DROP TABLE whale_X.*`; shared → partition DELETE on `tenant_id = X`).

### What this costs you

- **Doubled operational surface.** Two Spark jobs to monitor, two sets of checkpoints, two sets of Kafka topics, two sets of tables.
- **Schema evolution fan-out.** Adding `emission_class` to the whale table means doing it to every whale namespace + the shared one. Automation is possible but not free.
- **Promotion/demotion complexity.** Moving a tenant between classifications is a scheduled operation, not a config toggle.
- **Routing engine is now critical path.** A bug in the router's filter can silently drop or misroute events. Needs tests and alerting.
- **Shared tenants still contend among themselves.** Shared Nevada and shared Rhode Island are on the same Kafka topic and same Spark job. A noisy shared tenant degrades other shared tenants. The fix is to classify them as whale, which is a planned operation, not an auto-reaction.

### What this does NOT solve

- **Not security isolation.** See [[security-design]]. The whale namespace is a resource boundary, not an auth boundary. A compromised credential could still cross.
- **Not Postgres hotspot.** Debezium is still one connector with one replication slot. A whale's write volume still generates one shared WAL stream. If Postgres itself is the bottleneck, this design doesn't help.
- **Not catalog contention.** The REST catalog + Postgres backend is still shared infra. Enough commit volume could saturate the catalog's connection pool.
- **Not Iceberg write contention within a table.** Doesn't apply here (we never co-locate whale and shared on the same table) but worth being aware of if someone later decides to put a whale back on a shared table.

---

## Open questions worth deciding before building

1. **ContentBasedRouter vs custom SMT?** Groovy scripting is flexible but adds a dependency and has a per-event cost. A custom Java SMT compiled once might be worth it if the router handles 1000+ TPS.
2. **How does the router handle a new tenant?** When tenant 104 gets added to Postgres, does it default to shared? Does it need a routing rule update? What's the deploy cycle for routing config changes?
3. **Observability.** Which per-tenant metrics do we actually want? Suggestion: Kafka lag per topic, Spark input rate per job, Iceberg commit duration per table, end-to-end latency sampled from `source_ts_ms` to `silver_updated_at`.
4. **Shared tenant promotion threshold.** Is it based on observed load (reactive) or forecast (proactive)? What's the SLA guarantee for shared tenants that justifies promoting someone?
5. **Does the Gold batch job need to be per-classification too?** Rebuilding `gold.shared.*` separately from `gold.whale_california.*` means more Spark invocations but faster individual rebuilds. Likely yes.

---

## What's in the repo today vs this doc

Nothing in the repo implements this. Current pipeline is single-shared for all tenants. The architecture doc at the top of the conversation calls for this split; Demo 3 would be the first step in realizing it.

If you decide to build it, the minimal implementation PR looks like:

```
Add tenant router config to connect/dmv-postgres-connector.json       (SMT)
Add spark/jobs/kafka_to_bronze_whale.py                                (reads whale.*)
Add spark/jobs/bronze_to_silver_whale.py                               (merges to silver.whale_california.*)
Add spark/sql/whale_california_tables.sql                              (whale DDL)
Update spark/jobs/kafka_to_bronze.py                                   (keeps running for shared.*)
Update spark/jobs/bronze_to_silver.py                                  (points at shared topics only)
Add load generator script                                              (demo payload)
Add docs/demo-3-tenant-router.md                                       (implementation writeup)
```

Then do the actual noisy-neighbor measurements.
