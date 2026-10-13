# Security design — row-level + column-level + what we don't have yet

**Status: design doc, not implemented.** Our current stack has essentially no security — this document describes the gap, the options for closing it, and a pragmatic sequencing.

Nothing in this document is in the repo yet. It exists to drive a future planning conversation.

---

## Current state (honest)

| Component | Credential | Scope of access | Enforcement |
|---|---|---|---|
| Debezium → Postgres | `debezium` role | REPLICATION + SELECT on `vehicle`, `registration`, `payment` | Postgres role GRANT |
| Spark → REST catalog | — | Unauthenticated | None |
| Spark / REST catalog → RustFS | Hardcoded `rustfsadmin` / `rustfsadmin` in `spark-defaults.conf` and connector env | Full read/write on `s3://warehouse/*` | S3 key only |
| REST catalog → Postgres (JDBC backend) | `iceberg` role | Full access to `iceberg_catalog` DB | Postgres role GRANT |
| Any ad-hoc client → anything | Same hardcoded keys / direct Spark SQL | Full stack | None |

**Zero per-tenant authentication. Zero row filtering. Zero column masking.** A compromised Spark container reads every tenant's data; a leaked RustFS credential compromises every tenant's data. This is appropriate for a laptop PoC and inappropriate for anything else.

---

## Enforcement layers (where security can live in a lakehouse)

```
┌────────────────────────────────────────────────────────────┐
│ Application / BI tool  (dashboard, notebook, API consumer) │
└───────────────────┬────────────────────────────────────────┘
                    │  authenticated identity (OAuth / JWT)
                    ▼
┌────────────────────────────────────────────────────────────┐
│ Query gateway  (optional)                                   │
│   - inject tenant_id filters based on identity              │
│   - mask PII columns                                        │
│   - audit log                                               │
└───────────────────┬────────────────────────────────────────┘
                    ▼
┌────────────────────────────────────────────────────────────┐
│ Query engine  (Spark SQL, Trino, Dremio)                    │
│   - column projection via views                             │
│   - RLS / CLS policies (Trino + Ranger / Unity / Snowflake) │
│   - dynamic masking                                         │
└───────────────────┬────────────────────────────────────────┘
                    ▼
┌────────────────────────────────────────────────────────────┐
│ Catalog  (Iceberg REST — ours; or Polaris, Lakekeeper,      │
│  Nessie, Unity, Glue)                                       │
│   - namespace/table-level grants                            │
│   - principal auth (OAuth, bearer token)                    │
│   - vended S3 credentials (short-lived, scoped)             │
└───────────────────┬────────────────────────────────────────┘
                    ▼
┌────────────────────────────────────────────────────────────┐
│ Object storage  (RustFS)                                    │
│   - bucket / prefix policies                                │
│   - server-side encryption + per-column KMS keys            │
└────────────────────────────────────────────────────────────┘
```

Our stack today has only the two bottom layers (catalog and object storage), and both are open. Everything above the catalog shares one god identity. Any security story has to add at least the catalog-auth layer plus one of the layers above it.

---

## Row-level security — four patterns

### Pattern 1 — Per-tenant tables or namespaces (physical separation)

Each tenant gets its own table or namespace, grants are table-scoped.

```
silver.california.vehicle           → granted to CA principals only
silver.nevada.vehicle               → granted to NV principals only
silver.shared.vehicle               → granted to shared-tenant principals
```

- **Pros:** physical isolation; a compromised principal cannot even see other tenants' table identifiers; no runtime filter cost; cleanest audit story.
- **Cons:** table proliferation (N tenants × M logical tables); per-table maintenance burden (schema evolution has to be fanned out); your ingestion pipeline needs to route by tenant up-front (this is what the tenant router from Demo 3 would do).
- **Our architecture doc calls for this specifically for whale tenants.** The California DMV ("whale") gets dedicated tables; shared tenants (Nevada, Rhode Island) share a table and need a different mechanism.

### Pattern 2 — Per-tenant views on shared tables (logical separation)

```sql
CREATE VIEW silver.vehicle_tenant_101 AS
  SELECT * FROM silver.vehicle WHERE tenant_id = 101;
GRANT SELECT ON silver.vehicle_tenant_101 TO tenant_101_role;
REVOKE SELECT ON silver.vehicle            FROM tenant_101_role;
```

- **Pros:** one shared table, no extra storage; works today with Spark SQL + any catalog with grants.
- **Cons:** proliferation of views (N tenants × M tables); users have to know their own view name; depends on strict discipline that no principal gets SELECT on the base table.
- **Fits:** small-to-medium tenant counts, when you already know the full tenant set.

### Pattern 3 — Dynamic row filters based on authenticated identity

Supported out of the box by Databricks Unity Catalog, Snowflake, and Trino + Apache Ranger:

```sql
CREATE ROW FILTER tenant_filter ON silver.vehicle
  USING (tenant_id = (
    SELECT tenant_id FROM user_tenant_map WHERE user = current_user()
  ));
```

- **Pros:** one table, one filter, scales to arbitrary tenant counts; policy maintained in one place; audit via the policy engine.
- **Cons:** **not supported by stock Spark SQL.** Requires Trino + Ranger (OSS self-hosted), Unity Catalog (Databricks), or Snowflake. Our stack would need to introduce Trino as the user-facing query engine for this to work.
- **Fits:** medium-to-large tenant counts, teams that want OSS with policy-as-code.

### Pattern 4 — Query gateway (DIY)

A service in front of Spark/Trino that:

```
Request from user         →  authenticate (OAuth / JWT)
                          →  parse SQL
                          →  inject AND tenant_id = <authenticated_tenant>
                          →  forward to query engine
                          →  audit log
                          →  return result
```

- **Pros:** full control, works with any query engine, handles arbitrary business rules (e.g., "this user can see only tenants 101 and 102, but with projection X and Y").
- **Cons:** you build and run a service; must trust the gateway (users must have no way to bypass it — enforce via network policy, not just discipline).
- **Fits:** complex authorization rules, custom tenant → user mappings, legacy integrations.

### Pattern comparison

| Pattern | Spark SQL today? | Setup cost | Runtime cost | Scales to N tenants |
|---|---|---|---|---|
| 1. Per-tenant tables | Yes | High (tenant router + table fan-out) | Zero | Yes, but linear ops cost |
| 2. Per-tenant views | Yes | Low | Zero | Views grow linearly |
| 3. Dynamic row filter | **No** (need Trino + Ranger / UC / Snowflake) | High (new engine, policy infra) | Low per query | Yes, flat ops cost |
| 4. Query gateway | Yes | High (build + operate a service) | Latency + bottleneck | Yes, flat ops cost |

---

## Column-level security — three patterns

### Pattern 1 — Column-projecting views

```sql
CREATE VIEW silver.vehicle_public AS
  SELECT tenant_id, vehicle_id, make, model
  FROM silver.vehicle;
  -- vin and emission_class omitted
GRANT SELECT ON silver.vehicle_public TO reporting_role;
REVOKE SELECT ON silver.vehicle       FROM reporting_role;
```

- **Pros:** works today with our stack; simple; no runtime machinery.
- **Cons:** view proliferation if you have many `role × sensitivity` combinations; sensitive columns live in the base table unprotected if someone has base-table access.
- **Fits:** small set of discrete "profiles" (public, analyst, admin).

### Pattern 2 — Dynamic column masking

Same engine support story as Pattern 3 for RLS — needs Trino + Ranger, Unity Catalog, or Snowflake:

```sql
CREATE MASKING POLICY vin_mask AS (vin STRING) RETURNS STRING ->
  CASE WHEN is_member('pii_viewer') THEN vin
       ELSE regexp_replace(vin, '.{10}$', '**********')
  END;
ALTER TABLE silver.vehicle MODIFY COLUMN vin SET MASKING POLICY vin_mask;
```

- **Pros:** one table, one policy, per-user mask at query time.
- **Cons:** not supported by stock Spark SQL; same engine switch required.
- **Fits:** per-column, per-role, needing hundreds of policies maintained centrally.

### Pattern 3 — Column-level encryption at rest

Iceberg supports table encryption with per-column keys via Parquet modular encryption (Spark 3.4+, Iceberg 1.1+). Readers who don't have the KMS key can read non-encrypted columns but not the encrypted ones; the Parquet file itself is unreadable without the key.

```
Table properties:
  write.parquet.encryption.required-columns = "vin,emission_class"
  write.parquet.encryption.key              = "<KMS key reference>"
  write.parquet.encryption.footer-key       = "<KMS key reference>"
```

- **Pros:** cryptographic guarantee — holds even if someone exfiltrates the raw Parquet files; KMS access can be audited; column key rotation possible.
- **Cons:** KMS integration + operational burden; irretrievable if keys are lost; small per-query decryption overhead; harder to debug when it goes wrong.
- **Fits:** regulated data (PHI, PCI, trade secrets), high-value PII where "defense in depth" beyond logical access controls matters.

---

## Critical caveat: CDC bypasses OLTP security

Postgres has RLS and column grants — they protect **queries against the live Postgres server**, not the WAL. Debezium is a logical replication subscriber; it reads at the WAL layer and sees every row and every column regardless of RLS or column grants on the OLTP side.

**Any PII in Postgres ends up in Bronze unmasked.** `vin`, `emission_class`, future `date_of_birth`, future `ssn` — all of it flows through the pipeline bit-for-bit.

Three ways to handle this:

### At Debezium (preferred for PII)

Connector-level column controls:

```json
"column.exclude.list": "public.customer.ssn,public.customer.date_of_birth",
"column.mask.hash.sha-256.with.salt.<salt>": "public.customer.email,public.customer.phone"
```

- `column.exclude.list` — the column is literally not emitted. Bronze never sees it.
- `column.mask.hash.<algo>.with.salt.<salt>` — the column is replaced with a salted hash. Downstream can join on the hash but can't recover the original.
- **Pros:** PII never enters the lakehouse. Cleanest compliance story.
- **Cons:** if you later need the raw value, you have to re-ingest (and salted hashes are irreversible). Decision has to be made at ingest time.

### At the Bronze → Silver transform

`bronze_to_silver.py` could apply hashing / tokenization / exclusion. Downside: Bronze still has the raw data, so Bronze itself becomes a compliance surface — now you need access controls on Bronze too.

### At Silver via column encryption

Encrypt sensitive columns in Silver (and Gold). Only authorized readers have the KMS key. Bronze still has raw data (same downside as above) unless you also apply column controls at Debezium.

**Rule of thumb:** if the data is in a regulated category (PCI, PHI, PII that could identify a person), filter or hash at Debezium. If it's "operationally sensitive" (not strictly regulated but shouldn't be in every analyst's lap), encrypt at Silver.

---

## Catalog authentication is the foundation

Our current REST catalog (`tabulario/iceberg-rest`) has **no authentication at all** — anyone who can reach port 8181 is a superuser. Every security pattern above assumes an authenticated catalog that knows "who is this caller."

Three OSS replacements worth evaluating:

### Apache Polaris (incubating)

- Snowflake donation. REST catalog with OAuth2 principals, namespace/table grants, vended S3 credentials.
- Mature, backed by a large vendor, polished REST API, native integration with Iceberg clients.
- Not yet 1.0; incubator-level support guarantees.

### Lakekeeper

- MIT-licensed, Rust implementation. OpenID Connect for identity, OpenFGA for authorization, full REST catalog API.
- Smaller community but very actively developed.
- Supports vended credentials, warehouse-level isolation, project tenancy.

### Nessie

- Git-like versioning for Iceberg tables (branches, tags, merges). Also supports authentication.
- Great if you want data versioning as a feature; more than just a security replacement.

### Vended credentials — why this matters

Our Spark currently has hardcoded `rustfsadmin` keys with full access to `s3://warehouse/*`. An authenticated catalog hands out **short-lived, scoped-down credentials** per query — e.g., "you can read `s3://warehouse/silver/vehicle/data/tenant_id=101/*` for the next 15 minutes." Compromise of the Spark container no longer leaks everything.

This alone — vended credentials behind an authenticated catalog — closes the single biggest hole in our current stack. It doesn't give you RLS/CLS, but it does give you per-principal boundaries that physical storage respects.

---

## Pragmatic sequencing

Order from "cheap, high impact" to "expensive, deep":

### Immediate (hours of work)

- **Rotate the hardcoded RustFS credential.** It's visible in `spark-defaults.conf`, which is in git. Replace with env vars so at least rotation is possible.
- **Create per-tenant views in Silver** (Pattern 2 for RLS, Pattern 1 for CLS). Doesn't solve credential sharing but establishes a logical identity model that the next steps can slot into.

### Short term (days)

- **Swap the Iceberg REST catalog for Polaris or Lakekeeper.** Create principals per team / per tenant. Grant namespace/table permissions explicitly. Remove the shared S3 credentials from Spark config in favor of catalog-vended credentials. This single change takes you from "no auth anywhere" to "proper principal boundaries."
- **Debezium column controls** for any column you'd be embarrassed to find in a quarterly data audit. Even if you have nothing truly regulated yet, do it as a muscle-memory check before `vin` and `emission_class` become `social_security_number` and `drivers_license_number`.

### Medium term (weeks)

- **Introduce Trino + Apache Ranger as the user-facing query engine.** Spark stays for ETL (streaming + batch MERGE + Gold rebuilds). Trino + Ranger handles RLS / CLS / dynamic masking / audit log for every interactive query from BI tools and analysts.
- **Enable S3 server-side encryption** on RustFS (and later, on real S3).
- **Audit log everything.** Catalog auth events, query gateway requests, S3 access logs — route to a central aggregator (CloudWatch Logs, Loki, Splunk).

### Long term

- **Iceberg native column encryption + KMS integration** for regulated columns.
- **Query gateway** if the Trino + Ranger policy model isn't flexible enough for your business rules.
- **Tenant router + per-tenant namespaces** for whale tenants, letting you lean on physical isolation rather than runtime filters.
- **Compliance pipeline:** DELETE + `expire_snapshots` + `remove_orphan_files` + audit log entry, wrapped in a service that proves to an auditor that a right-to-be-forgotten request was fulfilled.

---

## What this means for our current repo

In the repo today, the only security-adjacent changes we could make without introducing a new service:

1. **Move hardcoded credentials to env vars.** Smallest step, biggest "defense in depth" improvement. Enables rotation.
2. **Debezium column controls** on the connector config. Would require defining a classification policy for each column — a planning exercise before code.
3. **Enable S3 server-side encryption on RustFS.** Doesn't give per-user control but at least encrypts at rest.
4. **Metadata hygiene already done** (Demo 5) — not security, but adjacent: makes the physical delete pipeline predictable.

Anything stronger needs a new service in the stack (authenticated catalog, Trino, Ranger, KMS). Those are not drive-by changes — they warrant their own phase.

---

## Explicit non-goals of our current pipeline

To be clear about what the current stack does *not* claim:

- Not multi-tenant secure. Partition pruning is a performance feature, not a security boundary.
- Not auditable. We log nothing about who queried what.
- Not PII-safe. CDC passes everything through.
- Not key-managed. Credentials are hardcoded in config.
- Not compliance-ready. DELETE + expire works mechanically, but there is no record of the operation.

Treat the current stack as "demonstrates the medallion mechanics; add security separately before anything leaves a laptop."
