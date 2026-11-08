# Database Migration Strategies: Embedded Startup vs. Dedicated Runner

## Overview

When designing cloud-native microservices, managing relational database schema changes (DDL) is a critical architectural decision. A common antipattern is relying on database container initialization scripts (such as PostgreSQL's `/docker-entrypoint-initdb.d/`), which only execute on a completely fresh volume and cannot handle ongoing updates, rollbacks, or multi-environment deployments.

This document explores the two primary approaches for executing database migrations, analyzing their trade-offs across security, scalability, and operations.

---

## The Two Main Approaches

### Strategy A: Embedded Migrations on Server Boot
* **Mechanism**: SQL migration files are embedded into the Go binary (using `go:embed`). When `main()` boots, it establishes a database connection and calls `migrate.Up()` before starting the HTTP server listener.
* **Target Environment**: Local development prototypes, single-instance applications, unit/integration test suites.

### Strategy B: Dedicated Migration Runner / Container
* **Mechanism**: Migrations are executed as an independent, single-shot process completely separate from the application web server.
  * In **Docker Compose**: An ephemeral container (`migrate/migrate`) runs before the application service starts (`depends_on: { migration: { condition: service_completed_successfully } }`).
  * In **Kubernetes**: An `InitContainer` or a pre-deployment Kubernetes `Job` runs to completion before application pods are scheduled.
  * In **CI/CD**: A deployment pipeline step executes the migration against the database before rolling out new application code.

---

## Detailed Trade-Off Comparison

| Dimension | Strategy A: Embedded in `main()` | Strategy B: Dedicated Runner |
| :--- | :--- | :--- |
| **Developer Experience (Local)** | **Highest**: Zero orchestration needed. Run `go run main.go` and the DB is automatically migrated. | **Moderate**: Requires Docker Compose orchestration or a manual CLI command to run migrations. |
| **Development Feedback Loop** | **Slower**: Adding or modifying a `.sql` file requires recompiling the Go application binary. | **Faster**: SQL files are mounted directly. Adding a file only requires re-running the migration container. |
| **Multi-Replica Deployments** | **High Risk**: Thundering herd and lock contention when 5–50 pods boot at the same time. | **Safe**: Guarantees exactly one runner executes sequentially before application pods start. |
| **Database Security (Least Privilege)** | **Violation**: Application runtime must hold administrative `DDL` permissions (`CREATE`, `ALTER`, `DROP`). | **Compliant**: Application only needs `DML` (`SELECT`, `INSERT`, `UPDATE`, `DELETE`). DDL is restricted to the migration runner. |
| **Container Lifecycle & Health Probes** | **High Risk**: Long DDL operations block HTTP server boot, triggering Kubernetes probe timeouts and `CrashLoopBackOff`. | **Safe**: Application container boots in milliseconds and serves `/healthz` immediately. |
| **Zero-Downtime Deployments (Canary / Blue-Green)** | **High Risk**: A canary pod running a new migration can break schema compatibility for active old pods. | **Controlled**: Schema migrations are phased (expand-and-contract pattern) independently of application rollout. |

---

## Deep Dive: The 4 Production Failure Modes of Strategy A

### 1. The Multi-Pod Lock Race (Advisory Lock Contention)
In Kubernetes or ECS, deploying a new service version launches multiple pods simultaneously.
* If 20 replicas boot at once, all 20 attempt to run `migrate.Up()`.
* Migration libraries (like `golang-migrate`) use database advisory locks (`pg_advisory_lock`) to prevent concurrent executions.
* However, 20 processes contending for the same lock frequently encounter connection timeouts or context cancellations. If a pod holding the advisory lock crashes or gets killed mid-transaction, the migration is left in a **dirty state** (`dirty = true` in `schema_migrations`). Once dirty, all subsequent startups will permanently fail until an engineer manually intervenes.

### 2. Violation of the Principle of Least Privilege (PoLP)
From a database security standpoint:
* **Application Runtime**: Only requires **DML** permissions: `SELECT`, `INSERT`, `UPDATE`, `DELETE`.
* **Migration Process**: Requires administrative **DDL** permissions: `CREATE TABLE`, `ALTER TABLE`, `DROP INDEX`, etc.

If the application binary connects using a user with DDL privileges, any SQL injection vulnerability or compromised third-party package allows an attacker to drop tables or alter schema definitions. In audited environments (SOC2, PCI-DSS, ISO 27001), granting DDL permissions to application runtime roles is considered an audit violation.

### 3. Readiness Probe Failures & CrashLoopBackOff
Consider an `ALTER TABLE` adding an index or modifying a column on a table containing 10 million rows:
* The migration might take 30 to 60 seconds to finish.
* If migrations run in `main()` before starting `http.ListenAndServe`:
  1. The container port is not open.
  2. The orchestrator's readiness probe (`GET /healthz`) fails repeatedly.
  3. The orchestrator marks the container as dead and terminates it (`SIGKILL`).
  4. The migration is interrupted mid-execution, and the deployment enters an infinite `CrashLoopBackOff`.

### 4. Canary & Blue/Green Deployments
In modern continuous delivery:
* A canary deployment routes 10% of user traffic to the new version while 90% stays on the old version.
* If the new version's pod automatically runs a breaking migration on boot, the active 90% old pods will immediately experience errors when interacting with altered columns.
* Decoupling migrations from application startup enforces the **Expand and Contract** pattern (first expand the schema with additive, backwards-compatible changes; then deploy code; then contract/clean up old columns later).

---

## How Big Tech Solves This (The Hybrid Pattern)

Many engineering organizations adopt a hybrid approach:

1. **Single Docker Image**:
   The Go binary contains both the migration engine and the application server, exposed via subcommands:
   ```bash
   # Used by Kubernetes Job / Migration Step:
   ./mgmt-api migrate up

   # Used by Application Pods / Services:
   ./mgmt-api server
   ```
2. **Distinct DB Credentials**:
   - The migration job runs with `DB_USER=migrator` (has DDL permissions).
   - The web server runs with `DB_USER=app` (restricted DML permissions).
3. **Local Development Simplicity**:
   In Docker Compose, an ephemeral container runs the migration step before starting the API server, mirroring production orchestration without complicating the local developer experience.
