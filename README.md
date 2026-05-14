# CatalystCore

A lightweight PySpark framework for orchestrating SQL transformations in a defined execution order.

CatalystCore runs SparkSQL files according to a JSON-based DAG specification, letting you express data pipelines as plain SQL with their dependencies declared as metadata. The framework handles Spark session management, execution order, logging, and error propagation — you focus on the SQL.

---

## Features

- **SQL-first** — write transformations as `.sql` files; no PySpark boilerplate per step
- **Metadata-driven DAGs** — declare execution order and dependencies in JSON, not in code
- **Multi-app support** — group related pipelines under `apps/`, each with its own DAG
- **Centralised configuration** — Spark, source, and target settings live in `config/`
- **Structured logging** — every run produces traceable logs under `logs/`
- **Single entry point** — launch any pipeline via `main.py`

---

## Repository Structure

```
catalystcore/
├── apps/             # SQL files grouped by application/pipeline
│   └── <app_name>/
│       ├── step_01.sql
│       ├── step_02.sql
│       └── ...
├── config/           # Spark and environment configuration
├── logs/             # Runtime logs (gitignored except .gitkeep)
├── metadata/         # JSON DAG definitions per app
│   └── <app_name>.json
├── main.py           # Framework entry point
└── README.md
```

---

## Architecture

```
        ┌──────────────┐
        │   main.py    │   ← entry point
        └──────┬───────┘
               │
   ┌───────────┴───────────┐
   ▼                       ▼
┌─────────┐         ┌────────────┐
│ config/ │         │ metadata/  │
│ (Spark, │         │ (JSON DAG) │
│  envs)  │         │            │
└────┬────┘         └─────┬──────┘
     │                    │
     └──────────┬─────────┘
                ▼
        ┌───────────────┐
        │  DAG Runner   │   ← resolves dependencies,
        │               │     executes SQL in order
        └───────┬───────┘
                ▼
        ┌───────────────┐
        │  apps/<app>/  │
        │   *.sql       │
        └───────┬───────┘
                ▼
        ┌───────────────┐
        │ Spark Session │
        └───────────────┘
```

---

## Prerequisites

- Python 3.9+
- Apache Spark 3.4+
- Access to your target catalogue (Hive Metastore, Iceberg, etc.)

---

## Installation

```bash
git clone <repo-url>
cd catalystcore
pip install -r requirements.txt
```

---

## Configuration

Spark and runtime configuration lives in `config/`. At a minimum you'll define:

- Spark master, executor resources, and packages
- Catalogue / metastore endpoints
- Source and target connection details
- Environment profiles (e.g. `dev`, `prod`)

See the example files in `config/` for the full list of supported options.

---

## Defining a Pipeline

A pipeline is a JSON file in `metadata/` that lists SQL steps and their dependencies.

**Example — `metadata/sales_reporting.json`:**

```json
{
  "app": "sales_reporting",
  "description": "Daily refresh of the sales reporting layer",
  "nodes": [
    {
      "id": "stg_orders",
      "sql_file": "stg_orders.sql",
      "depends_on": []
    },
    {
      "id": "stg_customers",
      "sql_file": "stg_customers.sql",
      "depends_on": []
    },
    {
      "id": "fact_orders",
      "sql_file": "fact_orders.sql",
      "depends_on": ["stg_orders", "stg_customers"]
    }
  ]
}
```

Each `sql_file` is resolved relative to `apps/<app>/`. The DAG runner executes nodes in topological order; independent nodes (e.g. `stg_orders` and `stg_customers`) can be run in parallel if enabled in `config/`.

---

## Running a Pipeline

```bash
python main.py --app sales_reporting
```

**Common options:**

| Flag        | Description                                                |
|-------------|------------------------------------------------------------|
| `--app`     | Name of the app to run (must match a file in `metadata/`)  |
| `--env`     | Environment profile from `config/` (e.g. `dev`, `prod`)    |
| `--dry-run` | Resolve and print the DAG without executing                |

> Adjust the flags above to match the actual CLI exposed by `main.py`.

---

## Logging

Each run writes logs to `logs/<app>/<run_id>/`, containing:

- The resolved DAG
- Per-step start/end timestamps and row counts
- Stack traces on failure

Logs are written locally; forward them to your central logging stack if needed.

---

## Development

To add a new pipeline:

1. Create `apps/<your_app>/` and drop your `.sql` files there
2. Create `metadata/<your_app>.json` describing the DAG
3. Run with `python main.py --app <your_app>`
4. Inspect logs under `logs/<your_app>/`

### SQL conventions

- One transformation per file — keep each step focused
- Use Spark SQL syntax compatible with the project's Spark version
- Reference upstream tables by their target name; CatalystCore guarantees they exist before a dependent step runs
- Lint your SQL with [`sparksql-lint`](#) before committing (recommended pre-commit hook)

---

## License

Internal — *(set this to your organisation's policy)*.
