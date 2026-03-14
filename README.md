# Portfolio Data Clearinghouse

A Flask application that ingests trade and position data from multiple formats, reconciles discrepancies, calculates portfolio metrics, and detects compliance violations.

## Quick Start

```bash
docker-compose up --build -d
```

This starts PostgreSQL and the web server. The API is available at `http://localhost:5000`.

## API Endpoints

### GET /ping

Health check endpoint. Returns service status and verifies database connectivity.

```bash
curl http://localhost:5000/ping
```

Returns `{"status": "healthy"}` (200) when the service and database are reachable, or `{"status": "unhealthy", "error": "..."}` (503) if the database is down.

### POST /ingest

Upload trade or position files. Ingestion runs in strict mode by default (rejects the entire file on any malformed row). Use `?mode=permissive` to skip bad rows instead.

File format is detected from the filename (`trade` or `position` in the name) and the header row for trade files.

```bash
# Strict mode (default)
curl -F "file=@sample_data/trades_format1.csv" http://localhost:5000/ingest | python3 -m json.tool

# Permissive mode
curl -F "file=@sample_data/trades_format1.csv" "http://localhost:5000/ingest?mode=permissive" | python3 -m json.tool

# Multiple files
curl -F "file=@sample_data/trades_format1.csv" \
     -F "file=@sample_data/trades_format2.psv" \
     -F "file=@sample_data/positions.yaml" \
     http://localhost:5000/ingest | python3 -m json.tool
```

### GET /positions

Returns positions for an account on a given date, including cost basis and percentage of account value.

```bash
curl "http://localhost:5000/positions?account=ACC001&date=2025-01-15" | python3 -m json.tool
```

### GET /compliance/concentration

Returns accounts with any single equity holding exceeding 20% of total account value.

```bash
curl "http://localhost:5000/compliance/concentration?date=2025-01-15" | python3 -m json.tool
```

### GET /reconciliation

Compares aggregated trades against bank positions for a given date. Reports quantity mismatches, missing records on either side.

```bash
curl "http://localhost:5000/reconciliation?date=2025-01-15" | python3 -m json.tool
```

## CLI Ingestion

Files can also be ingested via the command line:

```bash
python cli.py sample_data/trades_format1.csv sample_data/positions.yaml

# Permissive mode
python cli.py --permissive sample_data/trades_format1.csv
```

## Running Tests

Tests run against a PostgreSQL database (created automatically if it doesn't exist):

```bash
docker compose run test
```

Test database connection can be configured via environment variables: `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_TEST_DB`.

## Architecture Notes

- **Flask app factory pattern** with a flat modular structure: `routes.py`, `services.py`, `models.py`.
- **PostgreSQL** for both production and test databases, ensuring test fidelity.
- **SQLAlchemy** is used as the ORM. Flask-SQLAlchemy follows a singleton pattern for the `db` object, which is the framework's convention. In a greenfield project I would prefer explicit dependency injection (as found in frameworks like FastAPI), but chose not to fight Flask's idioms here.
- **Tables are created via `db.create_all()`**, which only handles initial creation. In a production system, Flask-Migrate (Alembic) would manage schema evolution through versioned migration scripts.
- **Ingestion** supports strict (default) and permissive modes. Strict rolls back the entire file on any error; permissive skips malformed rows and reports them.

## Re-initializing the Database

To wipe all data and start fresh (e.g. after ingesting duplicate files), remove the bind-mounted data directory and restart:

```bash
docker compose down && rm -rf pgdata && docker compose up --build -d
```

PostgreSQL data is persisted in the local `pgdata/` directory via a bind mount. Removing it forces a fresh database on next startup, and tables are recreated automatically via `db.create_all()`.

## Known Limitations

- **Ingestion performs upserts on composite keys.** Trades are deduplicated on `(trade_date, account_id, ticker, quantity, price, trade_type)`; positions on `(report_date, account_id, ticker)`. When a matching trade already exists, complementary fields (`settlement_date`, `custodian`) are merged via `COALESCE` so that ingesting the same trade from multiple sources (e.g. an internal CSV and a custodian pipe-delimited file) produces a single, complete row. The ingestion report includes a `records_updated` count alongside `records_ingested`. **Caveat:** two genuinely distinct trades that share all six key fields on the same day would be erroneously merged. A production system should require source-provided unique trade identifiers to eliminate this ambiguity.
- **Ingestion inserts rows one at a time.** Each record issues its own `INSERT ... ON CONFLICT` statement, resulting in N database round-trips per file. A production system should batch these into a single multi-row insert for better throughput on large files.
- **Position schema assumes one custodian per account.** Positions are deduplicated on `(report_date, account_id, ticker)`. This works when each account is managed by a single custodian, but would need adjustment if multiple custodians can hold shares of the same security for the same account. In that scenario, the natural key would need to include the custodian (e.g. `custodian_ref`), and downstream queries (portfolio percentages, compliance checks, reconciliation) would need to aggregate across custodian rows to produce correct account-level figures.
