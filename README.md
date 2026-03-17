# Portfolio Data Clearinghouse

A Flask application that ingests trade and position data from multiple formats, reconciles discrepancies, calculates portfolio metrics, and detects compliance violations.

## Requirements

Build a simplified portfolio data reconciliation system that ingests trade and position data from multiple sources in different formats, reconciles discrepancies, calculates portfolio metrics, and detects compliance violations.

### Key Deliverables

1. Flask application code for endpoints
2. SQLAlchemy models (database schema)
3. Data ingestion logic with quality checks (integrated or separate script)
4. Sample test queries or test data validating reconciliation logic
5. Functioning unit tests
6. GitHub repository
7. README.md

### Functional Requirements

1. Ingest files of two different formats for daily trades into a single relational database table.
2. Ingest a single format representing bank-broker positions into a second table.
3. Provide the following Flask endpoints:

| Endpoint | Method | Description |
|---|---|---|
| `/ingest` | POST | Load files, return data quality report (can also be a script) |
| `/positions?account=ACC001&date=2025-01-15` | GET | Positions with cost basis and market value |
| `/compliance/concentration?date=2025-01-15` | GET | Accounts exceeding 20% single-equity threshold with breach details |
| `/reconciliation?date=2025-01-15` | GET | Trade vs position file discrepancies on the provided day |

A compliance violation occurs when any one equity in an account holds over 20% of the value of that account.

### Input Formats

**Trade File Format 1** (CSV):
```
TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate
```

**Trade File Format 2** (pipe-delimited):
```
REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM
```

**Bank Position Format** (YAML):
```yaml
report_date: "20250115"
positions:
  - account_id: "ACC001"
    ticker: "AAPL"
    shares: 100
    market_value: 18550.00
    custodian_ref: "CUST_A_12345"
```

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

## Cost Basis Calculation

The `/positions` endpoint reports a `cost_basis` field for each holding. This is computed as `SUM(market_value)` across all trades for the account and ticker up to the requested date. Because sells are stored with negative `market_value` (quantity is negated at ingestion, and `market_value = quantity * price`), sells naturally reduce the sum.

This produces a **net investment** figure, not a traditional tax-lot cost basis. A true cost basis would track the original cost of only the shares still held, using a lot-selection method like FIFO or LIFO. For example, buying 100 shares at $10 and selling 50 at $15 yields a net-investment cost basis of $250 here, whereas a FIFO cost basis for the remaining 50 shares would be $500.

## Known Limitations

- **Ingestion performs upserts on composite keys.** Trades are deduplicated on `(trade_date, account_id, ticker, quantity, price, trade_type)`; positions on `(report_date, account_id, ticker)`. When a matching trade already exists, complementary fields (`settlement_date`, `custodian`) are merged via `COALESCE` so that ingesting the same trade from multiple sources (e.g. an internal CSV and a custodian pipe-delimited file) produces a single, complete row. The ingestion report includes a `records_updated` count alongside `records_ingested`. **Caveat:** two genuinely distinct trades that share all six key fields on the same day would be erroneously merged. A production system should require source-provided unique trade identifiers to eliminate this ambiguity.
- **Ingestion inserts rows one at a time.** Each record issues its own `INSERT ... ON CONFLICT` statement, resulting in N database round-trips per file. A production system should batch these into a single multi-row insert for better throughput on large files.
- **Position schema assumes one custodian per account.** Positions are deduplicated on `(report_date, account_id, ticker)`. This works when each account is managed by a single custodian, but would need adjustment if multiple custodians can hold shares of the same security for the same account. In that scenario, the natural key would need to include the custodian (e.g. `custodian_ref`), and downstream queries (portfolio percentages, compliance checks, reconciliation) would need to aggregate across custodian rows to produce correct account-level figures.
