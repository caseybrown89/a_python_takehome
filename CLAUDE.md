# Portfolio Data Clearinghouse

## Project Overview

Flask application for ingesting trade/position data, reconciling discrepancies, and detecting compliance violations (single equity > 20% of account value).

## Tech Stack

- Python 3.14, Flask 3.1.3, SQLAlchemy (via Flask-SQLAlchemy)
- PostgreSQL 18 (Docker Compose for both app and test databases)
- pytest with pytest-cov for testing

## Project Structure

- `app/__init__.py` - App factory (`create_app`), SQLAlchemy `db` singleton
- `app/config.py` - Configuration from environment variables
- `app/models.py` - `Trade` and `Position` SQLAlchemy models
- `app/services.py` - All business logic: ingestion, positions, compliance, reconciliation
- `app/routes.py` - Flask routes with `@require_date` decorator for query param parsing
- `cli.py` - CLI ingestion script (reuses services)
- `tests/conftest.py` - pytest fixtures, auto-creates test database in PostgreSQL

## Running

```bash
docker compose up --build        # Start app + database
docker compose run test          # Run tests with coverage
```

## Key Patterns

- **App factory pattern** with deferred imports inside `create_app()` to avoid circular imports
- **Flask-SQLAlchemy singleton** for `db` -- this is Flask convention, not a DI pattern
- **Strict/permissive ingestion modes** -- strict (default) rolls back on any error, permissive skips bad rows
- **Format detection** -- filename must contain "trade" or "position"; trade files matched by header row
- **SQL joins** for all query endpoints -- no in-memory data manipulation
- **FULL OUTER JOIN** (raw SQL) for reconciliation since SQLAlchemy ORM lacks native support

## Testing

- Tests run against PostgreSQL (not SQLite) to match production behavior
- `conftest.py` auto-creates `clearinghouse_test` database if it doesn't exist
- Each test gets a fresh set of tables (dropped in teardown)
- Test DB connection configured via env vars: `POSTGRES_HOST`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_PORT`, `POSTGRES_TEST_DB`
