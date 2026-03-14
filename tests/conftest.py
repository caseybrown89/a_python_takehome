import os

import pytest
import sqlalchemy

from app import create_app, db as _db

POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_TEST_DB = os.environ.get("POSTGRES_TEST_DB", "clearinghouse_test")

POSTGRES_DSN = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}"
TEST_DATABASE_URL = f"{POSTGRES_DSN}/{POSTGRES_TEST_DB}"


@pytest.fixture(scope="session", autouse=True)
def ensure_test_db():
    """Create the test database if it doesn't exist. Runs once per test session."""
    engine = sqlalchemy.create_engine(f"{POSTGRES_DSN}/postgres", isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        exists = conn.execute(
            sqlalchemy.text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": POSTGRES_TEST_DB},
        ).scalar()
        if not exists:
            conn.execute(sqlalchemy.text(f"CREATE DATABASE {POSTGRES_TEST_DB}"))
    engine.dispose()


@pytest.fixture
def app(ensure_test_db):
    """Create a test Flask app backed by a PostgreSQL test database."""
    app = create_app({
        "SQLALCHEMY_DATABASE_URI": TEST_DATABASE_URL,
        "TESTING": True,
    })
    yield app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def db(app):
    with app.app_context():
        yield _db
        _db.session.remove()
        _db.drop_all()


# ---------------------------------------------------------------------------
# Sample file contents
# ---------------------------------------------------------------------------

@pytest.fixture
def format1_content():
    return (
        "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
        "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"
        "2025-01-15,ACC001,MSFT,50,420.25,BUY,2025-01-17\n"
        "2025-01-15,ACC002,GOOGL,75,142.80,BUY,2025-01-17\n"
        "2025-01-15,ACC003,TSLA,150,238.45,SELL,2025-01-17\n"
    )


@pytest.fixture
def format2_content():
    return (
        "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"
        "20250115|ACC001|AAPL|100|18550.00|CUSTODIAN_A\n"
        "20250115|ACC001|MSFT|50|21012.50|CUSTODIAN_A\n"
        "20250115|ACC002|GOOGL|75|10710.00|CUSTODIAN_B\n"
    )


@pytest.fixture
def positions_content():
    return (
        'report_date: "20250115"\n'
        "positions:\n"
        '  - account_id: "ACC001"\n'
        '    ticker: "AAPL"\n'
        "    shares: 100\n"
        "    market_value: 18550.00\n"
        '    custodian_ref: "CUST_A_12345"\n'
        '  - account_id: "ACC001"\n'
        '    ticker: "MSFT"\n'
        "    shares: 50\n"
        "    market_value: 21012.50\n"
        '    custodian_ref: "CUST_A_12346"\n'
        '  - account_id: "ACC001"\n'
        '    ticker: "GOOGL"\n'
        "    shares: 75\n"
        "    market_value: 10710.00\n"
        '    custodian_ref: "CUST_A_12347"\n'
    )
