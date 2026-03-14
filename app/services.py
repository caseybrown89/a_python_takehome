import csv
import io
from datetime import datetime
from decimal import Decimal, InvalidOperation

import yaml
from sqlalchemy import func, text

from app import db
from app.models import Trade, Position


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class IngestionError(Exception):
    """Raised when a file cannot be ingested."""


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

def check_health():
    """Verify database connectivity by executing a simple query."""
    db.session.execute(text("SELECT 1"))
    return {"status": "healthy"}


# ---------------------------------------------------------------------------
# Format detection & ingestion
# ---------------------------------------------------------------------------

FORMAT_1_HEADER = "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate"
FORMAT_2_HEADER = "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM"


def detect_format(filename, content):
    """Detect file format from filename and header content.

    Filename must contain 'trade' or 'position'. For trade files, the header
    row is matched against known column specifications.
    """
    name_lower = filename.lower()

    if "position" in name_lower:
        return "positions"

    if "trade" not in name_lower:
        raise IngestionError(
            f"Cannot detect format: filename '{filename}' must contain 'trade' or 'position'"
        )

    first_line = content.split("\n", 1)[0].strip()

    if first_line == FORMAT_1_HEADER:
        return "format_1"
    if first_line == FORMAT_2_HEADER:
        return "format_2"

    raise IngestionError(
        f"Cannot detect trade format: header '{first_line}' does not match any known specification"
    )


def ingest_file(filename, content, strict=False):
    """Parse and ingest a file, returning a quality report dict.

    In strict mode, any row-level error rolls back the entire file.
    In permissive mode, malformed rows are skipped and reported.
    """
    fmt = detect_format(filename, content)

    parsers = {
        "format_1": _parse_format_1,
        "format_2": _parse_format_2,
        "positions": _parse_positions,
    }

    records, warnings, errors = parsers[fmt](content, filename)

    if strict and errors:
        db.session.rollback()
        raise IngestionError(
            f"Strict mode: aborting ingestion of '{filename}' due to errors: {errors}"
        )

    inserted = 0
    updated = 0
    for record in records:
        ins, upd = record.process()
        inserted += ins
        updated += upd
    db.session.commit()

    return {
        "filename": filename,
        "format_detected": fmt,
        "records_ingested": inserted,
        "records_updated": updated,
        "records_skipped": len(errors),
        "warnings": warnings,
        "errors": errors,
    }


def _parse_date(value, fmt="%Y-%m-%d"):
    """Parse a date string, trying common formats."""
    for f in (fmt, "%Y%m%d"):
        try:
            return datetime.strptime(value.strip(), f).date()
        except ValueError:
            continue
    return None


def _parse_format_1(content, filename):
    """Parse comma-delimited trade file (Format 1)."""
    records, warnings, errors = [], [], []
    reader = csv.DictReader(io.StringIO(content))

    for i, row in enumerate(reader, start=2):
        try:
            required = ["TradeDate", "AccountID", "Ticker", "Quantity", "Price", "TradeType"]
            missing = [f for f in required if not row.get(f, "").strip()]
            if missing:
                errors.append(f"Row {i}: missing fields {missing}")
                continue

            trade_date = _parse_date(row["TradeDate"])
            if not trade_date:
                errors.append(f"Row {i}: invalid date '{row['TradeDate']}'")
                continue

            quantity = Decimal(row["Quantity"])
            price = Decimal(row["Price"])
            trade_type = row["TradeType"].strip().upper()

            if price < 0:
                warnings.append(f"Row {i}: negative price {price}")

            if trade_type == "SELL":
                quantity = -abs(quantity)

            settlement_date = _parse_date(row.get("SettlementDate", ""))

            records.append(Trade(
                trade_date=trade_date,
                account_id=row["AccountID"].strip(),
                ticker=row["Ticker"].strip(),
                quantity=quantity,
                price=price,
                market_value=quantity * price,
                trade_type=trade_type,
                settlement_date=settlement_date,
                source_file=[filename],
            ))
        except (ValueError, KeyError, InvalidOperation) as e:
            errors.append(f"Row {i}: {e}")

    return records, warnings, errors


def _parse_format_2(content, filename):
    """Parse pipe-delimited trade file (Format 2)."""
    records, warnings, errors = [], [], []
    lines = content.strip().split("\n")

    for i, line in enumerate(lines[1:], start=2):
        try:
            parts = line.split("|")
            if len(parts) < 6:
                errors.append(f"Row {i}: expected 6 fields, got {len(parts)}")
                continue

            report_date = _parse_date(parts[0])
            if not report_date:
                errors.append(f"Row {i}: invalid date '{parts[0]}'")
                continue

            shares = Decimal(parts[3])
            market_value = Decimal(parts[4])
            price = market_value / shares if shares != 0 else Decimal(0)
            trade_type = "SELL" if shares < 0 else "BUY"

            records.append(Trade(
                trade_date=report_date,
                account_id=parts[1].strip(),
                ticker=parts[2].strip(),
                quantity=shares,
                price=abs(price),
                market_value=market_value,
                trade_type=trade_type,
                settlement_date=None,
                custodian=parts[5].strip(),
                source_file=[filename],
            ))
        except (ValueError, IndexError, InvalidOperation) as e:
            errors.append(f"Row {i}: {e}")

    return records, warnings, errors


def _parse_positions(content, filename):
    """Parse YAML bank position file."""
    records, warnings, errors = [], [], []

    try:
        data = yaml.safe_load(content)
    except yaml.YAMLError as e:
        return records, warnings, [f"YAML parse error: {e}"]

    report_date = _parse_date(str(data.get("report_date", "")))
    if not report_date:
        return records, warnings, ["Missing or invalid report_date"]

    for i, pos in enumerate(data.get("positions", []), start=1):
        try:
            required = ["account_id", "ticker", "shares", "market_value", "custodian_ref"]
            missing = [f for f in required if f not in pos]
            if missing:
                errors.append(f"Position {i}: missing fields {missing}")
                continue

            records.append(Position(
                report_date=report_date,
                account_id=str(pos["account_id"]).strip(),
                ticker=str(pos["ticker"]).strip(),
                shares=Decimal(str(pos["shares"])),
                market_value=Decimal(str(pos["market_value"])),
                custodian_ref=str(pos["custodian_ref"]).strip(),
            ))
        except (ValueError, KeyError, InvalidOperation) as e:
            errors.append(f"Position {i}: {e}")

    return records, warnings, errors


# ---------------------------------------------------------------------------
# Positions query
# ---------------------------------------------------------------------------

def get_positions(account_id, date):
    """Return positions for an account on a given date, with cost basis.

    Uses a LEFT JOIN from positions to aggregated trades to compute cost basis
    in a single query.
    """
    # Subquery: aggregate trade cost basis by account + ticker up to date
    trade_costs = db.session.query(
        Trade.account_id,
        Trade.ticker,
        func.sum(Trade.market_value).label("cost_basis"),
    ).filter(
        Trade.account_id == account_id,
        Trade.trade_date <= date,
    ).group_by(
        Trade.account_id, Trade.ticker,
    ).subquery()

    # Subquery: total account market value for percentage calculation
    total_value_q = db.session.query(
        func.sum(Position.market_value).label("total"),
    ).filter(
        Position.account_id == account_id,
        Position.report_date == date,
    ).scalar() or 0.0

    # Main query: positions LEFT JOIN trade costs
    rows = db.session.query(
        Position.ticker,
        Position.shares,
        Position.market_value,
        func.coalesce(trade_costs.c.cost_basis, 0.0).label("cost_basis"),
    ).outerjoin(
        trade_costs,
        (Position.account_id == trade_costs.c.account_id)
        & (Position.ticker == trade_costs.c.ticker),
    ).filter(
        Position.account_id == account_id,
        Position.report_date == date,
    ).all()

    results = []
    for row in rows:
        results.append({
            "ticker": row.ticker,
            "shares": row.shares,
            "market_value": row.market_value,
            "cost_basis": round(row.cost_basis, 2),
            "pct_of_account": round((row.market_value / total_value_q) * 100, 2) if total_value_q else 0,
        })

    return {
        "account": account_id,
        "date": date.isoformat(),
        "positions": results,
        "total_market_value": round(total_value_q, 2),
    }


# ---------------------------------------------------------------------------
# Compliance concentration
# ---------------------------------------------------------------------------

def check_concentration(date, threshold=20.0):
    """Find positions exceeding concentration threshold on a given date.

    Uses a window function to compute each holding's percentage of account
    total in a single query.
    """
    # Subquery: total market value per account
    account_totals = db.session.query(
        Position.account_id,
        func.sum(Position.market_value).label("total_value"),
    ).filter(
        Position.report_date == date,
    ).group_by(
        Position.account_id,
    ).subquery()

    # Main query: join positions with account totals, filter by threshold
    rows = db.session.query(
        Position.account_id,
        Position.ticker,
        Position.market_value,
        account_totals.c.total_value,
        (Position.market_value / account_totals.c.total_value * 100).label("concentration_pct"),
    ).join(
        account_totals,
        Position.account_id == account_totals.c.account_id,
    ).filter(
        Position.report_date == date,
        account_totals.c.total_value > 0,
        (Position.market_value / account_totals.c.total_value * 100) > threshold,
    ).all()

    violations = [
        {
            "account_id": row.account_id,
            "ticker": row.ticker,
            "market_value": round(row.market_value, 2),
            "total_account_value": round(row.total_value, 2),
            "concentration_pct": round(row.concentration_pct, 2),
            "threshold": threshold,
        }
        for row in rows
    ]

    return {"date": date.isoformat(), "violations": violations}


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

def reconcile(date):
    """Compare aggregated trades against positions for a given date.

    Uses a FULL OUTER JOIN between aggregated trades and positions to detect
    mismatches, missing records on either side, all in a single query.
    """
    # FULL OUTER JOIN via raw SQL (SQLAlchemy Core doesn't have a clean
    # full outer join API for subqueries)
    query = text("""
        SELECT
            COALESCE(t.account_id, p.account_id) AS account_id,
            COALESCE(t.ticker, p.ticker) AS ticker,
            t.trade_quantity,
            p.position_quantity,
            t.trade_value,
            p.position_value
        FROM (
            SELECT account_id, ticker,
                   SUM(quantity) AS trade_quantity,
                   SUM(market_value) AS trade_value
            FROM trade
            WHERE trade_date = :date
            GROUP BY account_id, ticker
        ) t
        FULL OUTER JOIN (
            SELECT account_id, ticker,
                   shares AS position_quantity,
                   market_value AS position_value
            FROM position
            WHERE report_date = :date
        ) p ON t.account_id = p.account_id AND t.ticker = p.ticker
        WHERE t.trade_quantity IS NULL
           OR p.position_quantity IS NULL
           OR t.trade_quantity != p.position_quantity
        ORDER BY account_id, ticker
    """)

    rows = db.session.execute(query, {"date": date}).fetchall()

    discrepancies = []
    for row in rows:
        if row.trade_quantity is not None and row.position_quantity is None:
            disc_type = "missing_from_positions"
        elif row.position_quantity is not None and row.trade_quantity is None:
            disc_type = "missing_from_trades"
        else:
            disc_type = "quantity_mismatch"

        entry = {
            "account_id": row.account_id,
            "ticker": row.ticker,
            "type": disc_type,
            "trade_quantity": row.trade_quantity,
            "position_quantity": row.position_quantity,
        }
        if disc_type == "quantity_mismatch":
            entry["difference"] = row.trade_quantity - row.position_quantity

        discrepancies.append(entry)

    return {"date": date.isoformat(), "discrepancies": discrepancies}
