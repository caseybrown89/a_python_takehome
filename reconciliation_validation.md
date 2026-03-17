# Reconciliation Logic Validation

This document demonstrates that the reconciliation logic correctly detects all three categories of discrepancy between aggregated trades and custodian-reported positions.

## Sample Input Data

### Trades (CSV)

```csv
TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate
2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17
2025-01-15,ACC001,GOOGL,100,142.80,BUY,2025-01-17
2025-01-15,ACC001,MSFT,50,420.25,BUY,2025-01-17
```

### Positions (YAML)

```yaml
report_date: "20250115"
positions:
  - account_id: "ACC001"
    ticker: "AAPL"
    shares: 100
    market_value: 18550.00
    custodian_ref: "CUST_A_12345"
  - account_id: "ACC001"
    ticker: "GOOGL"
    shares: 75
    market_value: 10710.00
    custodian_ref: "CUST_A_12347"
  - account_id: "ACC002"
    ticker: "TSLA"
    shares: 80
    market_value: 19076.00
    custodian_ref: "CUST_B_22347"
```

### Summary Table

| Account | Ticker | Trade Qty | Position Qty | Expected Result       |
|---------|--------|-----------|--------------|-----------------------|
| ACC001  | AAPL   | 100       | 100          | Match (no discrepancy)|
| ACC001  | GOOGL  | 100       | 75           | Quantity mismatch     |
| ACC001  | MSFT   | 50        | --           | Missing from positions|
| ACC002  | TSLA   | --        | 80           | Missing from trades   |

## How Reconciliation Works

The `reconcile()` function uses a single SQL query with a `FULL OUTER JOIN` between aggregated trades and positions for a given date:

```sql
WITH agg_trades AS (
    SELECT account_id, ticker,
           SUM(quantity) AS trade_quantity,
           SUM(market_value) AS trade_value
    FROM trade
    WHERE trade_date = :date
    GROUP BY account_id, ticker
),
positions AS (
    SELECT account_id, ticker,
           shares AS position_quantity,
           market_value AS position_value
    FROM position
    WHERE report_date = :date
)
SELECT
    COALESCE(t.account_id, p.account_id) AS account_id,
    COALESCE(t.ticker, p.ticker) AS ticker,
    t.trade_quantity,
    p.position_quantity,
    t.trade_value,
    p.position_value
FROM agg_trades t
FULL OUTER JOIN positions p
    ON t.account_id = p.account_id AND t.ticker = p.ticker
WHERE t.trade_quantity IS NULL
   OR p.position_quantity IS NULL
   OR t.trade_quantity != p.position_quantity
ORDER BY account_id, ticker
```

The `WHERE` clause filters out matching records (like ACC001/AAPL), so only discrepancies are returned. Each row is then classified:

- `trade_quantity IS NOT NULL` and `position_quantity IS NULL` &rarr; `missing_from_positions`
- `position_quantity IS NOT NULL` and `trade_quantity IS NULL` &rarr; `missing_from_trades`
- Both present but unequal &rarr; `quantity_mismatch` (with `difference` = trade - position)

## Expected API Response

`GET /reconciliation?date=2025-01-15` returns:

```json
{
  "date": "2025-01-15",
  "discrepancies": [
    {
      "account_id": "ACC001",
      "ticker": "GOOGL",
      "type": "quantity_mismatch",
      "trade_quantity": 100,
      "position_quantity": 75,
      "difference": 25
    },
    {
      "account_id": "ACC001",
      "ticker": "MSFT",
      "type": "missing_from_positions",
      "trade_quantity": 50,
      "position_quantity": null
    },
    {
      "account_id": "ACC002",
      "ticker": "TSLA",
      "type": "missing_from_trades",
      "trade_quantity": null,
      "position_quantity": 80
    }
  ]
}
```

## Scenario Walkthrough

### 1. Quantity Mismatch: ACC001 / GOOGL

- **Trades**: 1 BUY of 100 shares at $142.80 &rarr; aggregated trade quantity = **100**
- **Positions**: Custodian reports **75** shares
- **Discrepancy**: The FULL OUTER JOIN matches on `(ACC001, GOOGL)`. Both sides are present but `100 != 75`, so the `WHERE` clause keeps this row. Classified as `quantity_mismatch` with `difference = 100 - 75 = 25`.

### 2. Missing from Positions: ACC001 / MSFT

- **Trades**: 1 BUY of 50 shares at $420.25 &rarr; aggregated trade quantity = **50**
- **Positions**: No position record exists for ACC001/MSFT
- **Discrepancy**: The FULL OUTER JOIN produces a row with `trade_quantity = 50` and `position_quantity = NULL`. The `WHERE` clause keeps it (`p.position_quantity IS NULL`). Classified as `missing_from_positions`.

### 3. Missing from Trades: ACC002 / TSLA

- **Trades**: No trade record exists for ACC002/TSLA
- **Positions**: Custodian reports **80** shares at $19,076.00
- **Discrepancy**: The FULL OUTER JOIN produces a row with `trade_quantity = NULL` and `position_quantity = 80`. The `WHERE` clause keeps it (`t.trade_quantity IS NULL`). Classified as `missing_from_trades`.

### 4. Matching Record: ACC001 / AAPL (correctly excluded)

- **Trades**: 1 BUY of 100 shares at $185.50 &rarr; aggregated trade quantity = **100**
- **Positions**: Custodian reports **100** shares at $18,550.00
- **Result**: The FULL OUTER JOIN matches on `(ACC001, AAPL)`. Since `100 == 100`, the `WHERE` clause filters this row out. It does **not** appear in the discrepancies array.

## Test Suite Coverage

All 6 reconciliation tests pass (`tests/test_reconciliation.py`):

| Test                                       | What It Validates                                              |
|--------------------------------------------|----------------------------------------------------------------|
| `test_finds_quantity_mismatch`             | ACC001/GOOGL detected as `quantity_mismatch`, difference = 25  |
| `test_finds_missing_from_positions`        | ACC001/MSFT detected as `missing_from_positions`               |
| `test_finds_missing_from_trades`           | ACC002/TSLA detected as `missing_from_trades`                  |
| `test_matching_records_not_in_discrepancies`| ACC001/AAPL correctly excluded from results                   |
| `test_empty_date_returns_no_discrepancies` | Date with no data returns empty discrepancies list             |
| `test_via_http`                            | Full HTTP round-trip returns 200 with exactly 3 discrepancies  |
