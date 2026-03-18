from datetime import date

from app.services import ingest_file, reconcile


class TestReconciliation:

    def _seed(self, db):
        """Seed trades and positions with intentional discrepancies.

        Discrepancies:
        - ACC001 GOOGL: trade=100, position=75 (quantity mismatch)
        - ACC001 MSFT: in trades only (missing from positions)
        - ACC002 TSLA: in positions only (missing from trades)
        """
        trades = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"
            "2025-01-15,ACC001,GOOGL,100,142.80,BUY,2025-01-17\n"
            "2025-01-15,ACC001,MSFT,50,420.25,BUY,2025-01-17\n"
        )
        positions = (
            'report_date: "20250115"\n'
            "positions:\n"
            '  - account_id: "ACC001"\n'
            '    ticker: "AAPL"\n'
            "    shares: 100\n"
            "    market_value: 18550.00\n"
            '    custodian_ref: "CUST_A_12345"\n'
            '  - account_id: "ACC001"\n'
            '    ticker: "GOOGL"\n'
            "    shares: 75\n"
            "    market_value: 10710.00\n"
            '    custodian_ref: "CUST_A_12347"\n'
            '  - account_id: "ACC002"\n'
            '    ticker: "TSLA"\n'
            "    shares: 80\n"
            "    market_value: 19076.00\n"
            '    custodian_ref: "CUST_B_22347"\n'
        )
        ingest_file("trades.csv", trades)
        ingest_file("positions.yaml", positions)

    def test_finds_quantity_mismatch(self, db):
        self._seed(db)
        result = reconcile(date(2025, 1, 15))

        googl = next(
            d for d in result["discrepancies"]
            if d["account_id"] == "ACC001" and d["ticker"] == "GOOGL"
        )
        assert googl["type"] == "quantity_mismatch"
        assert googl["trade_quantity"] == 100
        assert googl["position_quantity"] == 75
        assert googl["difference"] == 25

    def test_finds_missing_from_positions(self, db):
        self._seed(db)
        result = reconcile(date(2025, 1, 15))

        msft = next(
            d for d in result["discrepancies"]
            if d["account_id"] == "ACC001" and d["ticker"] == "MSFT"
        )
        assert msft["type"] == "missing_from_positions"
        assert msft["trade_quantity"] == 50
        assert msft["position_quantity"] is None

    def test_finds_missing_from_trades(self, db):
        self._seed(db)
        result = reconcile(date(2025, 1, 15))

        tsla = next(
            d for d in result["discrepancies"]
            if d["account_id"] == "ACC002" and d["ticker"] == "TSLA"
        )
        assert tsla["type"] == "missing_from_trades"
        assert tsla["trade_quantity"] is None
        assert tsla["position_quantity"] == 80

    def test_matching_records_not_in_discrepancies(self, db):
        self._seed(db)
        result = reconcile(date(2025, 1, 15))

        aapl_discs = [
            d for d in result["discrepancies"]
            if d["account_id"] == "ACC001" and d["ticker"] == "AAPL"
        ]
        assert len(aapl_discs) == 0

    def test_empty_date_returns_no_discrepancies(self, db):
        result = reconcile(date(2099, 1, 1))
        assert result["discrepancies"] == []

    def test_via_http(self, client, db):
        self._seed(db)
        resp = client.get("/reconciliation?date=2025-01-15")
        assert resp.status_code == 200
        assert len(resp.get_json()["discrepancies"]) == 3


class TestReconciliationCumulative:
    """Tests verifying that reconciliation aggregates trades cumulatively
    (all trades up to the reconciliation date), since position files
    represent total holdings, not daily activity."""

    def test_cumulative_trade_aggregation(self, db):
        """Trades across two days should sum to match a cumulative position."""
        day1_trades = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-10,ACC001,AAPL,60,185.00,BUY,2025-01-12\n"
        )
        day2_trades = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,40,190.00,BUY,2025-01-17\n"
        )
        positions = (
            'report_date: "20250115"\n'
            "positions:\n"
            '  - account_id: "ACC001"\n'
            '    ticker: "AAPL"\n'
            "    shares: 100\n"
            "    market_value: 18800.00\n"
            '    custodian_ref: "CUST_A_001"\n'
        )
        ingest_file("trades_day1.csv", day1_trades)
        ingest_file("trades_day2.csv", day2_trades)
        ingest_file("positions.yaml", positions)

        result = reconcile(date(2025, 1, 15))
        aapl = [d for d in result["discrepancies"] if d["ticker"] == "AAPL"]
        assert len(aapl) == 0, "60 + 40 = 100 shares should match position"

    def test_cumulative_with_sells(self, db):
        """A BUY then partial SELL should net correctly against the position."""
        buy = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-10,ACC001,AAPL,100,185.00,BUY,2025-01-12\n"
        )
        sell = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,30,190.00,SELL,2025-01-17\n"
        )
        positions = (
            'report_date: "20250115"\n'
            "positions:\n"
            '  - account_id: "ACC001"\n'
            '    ticker: "AAPL"\n'
            "    shares: 70\n"
            "    market_value: 13300.00\n"
            '    custodian_ref: "CUST_A_001"\n'
        )
        ingest_file("trades_buy.csv", buy)
        ingest_file("trades_sell.csv", sell)
        ingest_file("positions.yaml", positions)

        result = reconcile(date(2025, 1, 15))
        aapl = [d for d in result["discrepancies"] if d["ticker"] == "AAPL"]
        assert len(aapl) == 0, "100 - 30 = 70 shares should match position"

    def test_future_trades_excluded(self, db):
        """Reconciling on day 1 should not include day 2 trades."""
        day1_trades = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-10,ACC001,AAPL,60,185.00,BUY,2025-01-12\n"
        )
        day2_trades = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,40,190.00,BUY,2025-01-17\n"
        )
        positions = (
            'report_date: "20250110"\n'
            "positions:\n"
            '  - account_id: "ACC001"\n'
            '    ticker: "AAPL"\n'
            "    shares: 60\n"
            "    market_value: 11100.00\n"
            '    custodian_ref: "CUST_A_001"\n'
        )
        ingest_file("trades_day1.csv", day1_trades)
        ingest_file("trades_day2.csv", day2_trades)
        ingest_file("positions.yaml", positions)

        result = reconcile(date(2025, 1, 10))
        aapl = [d for d in result["discrepancies"] if d["ticker"] == "AAPL"]
        assert len(aapl) == 0, "Only day 1 trades (60) should be compared"

    def test_cumulative_mismatch(self, db):
        """Multi-day trades that don't match position should report correct diff."""
        day1_trades = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-10,ACC001,AAPL,60,185.00,BUY,2025-01-12\n"
        )
        day2_trades = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,40,190.00,BUY,2025-01-17\n"
        )
        # Position says 90 but cumulative trades say 100
        positions = (
            'report_date: "20250115"\n'
            "positions:\n"
            '  - account_id: "ACC001"\n'
            '    ticker: "AAPL"\n'
            "    shares: 90\n"
            "    market_value: 17100.00\n"
            '    custodian_ref: "CUST_A_001"\n'
        )
        ingest_file("trades_day1.csv", day1_trades)
        ingest_file("trades_day2.csv", day2_trades)
        ingest_file("positions.yaml", positions)

        result = reconcile(date(2025, 1, 15))
        aapl = next(d for d in result["discrepancies"] if d["ticker"] == "AAPL")
        assert aapl["type"] == "quantity_mismatch"
        assert aapl["trade_quantity"] == 100
        assert aapl["position_quantity"] == 90
        assert aapl["difference"] == 10
