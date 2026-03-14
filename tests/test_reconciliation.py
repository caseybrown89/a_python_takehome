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
