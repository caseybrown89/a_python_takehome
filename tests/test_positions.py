from datetime import date

from app.services import ingest_file, get_positions


class TestPositionsEndpoint:

    def _seed(self, db):
        """Ingest sample trades and positions for ACC001."""
        trades = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"
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
            '    ticker: "MSFT"\n'
            "    shares: 50\n"
            "    market_value: 21012.50\n"
            '    custodian_ref: "CUST_A_12346"\n'
        )
        ingest_file("trades.csv", trades)
        ingest_file("positions.yaml", positions)

    def test_returns_positions_with_cost_basis(self, db):
        self._seed(db)
        result = get_positions("ACC001", date(2025, 1, 15))

        assert result["account"] == "ACC001"
        assert len(result["positions"]) == 2

        aapl = next(p for p in result["positions"] if p["ticker"] == "AAPL")
        assert aapl["shares"] == 100
        assert aapl["market_value"] == 18550.00
        assert aapl["cost_basis"] == 18550.00

    def test_percentage_of_account(self, db):
        self._seed(db)
        result = get_positions("ACC001", date(2025, 1, 15))

        total = result["total_market_value"]
        for p in result["positions"]:
            expected_pct = round((p["market_value"] / total) * 100, 2)
            assert p["pct_of_account"] == expected_pct

    def test_empty_for_unknown_account(self, db):
        result = get_positions("UNKNOWN", date(2025, 1, 15))
        assert result["positions"] == []
        assert result["total_market_value"] == 0

    def test_via_http(self, client, db):
        self._seed(db)
        resp = client.get("/positions?account=ACC001&date=2025-01-15")
        assert resp.status_code == 200
        assert len(resp.get_json()["positions"]) == 2

    def test_missing_account_param(self, client):
        resp = client.get("/positions?date=2025-01-15")
        assert resp.status_code == 400

    def test_missing_date_param(self, client):
        resp = client.get("/positions?account=ACC001")
        assert resp.status_code == 400

    def test_cost_basis_with_buy_and_sell(self, db):
        """Cost basis should reflect net investment: buys minus sells."""
        trades = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-10,ACC001,AAPL,100,185.50,BUY,2025-01-12\n"
            "2025-01-14,ACC001,AAPL,30,190.00,SELL,2025-01-16\n"
        )
        # Position as of Jan 15: 70 shares remaining at current price
        positions = (
            'report_date: "20250115"\n'
            "positions:\n"
            '  - account_id: "ACC001"\n'
            '    ticker: "AAPL"\n'
            "    shares: 70\n"
            "    market_value: 13300.00\n"
            '    custodian_ref: "CUST_A_99999"\n'
        )
        ingest_file("trades.csv", trades)
        ingest_file("positions.yaml", positions)

        result = get_positions("ACC001", date(2025, 1, 15))
        aapl = next(p for p in result["positions"] if p["ticker"] == "AAPL")

        # Buy: 100 * 185.50 = 18550.00
        # Sell: -30 * 190.00 = -5700.00  (quantity negated for sells)
        # Net investment cost basis: 18550.00 - 5700.00 = 12850.00
        assert aapl["cost_basis"] == 12850.00
        assert aapl["shares"] == 70

    def test_future_trades_excluded_from_cost_basis(self, db):
        """Trades after the query date should not affect cost basis."""
        past_trade = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-10,ACC001,AAPL,100,185.00,BUY,2025-01-12\n"
        )
        future_trade = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-20,ACC001,AAPL,50,200.00,BUY,2025-01-22\n"
        )
        positions = (
            'report_date: "20250115"\n'
            "positions:\n"
            '  - account_id: "ACC001"\n'
            '    ticker: "AAPL"\n'
            "    shares: 100\n"
            "    market_value: 19000.00\n"
            '    custodian_ref: "CUST_A_001"\n'
        )
        ingest_file("trades_past.csv", past_trade)
        ingest_file("trades_future.csv", future_trade)
        ingest_file("positions.yaml", positions)

        result = get_positions("ACC001", date(2025, 1, 15))
        aapl = next(p for p in result["positions"] if p["ticker"] == "AAPL")

        # Only the Jan 10 trade should count: 100 * 185.00 = 18500.00
        # The Jan 20 trade (50 * 200.00 = 10000.00) must be excluded
        assert aapl["cost_basis"] == 18500.00
