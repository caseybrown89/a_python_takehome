from datetime import date

from app.services import ingest_file, check_concentration


class TestComplianceConcentration:

    def _seed(self, db):
        """Seed positions where ACC001 AAPL is ~37% of account (violates 20%)."""
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
            '  - account_id: "ACC001"\n'
            '    ticker: "GOOGL"\n'
            "    shares: 75\n"
            "    market_value: 10710.00\n"
            '    custodian_ref: "CUST_A_12347"\n'
        )
        ingest_file("positions.yaml", positions)

    def test_detects_violations(self, db):
        self._seed(db)
        result = check_concentration(date(2025, 1, 15))

        # ACC001 total = 50272.50
        # AAPL = 18550 / 50272.50 = 36.9% -> violation
        # MSFT = 21012.50 / 50272.50 = 41.8% -> violation
        # GOOGL = 10710 / 50272.50 = 21.3% -> violation
        assert len(result["violations"]) == 3
        tickers = {v["ticker"] for v in result["violations"]}
        assert tickers == {"AAPL", "MSFT", "GOOGL"}

    def test_violation_fields(self, db):
        self._seed(db)
        result = check_concentration(date(2025, 1, 15))
        violation = next(v for v in result["violations"] if v["ticker"] == "AAPL")

        assert violation["account_id"] == "ACC001"
        assert violation["threshold"] == 20.0
        assert violation["concentration_pct"] > 20.0
        assert violation["total_account_value"] > 0

    def test_no_violations_below_threshold(self, db):
        # Four equal positions = 25% each, but use threshold of 30%
        positions = (
            'report_date: "20250115"\n'
            "positions:\n"
            '  - account_id: "ACC099"\n'
            '    ticker: "A"\n'
            "    shares: 100\n"
            "    market_value: 2500.00\n"
            '    custodian_ref: "REF1"\n'
            '  - account_id: "ACC099"\n'
            '    ticker: "B"\n'
            "    shares: 100\n"
            "    market_value: 2500.00\n"
            '    custodian_ref: "REF2"\n'
            '  - account_id: "ACC099"\n'
            '    ticker: "C"\n'
            "    shares: 100\n"
            "    market_value: 2500.00\n"
            '    custodian_ref: "REF3"\n'
            '  - account_id: "ACC099"\n'
            '    ticker: "D"\n'
            "    shares: 100\n"
            "    market_value: 2500.00\n"
            '    custodian_ref: "REF4"\n'
        )
        ingest_file("positions.yaml", positions)
        result = check_concentration(date(2025, 1, 15), threshold=30.0)
        acc099 = [v for v in result["violations"] if v["account_id"] == "ACC099"]
        assert len(acc099) == 0

    def test_empty_date_returns_no_violations(self, db):
        result = check_concentration(date(2099, 1, 1))
        assert result["violations"] == []

    def test_positions_from_other_dates_excluded(self, db):
        """Only positions matching the query date should factor into concentration."""
        jan15_positions = (
            'report_date: "20250115"\n'
            "positions:\n"
            '  - account_id: "ACC050"\n'
            '    ticker: "AAPL"\n'
            "    shares: 100\n"
            "    market_value: 5000.00\n"
            '    custodian_ref: "REF1"\n'
            '  - account_id: "ACC050"\n'
            '    ticker: "MSFT"\n'
            "    shares: 100\n"
            "    market_value: 5000.00\n"
            '    custodian_ref: "REF2"\n'
        )
        # A large position on a different date that would skew concentration
        # if it leaked into the Jan 15 query
        jan20_positions = (
            'report_date: "20250120"\n'
            "positions:\n"
            '  - account_id: "ACC050"\n'
            '    ticker: "AAPL"\n'
            "    shares: 500\n"
            "    market_value: 50000.00\n"
            '    custodian_ref: "REF3"\n'
            '  - account_id: "ACC050"\n'
            '    ticker: "MSFT"\n'
            "    shares: 10\n"
            "    market_value: 500.00\n"
            '    custodian_ref: "REF4"\n'
        )
        ingest_file("positions_jan15.yaml", jan15_positions)
        ingest_file("positions_jan20.yaml", jan20_positions)

        # On Jan 15, ACC050 has two equal 50/50 positions — both violate 20%
        # but each should be exactly 50%, not skewed by Jan 20 data
        result = check_concentration(date(2025, 1, 15))
        acc050 = [v for v in result["violations"] if v["account_id"] == "ACC050"]
        assert len(acc050) == 2
        for v in acc050:
            assert v["concentration_pct"] == 50.00
            assert v["total_account_value"] == 10000.00

        # On Jan 20, AAPL is ~99% of account — only AAPL should violate
        # If Jan 15 data leaked in, percentages and totals would differ
        result = check_concentration(date(2025, 1, 20))
        acc050 = [v for v in result["violations"] if v["account_id"] == "ACC050"]
        assert len(acc050) == 1
        assert acc050[0]["ticker"] == "AAPL"
        assert acc050[0]["total_account_value"] == 50500.00

    def test_via_http(self, client, db):
        self._seed(db)
        resp = client.get("/compliance/concentration?date=2025-01-15")
        assert resp.status_code == 200
        assert len(resp.get_json()["violations"]) == 3
