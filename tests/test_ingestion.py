import io

import pytest

from app.services import ingest_file, detect_format, IngestionError
from app.models import Trade, Position


class TestFormatDetection:

    def test_detects_format_1_from_header(self):
        content = "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
        assert detect_format("trades_daily.csv", content) == "format_1"

    def test_detects_format_2_from_header(self):
        content = "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"
        assert detect_format("trades_custodian.csv", content) == "format_2"

    def test_detects_positions_from_filename(self):
        assert detect_format("positions_bank.yaml", "report_date: ...") == "positions"

    def test_rejects_filename_without_trade_or_position(self):
        with pytest.raises(IngestionError, match="must contain 'trade' or 'position'"):
            detect_format("daily_data.csv", "some,header,row\n")

    def test_rejects_trade_file_with_unknown_header(self):
        with pytest.raises(IngestionError, match="does not match any known specification"):
            detect_format("trades.csv", "Col1,Col2,Col3\n")


class TestFormat1Ingestion:

    def test_ingests_valid_rows(self, db, format1_content):
        report = ingest_file("trades_f1.csv", format1_content)
        assert report["records_ingested"] == 4
        assert report["records_skipped"] == 0

    def test_buy_has_positive_quantity(self, db, format1_content):
        ingest_file("trades_f1.csv", format1_content)
        trade = Trade.query.filter_by(account_id="ACC001", ticker="AAPL").first()
        assert trade.quantity == 100
        assert trade.trade_type == "BUY"

    def test_sell_has_negative_quantity(self, db, format1_content):
        ingest_file("trades_f1.csv", format1_content)
        trade = Trade.query.filter_by(account_id="ACC003", ticker="TSLA").first()
        assert trade.quantity == -150
        assert trade.trade_type == "SELL"

    def test_source_file_is_filename(self, db, format1_content):
        ingest_file("trades_f1.csv", format1_content)
        trade = Trade.query.first()
        assert trade.source_file == ["trades_f1.csv"]

    def test_custodian_is_null(self, db, format1_content):
        ingest_file("trades_f1.csv", format1_content)
        trade = Trade.query.first()
        assert trade.custodian is None

    def test_market_value_computed(self, db, format1_content):
        ingest_file("trades_f1.csv", format1_content)
        trade = Trade.query.filter_by(account_id="ACC001", ticker="AAPL").first()
        assert trade.market_value == 100 * 185.50

    def test_strict_mode_rejects_bad_rows(self, db):
        content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "bad-date,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"
        )
        with pytest.raises(IngestionError, match="Strict mode"):
            ingest_file("trades.csv", content, strict=True)
        assert Trade.query.count() == 0

    def test_permissive_mode_skips_bad_rows(self, db):
        content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "bad-date,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"
            "2025-01-15,ACC001,MSFT,50,420.25,BUY,2025-01-17\n"
        )
        report = ingest_file("trades.csv", content, strict=False)
        assert report["records_ingested"] == 1
        assert report["records_skipped"] == 1


class TestFormat2Ingestion:

    def test_ingests_valid_rows(self, db, format2_content):
        report = ingest_file("trades_f2.csv", format2_content)
        assert report["records_ingested"] == 3
        assert report["format_detected"] == "format_2"

    def test_parses_yyyymmdd_date(self, db, format2_content):
        ingest_file("trades_f2.csv", format2_content)
        trade = Trade.query.first()
        assert str(trade.trade_date) == "2025-01-15"

    def test_derives_price_from_market_value(self, db, format2_content):
        ingest_file("trades_f2.csv", format2_content)
        trade = Trade.query.filter_by(account_id="ACC001", ticker="AAPL").first()
        assert trade.price == pytest.approx(185.50)

    def test_source_file_is_filename(self, db, format2_content):
        ingest_file("trades_f2.csv", format2_content)
        trade = Trade.query.first()
        assert trade.source_file == ["trades_f2.csv"]

    def test_custodian_parsed_from_source_system_column(self, db, format2_content):
        ingest_file("trades_f2.csv", format2_content)
        trade = Trade.query.filter_by(account_id="ACC001", ticker="AAPL").first()
        assert trade.custodian == "CUSTODIAN_A"


class TestPositionsIngestion:

    def test_ingests_valid_positions(self, db, positions_content):
        report = ingest_file("positions_bank.yaml", positions_content)
        assert report["records_ingested"] == 3
        assert report["format_detected"] == "positions"

    def test_position_fields(self, db, positions_content):
        ingest_file("positions_bank.yaml", positions_content)
        pos = Position.query.filter_by(account_id="ACC001", ticker="AAPL").first()
        assert pos.shares == 100
        assert pos.market_value == 18550.00
        assert pos.custodian_ref == "CUST_A_12345"

    def test_rejects_invalid_yaml(self, db):
        with pytest.raises(IngestionError, match="Strict mode"):
            ingest_file("positions.yaml", "{{invalid yaml", strict=True)


class TestFormat1EdgeCases:

    def test_missing_required_field_skipped(self, db):
        content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,,AAPL,100,185.50,BUY,2025-01-17\n"
        )
        report = ingest_file("trades.csv", content, strict=False)
        assert report["records_skipped"] == 1
        assert "missing fields" in report["errors"][0]

    def test_negative_price_produces_warning(self, db):
        content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,100,-5.00,BUY,2025-01-17\n"
        )
        report = ingest_file("trades.csv", content, strict=False)
        assert report["records_ingested"] == 1
        assert any("negative price" in w for w in report["warnings"])

    def test_non_numeric_quantity_skipped(self, db):
        content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "2025-01-15,ACC001,AAPL,abc,185.50,BUY,2025-01-17\n"
        )
        report = ingest_file("trades.csv", content, strict=False)
        assert report["records_skipped"] == 1


class TestFormat2EdgeCases:

    def test_too_few_fields_skipped(self, db):
        content = (
            "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"
            "20250115|ACC001|AAPL\n"
        )
        report = ingest_file("trades_f2.csv", content, strict=False)
        assert report["records_skipped"] == 1
        assert "expected 6 fields" in report["errors"][0]

    def test_invalid_date_skipped(self, db):
        content = (
            "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"
            "not-a-date|ACC001|AAPL|100|18550.00|CUSTODIAN_A\n"
        )
        report = ingest_file("trades_f2.csv", content, strict=False)
        assert report["records_skipped"] == 1
        assert "invalid date" in report["errors"][0]

    def test_non_numeric_shares_skipped(self, db):
        content = (
            "REPORT_DATE|ACCOUNT_ID|SECURITY_TICKER|SHARES|MARKET_VALUE|SOURCE_SYSTEM\n"
            "20250115|ACC001|AAPL|abc|18550.00|CUSTODIAN_A\n"
        )
        report = ingest_file("trades_f2.csv", content, strict=False)
        assert report["records_skipped"] == 1


class TestPositionsEdgeCases:

    def test_missing_report_date(self, db):
        lines = [
            "positions:",
            "  - account_id: ACC001",
            "    ticker: AAPL",
            "    shares: 100",
            "    market_value: 18550",
            "    custodian_ref: REF1",
        ]
        report = ingest_file("positions.yaml", "\n".join(lines), strict=False)
        assert report["records_skipped"] == 1
        assert "report_date" in report["errors"][0]

    def test_position_missing_required_field(self, db):
        lines = [
            'report_date: "20250115"',
            "positions:",
            "  - account_id: ACC001",
            "    ticker: AAPL",
            "    shares: 100",
        ]
        report = ingest_file("positions.yaml", "\n".join(lines), strict=False)
        assert report["records_skipped"] == 1
        assert "missing fields" in report["errors"][0]

    def test_position_non_numeric_shares(self, db):
        lines = [
            'report_date: "20250115"',
            "positions:",
            "  - account_id: ACC001",
            "    ticker: AAPL",
            "    shares: abc",
            "    market_value: 18550",
            "    custodian_ref: REF1",
        ]
        report = ingest_file("positions.yaml", "\n".join(lines), strict=False)
        assert report["records_skipped"] == 1


class TestIngestRoute:

    def test_ingest_valid_file(self, client, db, format1_content):
        data = {"file": (io.BytesIO(format1_content.encode()), "trades.csv")}
        resp = client.post("/ingest", data=data, content_type="multipart/form-data")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["files"][0]["records_ingested"] == 4

    def test_ingest_no_file(self, client, db):
        resp = client.post("/ingest")
        assert resp.status_code == 400
        assert "No file" in resp.get_json()["error"]

    def test_ingest_strict_mode_returns_422(self, client, db):
        bad_content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "bad-date,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"
        )
        data = {"file": (io.BytesIO(bad_content.encode()), "trades.csv")}
        resp = client.post("/ingest?mode=strict", data=data, content_type="multipart/form-data")
        assert resp.status_code == 422

    def test_ingest_permissive_mode(self, client, db):
        content = (
            "TradeDate,AccountID,Ticker,Quantity,Price,TradeType,SettlementDate\n"
            "bad-date,ACC001,AAPL,100,185.50,BUY,2025-01-17\n"
            "2025-01-15,ACC001,MSFT,50,420.25,BUY,2025-01-17\n"
        )
        data = {"file": (io.BytesIO(content.encode()), "trades.csv")}
        resp = client.post("/ingest?mode=permissive", data=data, content_type="multipart/form-data")
        assert resp.status_code == 200
        assert resp.get_json()["files"][0]["records_ingested"] == 1


class TestPingRoute:

    def test_ping_returns_healthy(self, client, db):
        resp = client.get("/ping")
        assert resp.status_code == 200
        assert resp.get_json()["status"] == "healthy"


class TestPositionUpsert:

    def test_duplicate_ingestion_no_duplicate_rows(self, db, positions_content):
        ingest_file("positions_bank.yaml", positions_content)
        assert Position.query.count() == 3
        report = ingest_file("positions_bank.yaml", positions_content)
        assert Position.query.count() == 3
        assert report["records_ingested"] == 0
        assert report["records_updated"] == 3

    def test_value_update_on_reload(self, db, positions_content):
        ingest_file("positions_bank.yaml", positions_content)
        pos = Position.query.filter_by(account_id="ACC001", ticker="AAPL").first()
        assert pos.shares == 100
        assert pos.market_value == 18550.00

        updated_content = (
            'report_date: "20250115"\n'
            "positions:\n"
            '  - account_id: "ACC001"\n'
            '    ticker: "AAPL"\n'
            "    shares: 200\n"
            "    market_value: 37100.00\n"
            '    custodian_ref: "CUST_A_99999"\n'
        )
        ingest_file("positions_bank.yaml", updated_content)
        pos = Position.query.filter_by(account_id="ACC001", ticker="AAPL").first()
        assert pos.shares == 200
        assert pos.market_value == 37100.00
        assert pos.custodian_ref == "CUST_A_99999"

    def test_insert_vs_update_counts(self, db, positions_content):
        report1 = ingest_file("positions_bank.yaml", positions_content)
        assert report1["records_ingested"] == 3
        assert report1["records_updated"] == 0

        report2 = ingest_file("positions_bank.yaml", positions_content)
        assert report2["records_ingested"] == 0
        assert report2["records_updated"] == 3


class TestTradeUpsert:

    def test_duplicate_ingestion_no_duplicate_rows(self, db, format1_content):
        ingest_file("trades_f1.csv", format1_content)
        assert Trade.query.count() == 4
        report = ingest_file("trades_f1.csv", format1_content)
        assert Trade.query.count() == 4
        assert report["records_ingested"] == 0
        assert report["records_updated"] == 4

    def test_format1_then_format2_merges_custodian(self, db, format1_content, format2_content):
        ingest_file("trades_f1.csv", format1_content)
        trade = Trade.query.filter_by(account_id="ACC001", ticker="AAPL").first()
        assert trade.custodian is None
        assert trade.settlement_date is not None

        ingest_file("trades_f2.csv", format2_content)
        trade = Trade.query.filter_by(account_id="ACC001", ticker="AAPL").first()
        assert trade.custodian == "CUSTODIAN_A"
        assert trade.settlement_date is not None
        assert Trade.query.filter_by(account_id="ACC001", ticker="AAPL").count() == 1

    def test_format2_then_format1_merges_settlement_date(self, db, format2_content, format1_content):
        ingest_file("trades_f2.csv", format2_content)
        trade = Trade.query.filter_by(account_id="ACC001", ticker="AAPL").first()
        assert trade.settlement_date is None
        assert trade.custodian == "CUSTODIAN_A"

        ingest_file("trades_f1.csv", format1_content)
        trade = Trade.query.filter_by(account_id="ACC001", ticker="AAPL").first()
        assert trade.settlement_date is not None
        assert trade.custodian == "CUSTODIAN_A"
        assert Trade.query.filter_by(account_id="ACC001", ticker="AAPL").count() == 1

    def test_report_includes_updated_count(self, db, format1_content, format2_content):
        report1 = ingest_file("trades_f1.csv", format1_content)
        assert report1["records_ingested"] == 4
        assert report1["records_updated"] == 0

        # format2 has 3 rows, all overlap with format1
        report2 = ingest_file("trades_f2.csv", format2_content)
        assert report2["records_ingested"] == 0
        assert report2["records_updated"] == 3

    def test_upsert_accumulates_source_files(self, db, format1_content, format2_content):
        ingest_file("trades_f1.csv", format1_content)
        trade = Trade.query.filter_by(account_id="ACC001", ticker="AAPL").first()
        assert trade.source_file == ["trades_f1.csv"]

        ingest_file("trades_f2.csv", format2_content)
        trade = Trade.query.filter_by(account_id="ACC001", ticker="AAPL").first()
        assert trade.source_file == ["trades_f1.csv", "trades_f2.csv"]

    def test_upsert_does_not_duplicate_source_file(self, db, format1_content):
        ingest_file("trades_f1.csv", format1_content)
        ingest_file("trades_f1.csv", format1_content)
        trade = Trade.query.filter_by(account_id="ACC001", ticker="AAPL").first()
        assert trade.source_file == ["trades_f1.csv"]


class TestInvalidDateParam:

    def test_invalid_date_format_returns_400(self, client):
        resp = client.get("/positions?account=ACC001&date=not-a-date")
        assert resp.status_code == 400
        assert "Invalid date format" in resp.get_json()["error"]
