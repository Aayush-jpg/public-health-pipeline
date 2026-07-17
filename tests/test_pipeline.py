"""
Unit tests for the Public Health Data Pipeline
"""
import pytest
from ingestion.ingest import safe_int, safe_float, safe_date


class TestSafeInt:
    def test_valid_integer(self):
        assert safe_int("42") == 42

    def test_float_string(self):
        assert safe_int("42.9") == 42 

    def test_comma_formatted(self):
        assert safe_int("1,234") == 1234

    def test_none_returns_none(self):
        assert safe_int(None) is None

    def test_empty_string_returns_none(self):
        assert safe_int("") is None

    def test_non_numeric_returns_none(self):
        assert safe_int("abc") is None

    def test_negative(self):
        assert safe_int("-5") == -5


class TestSafeFloat:
    def test_valid_float(self):
        assert safe_float("3.14") == pytest.approx(3.14)

    def test_comma_formatted(self):
        assert safe_float("1,234.56") == pytest.approx(1234.56)

    def test_none_returns_none(self):
        assert safe_float(None) is None

    def test_empty_string_returns_none(self):
        assert safe_float("") is None


class TestSafeDate:
    def test_iso_date(self):
        assert safe_date("2023-01-15") == "2023-01-15"

    def test_datetime_string(self):
        result = safe_date("2023-01-15T00:00:00.000")
        assert result == "2023-01-15"

    def test_none_returns_none(self):
        assert safe_date(None) is None

    def test_empty_string_returns_none(self):
        assert safe_date("") is None


class TestDataValidation:
    def test_batch_size_positive(self):
        from ingestion.ingest import BATCH_SIZE
        assert BATCH_SIZE > 0

    def test_cdc_endpoints_defined(self):
        from ingestion.ingest import CDC_ENDPOINTS
        assert "covid_deaths" in CDC_ENDPOINTS
        assert "chronic_disease" in CDC_ENDPOINTS
        assert "vaccination" in CDC_ENDPOINTS

    def test_endpoints_are_valid_urls(self):
        from ingestion.ingest import CDC_ENDPOINTS
        for name, url in CDC_ENDPOINTS.items():
            assert url.startswith("https://"), f"{name} endpoint must use HTTPS"
