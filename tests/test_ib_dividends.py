"""Tests for IBProvider dividend parsing (CalendarReport XML + tick 456).

These tests verify the XML parsing and tick string parsing logic
without requiring a live IB TWS/Gateway connection.
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

# We test the parsing helpers directly — no ib_insync import needed
# since IBProvider guards it behind _IB_AVAILABLE.

# Patch ib_insync before importing IBProvider
import sys
mock_ib_module = MagicMock()
sys.modules["ib_insync"] = mock_ib_module
mock_ib_module.IB = MagicMock
mock_ib_module.Stock = MagicMock
mock_ib_module.util = MagicMock

from marketdata.providers.ib import IBProvider


# ---- XML fixtures ----

CALENDAR_REPORT_REFINITIV = """\
<?xml version="1.0" encoding="UTF-8"?>
<CalendarReport>
  <Company>
    <CoIDs><CoID Type="CompanyName">Apple Inc</CoID></CoIDs>
  </Company>
  <DividendsData>
    <CashDividend>
      <ExDate>2024-08-12</ExDate>
      <RecordDate>2024-08-12</RecordDate>
      <PayDate>2024-08-15</PayDate>
      <DeclarationDate>2024-08-01</DeclarationDate>
      <Amount>0.25</Amount>
      <Currency>USD</Currency>
      <DividendType>Regular Cash</DividendType>
      <Frequency>Quarterly</Frequency>
    </CashDividend>
    <CashDividend>
      <ExDate>2024-05-10</ExDate>
      <RecordDate>2024-05-13</RecordDate>
      <PayDate>2024-05-16</PayDate>
      <DeclarationDate>2024-05-02</DeclarationDate>
      <Amount>0.25</Amount>
      <Currency>USD</Currency>
      <DividendType>Regular Cash</DividendType>
      <Frequency>Quarterly</Frequency>
    </CashDividend>
    <CashDividend>
      <ExDate>2024-02-09</ExDate>
      <Amount>0.24</Amount>
      <DividendType>Regular Cash</DividendType>
      <Frequency>Quarterly</Frequency>
    </CashDividend>
  </DividendsData>
</CalendarReport>
"""

CALENDAR_REPORT_EVENT_STYLE = """\
<?xml version="1.0" encoding="UTF-8"?>
<CalendarReport>
  <EventSchedule>
    <Event type="dividend">
      <EventDate>2024-11-08</EventDate>
      <Detail>
        <Amount>0.26</Amount>
        <Currency>USD</Currency>
        <PayDate>2024-11-15</PayDate>
        <RecordDate>2024-11-11</RecordDate>
        <DivType>Regular</DivType>
        <Freq>Quarterly</Freq>
      </Detail>
    </Event>
    <Event type="dividend">
      <EventDate>2024-08-09</EventDate>
      <Detail>
        <Amount>0.25</Amount>
        <Currency>USD</Currency>
        <DivType>Special</DivType>
      </Detail>
    </Event>
  </EventSchedule>
</CalendarReport>
"""

CALENDAR_REPORT_SLASH_DATES = """\
<?xml version="1.0" encoding="UTF-8"?>
<CalendarReport>
  <DividendsData>
    <CashDividend>
      <ExDate>02/09/2024</ExDate>
      <PayDate>02/15/2024</PayDate>
      <Amount>0.24</Amount>
      <DividendType>Regular Cash</DividendType>
    </CashDividend>
  </DividendsData>
</CalendarReport>
"""

CALENDAR_REPORT_EMPTY = """\
<?xml version="1.0" encoding="UTF-8"?>
<CalendarReport>
  <Company><CoIDs><CoID>Test</CoID></CoIDs></Company>
</CalendarReport>
"""

CALENDAR_REPORT_NO_AMOUNT = """\
<?xml version="1.0" encoding="UTF-8"?>
<CalendarReport>
  <DividendsData>
    <CashDividend>
      <ExDate>2024-08-12</ExDate>
    </CashDividend>
  </DividendsData>
</CalendarReport>
"""


@pytest.fixture
def ib_provider():
    """Create an IBProvider without actually connecting to IB."""
    with patch.object(IBProvider, "__init__", lambda self, **kw: None):
        provider = object.__new__(IBProvider)
        provider.host = "127.0.0.1"
        provider.port = 7497
        provider.client_id = 1
        provider._ib = None
        return provider


# ---- CalendarReport XML parsing tests ----

class TestCalendarReportParsing:
    def test_refinitiv_format(self, ib_provider):
        """Parse CashDividend elements with ISO dates."""
        mock_ib = MagicMock()
        mock_ib.reqFundamentalData.return_value = CALENDAR_REPORT_REFINITIV
        mock_contract = MagicMock()

        events = ib_provider._dividends_from_calendar_report(mock_ib, mock_contract, "AAPL")

        assert len(events) == 3
        # Verify first dividend (most recent by XML order)
        ev = events[0]
        assert ev.symbol == "AAPL"
        assert ev.ex_date == date(2024, 8, 12)
        assert ev.amount == 0.25
        assert ev.record_date == date(2024, 8, 12)
        assert ev.pay_date == date(2024, 8, 15)
        assert ev.declaration_date == date(2024, 8, 1)
        assert ev.dividend_type == "regular"
        assert ev.frequency == 4
        assert ev.currency == "USD"

    def test_refinitiv_minimal_fields(self, ib_provider):
        """Parse dividend with only ex_date and amount (no optional fields)."""
        mock_ib = MagicMock()
        mock_ib.reqFundamentalData.return_value = CALENDAR_REPORT_REFINITIV
        mock_contract = MagicMock()

        events = ib_provider._dividends_from_calendar_report(mock_ib, mock_contract, "AAPL")

        # Third dividend has minimal fields
        ev = events[2]
        assert ev.ex_date == date(2024, 2, 9)
        assert ev.amount == 0.24
        assert ev.record_date is None
        assert ev.pay_date is None

    def test_event_style_format(self, ib_provider):
        """Parse <Event type='dividend'> with nested <Detail>."""
        mock_ib = MagicMock()
        mock_ib.reqFundamentalData.return_value = CALENDAR_REPORT_EVENT_STYLE
        mock_contract = MagicMock()

        events = ib_provider._dividends_from_calendar_report(mock_ib, mock_contract, "AAPL")

        assert len(events) == 2

        # First event: regular, with pay/record dates
        ev0 = events[0]
        assert ev0.ex_date == date(2024, 11, 8)
        assert ev0.amount == 0.26
        assert ev0.pay_date == date(2024, 11, 15)
        assert ev0.record_date == date(2024, 11, 11)
        assert ev0.dividend_type == "regular"
        assert ev0.frequency == 4

        # Second event: special dividend
        ev1 = events[1]
        assert ev1.ex_date == date(2024, 8, 9)
        assert ev1.amount == 0.25
        assert ev1.dividend_type == "special"

    def test_slash_date_format(self, ib_provider):
        """Parse MM/DD/YYYY date format."""
        mock_ib = MagicMock()
        mock_ib.reqFundamentalData.return_value = CALENDAR_REPORT_SLASH_DATES
        mock_contract = MagicMock()

        events = ib_provider._dividends_from_calendar_report(mock_ib, mock_contract, "AAPL")

        assert len(events) == 1
        assert events[0].ex_date == date(2024, 2, 9)
        assert events[0].pay_date == date(2024, 2, 15)

    def test_empty_report(self, ib_provider):
        """No dividend elements returns empty list."""
        mock_ib = MagicMock()
        mock_ib.reqFundamentalData.return_value = CALENDAR_REPORT_EMPTY
        mock_contract = MagicMock()

        events = ib_provider._dividends_from_calendar_report(mock_ib, mock_contract, "AAPL")
        assert events == []

    def test_no_amount_skipped(self, ib_provider):
        """Dividend elements without an amount are skipped."""
        mock_ib = MagicMock()
        mock_ib.reqFundamentalData.return_value = CALENDAR_REPORT_NO_AMOUNT
        mock_contract = MagicMock()

        events = ib_provider._dividends_from_calendar_report(mock_ib, mock_contract, "AAPL")
        assert events == []

    def test_none_xml_returns_empty(self, ib_provider):
        """If reqFundamentalData returns None, return empty list."""
        mock_ib = MagicMock()
        mock_ib.reqFundamentalData.return_value = None
        mock_contract = MagicMock()

        events = ib_provider._dividends_from_calendar_report(mock_ib, mock_contract, "AAPL")
        assert events == []

    def test_invalid_xml_returns_empty(self, ib_provider):
        """Malformed XML returns empty list without raising."""
        mock_ib = MagicMock()
        mock_ib.reqFundamentalData.return_value = "<broken><xml"
        mock_contract = MagicMock()

        events = ib_provider._dividends_from_calendar_report(mock_ib, mock_contract, "AAPL")
        assert events == []

    def test_api_exception_returns_empty(self, ib_provider):
        """Exception from reqFundamentalData returns empty list."""
        mock_ib = MagicMock()
        mock_ib.reqFundamentalData.side_effect = RuntimeError("no subscription")
        mock_contract = MagicMock()

        events = ib_provider._dividends_from_calendar_report(mock_ib, mock_contract, "AAPL")
        assert events == []


# ---- Generic tick 456 parsing tests ----

class TestTickDividendParsing:
    def test_valid_tick_string(self, ib_provider):
        """Parse standard tick 456 format: past12,next12,nextDate,nextAmt."""
        mock_ib = MagicMock()
        mock_ticker = MagicMock()
        mock_ticker.dividends = "0.96,0.96,2024-08-10,0.24"
        mock_ib.reqMktData.return_value = mock_ticker
        mock_contract = MagicMock()

        events = ib_provider._dividends_from_tick(mock_ib, mock_contract, "MSFT")

        assert len(events) == 1
        assert events[0].symbol == "MSFT"
        assert events[0].ex_date == date(2024, 8, 10)
        assert events[0].amount == 0.24
        assert events[0].dividend_type == "regular"
        mock_ib.cancelMktData.assert_called_once()

    def test_no_dividends_attr(self, ib_provider):
        """Ticker without dividends attribute returns empty list."""
        mock_ib = MagicMock()
        mock_ticker = MagicMock(spec=[])  # No attributes
        mock_ib.reqMktData.return_value = mock_ticker
        mock_contract = MagicMock()

        events = ib_provider._dividends_from_tick(mock_ib, mock_contract, "MSFT")
        assert events == []

    def test_empty_string(self, ib_provider):
        """Empty dividends string returns empty list."""
        mock_ib = MagicMock()
        mock_ticker = MagicMock()
        mock_ticker.dividends = ""
        mock_ib.reqMktData.return_value = mock_ticker
        mock_contract = MagicMock()

        events = ib_provider._dividends_from_tick(mock_ib, mock_contract, "MSFT")
        assert events == []

    def test_too_few_parts(self, ib_provider):
        """Incomplete tick string returns empty list."""
        mock_ib = MagicMock()
        mock_ticker = MagicMock()
        mock_ticker.dividends = "0.96,0.96"
        mock_ib.reqMktData.return_value = mock_ticker
        mock_contract = MagicMock()

        events = ib_provider._dividends_from_tick(mock_ib, mock_contract, "MSFT")
        assert events == []

    def test_invalid_amount(self, ib_provider):
        """Non-numeric amount returns empty list."""
        mock_ib = MagicMock()
        mock_ticker = MagicMock()
        mock_ticker.dividends = "0.96,0.96,2024-08-10,N/A"
        mock_ib.reqMktData.return_value = mock_ticker
        mock_contract = MagicMock()

        events = ib_provider._dividends_from_tick(mock_ib, mock_contract, "MSFT")
        assert events == []

    def test_exception_returns_empty(self, ib_provider):
        """Exception from reqMktData returns empty list."""
        mock_ib = MagicMock()
        mock_ib.reqMktData.side_effect = RuntimeError("timeout")
        mock_contract = MagicMock()

        events = ib_provider._dividends_from_tick(mock_ib, mock_contract, "MSFT")
        assert events == []


# ---- get_dividends integration (both methods) ----

class TestGetDividendsFallback:
    def test_uses_calendar_report_first(self, ib_provider):
        """get_dividends uses CalendarReport when available."""
        mock_ib = MagicMock()
        mock_ib.reqFundamentalData.return_value = CALENDAR_REPORT_REFINITIV
        ib_provider._ib = mock_ib
        ib_provider._connect = lambda: mock_ib

        events = ib_provider.get_dividends("AAPL", limit=2)

        assert len(events) == 2
        # Sorted by ex_date descending
        assert events[0].ex_date >= events[1].ex_date
        # Should not call reqMktData since CalendarReport succeeded
        mock_ib.reqMktData.assert_not_called()

    def test_falls_back_to_tick(self, ib_provider):
        """get_dividends falls back to tick 456 when CalendarReport is empty."""
        mock_ib = MagicMock()
        mock_ib.reqFundamentalData.return_value = CALENDAR_REPORT_EMPTY
        mock_ticker = MagicMock()
        mock_ticker.dividends = "0.96,0.96,2024-08-10,0.24"
        mock_ib.reqMktData.return_value = mock_ticker
        ib_provider._ib = mock_ib
        ib_provider._connect = lambda: mock_ib

        events = ib_provider.get_dividends("AAPL")

        assert len(events) == 1
        assert events[0].ex_date == date(2024, 8, 10)
        assert events[0].amount == 0.24

    def test_both_methods_fail_returns_empty(self, ib_provider):
        """Returns empty list when both methods produce nothing."""
        mock_ib = MagicMock()
        mock_ib.reqFundamentalData.return_value = CALENDAR_REPORT_EMPTY
        mock_ticker = MagicMock()
        mock_ticker.dividends = ""
        mock_ib.reqMktData.return_value = mock_ticker
        ib_provider._ib = mock_ib
        ib_provider._connect = lambda: mock_ib

        events = ib_provider.get_dividends("AAPL")
        assert events == []


# ---- Helper method tests ----

class TestHelperMethods:
    def test_clean_tag_no_namespace(self, ib_provider):
        assert ib_provider._clean_tag("ExDate") == "exdate"

    def test_clean_tag_with_namespace(self, ib_provider):
        assert ib_provider._clean_tag("{http://example.com}ExDate") == "exdate"

    def test_parse_date_iso(self, ib_provider):
        assert ib_provider._parse_date_flexible("2024-08-12") == date(2024, 8, 12)

    def test_parse_date_slash(self, ib_provider):
        assert ib_provider._parse_date_flexible("02/09/2024") == date(2024, 2, 9)

    def test_parse_date_compact(self, ib_provider):
        assert ib_provider._parse_date_flexible("20240812") == date(2024, 8, 12)

    def test_parse_date_none(self, ib_provider):
        assert ib_provider._parse_date_flexible(None) is None

    def test_parse_date_invalid(self, ib_provider):
        assert ib_provider._parse_date_flexible("not-a-date") is None

    def test_parse_date_whitespace(self, ib_provider):
        assert ib_provider._parse_date_flexible("  2024-08-12  ") == date(2024, 8, 12)
