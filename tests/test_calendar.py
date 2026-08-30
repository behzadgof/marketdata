"""Tests for trading calendar."""

from datetime import date, datetime, time

from marketdata.calendar import (
    AD_HOC_CLOSURES,
    get_trading_dates,
    is_half_day,
    is_holiday,
    is_market_open,
    is_trading_day,
    market_close_time,
    market_open_time,
    next_market_open,
)


class TestIsHoliday:
    def test_new_years(self):
        assert is_holiday(date(2024, 1, 1))

    def test_mlk_day(self):
        assert is_holiday(date(2024, 1, 15))  # 3rd Monday of Jan 2024

    def test_presidents_day(self):
        assert is_holiday(date(2024, 2, 19))

    def test_good_friday(self):
        assert is_holiday(date(2024, 3, 29))

    def test_memorial_day(self):
        assert is_holiday(date(2024, 5, 27))

    def test_juneteenth(self):
        assert is_holiday(date(2024, 6, 19))

    def test_independence_day(self):
        assert is_holiday(date(2024, 7, 4))

    def test_labor_day(self):
        assert is_holiday(date(2024, 9, 2))

    def test_thanksgiving(self):
        assert is_holiday(date(2024, 11, 28))

    def test_christmas(self):
        assert is_holiday(date(2024, 12, 25))

    def test_regular_day_not_holiday(self):
        assert not is_holiday(date(2024, 1, 16))  # Tuesday after MLK

    def test_juneteenth_not_before_2022(self):
        assert not is_holiday(date(2021, 6, 19))

    def test_holiday_on_sunday_observed_monday(self):
        # 2023-01-01 is Sunday → observed 2023-01-02 (Monday)
        assert is_holiday(date(2023, 1, 2))

    def test_independence_day_saturday_observed_friday(self):
        # 2020-07-04 is Saturday → observed 2020-07-03
        assert is_holiday(date(2020, 7, 3))


class TestIsTradingDay:
    def test_weekday_no_holiday(self):
        assert is_trading_day(date(2024, 1, 16))

    def test_weekend(self):
        assert not is_trading_day(date(2024, 1, 13))  # Saturday
        assert not is_trading_day(date(2024, 1, 14))  # Sunday

    def test_holiday(self):
        assert not is_trading_day(date(2024, 1, 1))


class TestGetTradingDates:
    def test_one_week(self):
        dates = get_trading_dates(date(2024, 1, 15), date(2024, 1, 19))
        # MLK Day is 2024-01-15 (holiday), so only Tue-Fri = 4 days
        assert len(dates) == 4
        assert date(2024, 1, 15) not in dates

    def test_empty_range(self):
        dates = get_trading_dates(date(2024, 1, 20), date(2024, 1, 19))
        assert dates == []

    def test_weekend_only(self):
        dates = get_trading_dates(date(2024, 1, 13), date(2024, 1, 14))
        assert dates == []


class TestHalfDay:
    def test_black_friday(self):
        assert is_half_day(date(2024, 11, 29))  # Day after Thanksgiving

    def test_christmas_eve_weekday(self):
        assert is_half_day(date(2024, 12, 24))  # Tuesday


class TestMarketHours:
    def test_open_time(self):
        assert market_open_time(date(2024, 1, 16)) == time(9, 30)

    def test_close_time_regular(self):
        assert market_close_time(date(2024, 1, 16)) == time(16, 0)

    def test_close_time_half_day(self):
        assert market_close_time(date(2024, 11, 29)) == time(13, 0)


class TestAdHocClosures:
    """One-off full closures no weekday rule can derive.

    Without these the calendar reports them as ordinary trading days, and a
    consumer diffing expected sessions against actual data sees a whole
    market's worth of missing bars with no reason for it.
    """

    def test_national_day_of_mourning_is_not_a_trading_day(self):
        # 2025-01-09 was a Thursday. The NYSE closed for the full day for
        # President Carter's National Day of Mourning.
        d = date(2025, 1, 9)
        assert d.weekday() < 5, "must be a weekday or this proves nothing"
        assert is_trading_day(d) is False

    def test_ad_hoc_closure_reports_as_a_holiday(self):
        assert is_holiday(date(2025, 1, 9)) is True

    def test_ad_hoc_closure_is_excluded_from_get_trading_dates(self):
        """The entry point most consumers actually use."""
        days = get_trading_dates(date(2025, 1, 6), date(2025, 1, 10))
        assert date(2025, 1, 9) not in days
        # The surrounding weekdays are untouched.
        assert date(2025, 1, 8) in days
        assert date(2025, 1, 10) in days

    def test_only_affects_its_own_year(self):
        """The set is filtered by year, so a closure cannot leak into another."""
        from marketdata.calendar import _nyse_holidays
        assert date(2025, 1, 9) in _nyse_holidays(2025)
        assert len(_nyse_holidays(2026)) == 10, "2026 must keep the 10 rule-derived days"

    def test_every_listed_closure_is_a_weekday(self):
        """A weekend entry would be inert and signals a typo -- the market is
        already closed, so it would never change an answer."""
        for d in AD_HOC_CLOSURES:
            assert d.weekday() < 5, f"{d} is a {d.strftime('%A')}"

    def test_no_listed_closure_duplicates_a_rule_derived_holiday(self):
        """A date already derived by rule does not belong here; listing it
        hides the fact that the rule covers it."""
        from marketdata.calendar import _nyse_holidays
        for d in AD_HOC_CLOSURES:
            rule_derived = _nyse_holidays(d.year) - AD_HOC_CLOSURES
            assert d not in rule_derived, f"{d} is already derived by rule"
