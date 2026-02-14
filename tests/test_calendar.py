"""Tests for trading calendar."""

from datetime import date, datetime, time

from marketdata.calendar import (
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
