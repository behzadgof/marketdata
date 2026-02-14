"""NYSE trading calendar — holidays, half days, market hours.

No external dependencies — uses hardcoded holiday rules for NYSE.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

# US Eastern timezone offset helper (simplified: no DST logic, use
# dateutil/zoneinfo if available, else fixed -5)
try:
    from zoneinfo import ZoneInfo
    _ET = ZoneInfo("America/New_York")
except ImportError:
    try:
        from dateutil.tz import gettz
        _ET = gettz("America/New_York")  # type: ignore[assignment]
    except ImportError:
        from datetime import timezone as _tz
        _ET = _tz(timedelta(hours=-5))  # type: ignore[assignment]


# ---- Fixed-date holidays ----

def _new_years(year: int) -> date:
    d = date(year, 1, 1)
    if d.weekday() == 6:  # Sunday → observed Monday
        return date(year, 1, 2)
    return d


def _juneteenth(year: int) -> date:
    """Juneteenth — observed since 2022."""
    if year < 2022:
        return date(1, 1, 1)  # sentinel, won't match
    d = date(year, 6, 19)
    if d.weekday() == 5:
        return date(year, 6, 18)
    if d.weekday() == 6:
        return date(year, 6, 20)
    return d


def _independence_day(year: int) -> date:
    d = date(year, 7, 4)
    if d.weekday() == 5:
        return date(year, 7, 3)
    if d.weekday() == 6:
        return date(year, 7, 5)
    return d


def _christmas(year: int) -> date:
    d = date(year, 12, 25)
    if d.weekday() == 5:
        return date(year, 12, 24)
    if d.weekday() == 6:
        return date(year, 12, 26)
    return d


# ---- Rule-based holidays (Nth weekday of month) ----

def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """Get the nth occurrence of a weekday in a month (1-indexed)."""
    first = date(year, month, 1)
    # Days until the first occurrence of `weekday`
    delta = (weekday - first.weekday()) % 7
    first_occ = first + timedelta(days=delta)
    return first_occ + timedelta(weeks=n - 1)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    """Get the last occurrence of a weekday in a month."""
    if month == 12:
        last_day = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    delta = (last_day.weekday() - weekday) % 7
    return last_day - timedelta(days=delta)


def _mlk_day(year: int) -> date:
    """3rd Monday of January."""
    return _nth_weekday(year, 1, 0, 3)


def _presidents_day(year: int) -> date:
    """3rd Monday of February."""
    return _nth_weekday(year, 2, 0, 3)


def _good_friday(year: int) -> date:
    """Good Friday (anonymous algorithm for Easter)."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7  # noqa: E741
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    easter = date(year, month, day)
    return easter - timedelta(days=2)


def _memorial_day(year: int) -> date:
    """Last Monday of May."""
    return _last_weekday(year, 5, 0)


def _labor_day(year: int) -> date:
    """1st Monday of September."""
    return _nth_weekday(year, 9, 0, 1)


def _thanksgiving(year: int) -> date:
    """4th Thursday of November."""
    return _nth_weekday(year, 11, 3, 4)


def _nyse_holidays(year: int) -> set[date]:
    """All NYSE holidays for a given year."""
    holidays = {
        _new_years(year),
        _mlk_day(year),
        _presidents_day(year),
        _good_friday(year),
        _memorial_day(year),
        _juneteenth(year),
        _independence_day(year),
        _labor_day(year),
        _thanksgiving(year),
        _christmas(year),
    }
    # Remove sentinel dates
    holidays.discard(date(1, 1, 1))
    return holidays


# ---- Half days ----

def _nyse_half_days(year: int) -> set[date]:
    """NYSE early close days (1:00 PM ET)."""
    half_days: set[date] = set()

    # Day before Independence Day (if weekday)
    jul4_obs = _independence_day(year)
    day_before = jul4_obs - timedelta(days=1)
    if day_before.weekday() < 5:
        half_days.add(day_before)

    # Black Friday (day after Thanksgiving)
    bf = _thanksgiving(year) + timedelta(days=1)
    half_days.add(bf)

    # Christmas Eve (if weekday and not a holiday)
    dec24 = date(year, 12, 24)
    if dec24.weekday() < 5 and dec24 not in _nyse_holidays(year):
        half_days.add(dec24)

    return half_days


# ---- Public API ----

def is_holiday(d: date) -> bool:
    """Check if a date is an NYSE holiday."""
    return d in _nyse_holidays(d.year)


def is_half_day(d: date) -> bool:
    """Check if a date is an NYSE early-close day."""
    return d in _nyse_half_days(d.year)


def is_trading_day(d: date) -> bool:
    """Check if a date is a trading day (weekday and not a holiday)."""
    return d.weekday() < 5 and not is_holiday(d)


def get_trading_dates(start: date, end: date) -> list[date]:
    """Return all trading dates in the range [start, end]."""
    dates: list[date] = []
    current = start
    while current <= end:
        if is_trading_day(current):
            dates.append(current)
        current += timedelta(days=1)
    return dates


def market_open_time(d: date) -> time:
    """Regular market open time (always 9:30 ET)."""
    return time(9, 30)


def market_close_time(d: date) -> time:
    """Market close time — 13:00 ET on half days, 16:00 ET otherwise."""
    if is_half_day(d):
        return time(13, 0)
    return time(16, 0)


def is_market_open(dt: datetime | None = None) -> bool:
    """Check if the market is currently open.

    Args:
        dt: Datetime to check. Defaults to now (US Eastern).
    """
    if dt is None:
        dt = datetime.now(_ET)
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=_ET)
    else:
        dt = dt.astimezone(_ET)

    d = dt.date()
    if not is_trading_day(d):
        return False

    t = dt.time()
    return market_open_time(d) <= t < market_close_time(d)


def next_market_open(from_dt: datetime | None = None) -> datetime:
    """Get the next market open datetime (US Eastern)."""
    if from_dt is None:
        from_dt = datetime.now(_ET)
    elif from_dt.tzinfo is None:
        from_dt = from_dt.replace(tzinfo=_ET)
    else:
        from_dt = from_dt.astimezone(_ET)

    d = from_dt.date()
    t = from_dt.time()

    # If before open today, return today's open
    if is_trading_day(d) and t < market_open_time(d):
        return datetime.combine(d, market_open_time(d), tzinfo=_ET)

    # Otherwise, find next trading day
    d += timedelta(days=1)
    while not is_trading_day(d):
        d += timedelta(days=1)
    return datetime.combine(d, market_open_time(d), tzinfo=_ET)
