from cookieleveling.db import (
    fetch_host_top20_monthly,
    fetch_host_top20_total,
    fetch_host_top20_weekly,
)


def compute_host_top20_monthly(guild_id: int, *, limit: int = 20) -> list[dict]:
    return [dict(row) for row in fetch_host_top20_monthly(guild_id, limit=limit)]


def compute_host_top20_total(guild_id: int, *, limit: int = 20) -> list[dict]:
    return [dict(row) for row in fetch_host_top20_total(guild_id, limit=limit)]


def compute_host_top20_weekly(guild_id: int, *, limit: int = 20) -> list[dict]:
    return [dict(row) for row in fetch_host_top20_weekly(guild_id, limit=limit)]
