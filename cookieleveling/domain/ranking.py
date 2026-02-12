from cookieleveling.db import (
    fetch_lifetime_candidates,
    fetch_rank_candidates,
    fetch_weekly_candidates,
)


def compute_top20(guild_id: int, *, limit: int = 20) -> list[dict]:
    return [dict(row) for row in fetch_rank_candidates(guild_id, limit=limit)]


def compute_lifetime_top20(guild_id: int, *, limit: int = 20) -> list[dict]:
    return [dict(row) for row in fetch_lifetime_candidates(guild_id, limit=limit)]


def compute_weekly_top20(guild_id: int, *, limit: int = 20) -> list[dict]:
    return [dict(row) for row in fetch_weekly_candidates(guild_id, limit=limit)]
