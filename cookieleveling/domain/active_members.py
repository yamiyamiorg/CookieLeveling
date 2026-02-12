from __future__ import annotations

from typing import Iterable

import discord


def filter_active_rows(
    guild: discord.Guild, rows: Iterable[dict] | Iterable
) -> list:
    """Return only rows whose user_id is a current guild member.

    Uses guild.get_member only (no fetch) to decide membership.
    """
    active_rows: list = []
    for row in rows:
        user_id = row["user_id"]
        if guild.get_member(user_id) is None:
            continue
        active_rows.append(row)
    return active_rows
