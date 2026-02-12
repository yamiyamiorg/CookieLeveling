from .core import get_connection


def prune_weekly_xp(min_week_key: str) -> None:
    conn = get_connection()
    conn.execute(
        "DELETE FROM user_weekly_xp WHERE week_key < ?",
        (min_week_key,),
    )
    conn.execute(
        "DELETE FROM host_weekly_xp WHERE week_key < ?",
        (min_week_key,),
    )
    conn.commit()
