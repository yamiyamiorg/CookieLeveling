from .config import Config

GRANT_XP_LIMIT = 10_000
SET_XP_LIMIT = 1_000_000


def ensure_debug_mutations(config: Config) -> str | None:
    if not config.debug_mutations:
        return "DEBUG_MUTATIONS=1が必要"
    return None


def validate_grant_xp(season: int, lifetime: int) -> str | None:
    if season > GRANT_XP_LIMIT or lifetime > GRANT_XP_LIMIT:
        return "上限超過"
    return None


def validate_set_xp(
    season: int, lifetime: int, rem_lifetime: float | None
) -> str | None:
    if season > SET_XP_LIMIT or lifetime > SET_XP_LIMIT:
        return "上限超過"
    if rem_lifetime is not None and not (0 <= rem_lifetime < 1):
        return "rem_lifetime範囲外"
    return None
