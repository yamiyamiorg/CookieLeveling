import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    discord_token: str
    discord_client_id: str
    guild_id: int
    tz: str
    data_dir: str
    db_path: str
    role_season_1: int | None
    role_season_2: int | None
    role_season_3: int | None
    role_season_4: int | None
    role_season_5: int | None
    role_season_top10: int | None


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def load_config() -> Config:
    discord_token = _get_required_env("DISCORD_TOKEN")
    discord_client_id = _get_required_env("DISCORD_CLIENT_ID")
    guild_id = int(_get_required_env("GUILD_ID"))
    tz = os.getenv("TZ", "Asia/Tokyo")
    data_dir = os.getenv("DATA_DIR", "/opt/CookieLeveling/data")
    db_path = os.getenv("DB_PATH", "/opt/CookieLeveling/data/cookieleveling.sqlite")
    role_season_1 = _get_optional_int("ROLE_SEASON_1")
    role_season_2 = _get_optional_int("ROLE_SEASON_2")
    role_season_3 = _get_optional_int("ROLE_SEASON_3")
    role_season_4 = _get_optional_int("ROLE_SEASON_4")
    role_season_5 = _get_optional_int("ROLE_SEASON_5")
    role_season_top10 = _get_optional_int("ROLE_SEASON_TOP10")
    return Config(
        discord_token=discord_token,
        discord_client_id=discord_client_id,
        guild_id=guild_id,
        tz=tz,
        data_dir=data_dir,
        db_path=db_path,
        role_season_1=role_season_1,
        role_season_2=role_season_2,
        role_season_3=role_season_3,
        role_season_4=role_season_4,
        role_season_5=role_season_5,
        role_season_top10=role_season_top10,
    )


def _get_optional_int(name: str) -> int | None:
    value = os.getenv(name)
    if not value:
        return None
    return int(value)
