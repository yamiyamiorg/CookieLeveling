import os
from dataclasses import dataclass

EXCLUDED_USER_IDS: frozenset[int] = frozenset(
    {
        1216428921461936130,
        726187836931309629,
        1457749158822940749,
        1170336962599735327,
        1421241362564513946,
    }
)

@dataclass(frozen=True)
class Config:
    discord_token: str
    discord_client_id: str
    guild_id: int
    tz: str
    data_dir: str
    db_path: str


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
    return Config(
        discord_token=discord_token,
        discord_client_id=discord_client_id,
        guild_id=guild_id,
        tz=tz,
        data_dir=data_dir,
        db_path=db_path,
    )
