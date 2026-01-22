# CookieLeveling

Discord leveling bot for a single guild.

## Requirements

- Docker + Docker Compose

## Setup

1. Copy `.env.example` to `.env` and fill in required values.
2. Build and run:

```bash
docker compose up --build
```

## Notes

- Database file is stored at `./data/cookieleveling.sqlite` via volume mount.
- Logs are written to stdout.

## Debug commands (admin only)

- 破壊的コマンドは `DEBUG_MUTATIONS=1` が必要。
- `/debug grantxp target season lifetime`
- `/debug setxp target season lifetime rem_lifetime`
- `/debug setvc target in_vc`
- `/debug tick minute`
- `/debug tick rankboard`
- `/debug tick roles`
