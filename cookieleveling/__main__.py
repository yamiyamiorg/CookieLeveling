import logging

from .bot import CookieLevelingBot
from .config import load_config


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    config = load_config()
    bot = CookieLevelingBot(config)
    bot.run(config.discord_token)


if __name__ == "__main__":
    main()
