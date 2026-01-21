from .config import Config


def ensure_debug_mutations(config: Config) -> bool:
    return config.debug_mutations
