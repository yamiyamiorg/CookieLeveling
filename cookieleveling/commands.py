import discord
from discord import app_commands

from .config import Config
from .db import get_connection


def setup_commands(bot: discord.Client, config: Config) -> None:
    tree = bot.tree

    debug_group = app_commands.Group(name="debug", description="Debug commands")

    @debug_group.command(name="status", description="Show bot status")
    @app_commands.checks.has_permissions(administrator=True)
    async def debug_status(interaction: discord.Interaction) -> None:
        conn = get_connection()
        row = conn.execute("SELECT schema_version FROM meta LIMIT 1").fetchone()
        schema_version = row["schema_version"] if row else "unknown"
        await interaction.response.send_message(
            f"OK. schema_version={schema_version}", ephemeral=True
        )

    tree.add_command(debug_group, guild=discord.Object(id=config.guild_id))
