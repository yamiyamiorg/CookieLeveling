# Test Plan

## Phase1
- Start with Docker and confirm "ready" log line appears.
- Run `/debug status` and confirm ephemeral response.
- Confirm SQLite file exists at `data/cookieleveling.sqlite` and stays small.

## Phase2
- Join/leave a voice channel and verify DB updates via `/debug vc`.
- Restart bot and confirm voice state is restored.

## Phase3
- Stay in VC for at least 2 minutes and confirm XP increments.
- Use `/optout` and verify XP stops; `/optin` resumes.
- Verify `/debug user` reflects expected XP changes.

## Phase4
- Ensure `/debug top10` returns stable ordered results.
- Verify tie-break order when XP is equal.

## Phase5
- Run `/rankboard set` and confirm message placement.
- Run `/debug tick rankboard` (DEBUG_MUTATIONS=1) to update attachments.

## Phase6
- Run `/debug tick roles` (DEBUG_MUTATIONS=1) and verify role changes.
- Confirm role snapshot table remains one row per guild.

## Phase7
- Wait for month boundary (or simulate) and confirm season XP resets.
