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
- (Optional) `joined_at` を過去に調整して、Lifetime XPが 1.0/0.5/0.25 の係数で増えることを確認する。

## Phase4
- Ensure `/debug top10` returns stable ordered results.
- Verify tie-break order when XP is equal.

## Phase5
- Run `/rankboard set` and confirm message placement.
- Run `/rankboard set` again in another channel and confirm the old message is edited with a move notice.
- Run `/debug tick rankboard` (DEBUG_MUTATIONS=1) to update attachments.

## Phase6
- Run `/debug tick roles` (DEBUG_MUTATIONS=1) and verify role changes.
- Confirm role snapshot table remains one row per guild.

## Phase7
- Wait for month boundary (or simulate) and confirm season XP resets.

## Debug destructive commands
- 破壊的コマンドは `DEBUG_MUTATIONS=1` が必要。
- `/debug grantxp target season lifetime` (max: 10000)
- `/debug setxp target season lifetime rem_lifetime` (max: 1000000, rem_lifetime=0<=x<1)
- `/debug setvc target in_vc`
- `/debug tick minute`
- `/debug tick rankboard` (未設置なら未設置が返る)
- `/debug tick roles` (ROLE_*未設定なら理由が返る)
