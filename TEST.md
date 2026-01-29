# Test Plan

## Phase1
- Start with Docker and confirm "ready" log line appears.
- Confirm SQLite file exists at `data/cookieleveling.sqlite` and stays small.

## Phase2
- Join/leave a voice channel and verify no errors appear in logs.

## Phase3
- Stay in VC for at least 2 minutes and confirm XP increments.
- Use `/optout` and verify XP stops; `/optin` resumes.
- (Optional) `joined_at` を過去に調整して、Lifetime XPが 1.0/0.5/0.25 の係数で増えることを確認する。

## Phase4
- Ensure ranking order is stable in the rankboard output.

## Phase5
- Run `/rankboard set` and confirm message placement.
- Run `/rankboard set` again in another channel and confirm the old message is edited with a move notice.

## Phase6
- Confirm role snapshot table remains one row per guild.

## Phase7
- Wait for month boundary (or simulate) and confirm season XP resets.

## Regression
- Run `/level` and confirm an ephemeral image response is always returned.
- Toggle `/optout` and confirm the card shows "optout中（XP加算なし）".
- Verify XP bars render at 0/50/100% without layout breakage.
- Confirm no rankboard preview commands exist in the command list.
