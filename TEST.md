# Test Plan

## Phase1
- Start with Docker and confirm "ready" log line appears.
- Confirm SQLite file exists at `data/cookieleveling.sqlite` and stays small.

## Phase2
- Join/leave a voice channel and verify no errors appear in logs.

## Phase3
- Stay in VC for at least 2 minutes and confirm XP increments.
- Use `/optout` and verify XP stops; `/optin` resumes.
- Confirm lifetime XP increases 1 per minute while in VC.

## Phase4
- Ensure ranking order is stable in the rankboard output (Top20).

## Phase5
- Run `/rankboard set` and confirm two messages are placed (season/lifetime).
- Run `/rankboard set` again in another channel and confirm the old two messages are edited with a move notice.

## Phase6
- Wait for month boundary (or simulate) and confirm season XP resets.

## Regression
- Run `/level` and confirm an ephemeral image response is always returned.
- Toggle `/optout` and confirm the card shows "optout中（XP加算なし）".
- Verify XP bars render at 0/50/100% without layout breakage.
- Confirm no rankboard preview commands exist in the command list.
- Restart the bot and confirm host confirmation status matches pre-restart state.
- Confirm host XP continues when the VC contains only excluded users.
- Confirm users not marked deleted remain visible on rankboards after restarts.
- Restart immediately and confirm rankboard entry count does not drop.
- Restart the bot and confirm rankboard entry count is unchanged (left users only).
- Simulate API fetch failures and confirm no users disappear from rankboards.
- Confirm only users confirmed left (404) are removed from rankboards.
- Rejoin a previously-left user and confirm they return to rankboards after activity.
