# AGENTS.md

## Server Safety

- Treat the production server as stateful. Do not assume it is a clean deploy target.
- Before touching server files, always inspect:
  - `git status --short`
  - running trader processes
  - session files
  - env vars required by current config
- Do not overwrite runtime/state files from local to server:
  - `config.yaml`
  - `*.session`
  - `trader.db*`
  - `.venv/`
  - logs and backups
- Do not use broad `rsync` or copy commands against the server until excludes are verified line by line.
- Before any server-side code sync, make a dated backup of:
  - changed code files
  - `config.yaml`
  - `*.session`
  - `trader.db*` when feasible

## Telegram Session

- `ivan_listener.session` is critical runtime state. Losing or replacing it can break login.
- After any successful Telegram login on server, immediately back up the session file:
  - `mkdir -p /root/Following/backups/session`
  - `cp -f /root/Following/ivan_listener.session /root/Following/backups/session/ivan_listener.session.$(date +%Y%m%dT%H%M%S)`
- If Telethon prints `Please enter your phone (or bot token):`, assume session state is missing or invalid.
- `database is locked` during Telethon startup usually means the SQLite session file is being touched by multiple trader processes at once.

## SQLite / Process Safety

- Only one `python -m trader --config /root/Following/config.yaml` process should run at a time.
- Before restarting, verify the current process count.
- `trader.db` uses WAL mode. `trader.db-wal` and `trader.db-shm` are normal while the process is live.
- Do not delete WAL/SHM files while the trader process is running.
- If SQLite reports `database is locked`, first check for duplicate trader processes before touching DB files.

## Config / Env Vars

- Current server config can require different API keys for LLM and VLM independently.
- If `vlm.enabled=true` and provider is `qwen`, `DASHSCOPE_API_KEY` must exist in the server environment.
- Do not assume OpenAI/DeepSeek keys cover VLM.
- Read `config.yaml` on the server before changing env vars or startup commands.

## TP / SL Logic

- Intended behavior:
  - place TP/SL once after fill
  - record which TP levels have already filled
  - only rearm remaining TP levels
  - never rearm already-filled TP levels
- Filled TP progress is stored in `trade_threads.filled_tp_points_json`.
- When a TP fills, remaining TP levels may need immediate rearm if the exchange cancels sibling TP orders.
- Beware of cooldown windows: a recent TP submit cooldown must not block rearming remaining TP after a real TP fill.
- Remaining risk cases to think through before changing TP logic:
  - restart between exchange TP fill and local reconciliation
  - manual TP changes on an existing thread
  - orders missing `thread_id`
  - exchange plan orders that appear as generic `close`

## Deployment Checklist

- Read-only checks first:
  - repo status
  - active process count
  - latest logs
  - config and env requirements
- Back up session and config.
- Apply the smallest possible change.
- Re-check process count after restart.
- Confirm logs show listener started successfully.
- If the server has local uncommitted code changes, do not overwrite them blindly. Diff and merge intentionally.
