---
name: mine
description: >
  Mine data and earn $aMine rewards on the Mine Worknet. This skill manages
  autonomous miners and validators — use it whenever the user mentions mining,
  validating, aMine, ocDATA, Mine Worknet, or earning crypto rewards by crawling
  data. Covers: start/stop miner or validator, check status/scores/earnings,
  select datasets, handle PoW challenges, diagnose errors (401, auth, stake),
  view epoch progress, and validator staking guidance. Miners need NO staking —
  just register and start. Trigger for casual phrases too: "go
  online", "start earning", "check my submissions", "why is my miner stuck",
  "how much have I earned", "validator not working". NOT for AWP transfers,
  RootNet staking, smart contracts, or general server ops.
version: 0.17.3
bootstrap: ./scripts/bootstrap.sh
windows_bootstrap: ./scripts/bootstrap.cmd
smoke_test: ./scripts/smoke_test.py
requires:
  bins:
    - npm
    - git
  anyBins:
    - python
    - python3
    - py
metadata:
  emoji: "⛏️"
  homepage: https://github.com/data4agent/mine-skill
---

# Mine

Autonomous data mining & validation on AWP. Agents earn $aMine rewards by
crawling public data and validating others' submissions.

## Agent-First Design

This skill is designed for **agent execution** — all commands must be run by you
(the agent) in the terminal. **Never show command text to the user.** Instead,
execute commands yourself and present the results as natural-language summaries.

Every command outputs JSON with:
- `user_message` — show this to the user (natural language)
- `user_actions` — action options to present (natural language labels)
- `_internal` — for your eyes only, contains `action_map` mapping labels to commands

**Workflow**: Run command → parse JSON → show `user_message` to user → if user
picks an action from `user_actions`, look up the command in `_internal.action_map`
and execute it.

## Rules

1. **Execute commands yourself.** Never display `python scripts/run_tool.py ...` to
   the user. Run the command, read the JSON output, and speak to the user in natural
   language based on `user_message`.

2. **Only use `run_tool.py` commands.** Never make HTTP requests (curl, fetch, httpx,
   requests, WebFetch) to the platform. Never construct JSON-RPC payloads. The platform
   requires cryptographic signatures — raw HTTP calls always fail with 401.

3. **Never expose secrets.** Do not print `AWP_WALLET_TOKEN`, `VALIDATOR_PRIVATE_KEY`,
   private keys, mnemonics, or `.env` contents. To check if set: `[ -n "$VAR" ] && echo "set"`.

4. **Use `_internal` for next steps.** When the JSON output contains `_internal.action_map`,
   use it to determine which command to run next. Never show `_internal` content to the user.

5. **NEVER say the miner is "waiting for the platform to assign tasks".** This phrase
   is factually wrong and is the most common hallucination. Miners are **self-driven**:
   they pull random seed URLs from dataset `source_domains` (Wikipedia MediaWiki Random
   API, arXiv random listings, Amazon random bestseller categories, etc.), crawl them
   directly, and submit the results. The platform never pushes a work queue to miners.
   If a miner shows `processed=0`, it means Discovery hasn't produced a followup yet
   — NOT that it is waiting for an assignment. See "Task Acquisition Model" below.

## Task Acquisition Model (CRITICAL — read before talking about tasks)

Miner and Validator acquire work in **fundamentally different ways**. Mixing up
these models is the #1 source of user-facing hallucinations. Internalize this
table before you say anything about "tasks" to the user:

| Role | How work is acquired | Is this push or pull? | What does "no task" mean? |
|------|---------------------|------------------------|---------------------------|
| **Miner** | Self-generates work by pulling random seeds from `dataset.source_domains` (Discovery), plus opportunistic Backend Claim for repeat-crawl. | **Pull**. Miner drives its own iteration loop. | Discovery cooldown ended without producing a followup URL this iteration, OR dataset backlog is empty. The miner will try again next iteration. **It is NOT idle waiting for an assignment.** |
| **Validator** | Platform pushes evaluation tasks via WebSocket once the validator is in the ready pool. | **Push**. Validator blocks on WS until the platform sends a task. | Genuinely no task pushed yet — the validator IS waiting, and this is expected behavior for a new validator or a quiet period. |

**Consequences for how you talk to the user:**

- For a miner with `processed=0 submitted=0`: say *"the miner is running and pulling
  Discovery seeds — no submissions yet this session"*. **Never** say *"waiting for the
  platform to assign a task"*, *"waiting for tasks"*, *"task queue is empty"*, or
  *"platform has not sent any work"*. All four phrasings are wrong for a miner.
- For a validator with `tasks_received=0`: saying *"waiting for the platform to push
  evaluation tasks"* **is correct**. Validators genuinely wait.

If the user asks "why is my miner stuck / why no tasks", the correct answer is one
of: (a) Discovery is in cooldown for the selected dataset, (b) the dataset `source_domains`
exhausted its random-seed budget for this iteration, (c) bootstrap never ran so the
worker can't import its deps, (d) submission gate is blocking on unanswered PoW. Run
`doctor` and check `agent-control status` — **don't invent a "task queue" explanation**.

## Welcome Screen

On first launch (no worker running), show this and **ask the user to choose a role**:

```text
mine - autonomous data mining

crawl data. earn rewards. fully autonomous.

-- choose your role ----------------
1. Miner      - crawl public data, earn $aMine
2. Validator  - evaluate submissions, earn $aMine
------------------------------------

which role? (1 or 2)
```

**Do NOT skip this step.** The user must choose before any worker starts.

- "mine", "miner", "start mining", "1" -> **Start Mining**
- "validate", "validator", "start validating", "2" -> **Start Validator**
- If unclear, ask again

## Mining Architecture

### Task Sources

Each worker iteration (`run_iteration`) collects tasks from three independent sources:

| Source | Class | Where tasks come from | Filtered by `selected_dataset_ids` |
|--------|-------|----------------------|-----------------------------------|
| **Backend Claim** | `BackendClaimSource` | Platform claim API (repeat-crawl / refresh) | No |
| **Dataset Discovery** | `DatasetDiscoverySource` | Locally generated seed URLs from dataset `source_domains` | Yes |
| **Resume** | `ResumeQueueSource` | Backlog / auth_pending from previously failed or paused tasks | No |

All three sources are **collected in parallel, merged, and deduplicated**. Up to `max_parallel` items enter the current iteration.

> **"no task available" means none of the three sources produced an executable task** — most
> commonly because Backend Claim returned nothing and Discovery is in cooldown. This does
> **not** mean your miner is banned.

### Two-Phase Discovery Crawl

Dataset Discovery operates in **two phases**:

1. **discover-crawl** (discovery phase): crawl seed pages (e.g. arXiv listing pages, Amazon
   bestseller pages), extract links, and enqueue `discovery_followup` tasks into the backlog.
2. **run** (fetch phase): followup tasks are executed in subsequent iterations with the `run`
   command, fetching structured data and submitting to the platform.

Wikipedia is special: it calls the MediaWiki Random API for random article URLs directly,
skipping the discover-crawl phase entirely.

### Mining Iteration Loop

Each iteration follows this sequence:

```text
1. POST /api/mining/v1/heartbeat         <- refresh online status + credit info
2. Collect work items (discovery URLs, backend claims, backlog)
3. For each URL:
   a. GET /api/core/v1/url/check         <- MUST check URL occupancy BEFORE crawling
      If occupied=true: skip this URL
   b. Crawl the page (API/HTTP backend)
   c. POST /api/core/v1/dedup-occupancies/check  <- hash dedup before submit
   d. GET /api/mining/v1/miners/me/submission-gate  <- check PoW BEFORE each submit
      If state="checking": answer PoW challenge first
      POST /api/mining/v1/pow-challenges/{id}/answer
   e. POST /api/mining/v1/submissions    <- submit structured data
      If still challenge_required: answer and resubmit
4. For repeat_crawl tasks:
   - Only report cleaned_data (no structured data submission needed)
   - POST /api/mining/v1/repeat-crawl-tasks/{id}/report
```

**Key rules:**
- Always check URL occupancy BEFORE crawling (step 3a)
- Always check submission gate BEFORE each submission (step 3d) —
  novice miners have 100% PoW probability
- Submission failures with conflicts (dedup, url_pattern_mismatch) are
  discarded, NOT re-queued. Only transient errors (5xx, timeout) are retried
- repeat_crawl tasks only report cleaned_data — no structured data submission
- Discovery and refresh paths submit via `POST /api/mining/v1/submissions`

### Dataset Selection

- Platform returns only 1 dataset — auto-selected.
- Platform returns multiple datasets with none selected — enters `selection_required`; user must choose before starting.
- `selected_dataset_ids` only filters **Discovery / followup** source tasks; Backend Claim tasks are not affected.

### Credit Tier & Limits

| Tier | `credit_score` | Backend Claim | Discovery Submissions |
|------|---------------|---------------|----------------------|
| novice | 0 | Platform may not assign tasks | Normal submission, but epoch settlement gate applies |
| higher | > 0 | Normal assignment | Normal |

Epoch settlement gate: `task_count >= 80` and `avg_score >= 60` (see protocol v2.0).
A novice miner's primary path is through **Discovery self-crawling** to accumulate submissions and scores.

## Miner Workflow

### Participation conditions (STRICT)

To become a miner, the user needs exactly these things — nothing more:

1. **A registered on-chain wallet address.** Registration is performed via the
   AWP Skill (on-chain call). If not registered, `agent-start` returns
   `address_not_registered` and the fix is to install/use the AWP Skill.
2. **A working internet connection.**
3. **The bootstrap script to have run at least once** on this machine (installs
   the Python virtualenv + dependencies + Playwright chromium + awp-wallet).

**Things the miner explicitly does NOT need** — never tell the user otherwise:

- ❌ NO staking of AWP or any other token.
- ❌ NO application, approval, whitelist, KYC, or pending review.
- ❌ NO allocation of tokens to the Mine Worknet.
- ❌ NO task assignment from the platform — the miner pulls work itself.
- ❌ NO queueing or waiting period before it can start earning.

Validator staking (10,000 AWP) is a **validator-only** requirement. If a user
asks about staking while wanting to mine, correct them: miners don't stake.

### Start Mining — exact command sequence

Execute these steps in order. **Do not skip Step 0 even once per machine.**

**Step 0 — Bootstrap (once per machine):**

```bash
cd {baseDir} && bash scripts/bootstrap.sh
```

This creates `.venv/`, installs `requirements-core.txt` +
`requirements-browser.txt`, installs `awp-wallet` from GitHub, and downloads
Playwright chromium. If you skip this, every subsequent command dies with
`ModuleNotFoundError`. After the first successful run the script is a no-op;
always run it again on a fresh machine or a fresh clone.

**Step 1 — Readiness check:**

```bash
cd {baseDir} && python scripts/run_tool.py agent-status
```

Parse the JSON. If `ready=false`, execute `_internal.action_map[<label>]` or
`_internal.next_command` to fix the blocker (usually AWP registration or
bootstrap). Do NOT proceed to Step 2 until `ready=true`.

**Step 2 — Start the self-driven mining loop:**

```bash
cd {baseDir} && python scripts/run_tool.py agent-start
```

If the JSON returns `state=selection_required`, present the dataset names
from `user_message` to the user, wait for their choice, then re-run with
the selected id:

```bash
cd {baseDir} && python scripts/run_tool.py agent-start <datasetId>
```

**Step 3 — Confirm & poll.** From this moment the background worker is already
pulling Discovery seeds and crawling. Tell the user mining is active and ask
them to say "status" when they want an update. Do not poll aggressively.

**What happens AFTER `agent-start` succeeds** (internalize this so you don't
hallucinate a waiting state):

```
loop forever:
    1. heartbeat                             — refresh online status
    2. Discovery: pull random seeds from dataset.source_domains
       (Wikipedia MediaWiki Random, arXiv random offset, Amazon random
        bestseller category, etc.)
    3. Backend Claim: opportunistically pick up repeat-crawl tasks
    4. Resume: re-attempt previously failed / auth_pending URLs
    5. For each URL: check occupancy → crawl → dedup → PoW gate → submit
    6. sleep briefly, go to 1
```

Nothing in this loop blocks on platform push. The miner is **always doing
something** as long as the process is running.

### Check Status

Run in terminal and show `user_message` to user:

```bash
cd {baseDir} && python scripts/run_tool.py agent-control status
```

### Stop / Pause / Resume

Run the appropriate command based on user intent:

```bash
cd {baseDir} && python scripts/run_tool.py agent-control stop
cd {baseDir} && python scripts/run_tool.py agent-control pause
cd {baseDir} && python scripts/run_tool.py agent-control resume
```

### List Datasets

```bash
cd {baseDir} && python scripts/run_tool.py list-datasets
```

### Diagnose

```bash
cd {baseDir} && python scripts/run_tool.py doctor
```

## Validator Workflow

### Participation conditions (STRICT)

To become a validator, the user needs exactly these things:

1. **A registered on-chain wallet address** (same AWP registration as a miner).
2. **10,000 AWP staked on the Mine Worknet** — the minimum stake. This can be
   either:
   - **Option A (agent stakes):** the agent stakes its own AWP and allocates it
     to the Mine Worknet via the AWP Skill, OR
   - **Option B (user delegates):** the user stakes AWP themselves and delegates
     it to the agent on the Mine Worknet.
3. **A working LLM backend** — openclaw CLI in PATH OR `MINE_GATEWAY_TOKEN` set.
   The evaluation engine routes through `llm_enrich` (CLI → gateway → API).
   Without any path reachable, validator-start refuses to run.
4. **The bootstrap script to have run at least once** on this machine.

**Things the validator does NOT need** — do not tell the user otherwise:

- ❌ NO manual approval or whitelist review. Meeting the stake requirement
  auto-approves the application.
- ❌ NO "pending review" state. A 403 from `submit_validator_application`
  means insufficient stake, not a review queue.

**Unlike miners, validators DO wait.** Once the validator is in the ready pool,
it blocks on a WebSocket and waits for the platform to push evaluation tasks.
Saying *"validator is waiting for the platform to push tasks"* is **correct**
for a validator. (Saying the same thing about a miner is wrong — see "Task
Acquisition Model" above.)

### Validator WebSocket message types

The platform pushes these message types via `/api/mining/v1/ws`:

| Type | When | What the validator does |
|------|------|------------------------|
| `evaluation_task` | New task assigned (task_id only) | HTTP POST /evaluation-tasks/claim → get assignment_id + data → evaluate → report |
| `cooldown` | After task completion | Sleep `retry_after_seconds` before accepting next task |
| `error` | Claim/ack/reject failure | Log the error; if `code=validator_cooldown`, sleep `retry_after_seconds` |

If the validator falls back to HTTP polling (`POST /api/mining/v1/evaluation-tasks/claim`):
- 200 = task claimed successfully with assignment_id + full data
- 404 = no task available (normal)
- 409 `validator_cooldown` = cooldown active; response includes `retry_after_seconds`
- 428 `pow_required` = PoW challenge; validator must solve a logic puzzle via LLM
  and POST answer to `/api/mining/v1/pow-challenges/{id}/answer`. After passing,
  retry claim immediately. After failing, next claim returns 409 cooldown.

### Start Validating — exact command sequence

**Step 0 — Bootstrap (once per machine):**

```bash
cd {baseDir} && bash scripts/bootstrap.sh
```

Same mandatory bootstrap as the miner flow. Never skip.

**Step 1 — Start the validator:**

```bash
cd {baseDir} && python scripts/run_tool.py validator-start
```

This submits the validator application (auto-approved if stake ≥ 10,000 AWP
on the Mine Worknet), verifies the LLM backend is reachable, and connects the
WebSocket client into the ready pool.

If the command returns:
- `state=no_llm_backend` → see "Error Recovery" (install openclaw CLI or set
  `MINE_GATEWAY_TOKEN`).
- 403 / `insufficient_stake` → tell the user to stake 10,000 AWP via the AWP
  Skill (Option A or B above), then retry.
- `pending_review` → this should never happen under the current protocol. If
  it does, run `validator-doctor` and surface the real reason.

**Stake must remain allocated for the entire duration of validation.** If the
user withdraws stake mid-session, the validator will be evicted from the ready
pool and stop receiving tasks.

**Rewards are NOT affected by who staked.** Whether the agent self-stakes or
the user delegates, all rewards go to the agent's designated reward address
— same as miner rewards.

### Terminology: match vs mismatch

When reporting validator status to the user, use the correct terminology:

- **match** — the validator judged that the miner's data is consistent with the
  re-crawled version. A score (0-100) reflects data quality.
- **mismatch** — the validator judged that the miner's data is NOT consistent
  with the re-crawl (e.g. fabricated, stale, or significantly different content).

**Both verdicts are submitted to the platform as valid evaluations.** A mismatch
is NOT a rejection of the validator's work — it is the validator correctly
flagging bad miner data. Do NOT describe mismatch as "rejected by the platform"
or "submission failed". The correct framing is: "the validator evaluated X tasks
and reported them to the platform (Y match, Z mismatch)."

### Check Status / Stop

```bash
cd {baseDir} && python scripts/run_tool.py validator-control status
cd {baseDir} && python scripts/run_tool.py validator-control stop
```

### Diagnose

```bash
cd {baseDir} && python scripts/run_tool.py validator-doctor
```

## Debugging Background Workers

Background mining/validation workers write all output (including errors) to log files.
The `agent-control status` command automatically surfaces recent errors from the log.
If you need more detail, the log path is in the `_internal.log_path` field of the status response:

```bash
cd {baseDir} && tail -50 output/agent-runs/<session_id>.log
```

Always check `agent-control status` first — it shows recent errors without needing to read the log directly.

## Error Recovery

If any command fails with `ModuleNotFoundError` or missing package errors:

1. Run `bash scripts/bootstrap.sh` to install all dependencies
2. This MUST be done before any other command will work
3. It only needs to run once — subsequent launches reuse the virtualenv

If any command returns a `401` or authentication error:

1. Run `python scripts/run_tool.py doctor` to diagnose
2. Follow the fix instructions in the output
3. Common causes: expired wallet session, missing AWP registration

If the error is `address_not_registered` or `registration_required`:

1. The wallet needs to be registered on-chain before mining can start
2. Tell the user to **install and use the AWP Skill** to complete registration
3. If the AWP Skill is not installed, guide the user to install it first
4. After registration completes, retry `python scripts/run_tool.py agent-start`

**Do NOT** tell users to register on a website or manually call any registration API.
The AWP Skill handles the entire on-chain registration flow automatically.

If the validator returns `403`, `permission denied`, or `insufficient_stake`:

1. **Validator requires a minimum of 10,000 AWP staked on the Mine Worknet.**
   The minimum may increase as more validators join. Meeting the stake
   requirement is the only condition — no manual approval or review needed.
2. There are two ways to meet this requirement:
   - **Option A (agent stakes):** The agent stakes its own AWP and allocates
     the stake to the Mine Worknet. Use the AWP Skill to do this.
   - **Option B (user delegates):** The user stakes AWP themselves and
     delegates the stake to the agent on the Mine Worknet.
3. **Stake must remain allocated for the entire duration of validation.**
   If stake is withdrawn or falls below the minimum, the validator will be
   evicted from the ready pool.
4. Staking is only a participation requirement — **rewards are NOT affected by
   who staked**. All mining/validation rewards go to the agent's designated
   reward address, same as miner rewards.
5. After staking completes, retry `python scripts/run_tool.py validator-start`

**Do NOT** suggest the user is "pending review" or needs manual approval when the
error is 403 — it means insufficient stake, not a review issue. Anyone who meets
the stake requirement can become a validator immediately.

If `validator-doctor` or `validator-start` reports `no_llm_backend` / "no LLM
backend available":

1. The validator's evaluation engine routes LLM calls through (in order):
   **OpenClaw CLI → OpenClaw gateway → OpenAI-compatible API**. At least one
   path must be reachable, otherwise every evaluation task fails.
2. Two ways to fix it — pick whichever matches the host environment:
   - **Option A — install the OpenClaw CLI** so that `which openclaw` succeeds.
     This is the preferred option when the skill runs inside an OpenClaw host.
   - **Option B — configure a gateway / API fallback** by exporting
     `MINE_GATEWAY_TOKEN` (and optionally `MINE_GATEWAY_BASE_URL` /
     `MINE_GATEWAY_MODEL` if the defaults don't match). This lets the validator
     run on any host without the CLI.
3. Re-run `python scripts/run_tool.py validator-doctor` to confirm
   `llm_backend.ok = true` before retrying `validator-start`.

Do **not** try to work around this by editing evaluation prompts or disabling
the LLM call — the validator protocol requires an LLM-scored verdict for every
task, so a working backend is mandatory.

If you see `missing_auth_headers` or `signer_mismatch`, it means something
bypassed `run_tool.py`. Stop and use the commands listed above instead.

**Never attempt to fix auth by making HTTP calls, adding headers, or reading
signing code.** The `doctor` command handles all auth diagnostics.

## Intent Routing

| User says | Action to take |
|-----------|---------------|
| "start" / "go online" | Run `agent-start` or `validator-start` (depends on role) |
| "status" / "how am I doing" | Run `agent-control status` or `validator-control status` |
| "stop" | Run `agent-control stop` or `validator-control stop` |
| "pause" | Run `agent-control pause` (miner only) |
| "resume" | Run `agent-control resume` (miner only) |
| "datasets" / "what can I mine" | Run `list-datasets` |
| "diagnose" / "doctor" / "fix" | Run `doctor` or `validator-doctor` |
| "help" | Tell the user what actions are available in natural language |
| "switch role" | Re-show Welcome Screen |
| "check connectivity" / "heartbeat" | Run `doctor` (never direct HTTP) |
| "401 error" / "auth error" | Run `doctor` (see Error Recovery) |

## Sub-Agent Guidelines

- **One mining worker per session** — do not spawn multiple concurrent miners
- Use `agent-control status` to poll progress
- Use `agent-control stop` to terminate
- All platform interaction goes through `run_tool.py` — this applies to sub-agents too

## Configuration

No environment variables needed. Everything is auto-detected.

Runtime overrides (optional, via `.env` or shell):

| Variable | Default | Description |
|----------|---------|-------------|
| `PLATFORM_BASE_URL` | `https://api.minework.net` | Platform API endpoint |
| `MINER_ID` | `mine-agent` | Miner identifier |
| `WORKER_MAX_PARALLEL` | `3` | Concurrent crawl workers |

For validator settings, see `docs/ENVIRONMENT.md`.

## Advanced

Read these docs only when needed for the specific topic:

- [Browser session & login](./docs/BROWSER_SESSION.md)
- [Internal commands & rules](./docs/INTERNAL_COMMANDS.md)
- [Agent guide](./docs/AGENT_GUIDE.md)
- [Environment](./docs/ENVIRONMENT.md)
- [Validator Protocol](./references/protocol-validator.md)
