# GitHub Actions Keep-Alive

GitHub disables scheduled workflows when a repository is inactive (no commits **and** no workflow runs) for 60 consecutive days. To keep the data-ingestion workflow alive while development is quiet, run the provided script from a personal machine on the **first Monday of every month at 12:00 PM Mountain Time**.

## Prerequisites

1. Install the GitHub CLI and jq:
   ```bash
   # macOS
   brew install gh jq

   # Debian/Ubuntu
   sudo apt install gh jq
   ```
2. Authenticate with a token that has the `workflow` scope:
   ```bash
   gh auth login --scopes "workflow"
   # If already logged in:
   gh auth refresh --scopes "workflow"
   ```
3. Clone this repository (already done if you are reading the docs locally).

## Script

The script lives at `scripts/trigger_actions_keepalive.sh`. It:

1. Ensures the target workflow is enabled (`spotify-pipeline.yml` by default).
2. Fires a `workflow_dispatch` run against the `main` branch.

If your workflow filename differs, edit the `WORKFLOW_FILE=` line near the top of the script. The script will automatically resolve the corresponding workflow ID if the CLI cannot locate it by name.

## Scheduling with cron

Edit your crontab:

```bash
crontab -e
```

Add the following entry (adjust the paths as needed):

```cron
# Run at 12:00 PM Mountain Time on the first Monday of each month
0 12 1-7 * 1 TZ=America/Denver /path/to/scripts/trigger_actions_keepalive.sh >> /path/to/workflow-trigger.log 2>&1
```

Explanation:

- `0 12` – run at minute 0, hour 12 (noon).
- `1-7` – only on days 1 through 7 of the month.
- `*` – every month.
- `1` – Monday (0=Sunday, 1=Monday).
- `TZ=America/Denver` – pins the time zone to Mountain Time even if the host uses UTC.
- Output is appended to `workflow-trigger.log` so you can audit successful runs.

## Testing

Before relying on cron, run the script manually once:

```bash
cd /path/to/spotify-snowflake-pipeline
./scripts/trigger_actions_keepalive.sh
```

You should see log output and a new run appear under the repository’s **Actions** tab.

## Troubleshooting

- **`gh: command not found`** – Ensure the GitHub CLI is installed and on `PATH`.
- **Authentication errors** – rerun `gh auth login` and confirm the token has `workflow` scope.
- **Workflow not found** – double-check the `WORKFLOW` variable matches the filename under `.github/workflows/`.
- **Crontab didn’t run** – verify the machine was online at the scheduled time. `cron` will not retry missed runs automatically.


