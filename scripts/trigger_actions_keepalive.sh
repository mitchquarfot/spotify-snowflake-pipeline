#!/usr/bin/env bash
#
# Trigger the GitHub Actions workflow on a schedule to keep it from being
# auto-disabled after 60 days of inactivity. Intended to be executed from a
# machine you control via cron (e.g. first Monday of every month at noon MT).
#
# Requirements:
#   - GitHub CLI installed (`brew install gh` or `sudo apt install gh`)
#   - jq installed (`brew install jq` or `sudo apt install jq`)
#   - `gh auth login` completed with `workflow` scope
#   - Network access at the scheduled time
#
# Usage:
#   gh workflow run must point at your workflow file name (default below).
#   Adjust REPO, WORKFLOW, and REF as needed before scheduling.
#

set -euo pipefail

REPO="mquarfot/spotify-snowflake-pipeline"
WORKFLOW_FILE="spotify-pipeline.yml"
REF="main"

log() {
  printf '[%s] %s\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "$*"
}

resolve_workflow_identifier() {
  local identifier="${WORKFLOW_FILE}"
  if gh workflow view "${identifier}" --repo "${REPO}" >/dev/null 2>&1; then
    echo "${identifier}"
    return 0
  fi

  local workflow_id
  if ! workflow_json=$(gh api "repos/${REPO}/actions/workflows" 2>/dev/null); then
    log "ERROR: Unable to query workflows via GitHub API. Make sure you ran 'gh auth login' with the 'workflow' scope and that the repository exists."
    exit 1
  fi

  workflow_id=$(printf '%s' "${workflow_json}" | jq -r ".workflows[] | select(.path == \".github/workflows/${WORKFLOW_FILE}\") | .id")
  if [[ -n "${workflow_id}" ]]; then
    log "Resolved workflow '${WORKFLOW_FILE}' to ID ${workflow_id}"
    echo "${workflow_id}"
    return 0
  fi

  log "ERROR: Unable to find workflow '${WORKFLOW_FILE}' in ${REPO}."
  log "Verify the filename and that you have access to the repository."
  exit 1
}

WORKFLOW_IDENTIFIER=$(resolve_workflow_identifier)

log "Ensuring workflow '${WORKFLOW_IDENTIFIER}' is enabled on ${REPO}"
if ! gh workflow enable "${WORKFLOW_IDENTIFIER}" --repo "${REPO}" >/dev/null 2>&1; then
  log "Workflow already enabled"
fi

log "Triggering workflow_dispatch for ${WORKFLOW_IDENTIFIER} on ref ${REF}"
gh workflow run "${WORKFLOW_IDENTIFIER}" --repo "${REPO}" --ref "${REF}"

log "Done"


