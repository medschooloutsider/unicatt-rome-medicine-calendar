# UCSC calendar reliability bundle

This bundle is the cutover-ready replacement for the Vercel Blob refresh path.
It is intentionally boring: a scheduled GitHub Actions job polls the official
UCSC endpoints, writes static ICS/JSON files to GitHub Pages, and commits the
ledger and health receipts alongside them.

## Safety contract

- A network error, HTTP error, malformed JSON response, duplicate UID, or
  empty response after durable history exists leaves the previous ICS file in
  place and marks the feed `degraded` and `stale`.
- An explicitly suspended source row becomes `STATUS:CANCELLED` immediately.
- A future event missing from one valid snapshot becomes `STATUS:TENTATIVE`.
  It becomes `STATUS:CANCELLED` only after a second consecutive valid snapshot
  omits it. The UID and event remain in the feed for audit and rollback.
- Past events are never erased.
- Every successful poll records the source snapshot hash, publication hash,
  last successful fetch, last publication, and event ledger.
- If a state JSON is empty or corrupt, the next run reconstructs it from the
  last verified public status before polling, so one transient filesystem
  failure cannot suppress the publication.
- Telegram delivery is an optional, secret-safe alert layer. Successful
  fingerprints are deduplicated in `alerts/telegram-delivery.json`; transient
  failures retry and are recorded without invalidating a good publication.
  The sentinel uses the durable GitHub issue as its fallback and the weekly
  canary proves that the Bot API accepts a heartbeat.

## Repository layout for cutover

The destination GitHub repository must contain this module plus the existing
normalizer files `medical_curriculum_tooling/unicatt_calendar_broadcast.py`
and `medical_curriculum_tooling/tartu_y1_station_source.py`. Serve `public/`
with GitHub Pages. Do not commit Telegram tokens; add
`CALENDAR_ALERT_TELEGRAM_BOT_TOKEN` and `CALENDAR_ALERT_TELEGRAM_CHAT_ID` as
repository secrets. Public repositories use GitHub-hosted standard runners and
GitHub Pages under GitHub Free within GitHub's documented limits.

The workflow is stored here as a handoff template. In the destination
repository it must live at the repository-root path
`.github/workflows/calendar-publish.yml`; the workflow commands already use
the repository root and the `medical_curriculum_tooling` module path.

The deployed replacement repository is
`https://github.com/medschooloutsider/unicatt-rome-medicine-calendar` with
Pages at `https://medschooloutsider.github.io/unicatt-rome-medicine-calendar/`.
The old Vercel URL remains a rollback copy until the new URL has passed the
side-by-side check. The Apple Calendar subscription is not changed by this
bundle.

## Local proof

Run the focused suite from the LM41 hub root (stdlib only; no pytest install is required):

```bash
python3 -m unittest discover -s medical_curriculum_tooling/calendar_reliability/tests -p 'test_*.py' -v
```

For a live dry-run, use a temporary public/state directory and inspect the
result before committing anything. A real cutover still requires the user to
manually change the Apple Calendar subscription URL after 48 hours of matching
UIDs and a synthetic cancellation test.

