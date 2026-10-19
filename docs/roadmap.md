# Public value and sponsorship readiness

## Intended public value

Serve maintainers and operators who need trustworthy change events from public
release feeds, data catalogs and announcements. Reproduce failure cases offline.
Make adapters independent from storage and notification delivery.

## Milestones

1. Local alpha: complete-snapshot contract, SQLite atomic events, per-recipient
   retries, offline CLI, failure tests and documentation.
2. Useful public beta: tested HTTP adapter with conditional requests and bounded
   response size, explicit timeouts and rate-limit handling; continuous runner;
   inspect/replay/retention commands; Windows and Linux CI actually run.
3. Independent adoption: at least two real integrations, documented limitations,
   external contributor feedback, reproducible bug reports and tagged releases.
4. Sponsorship application: link public repository, maintainer role, released
   artifacts, actual CI results, actual usage and a concrete maintenance plan.

These milestones are project goals, not official OpenAI acceptance thresholds.
No public adoption, passing hosted CI or grant eligibility is claimed by this alpha.

## Local 0.2.0 increment

Implemented: HTTP JSON conditional fetch, validated in-memory representation cache,
bounded body size, socket timeout, Retry-After parsing, nonoverlapping foreground
runner and loopback/fake-clock regression tests. Hosted CI has not been executed.
Remaining beta work includes persistent cooldown, source configuration binding,
inspect/replay/retention, delivery leases and independent integrations.

## Codex for Open Source

Official source checked 2026-10-17:
https://developers.openai.com/community/codex-for-oss

The program offers eligible maintainers six months of ChatGPT Pro with Codex,
conditional Codex Security access, and API credits for core OSS workflows.
It invites core maintainers and widely used public projects, while allowing
applications explaining other ecosystem importance. Approval is discretionary.

Application draft, to complete with verified evidence:
- Problem: monitoring failures can silently corrupt baselines or lose notifications.
- Contribution: transactional snapshots and durable per-recipient events with
  offline fault reproduction and independently implementable adapters.
- Evidence: [public repository], [release], [hosted CI], [external integrations],
  [maintainer role], [real usage or issue reports].
- Proposed Codex use: reproduce reported defects, review adapter changes,
  strengthen cross-platform regression coverage and maintain release documentation.
- API credit request: only if an actual API-based maintainer workflow exists;
  describe workflow, measured workload, budget and human review boundaries.

Do not invent stars, downloads, users, integrations or benchmark results.
Local implementation is not a grant approval or a submitted application.
