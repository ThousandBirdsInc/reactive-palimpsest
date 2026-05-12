# Security process

Two CI hooks gate dependency-level security:

| Job                     | Trigger                  | What it does |
|-------------------------|--------------------------|--------------|
| `ci.yml::deny`          | every PR + push to main  | `cargo deny check` (license, advisories, bans, sources) |
| `audit.yml::audit`      | weekly cron + manual     | `cargo audit --deny warnings` and `cargo deny check advisories` |

## Cadence

- **Per-PR**: cargo-deny runs unconditionally. A new advisory landing
  in a transitive dep blocks merge. License or banned-source violations
  also block.
- **Weekly RustSec sweep** (`audit.yml`): scheduled Mondays 13:00 UTC.
  cargo-audit and cargo-deny advisories are both run; unmaintained
  crates and informational advisories trip the build because we want
  the signal even before they harden into vulnerabilities.

## Granting an advisory exemption

Don't, by default. The first instinct should be to bump the dependency
or replace it.

If a temporary exemption is genuinely necessary (e.g., an advisory in
a transitive crate whose upstream hasn't shipped a fix yet):

1. Open a tracking issue describing the advisory, the affected
   dependency path, and the exposure (which Palimpsest code paths use
   the vulnerable surface, if any).
2. Add the RUSTSEC id to `deny.toml`'s `[advisories.ignore]` with a
   comment that includes the issue number and an expiry date.
3. Schedule a calendar reminder for the expiry. When it lapses,
   re-evaluate.

Exemptions are reviewed at every release cut; an exemption that has
outlived its expiry is itself a release blocker.

## Out-of-cycle action

If `cargo audit` flips red between scheduled runs (e.g., a
ring-zero advisory in `tokio`), the on-call engineer kicks
`audit.yml` manually from the Actions tab and treats the result as
P1.

## Reporting a vulnerability *in Palimpsest*

Email <security@thousandbirds.ai> with details. Do not file a public
issue. We aim to acknowledge within 2 business days and ship a fix or
mitigation within 14 days for high-severity issues.
