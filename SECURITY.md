# Security policy

Do not open a public issue for a suspected vulnerability. Send a private GitHub
security advisory to the repository maintainers with the affected version,
impact, reproduction, and any proposed mitigation. Do not include production
credentials or customer task payloads.

Security support covers the latest published minor release on the platforms in
`docs/support-policy.md`. Critical queue-correctness issues may require an
upgrade during the pre-1.0 period.

Operators must use Redis ACLs, TLS on untrusted networks, secret-managed
credentials, `noeviction`, bounded pools/timeouts, and tested persistence and
backups. EffectMQ health/log/event surfaces are designed not to expose Redis
credentials, but application payloads and typed errors can contain sensitive
data. Restrict Redis, telemetry, dead-letter, and event access accordingly and
choose retention windows that satisfy data-minimization requirements.

Package releases are expected to use npm trusted publishing, provenance, an
exact-commit CI gate, and the repository's compatibility/package/soak evidence.
Consumers should verify package integrity and pin dependencies through a
lockfile.
