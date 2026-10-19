# Security

Do not put secrets or personal data into snapshots. SQLite stores full event
history without encryption. Treat adapters as trusted code and review them.
This alpha is intended for operator-controlled local use, not untrusted multi-tenant
workloads. There is no authentication layer or hosted service.

Before public release the maintainer must publish a private vulnerability reporting
channel or enable GitHub private vulnerability reporting. Until then, do not file
sensitive details in public issues; coordinate privately with the repository owner.
