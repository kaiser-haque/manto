# Security Policy

## Reporting vulnerabilities

Do not disclose security vulnerabilities in public issues.

Use the repository's configured private security reporting mechanism when available.

## Scope

Security concerns include:

- credential leakage,
- unsafe deserialization,
- message/header injection,
- dependency vulnerabilities,
- sensitive information in logs,
- authentication/authorization bypasses.

## Secure development rules

Manto must not log secrets or full message payloads by default. Serialization and header handling must be reviewed for untrusted input.
