# Security Model

## Threats

Manto handles messages and headers originating from distributed systems and should assume they may be malformed or untrusted.

## Rules

- Use safe Jackson configuration.
- Avoid unsafe polymorphic deserialization.
- Validate configuration.
- Do not trust message headers blindly.
- Do not log secrets or full payloads by default.
- Keep dependencies current.
- Run dependency vulnerability checks in CI.

## Credentials

Kafka credentials must come from application configuration/secret management, not from Manto source code.

Manto should not invent a secret-management subsystem in v1.0.
