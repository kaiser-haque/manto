# Maven Central Publication

## Goal

Publish Manto artifacts so external Maven users can consume them without cloning the repository.

## Namespace

Use a verified Maven Central namespace. A GitHub-backed `io.github.<github-username>` namespace is suitable if available and verified.

Do not hard-code the final namespace until ownership has been confirmed.

## Required release metadata

Artifacts should contain:

- groupId
- artifactId
- version
- name
- description
- URL
- SCM information
- license
- developer information where appropriate

## Required artifacts

Publish:

- main JAR
- POM
- sources JAR
- Javadoc JAR
- cryptographic signatures

## Security

Credentials must be stored in GitHub Actions secrets or the chosen CI secret store. Never commit credentials.

## Release automation

Preferred flow:

```text
Git tag
  -> GitHub Actions
  -> Maven build
  -> tests
  -> sign
  -> publish
```

Before release, verify current Maven Central publishing requirements and use their current official documentation; publishing rules can change.
