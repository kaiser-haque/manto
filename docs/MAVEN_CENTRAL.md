# Maven Central Publication

## Goal

Publish Manto artifacts so external Maven users can consume them without cloning the repository.

## Namespace — Verification (2026-09-02)

Manto uses the GitHub-verified namespace `io.github.kaiser-haque`.

*Verification status (checked 2026-09-02 against https://central.sonatype.org/register/namespace/ and https://central.sonatype.org/publish/requirements/):*

- **GitHub-backed namespace** `io.github.<github-username>` is auto-verified for the
  GitHub account that signs up at https://central.sonatype.com. For this repository
  (`https://github.com/kaiser-haque/manto`) the verified namespace is
  `io.github.kaiser-haque` (owner `kaiser-haque`).
- The placeholder `io.github.manto` used during early development **is not** auto-verified
  for `kaiser-haque` — Central Portal scopes `io.github.*` to the authenticated
  GitHub username, not an organization name (see "GitHub Namespaces" docs: only
  `io.github.<your username>` is auto-provisioned). To publish under
  `io.github.manto` you would need to sign up as GitHub user `manto` or verify a
  custom domain via DNS TXT. Therefore the Maven coordinates were migrated to
  `io.github.kaiser-haque` before the first Central release.
- **Java package vs. Maven coordinate**: source packages remain `io.github.manto.*`
  (`MantoProducer.java:1`, `MantoKafkaProducer.java:1`, etc.) for backwards
  compatibility — package names do not have to equal `groupId`. The published
  coordinates are `io.github.kaiser-haque:manto-*`.

To verify/claim the namespace before the first publish:

1. Log in to https://central.sonatype.com with the `kaiser-haque` GitHub account.
2. Check **Publishing > Namespaces** — `io.github.kaiser-haque` should appear as
   **Verified**. If not, click **Add Namespace** → `io.github.kaiser-haque` →
   **Verify Namespace** (requires creating a temporary public repository named after
   the verification key, see https://central.sonatype.org/register/namespace/#by-code-hosting-services).
3. Subsequent publishes with `groupId=io.github.kaiser-haque` will pass namespace
   validation.

Do not hard-code a different namespace until ownership of that namespace is
confirmed in the Portal (see above). GroupId changes after the first Central
release are irreversible (Maven Central is immutable).

## Required release metadata — Current requirements (verified 2026-09-02)

Per https://central.sonatype.org/publish/requirements/ every deployed `pom`
must contain (parent `pom.xml:13`):

- `groupId` — `io.github.kaiser-haque`
- `artifactId` — `manto-core`, `manto-kafka`, `manto-spring-boot-autoconfigure`,
  `manto-spring-boot-starter`, `manto-test`, parent `manto`
- `version` — SemVer, **must not** end with `-SNAPSHOT` for releases
- `packaging` — `pom` (parent) / `jar` (modules) — otherwise inferred
- `name` — `Manto`, `Manto Core`, etc. (`pom.xml:13`)
- `description` — short framework description (`pom.xml:14`)
- `url` — `https://github.com/kaiser-haque/manto` (`pom.xml:15`)
- `licenses` — Apache-2.0 (`pom.xml:17`)
- `developers` — at least one developer with `name`/`email` (`pom.xml:25` —
  `kaiser-haque <khaque444@gmail.com>` plus Contributors)
- `scm` — `connection`, `developerConnection`, `url`, `tag` (`pom.xml:33`)
- `issueManagement` — GitHub issues (`pom.xml:40`)

The build also sets `project.build.outputTimestamp` (`pom.xml:59`) for
reproducible builds and enforces `requireMavenVersion 3.9.0` /
`requireJavaVersion 21` / `requireUpperBoundDeps` via `maven-enforcer-plugin`.

## Required artifacts

For each artifact with packaging `jar` Central requires (via
`central-publishing-maven-plugin` validation):

- main JAR — `manto-*.jar`
- POM — `manto-*.pom`
- **sources JAR** — `*-sources.jar` via `maven-source-plugin:3.3.1`
  (`pom.xml` `<artifactId>maven-source-plugin</artifactId>` with
  `jar-no-fork` in `release` profile)
- **Javadoc JAR** — `*-javadoc.jar` via `maven-javadoc-plugin:3.12.0`
  (`doclint=all,-missing`, `failOnError=true`, `failOnWarnings=false`)
- **cryptographic signatures** — `.asc` for every file via
  `maven-gpg-plugin:3.2.7` (`gpgArguments --pinentry-mode loopback`,
  bound to `verify` phase in `release` profile)
- **checksums** — `.md5`/`.sha1` (required) plus `.sha256`/`.sha512`
  generated automatically by `central-publishing-maven-plugin` with
  `checksums=all`.

Releases are published with the **Sonatype Central Portal** and the
`org.sonatype.central:central-publishing-maven-plugin:0.9.0`
(not the legacy OSSRH `nexus-staging-maven-plugin`). The plugin is configured
in `pom.xml` pluginManagement and activated in the `release` profile:

```xml
<plugin>
  <groupId>org.sonatype.central</groupId>
  <artifactId>central-publishing-maven-plugin</artifactId>
  <version>0.9.0</version>
  <extensions>true</extensions>
  <configuration>
    <publishingServerId>central</publishingServerId>
    <autoPublish>false</autoPublish>
    <waitUntil>validated</waitUntil>
    <checksums>all</checksums>
  </configuration>
</plugin>
```

`sources`/`javadoc`/`gpg` are **release-only** (profile `release`) to keep normal
`mvn verify` fast. Verify locally without a real GPG key:

```bash
mvn -B clean verify -P release -Dgpg.skip=true
```

With a configured key the full release validation is:

```bash
export GPG_PASSPHRASE="..."
mvn -B clean verify -P release
# then upload (requires Central token in ~/.m2/settings.xml):
mvn -B deploy -P release
```

## Security — CI secrets without exposing credentials

Credentials are **never committed**. They live in
GitHub Actions repository secrets (Settings > Secrets and variables > Actions)
and are injected via `settings.xml` at build time:

| Secret | Purpose | How to create |
|---|---|---|
| `CENTRAL_USERNAME` | Central Portal token username | https://central.sonatype.com → Account → Generate User Token |
| `CENTRAL_TOKEN` | Central Portal token password | same as above (the token's password) |
| `GPG_PRIVATE_KEY` | ASCII-armored private key | `gpg --export-secret-keys --armor <keyId>` |
| `GPG_PASSPHRASE` | GPG key passphrase | chosen at `gpg --gen-key` |

CI configuration (`.github/workflows/release.yml:37`):

- `actions/setup-java@v4` with `server-id: central`,
  `server-username: CENTRAL_USERNAME`, `server-password: CENTRAL_TOKEN`,
  `gpg-private-key: ${{ secrets.GPG_PRIVATE_KEY }}`,
  `gpg-passphrase: GPG_PASSPHRASE` — writes `~/.m2/settings.xml` with the
  server entry **only in the runner's memory**; secrets are masked in logs.
- `gpg --keyserver keyserver.ubuntu.com --send-keys <keyId>` must be done
  once before the first release so verifiers can fetch the public key (also
  `keys.openpgp.org` and `pgp.mit.edu` are accepted).
- `maven-gpg-plugin` uses `--pinentry-mode loopback` and reads the passphrase
  from `-Dgpg.passphrase` / `GPG_PASSPHRASE` environment variable — never from
  a file in the repo.
- Snapshot publishing (optional) uses the same `central` server id with
  `https://central.sonatype.com/repository/maven-snapshots`
  (`pom.xml:45` `distributionManagement`).

## Release automation — Safe workflow

Preferred flow (docs/RELEASE_STRATEGY.md + this file):

```
Git tag vX.Y.Z
  -> GitHub Actions (.github/workflows/release.yml)
  -> Maven build (mvn -B clean verify -P release)
  -> tests (surefire + Testcontainers)
  -> sources + javadoc generation
  -> GPG sign (maven-gpg-plugin)
  -> publish (central-publishing-maven-plugin, autoPublish=false)
  -> Portal validation (waitUntil=validated)
  -> Human clicks Publish on https://central.sonatype.com/publishing/deployments
```

**Workflow triggers** (`.github/workflows/release.yml:20`):

- `push` of tags `v*.*.*` (e.g. `v0.9.0`, `v1.0.0`)
- `workflow_dispatch` with inputs:
  - `version` — optional explicit version (defaults to `pom` version)
  - `dryRun` — default `true` — when true, the job **validates only**
    (`mvn verify -P release`) and skips `mvn deploy`; when `false` it uploads.

**Safety rails:**

- `autoPublish=false` + `waitUntil=validated` — uploads are validated but
  **never auto-published** to Maven Central. Even a `v1.0.0` tag push requires a
  human to click **Publish** in the Portal UI.
- `-SNAPSHOT` versions are rejected (Central requires release versions).
- Tag/pom mismatch warns but does not auto-correct.
- `concurrency` group prevents parallel publishes on the same ref.

**How to do a dry-run today (no Central upload):**

```bash
# Local validation (no secrets needed):
mvn -B clean verify -P release -Dgpg.skip=true

# Via GitHub (requires secrets but skips upload):
# Actions → Release to Maven Central → Run workflow → dryRun: true

# Generate a bundle without uploading (outputs to target/central-staging):
mvn -B clean verify -P release -Dgpg.skip=true
# or with signing:
mvn -B clean verify -P release
```

**How the 1.0.0 release works:**

1. Update `pom.xml` version to `1.0.0`
   via `mvn versions:set -DnewVersion=1.0.0` + commit.
2. Create and push tag `v1.0.0`: `git tag v1.0.0 && git push origin v1.0.0`
   — workflow uploads bundle with `mvn deploy -P release`.
3. In https://central.sonatype.com → **Publishing** → **Deployments** →
   find deployment `v1.0.0` → review **Validation Results** (requirements,
   javadoc/sources presence, signatures) → click **Publish**.
4. Within minutes the artifacts appear at
   https://central.sonatype.com/artifact/io.github.kaiser-haque/manto-spring-boot-starter/
   and sync to https://repo.maven.apache.org/maven2/.
5. Verify from a clean external project (no local `mvn install`):

   ```xml
   <dependency>
     <groupId>io.github.kaiser-haque</groupId>
     <artifactId>manto-spring-boot-starter</artifactId>
     <version>1.0.0</version>
   </dependency>
   ```

Before release, verify current Maven Central publishing requirements and use
their current official documentation; publishing rules can change. This file was
last verified against https://central.sonatype.org/publish/requirements/,
https://central.sonatype.org/publish/publish-portal-maven/,
https://central.sonatype.org/publish/requirements/gpg/,
https://central.sonatype.org/register/namespace/ on **2026-09-02**.

