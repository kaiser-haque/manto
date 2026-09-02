# Release Strategy

## Versioning

Use Semantic Versioning.

- `0.x` for development releases.
- `1.0.0` for the first stable public release.
- breaking changes increment the major version.
- backward-compatible features increment minor.
- fixes increment patch.

## Release checklist

1. Run `mvn clean verify` (and `mvn -P release verify -Dgpg.skip=true` for sources/javadoc).
2. Review public API (no breaking changes after 1.0.0; Javadoc must build: `mvn javadoc:javadoc`).
3. Review dependencies (`mvn dependency:tree`, `mvn versions:display-dependency-updates`).
4. Update `CHANGELOG.md` and `pom.xml` version (remove `-SNAPSHOT` via `mvn versions:set -DnewVersion=X.Y.Z`).
5. Update version in `docs/MAVEN_CENTRAL.md` examples if needed.
6. Build source (`maven-source-plugin`) and Javadoc (`maven-javadoc-plugin`) artifacts via `mvn -P release verify`.
7. Sign artifacts (`maven-gpg-plugin` — requires `GPG_PRIVATE_KEY` + `GPG_PASSPHRASE`; public key on `keyserver.ubuntu.com`).
8. Publish to Maven Central via Central Portal:
   - `mvn -P release deploy` (uploads bundle, `autoPublish=false`, `waitUntil=validated`; see `.github/workflows/release.yml`)
   - Review Validation Results on https://central.sonatype.com/publishing/deployments and click **Publish**
   - Alternatively trigger `Release to Maven Central` workflow with `dryRun=false` or push tag `vX.Y.Z`.
9. Create Git tag (`git tag vX.Y.Z && git push origin vX.Y.Z`) — CI tags trigger the same upload.
10. Verify dependency resolution from a clean external project (no `mvn install`):
   - `io.github.kaiser-haque:manto-spring-boot-starter:X.Y.Z` from https://central.sonatype.com/
11. Create GitHub release (attach bundle checksum, link to Central).

## Release candidate

Use `0.9.0` as the final candidate before `1.0.0`.

## Post-release

Test from a project that does not depend on Manto's local source build:

```xml
<dependency>
  <groupId>io.github.kaiser-haque</groupId>
  <artifactId>manto-spring-boot-starter</artifactId>
  <version>1.0.0</version>
</dependency>
```

Verified Central coordinate is `io.github.kaiser-haque` (namespace `io.github.kaiser-haque` auto-verified for GitHub user `kaiser-haque`; see `docs/MAVEN_CENTRAL.md`). Java packages stay `io.github.manto.*`.
