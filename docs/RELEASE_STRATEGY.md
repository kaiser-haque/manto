# Release Strategy

## Versioning

Use Semantic Versioning.

- `0.x` for development releases.
- `1.0.0` for the first stable public release.
- breaking changes increment the major version.
- backward-compatible features increment minor.
- fixes increment patch.

## Release checklist

1. Run `mvn clean verify`.
2. Review public API.
3. Review dependencies.
4. Update CHANGELOG.
5. Update version.
6. Build source and Javadoc artifacts.
7. Sign artifacts.
8. Publish to Maven Central.
9. Create Git tag.
10. Verify dependency resolution from a clean external project.
11. Create GitHub release.

## Release candidate

Use `0.9.0` as the final candidate before `1.0.0`.

## Post-release

Test:

```xml
<dependency>
  <groupId>...</groupId>
  <artifactId>manto-spring-boot-starter</artifactId>
  <version>1.0.0</version>
</dependency>
```

from a project that does not depend on Manto's local source build.
