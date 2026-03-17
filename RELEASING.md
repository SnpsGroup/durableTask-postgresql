# Release Policy and Versioning

## Versioning Semantics
This project strictly follows [Semantic Versioning 2.0.0](https://semver.org/).

Given a version number `MAJOR.MINOR.PATCH`, increment the:
1. **MAJOR** version when you make incompatible API changes,
2. **MINOR** version when you add functionality in a backwards compatible manner, and
3. **PATCH** version when you make backwards compatible bug fixes.

### Pre-release Phases

Before a stable release is cut, the versions flow through a set of pre-release phases to indicate readiness and stability:

1. **`alpha` (e.g., `1.0.0-alpha.1`)**
   - **Meaning:** Feature development is in progress. The API may be unstable and subject to breaking changes without major version bumps.
   - **Target Audience:** Internal developers and brave early adopters willing to provide feedback.

2. **`rc` [Release Candidate] (e.g., `1.0.0-rc.1`)**
   - **Meaning:** Feature freeze. The release is believed to be ready for production but requires final, wider validation. No new features; only critical bug fixes.
   - **Target Audience:** General audience testing in pre-production or non-critical production environments.

3. **`stable` (e.g., `1.0.0`)**
   - **Meaning:** Production ready. The API is frozen for this major version line.
   - **Target Audience:** All consumers.

## Changelog Update Rules
- All notable changes to this project will be documented in `CHANGELOG.md`.
- Changes must be categorized under `Added`, `Changed`, `Deprecated`, `Removed`, `Fixed`, or `Security`.
- The changelog MUST be updated as part of the Pull Request that introduces the change. Do not wait for release time to aggregate changes. 
- Refer to the template in `CHANGELOG.md` for formatting rules.
