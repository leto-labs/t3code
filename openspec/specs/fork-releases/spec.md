# Fork releases

## Purpose

A fork that ships its own builds must keep its machines on those builds. A fork
runtime that updated from upstream's releases would silently replace the fork's
features with upstream's.

## Requirements

### Requirement: builds update from their own repository

A release build MAY name a GitHub repository as its release source. `t3 update`,
the boot service, and the install script MUST then find and download releases
from that repository. A build that names none MUST keep using upstream's.

#### Scenario: fork runtime updates

- **WHEN** a runtime built for a fork's repository runs `t3 update`
- **THEN** it installs the newest stable release of that repository
- **AND** it does not install an upstream release

### Requirement: tags publish releases

Pushing a fork release tag MUST check the fork's own commits against the
upstream release they are based on, build the Linux x64 CLI archive with the
repository as its release source, smoke-test it, and publish it with its
checksums as the repository's latest GitHub Release. A failed check MUST stop
the publication. Upstream's repository MUST NOT run this publication.

#### Scenario: fork release tag is pushed

- **WHEN** a tag such as `v0.1.7-t3custom.1` is pushed to the fork
- **THEN** a release with `t3-0.1.7-t3custom.1-linux-x64.tar.gz` and `SHA256SUMS` is published
- **AND** `t3 update` on a fork runtime can find it

#### Scenario: fork commits fail their checks

- **WHEN** a release tag's commits fail `check:changed` against their upstream base tag
- **THEN** no release is published for that tag
