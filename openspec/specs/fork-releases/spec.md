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
