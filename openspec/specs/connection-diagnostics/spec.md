# Connection diagnostics

## Purpose

Users on remote connections need a cheap, local view of connection health that
works even while the environment is disconnected.

## Requirements

### Requirement: lightweight connection view

The Diagnostics page MUST show, per environment, the connection path, phase,
recent probe round trips, reconnect count, socket errors, stream failures, and
the last error kind. Collecting this MUST NOT serialize or measure message
payloads.

#### Scenario: environment is disconnected

- **WHEN** an environment is reconnecting
- **THEN** its diagnostics remain visible with the last known values

### Requirement: mobile summary

Mobile MUST show only phase, round-trip time, and reconnect count per
environment.

#### Scenario: mobile diagnostics row

- **WHEN** the user opens diagnostics on mobile
- **THEN** each environment row shows phase, round-trip time, and reconnects
