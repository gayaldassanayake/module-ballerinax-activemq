# Changelog

This file contains all the notable changes done to the Ballerina ActiveMQ package through the releases.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- Moved the test-only `TestProducer` native helper out of the released `activemq-native` jar into a
  separate `testOnly`-scoped jar, so test utilities no longer ship in the production artifact.

### Removed
- Removed the unused, auto-generated `gradle/libs.versions.toml` stub.
