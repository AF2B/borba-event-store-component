# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.0] - 2026-10-06

### Added

- Optimistic concurrency: an event is stored only if its version is one more than the last of its aggregate, checked by the
  database in the statement that stores it. When it is not, `append!` returns `{:error :version-conflict}` with the
  `:expected-version` and the `:actual-version`, and stores nothing. Of several commands that append the same version at once, one
  is stored.
- `:position`, a global order of the events that a timestamp cannot give, and `get-all-events`, the feed of every aggregate for a
  projection.
- Paging for `get-events-by-type` and `get-all-events`: `:after` and `:limit`, 1000 by default and 10000 at most. A limit or a
  position that is not valid throws before the query.
- `:from-version` for `get-events`.
- Triggers that make the table append-only: an `UPDATE`, a `DELETE` and a `TRUNCATE` are refused.
- The table is made under an advisory lock, so services that start together do not trip over each other.
- The events are checked before they are stored, and everything that is wrong with one is said at once.
- A test suite with an integration suite against a real PostgreSQL, run by the pipeline.

### Changed

- **Breaking:** the payload and the metadata come back as data. `get-events` and the others returned a `PGobject` for each, since
  nothing parsed the JSONB that was written.
- **Breaking:** the time of an event is the one of the database, where it was the one of the service, and it is read as an `Instant`.
- **Breaking:** `append!` refuses a version that is not one more than the last, where it accepted any that was not taken.
- **Breaking:** `get-events-by-type` is in the order the events were stored in and is bounded, where it was in the order of the
  timestamps and returned every event of the type.
- **Breaking:** the table has `position` as its primary key. A table made by 1.0 has to be migrated, since `CREATE TABLE IF NOT
  EXISTS` does not change a table that exists.
- JSON is written with jsonista, and Cheshire is no longer a dependency. The component uses `borba-sql-client-component` for the
  database, and logs through `tools.logging`.
- Moves to Integrant 1.0.
- The published library is named `io.github.af2b/borba-event-store-component`.

### Security

- Pins Jackson to 2.22.3. The 2.22.2 that jsonista 1.0.1 brings has four high advisories (GHSA-7hhh-6rmp-j9qf, GHSA-p6pp-m3f8-5c89,
  GHSA-cxp5-3px4-pw24 and GHSA-wv8q-qhhj-9h54), which the dependency scan of the pipeline reported in
  `borba-handlers-component`, which has the same dependency.

## [1.0.0] - 2026-03-29

First release: the `:components/event-store` Integrant component, which makes the `events` table, and `append!`, `get-events`,
`get-events-by-type`, `get-latest-version` and `get-aggregate-snapshot`.

[Unreleased]: https://github.com/AF2B/borba-event-store-component/compare/v2.0.0...HEAD
[2.0.0]: https://github.com/AF2B/borba-event-store-component/compare/v1.0.0...v2.0.0
[1.0.0]: https://github.com/AF2B/borba-event-store-component/releases/tag/v1.0.0
