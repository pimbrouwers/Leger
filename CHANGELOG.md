# Changelog

All notable changes to this project will be documented in this file.

## [2.0.0] - 2026-01-??

### Added

- Support for `IAsyncEnumerable<T>` using the new `StreamAsync` methods and overloads (available for `IDbCommand`, `IDbTransaction`, `IDbConnection` and `IDbConnectionFactory`).
- Index-based reading methods for `IDataRecord` (e.g. `ReadString(int index)`) for improved performance in tight loops.
- Add new error type `DatabaseReadFieldException` for errors that occur when reading fields from a data reader.
- Added new error codes:
    - `DatabaseErrorCode.NoCommandText` when no command text is provided.
    - `DatabaseErrorCode.InvalidCommandType` when an invalid command type is specified.
    - `DatabaseErrorCode.CouldNotMapDataReader` when mapping from a data reader fails.
    - `DatabaseErrorCode.CouldNotMapDataReaderFirst` when mapping the first record from a data reader fails.
- Support for asynchronous `IDataReader` mapping using `MapAsync` and `MapFirstAsync` methods when using `ReadAsync`.

### Updated

- Improved error handling and reporting for mapping and execution operations.

### Removed

- SQL statements from exception messages to enhance security.

### Fixed

- Create consistency in DBNull handling when working with `IDataRecord`.

## [1.1.0] - 2025-07029

### Added

- `netstandard2.1` support.
- `DatabaseErrorCode.FieldNotFound` to improve error output when a field is not found.

## [1.0.1] - 2025-02-07

### Added

- An integer command timeout setting for all command extension methods.

## [1.0.0] - 2024-11-25

> Hello world!
