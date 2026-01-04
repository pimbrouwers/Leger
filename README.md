# Léger

[![NuGet Version](https://img.shields.io/nuget/v/Leger.svg)](https://www.nuget.org/packages/Leger)
[![build](https://github.com/pimbrouwers/leger/actions/workflows/build.yml/badge.svg)](https://github.com/pimbrouwers/leger/actions/workflows/build.yml)
[![license](https://img.shields.io/github/license/pimbrouwers/Leger.svg)](https://github.com/pimbrouwers/Leger/blob/master/LICENSE)
![aot](https://img.shields.io/badge/aot-compatible-green.svg)
![net8.0](https://img.shields.io/badge/net-8.0-blue.svg)
![net6.0](https://img.shields.io/badge/net-6.0-blue.svg)
![netstandard2.1](https://img.shields.io/badge/netstandard-2.1-blue.svg)

Léger is a library that aims to make working with [ADO.NET](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/ado-net-overview) from C# *a lot* simpler. Functionality is delivered through an API that bears a resemblance to [Dapper](https://github.com/DapperLib/Dapper) with a focus on manual mapping and a few extra goodies built-in.

## Key Features

- No reflection, no magic, no surprises.
- Simple and **uniform** execution model, delivered as `IDbConnection`, `IDbCommand`, `IDbTransaction` and [`IDbConnectionFactory`](#idbconnectionfactory) extension methods.
- Safe value reading via `IDataRecord` and `IDataReader` [extensions](#idatareader-extension-methods).
- [Enhanced](#exceptions) exception output.
- [Async](#an-example-using-sqlite) versions of all data access methods.
- Support for `IAsyncEnumerable<T>` via the [StreamAsync](#stream-results-asynchronously-using-iasyncenumerablet) methods.

## Design Goals

- Augment the base ADO.NET functionality as little as possible and adhering to internal naming conventions.
- Encourage manual mappings by providing a succinct and safe methodology to obtain values from tabular data.
- Provide an easy to reason about execution model.
- Support asynchronous database workflows.

## Getting Started

Install the Leger NuGet package:

```
PM>  Install-Package Leger
```

Or using the dotnet CLI

```
dotnet add package Leger
```

### Quick Start

```csharp
using System;
using System.Data.Sqlite;
using Leger;

using var connection = new SqliteConnection("{your connection string}");

var books = connection.Query("""
    SELECT name FROM book WHERE author_id = @author_id"
    """,
    new("author_id", 1),
    rd => rd.ReadString("name"));

foreach(var book in books)
{
    Console.WriteLine(book);
}
```

## An Example using SQLite

For this example, assume we have an `IDbConnection` named `connection`:

```csharp
using var connection = new SqliteConnection("Data Source=author.db");
```

With the following database schema:

```sql
CREATE TABLE author (
    author_id INTEGER PRIMARY KEY,
    full_name TEXT);
```

And domain model:

```csharp
using Leger;

public record Author(
    int AuthorId,
    string FullName);

public static class AuthorReader
{
    public static Author Map(IDataRecord rd) =>
        new(AuthorId = rd.ReadInt32("author_id"),
            FullName = rd.ReadString("full_name"));
        // or, if you the indexes are known and fixed, for better performance:
        new(AuthorId = rd.ReadInt32(0),
            FullName = rd.ReadString(1));
}
```

### Query for multiple strongly-type results

```csharp
using Leger;

var authors = connection.Query("""
    SELECT author_id, full_name FROM author
    """,
    AuthorReader.Map);

// async
var authors = await connection.QueryAsync("""
    SELECT author_id, full_name FROM author
    """,
    AuthorReader.Map);
```

### Query for a single strongly-type result

```csharp
using Leger;

// `QuerySingle` is optimized to dispose the `IDataReader` after safely reading the first `IDataRecord`.
var author = connection.QuerySingle("""
    SELECT author_id, full_name FROM author WHERE author_id = @author_id
    """,
    new("author_id", authorId),
    AuthorReader.Map);

// async
var author = await connection.QuerySingleAsync("""
    SELECT author_id, full_name FROM author WHERE author_id = @author_id
    """,
    new("author_id", authorId),
    AuthorReader.Map);
```

### Execute a statement

```csharp
using Leger;

connection.Execute("""
    INSERT INTO author (full_name) VALUES (@full_name)
    """,
    new("full_name", "John Doe"));

// async
await connection.ExecuteAsync("""
    INSERT INTO author (full_name) VALUES (@full_name)
    """,
    new("full_name", "John Doe"));
```

### Execute a statement multiple times

```csharp
using Leger;

public record NewAuthor(
    string FullName);

IEnumerable<NewAuthor> newAuthors = [new("John Doe"), new("Jane Doe")];

connection.ExecuteMany("""
    INSERT INTO author (full_name) VALUES (@full_name)
    """,
    newAuthors.Select(a => new DbParams("full_name", a.FullName)));

// async
await connection.ExecuteManyAsync("""
    INSERT INTO author (full_name) VALUES (@full_name)
    """,
    newAuthors.Select(a => new DbParams("full_name", a.FullName)));
```

### Execute a statement transactionally

```csharp
using Leger;

using var transaction = connection.CreateTransaction();

transaction.Execute("""
    UPDATE author SET full_name = @full_name where author_id = @author_id
    """,
    new(){
        { "author_id", author.AuthorId },
        { "full_name", author.FullName }
    });

transaction.Commit();
```

### Execute a scalar command (single value)

```csharp
using Leger;

var count = connection.Scalar("""
    SELECT COUNT(*) AS author_count FROM author
    """,
    new("full_name", "John Doe"));

// async
var count = await connection.ScalarAsync("""
    SELECT COUNT(*) AS author_count FROM author
    """,
    new("full_name", "John Doe"));
```

### Manually read data using `IDataReader`

```csharp
using Leger;

var authors = connection.Read("""
    SELECT author_id, full_name FROM author
    """,
    rd => rd.Map(AuthorReader.Map));

// async
var authors = await connection.ReadAsync("""
    SELECT author_id, full_name FROM author
    """,
    rd => rd.Map(AuthorReader.Map));

// async, async mapping
var authors = await connection.ReadAsync("""
    SELECT author_id, full_name FROM author
    """,
    async rd => await rd.MapAsync(AuthorReader.Map));
```

### Stream results asynchronously using `IAsyncEnumerable<T>`

```csharp
using Leger;

await foreach (var author in connection.StreamAsync("""
    SELECT author_id, full_name FROM author
    """,
    AuthorReader.Map))
{
    Console.WriteLine($"{author.AuthorId}: {author.FullName}");
}
```

## `IDbConnectionFactory`

The `IDbConnectionFactory` interface is provided to allow for the creation of `IDbConnection` instances. This is useful when you want to abstract the creation of connections and even moreso in multi-data source scenarios.

Implementing the interface is straightforward, an example using `System.Data.Sqlite` is shown below:

```csharp
using Leger;

public class SqliteConnectionFactory(connectionString)
    : IDbConnectionFactory
{
    public IDbConnection CreateConnection() =>
        new SqliteConnection(connectionString);
}
```

The expectation is that the connection is left closed, and the API methods will open and close the connection as needed.

## `IDataRecord` Extension Methods

To make obtaining values from `IDataRecord` more straight-forward, 2 sets of extension methods are available for:

- Get value, automatically defaulted
- Get value as Nullable<'a>

Assume we have an active `IDataRecord` called `rd and are currently reading a row, the following extension methods are available to simplify reading values:

```csharp
public static string ReadString(this IDataRecord rd, string field);
public static char ReadChar(this IDataRecord rd, string field);
public static bool ReadBoolean(this IDataRecord rd, string field);
public static bool ReadBool(this IDataRecord rd, string field);
public static byte ReadByte(this IDataRecord rd, string field);
public static short ReadInt16(this IDataRecord rd, string field);
public static short ReadShort(this IDataRecord rd, string field);
public static int ReadInt32(this IDataRecord rd, string field);
public static int ReadInt(this IDataRecord rd, string field);
public static int ReadInt(this IDataRecord rd, string field);
public static long ReadInt64(this IDataRecord rd, string field);
public static long ReadLong(this IDataRecord rd, string field);
public static decimal ReadDecimal(this IDataRecord rd, string field);
public static double ReadDouble(this IDataRecord rd, string field);
public static float ReadFloat(this IDataRecord rd, string field);
public static Guid ReadGuid(this IDataRecord rd, string field);
public static DateTime ReadDateTime(this IDataRecord rd, string field);
public static byte[] ReadBytes(this IDataRecord rd, string field);

public static bool? ReadNullableBoolean(this IDataRecord rd, string field);
public static bool? ReadNullableBool(this IDataRecord rd, string field);
public static byte? ReadNullableByte(this IDataRecord rd, string field);
public static short? ReadNullableInt16(this IDataRecord rd, string field);
public static short? ReadNullableShort(this IDataRecord rd, string field);
public static int? ReadNullableInt32(this IDataRecord rd, string field);
public static int? ReadNullableInt(this IDataRecord rd, string field);
public static int? ReadNullableInt(this IDataRecord rd, string field);
public static long? ReadNullableInt64(this IDataRecord rd, string field);
public static long? ReadNullableLong(this IDataRecord rd, string field);
public static decimal? ReadNullableDecimal(this IDataRecord rd, string field);
public static double? ReadNullableDouble(this IDataRecord rd, string field);
public static float? ReadNullableFloat(this IDataRecord rd, string field);
public static Guid? ReadNullableGuid(this IDataRecord rd, string field);
public static DateTime? ReadNullableDateTime(this IDataRecord rd, string field);

public static string ReadString(this IDataRecord rd, int i);
public static char ReadChar(this IDataRecord rd, int i);
public static bool ReadBoolean(this IDataRecord rd, int i);
public static bool ReadBool(this IDataRecord rd, int i);
public static byte ReadByte(this IDataRecord rd, int i);
public static short ReadInt16(this IDataRecord rd, int i);
public static short ReadShort(this IDataRecord rd, int i);
public static int ReadInt32(this IDataRecord rd, int i);
public static int ReadInt(this IDataRecord rd, int i);
public static int ReadInt(this IDataRecord rd, int i);
public static long ReadInt64(this IDataRecord rd, int i);
public static long ReadLong(this IDataRecord rd, int i);
public static decimal ReadDecimal(this IDataRecord rd, int i);
public static double ReadDouble(this IDataRecord rd, int i);
public static float ReadFloat(this IDataRecord rd, int i);
public static Guid ReadGuid(this IDataRecord rd, int i);
public static DateTime ReadDateTime(this IDataRecord rd, int i);
public static byte[] ReadBytes(this IDataRecord rd, int i);

public static bool? ReadNullableBoolean(this IDataRecord rd, int i);
public static bool? ReadNullableBool(this IDataRecord rd, int i);
public static byte? ReadNullableByte(this IDataRecord rd, int i);
public static short? ReadNullableInt16(this IDataRecord rd, int i);
public static short? ReadNullableShort(this IDataRecord rd, int i);
public static int? ReadNullableInt32(this IDataRecord rd, int i);
public static int? ReadNullableInt(this IDataRecord rd, int i);
public static int? ReadNullableInt(this IDataRecord rd, int i);
public static long? ReadNullableInt64(this IDataRecord rd, int i);
public static long? ReadNullableLong(this IDataRecord rd, int i);
public static decimal? ReadNullableDecimal(this IDataRecord rd, int i);
public static double? ReadNullableDouble(this IDataRecord rd, int i);
public static float? ReadNullableFloat(this IDataRecord rd, int i);
public static Guid? ReadNullableGuid(this IDataRecord rd, int i);
public static DateTime? ReadNullableDateTime(this IDataRecord rd, int i);
```

### High Performance Mapping

The `IDataRecord` extension methods are designed to be performant, avoiding reflection and unnecessary boxing/unboxing. They operate without caching, ensuring that each call retrieves the value directly from the underlying data source. This however comes at a cost of invoking `GetOrdinal(name)` and checking `DBNull` for each field read, which is a trade-off for performance and simplicity.

To reduce the overhead of name-based lookups, you can use the index-based overloads of the extension methods. In application hot paths, or where you are looking for near zero overhead, you can use the built-in `Get[Type](int i)` methods of `IDataRecord` directly, which are optimized for performance. This requires you to know the index of the field you want to read, but will yield the best performance.

An example of using the `Get[Type](int i)` methods directly, revisiting the `AuthorReader` example:

```csharp
public static class AuthorReader
{
    public static Author Map(IDataRecord rd) =>
        new(AuthorId = rd.GetInt32(0),
            FullName = rd.GetString(1));
}
```

> Note the similarity to the previous example, but now using `GetInt32(0)` and `GetString(1)` instead of the extension methods. This is a more performant approach, especially in scenarios where you are reading many records in a tight loop.

## Errors and Exceptions

Leger provides enhanced exception output to help you quickly identify and resolve issues. The following exceptions are thrown:

- `DatabaseExecutionException`
    - Thrown when an error occurs during the execution of a command.
    - Includes the SQL statement that was executed,
    - Or, the field name and value that caused the error.
- `DatabaseConnectionException`
    - Thrown when a connection cannot be established.
    - Alias for `new DatabaseExecutionException(DatabaseErrorCode.CouldNotOpenConnection, ...)`.
- `DatabaseTransactionException`
    - Thrown when an error occurs during a transaction.
    - Alias for `new DatabaseExecutionException(DatabaseErrorCode.CouldNotBeginTransaction, ...)`.

DatabaseExecutionException receives context through the `DatabaseErrorCode` enum:

```csharp
public enum DatabaseErrorCode
{
    /// <summary> Could not open connection. </summary>
    CouldNotOpenConnection = 1000,

    /// <summary>Could not begin transaction. </summary>
    CouldNotBeginTransaction = 2000,

    /// <summary>Could not commit transaction. </summary>
    CouldNotExecuteNonQuery = 4000,
    /// <summary>Could not execute scalar. </summary>
    CouldNotExecuteScalar = 4001,
    /// <summary>Could not execute reader. </summary>
    CouldNotExecuteReader = 4002,

    /// <summary>Could not cast value. </summary>
    CouldNotCastValue = 5000,

    /// <summary>Field not found in the database record.</summary>
    FieldNotFound = 6000,
}
```

## Contribute

Thank you for considering contributing to Leger, and to those who have already contributed! I appreciate (and actively resolve) PRs of all shapes and sizes.

I kindly ask that before submitting a pull request, you first submit an [issue](https://github.com/pimbrouwers/leger/issues) or open a [discussion](https://github.com/pimbrouwers/leger/discussions).

If functionality is added to the API, or changed, please kindly update the relevant [document](https://github.com/pimbrouwers/leger/tree/master/docs). Unit tests must also be added and/or updated before a pull request can be successfully merged.

Only pull requests which pass all build checks and comply with the general coding guidelines can be approved.

If you have any further questions, submit an [issue](https://github.com/pimbrouwers/leger/issues) or open a [discussion](https://github.com/pimbrouwers/leger/discussions).

## License

Licensed under [MIT](https://github.com/pimbrouwers/leger/blob/master/LICENSE).
