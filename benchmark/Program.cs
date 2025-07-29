using System.Data;
using BenchmarkDotNet.Attributes;
using Dapper;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Leger;
using BenchmarkDotNet.Running;

static class Program
{
    static void Main()
    {
        BenchmarkRunner.Run<AdoNetBenchmark>();
    }
}

[MemoryDiagnoser]
public class AdoNetBenchmark
{
    private readonly IDbConnection _connection = new SqliteConnection(@"Data Source=.\benchmark.db;Cache=Shared");

    [GlobalSetup]
    public void Setup()
    {
        _connection.Open();

        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS BatchInstanceResult (
                  Id INTEGER PRIMARY KEY
                , BatchInstanceId TEXT
                , IdJob TEXT
                , JobTitle TEXT
                , JobUrl TEXT
                , JobDesc TEXT
                , GoogleDate DATETIME
                , TimestampInsert DATETIME
                , IsTerminal BOOLEAN
                , IsLeadership BOOLEAN
                , IsTech BOOLEAN);
        """;

        cmd.ExecuteNonQuery();

        cmd.CommandText = "SELECT COUNT(*) FROM BatchInstanceResult";
        var rowCount = (long?)cmd.ExecuteScalar();

        if (rowCount is null || rowCount == 0)
        {

            for (var i = 0; i < 1_000; i++)
            {
                cmd.CommandText = $"""
                INSERT INTO BatchInstanceResult (BatchInstanceId, IdJob, JobTitle, JobUrl, JobDesc, GoogleDate, TimestampInsert, IsTerminal, IsLeadership, IsTech)
                VALUES('batch-{i}', 'job-{i}', 'Job Title {i}', 'http://example.com/job-{i}', 'Job Description {i}', '2023-10-01 00:00:00', '2023-10-01 00:00:00', NULL, 0, 1);
            """;

                cmd.ExecuteNonQuery();
            }
        }
    }

    [Benchmark(Baseline = true)]
    public List<BatchInstanceResult> AdoNet()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "SELECT * FROM BatchInstanceResult";
        using var rd = cmd.ExecuteReader();

        var results = new List<BatchInstanceResult>();
        while (rd.Read())
        {
            results.Add(new BatchInstanceResult
            {
                Id = rd.GetInt32(0),
                BatchInstanceId = rd.GetString(1),
                IdJob = rd.GetString(2),
                JobTitle = rd.GetString(3),
                JobUrl = rd.GetString(4),
                JobDesc = rd.GetString(5),
                GoogleDate = rd.GetDateTime(6),
                TimestampInsert = rd.GetDateTime(7),
                IsTerminal = rd.IsDBNull(8) ? (bool?)null : rd.GetBoolean(8),
                IsLeadership = rd.GetBoolean(9),
                IsTech = rd.GetBoolean(10)
            });
        }

        return results;
    }

    [Benchmark]
    public List<BatchInstanceResult> Dapper()
    {
        return _connection.Query<BatchInstanceResult>(
            "select * from BatchInstanceResult").ToList();
    }

    [Benchmark]
    public List<BatchInstanceResult> Leger()
    {
        return _connection.Query(
            "select * from BatchInstanceResult",
            rd => new BatchInstanceResult
            {
                Id = rd.GetInt32(0),
                BatchInstanceId = rd.GetString(1),
                IdJob = rd.GetString(2),
                JobTitle = rd.GetString(3),
                JobUrl = rd.GetString(4),
                JobDesc = rd.GetString(5),
                GoogleDate = rd.GetDateTime(6),
                TimestampInsert = rd.GetDateTime(7),
                IsTerminal = rd.IsDBNull(8) ? (bool?)null : rd.GetBoolean(8),
                IsLeadership = rd.GetBoolean(9),
                IsTech = rd.GetBoolean(10)
            }).ToList();
    }

    [Benchmark]
    public List<BatchInstanceResult> EfCore()
    {
        using var context = new EfContext(_connection.ConnectionString);
        return context.BatchInstanceResults.ToList();
    }

    [Benchmark]
    public List<BatchInstanceResult> EfCoreNoTracking()
    {
        using var context = new EfContext(_connection.ConnectionString);
        return context.BatchInstanceResults.AsNoTracking().ToList();
    }

    [Benchmark]
    public List<BatchInstanceResult> EfCoreSqlQuery()
    {
        using var context = new EfContext(_connection.ConnectionString);
        return context.BatchInstanceResults.FromSqlRaw("SELECT * FROM BatchInstanceResult").ToList();
    }
}


public sealed class EfContext(string connectionString) : DbContext
{
    public DbSet<BatchInstanceResult> BatchInstanceResults { get; set; } = null!;

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseSqlite(connectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<BatchInstanceResult>().ToTable("BatchInstanceResult");
    }
}

public class BatchInstanceResult
{
    public int Id { get; set; }
    public string BatchInstanceId { get; set; } = "";
    public string IdJob { get; set; } = "";
    public string JobTitle { get; set; } = "";
    public string JobUrl { get; set; } = "";
    public string JobDesc { get; set; } = "";
    public DateTime GoogleDate { get; set; }
    public DateTime TimestampInsert { get; set; }
    public bool? IsTerminal { get; set; }
    public bool IsLeadership { get; set; }
    public bool IsTech { get; set; }
}

public static class BatchInstanceResultReader
{
    public static BatchInstanceResult Map(this IDataReader rd)
    {
        return new BatchInstanceResult
        {
            Id = rd.GetInt32(0),
            BatchInstanceId = rd.GetString(1),
            IdJob = rd.GetString(2),
            JobTitle = rd.GetString(3),
            JobUrl = rd.GetString(4),
            JobDesc = rd.GetString(5),
            GoogleDate = rd.GetDateTime(6),
            TimestampInsert = rd.GetDateTime(7),
            IsTerminal = rd.IsDBNull(8) ? (bool?)null : rd.GetBoolean(8),
            IsLeadership = rd.GetBoolean(9),
            IsTech = rd.GetBoolean(10)
        };
    }
}
