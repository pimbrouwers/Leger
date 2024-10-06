namespace Leger;

using System.Data;
using System.Data.Common;

public static class IDataReaderExtensions
{
    public static IEnumerable<T> Map<T>(
        this IDataReader rd,
        Func<IDataReader, T> map)
    {
        var records = new List<T>();

        while (rd.Read())
        {
            records.Add(map(rd));
        }

        return records;
    }

    public static T? MapFirst<T>(
        this IDataReader rd,
        Func<IDataReader, T> map)
    {
        if (rd.Read())
        {
            return map(rd);
        }
        else
        {
            return default;
        }
    }

    public static IEnumerable<T> MapNext<T>(
        this IDataReader rd,
        Func<IDataReader, T> map) =>
        rd.NextResult() ? rd.Map(map) : [];

    public static T? MapFirstNext<T>(
        this IDataReader rd,
        Func<IDataReader, T> map) =>
        rd.NextResult() ? rd.MapFirst(map) : default;

    public static async Task<IEnumerable<T>> MapAsync<T>(
        this IDataReader rd,
        Func<IDataReader, T> map,
        CancellationToken? cancellationToken = null)
    {
        var records = new List<T>();
        if (rd is DbDataReader dbRd)
        {
            while (await dbRd.ReadAsync(cancellationToken ?? CancellationToken.None))
            {
                records.Add(map(rd));
            }
        }

        return records;
    }

    public static async Task<IEnumerable<T>> MapNextAsync<T>(
        this IDataReader rd,
        Func<IDataReader, T> map,
        CancellationToken? cancellationToken = null)
    {
        if (rd is DbDataReader dbRd)
        {
            if (await dbRd.NextResultAsync(cancellationToken ?? CancellationToken.None))
            {
                return await rd.MapAsync(map, cancellationToken);
            }
        }

        return [];
    }

    public static async Task<T?> MapFirstAsync<T>(
        this IDataReader rd,
        Func<IDataReader, T> map,
        CancellationToken? cancellationToken = null)
    {
        if (rd is DbDataReader dbRd)
        {
            if (await dbRd.ReadAsync(cancellationToken ?? CancellationToken.None))
            {
                return map(dbRd);
            }

            return default;
        }
        else
        {
            return default;
        }
    }

    public static async Task<T?> MapFirstNextAsync<T>(
        this IDataReader rd,
        Func<IDataReader, T> map,
        CancellationToken? cancellationToken = null)
    {
        if (rd is DbDataReader dbRd)
        {
            if (await dbRd.NextResultAsync(cancellationToken ?? CancellationToken.None))
            {
                return await rd.MapFirstAsync(map, cancellationToken);
            }

            return default;
        }
        else
        {
            return default;
        }
    }
}
