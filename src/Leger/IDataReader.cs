namespace Leger
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Extensions for <see cref="IDataReader"/>.
    /// </summary>
    public static class IDataReaderExtensions
    {
        /// <summary>
        /// Maps the <see cref="IDataReader"/> to a collection of <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="rd"></param>
        /// <param name="map"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Maps the <see cref="IDataReader"/> to a single <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="rd"></param>
        /// <param name="map"></param>
        /// <returns></returns>
        public static T MapFirst<T>(
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

        /// <summary>
        /// Attempts to advance the <see cref="IDataReader"/> then maps to a collection of <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="rd"></param>
        /// <param name="map"></param>
        /// <returns></returns>
        public static IEnumerable<T> MapNext<T>(
            this IDataReader rd,
            Func<IDataReader, T> map) =>
            rd.NextResult() ? rd.Map(map) : Enumerable.Empty<T>();

        /// <summary>
        /// Attempts to advance the <see cref="IDataReader"/> then maps to a single <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="rd"></param>
        /// <param name="map"></param>
        /// <returns></returns>
        public static T MapFirstNext<T>(
            this IDataReader rd,
            Func<IDataReader, T> map) =>
            rd.NextResult() ? rd.MapFirst(map) : default;

        /// <summary>
        /// Maps the <see cref="IDataReader"/> to a collection of <typeparamref name="T"/> asynchronously.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="rd"></param>
        /// <param name="map"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Maps the <see cref="IDataReader"/> to a single <typeparamref name="T"/> asynchronously.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="rd"></param>
        /// <param name="map"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
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

            return Enumerable.Empty<T>();
        }

        /// <summary>
        /// Maps the <see cref="IDataReader"/> to a single <typeparamref name="T"/> asynchronously.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="rd"></param>
        /// <param name="map"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<T> MapFirstAsync<T>(
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

        /// <summary>
        /// Maps the <see cref="IDataReader"/> to a single <typeparamref name="T"/> asynchronously.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="rd"></param>
        /// <param name="map"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public static async Task<T> MapFirstNextAsync<T>(
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
}
