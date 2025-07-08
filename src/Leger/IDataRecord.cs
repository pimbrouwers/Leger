namespace Leger
{
    using System;
    using System.Data;
    using System.IO;

    /// <summary>
    /// Extensions for <see cref="IDataRecord"/>.
    /// </summary>
    public static class IDataRecordExtensions
    {
        /// <summary> Reads a string from the <see cref="IDataRecord"/>. </summary>
        public static string ReadString(this IDataRecord rd, string field) =>
            rd.ReadNullableString(field) ?? string.Empty;

        /// <summary> Reads a char from the <see cref="IDataRecord"/>. </summary>
        public static char ReadChar(this IDataRecord rd, string field) =>
            rd.ReadNullableChar(field) ?? char.MinValue;

        /// <summary> Reads a boolean from the <see cref="IDataRecord"/>. </summary>
        public static bool ReadBoolean(this IDataRecord rd, string field) =>
            rd.ReadNullableBoolean(field) ?? false;

        /// <summary> Reads a boolean from the <see cref="IDataRecord"/>. </summary>
        public static bool ReadBool(this IDataRecord rd, string field) =>
            rd.ReadBoolean(field);

        /// <summary> Reads a byte from the <see cref="IDataRecord"/>. </summary>
        public static byte ReadByte(this IDataRecord rd, string field) =>
            rd.ReadNullableByte(field) ?? 0;

        /// <summary> Reads a short from the <see cref="IDataRecord"/>. </summary>
        public static short ReadInt16(this IDataRecord rd, string field) =>
            rd.ReadNullableInt16(field) ?? 0;

        /// <summary> Reads a short from the <see cref="IDataRecord"/>. </summary>
        public static short ReadShort(this IDataRecord rd, string field) =>
            rd.ReadInt16(field);

        /// <summary> Reads an int from the <see cref="IDataRecord"/>. </summary>
        public static int ReadInt32(this IDataRecord rd, string field) =>
            rd.ReadNullableInt32(field) ?? 0;

        /// <summary> Reads an int from the <see cref="IDataRecord"/>. </summary>
        public static int ReadInt(this IDataRecord rd, string field) =>
            rd.ReadInt32(field);

        /// <summary> Reads a long from the <see cref="IDataRecord"/>. </summary>
        public static long ReadInt64(this IDataRecord rd, string field) =>
            rd.ReadNullableInt64(field) ?? 0L;

        /// <summary> Reads a long from the <see cref="IDataRecord"/>. </summary>
        public static long ReadLong(this IDataRecord rd, string field) =>
            rd.ReadInt64(field);

        /// <summary> Reads a decimal from the <see cref="IDataRecord"/>. </summary>
        public static decimal ReadDecimal(this IDataRecord rd, string field) =>
            rd.ReadNullableDecimal(field) ?? 0.0M;

        /// <summary> Reads a double from the <see cref="IDataRecord"/>. </summary>
        public static double ReadDouble(this IDataRecord rd, string field) =>
            rd.ReadNullableDouble(field) ?? 0.0;

        /// <summary> Reads a float from the <see cref="IDataRecord"/>. </summary>
        public static float ReadFloat(this IDataRecord rd, string field) =>
            rd.ReadNullableFloat(field) ?? 0.0F;

        /// <summary> Reads a guid from the <see cref="IDataRecord"/>. </summary>
        public static Guid ReadGuid(this IDataRecord rd, string field) =>
            rd.ReadNullableGuid(field) ?? Guid.Empty;

        /// <summary> Reads a date time from the <see cref="IDataRecord"/>. </summary>
        public static DateTime ReadDateTime(this IDataRecord rd, string field) =>
            rd.ReadNullableDateTime(field) ?? DateTime.MinValue;
#if NET6_0_OR_GREATER
        /// <summary> Reads a date from the <see cref="IDataRecord"/>. </summary>
        public static DateOnly ReadDateOnly(this IDataRecord rd, string field) =>
            rd.ReadNullableDateOnly(field) ?? DateOnly.MinValue;

        /// <summary> Reads a time from the <see cref="IDataRecord"/>. </summary>
        public static TimeOnly ReadTimeOnly(this IDataRecord rd, string field) =>
            rd.ReadNullableTimeOnly(field) ?? TimeOnly.MinValue;
#endif
        /// <summary> Reads a byte array from the <see cref="IDataRecord"/>. </summary>
        public static byte[] ReadBytes(this IDataRecord rd, string field) =>
            rd.ReadRefValueByField(field, rd.StreamBytes) ?? new byte[] { };

        /// <summary> Reads a nullable string from the <see cref="IDataRecord"/>. </summary>
        public static string ReadNullableString(this IDataRecord rd, string field) =>
            rd.ReadRefValueByField(field, rd.GetString);

        /// <summary> Reads a nullable char from the <see cref="IDataRecord"/>. </summary>
        public static char? ReadNullableChar(this IDataRecord rd, string field) =>
            rd.ReadRefValueByField(field, rd.GetChar);

        /// <summary> Reads a nullable boolean from the <see cref="IDataRecord"/>. </summary>
        public static bool? ReadNullableBoolean(this IDataRecord rd, string field) =>
            rd.ReadValueByField(field, rd.GetBoolean);

        /// <summary> Reads a nullable boolean from the <see cref="IDataRecord"/>. </summary>
        public static bool? ReadNullableBool(this IDataRecord rd, string field) =>
            rd.ReadNullableBoolean(field);

        /// <summary> Reads a nullable byte from the <see cref="IDataRecord"/>. </summary>
        public static byte? ReadNullableByte(this IDataRecord rd, string field) =>
            rd.ReadValueByField(field, rd.GetByte);

        /// <summary> Reads a nullable short from the <see cref="IDataRecord"/>. </summary>
        public static short? ReadNullableInt16(this IDataRecord rd, string field) =>
            rd.ReadValueByField(field, rd.GetInt16);

        /// <summary> Reads a nullable short from the <see cref="IDataRecord"/>. </summary>
        public static short? ReadNullableShort(this IDataRecord rd, string field) =>
            rd.ReadNullableInt16(field);

        /// <summary> Reads a nullable int from the <see cref="IDataRecord"/>. </summary>
        public static int? ReadNullableInt32(this IDataRecord rd, string field) =>
            rd.ReadValueByField(field, rd.GetInt32);

        /// <summary> Reads a nullable int from the <see cref="IDataRecord"/>. </summary>
        public static int? ReadNullableInt(this IDataRecord rd, string field) =>
            rd.ReadNullableInt32(field);

        /// <summary> Reads a nullable long from the <see cref="IDataRecord"/>. </summary>
        public static long? ReadNullableInt64(this IDataRecord rd, string field) =>
            rd.ReadValueByField(field, rd.GetInt64);

        /// <summary> Reads a nullable long from the <see cref="IDataRecord"/>. </summary>
        public static long? ReadNullableLong(this IDataRecord rd, string field) =>
            rd.ReadNullableInt64(field);

        /// <summary> Reads a nullable decimal from the <see cref="IDataRecord"/>. </summary>
        public static decimal? ReadNullableDecimal(this IDataRecord rd, string field) =>
            rd.ReadValueByField(field, rd.GetDecimal);

        /// <summary> Reads a nullable double from the <see cref="IDataRecord"/>. </summary>
        public static double? ReadNullableDouble(this IDataRecord rd, string field) =>
            rd.ReadValueByField(field, rd.GetDouble);

        /// <summary> Reads a nullable float from the <see cref="IDataRecord"/>. </summary>
        public static float? ReadNullableFloat(this IDataRecord rd, string field) =>
            rd.ReadValueByField(field, rd.GetFloat);

        /// <summary> Reads a nullable guid from the <see cref="IDataRecord"/>. </summary>
        public static Guid? ReadNullableGuid(this IDataRecord rd, string field) =>
            rd.ReadValueByField(field, rd.GetGuid);

        /// <summary> Reads a nullable date time from the <see cref="IDataRecord"/>. </summary>
        public static DateTime? ReadNullableDateTime(this IDataRecord rd, string field) =>
            rd.ReadValueByField(field, rd.GetDateTime);
#if NET6_0_OR_GREATER
        /// <summary> Reads a nullable date from the <see cref="IDataRecord"/>. </summary>
        public static DateOnly? ReadNullableDateOnly(this IDataRecord rd, string field) =>
            rd.ReadValueByField(field, i => DateOnly.FromDateTime(rd.GetDateTime(i)));

        /// <summary> Reads a nullable time from the <see cref="IDataRecord"/>. </summary>
        public static TimeOnly? ReadNullableTimeOnly(this IDataRecord rd, string field) =>
            rd.ReadValueByField(field, i => TimeOnly.FromDateTime(rd.GetDateTime(i)));
#endif
        private static T ReadRefValueByField<T>(
            this IDataRecord rd,
            string fieldName,
            Func<int, T> map)
        {

            try
            {
                var i = rd.GetOrdinal(fieldName);

                if (rd.IsDBNull(i))
                {
                    throw new InvalidCastException($"Field '{fieldName}' is null.");
                }

                return map(i);
            }
            catch (IndexOutOfRangeException ex)
            {
                throw new DatabaseExecutionException(DatabaseErrorCode.FieldNotFound, fieldName, ex);
            }
            catch (InvalidCastException ex)
            {
                throw new DatabaseExecutionException(DatabaseErrorCode.CouldNotCastValue, fieldName, ex);
            }
        }

        private static T? ReadValueByField<T>(
            this IDataRecord rd,
            string fieldName,
            Func<int, T> map) where T : struct
        {

            try
            {
                var i = rd.GetOrdinal(fieldName);

                if (rd.IsDBNull(i))
                {
                    return null;
                }

                return map(i);
            }
            catch (IndexOutOfRangeException ex)
            {
                throw new DatabaseExecutionException(DatabaseErrorCode.FieldNotFound, fieldName, ex);
            }
            catch (InvalidCastException ex)
            {
                throw new DatabaseExecutionException(DatabaseErrorCode.CouldNotCastValue, fieldName, ex);
            }
        }

        private static byte[] StreamBytes(this IDataRecord rd, int ordinal)
        {
            var bufferSize = 1024;
            var buffer = new byte[bufferSize];
            long bytesReturned;
            var startIndex = 0;

            using (var ms = new MemoryStream())
            {
                bytesReturned = rd.GetBytes(ordinal, startIndex, buffer, 0, bufferSize);
                while (bytesReturned == bufferSize)
                {
                    ms.Write(buffer, 0, (int)bytesReturned);
                    ms.Flush();

                    startIndex += bufferSize;
                    bytesReturned = rd.GetBytes(ordinal, startIndex, buffer, 0, bufferSize);
                }

                ms.Write(buffer, 0, (int)bytesReturned);
                ms.Flush();

                return ms.ToArray();
            }
        }
    }
}
