namespace Leger {
    using System;
    using System.Data;
    using System.IO;

    /// <summary>
    /// Extensions for <see cref="IDataRecord"/>.
    /// </summary>
    public static class IDataRecordExtensions {
        //
        // Field-based reading

        /// <summary> Reads a string from the <see cref="IDataRecord"/>. </summary>
        public static string ReadString(this IDataRecord rd, string field) => rd.ReadNullableString(field) ?? string.Empty;

        /// <summary> Reads a char from the <see cref="IDataRecord"/>. </summary>
        public static char ReadChar(this IDataRecord rd, string field) => rd.ReadNullableChar(field) ?? char.MinValue;

        /// <summary> Reads a boolean from the <see cref="IDataRecord"/>. </summary>
        public static bool ReadBoolean(this IDataRecord rd, string field) => rd.ReadNullableBoolean(field) ?? false;

        /// <summary> Reads a boolean from the <see cref="IDataRecord"/>. </summary>
        public static bool ReadBool(this IDataRecord rd, string field) => rd.ReadBoolean(field);

        /// <summary> Reads a byte from the <see cref="IDataRecord"/>. </summary>
        public static byte ReadByte(this IDataRecord rd, string field) => rd.ReadNullableByte(field) ?? 0;

        /// <summary> Reads a short from the <see cref="IDataRecord"/>. </summary>
        public static short ReadInt16(this IDataRecord rd, string field) => rd.ReadNullableInt16(field) ?? 0;

        /// <summary> Reads a short from the <see cref="IDataRecord"/>. </summary>
        public static short ReadShort(this IDataRecord rd, string field) => rd.ReadInt16(field);

        /// <summary> Reads an int from the <see cref="IDataRecord"/>. </summary>
        public static int ReadInt32(this IDataRecord rd, string field) => rd.ReadNullableInt32(field) ?? 0;

        /// <summary> Reads an int from the <see cref="IDataRecord"/>. </summary>
        public static int ReadInt(this IDataRecord rd, string field) => rd.ReadInt32(field);

        /// <summary> Reads a long from the <see cref="IDataRecord"/>. </summary>
        public static long ReadInt64(this IDataRecord rd, string field) => rd.ReadNullableInt64(field) ?? 0L;

        /// <summary> Reads a long from the <see cref="IDataRecord"/>. </summary>
        public static long ReadLong(this IDataRecord rd, string field) => rd.ReadInt64(field);

        /// <summary> Reads a decimal from the <see cref="IDataRecord"/>. </summary>
        public static decimal ReadDecimal(this IDataRecord rd, string field) => rd.ReadNullableDecimal(field) ?? 0.0M;

        /// <summary> Reads a double from the <see cref="IDataRecord"/>. </summary>
        public static double ReadDouble(this IDataRecord rd, string field) => rd.ReadNullableDouble(field) ?? 0.0;

        /// <summary> Reads a float from the <see cref="IDataRecord"/>. </summary>
        public static float ReadFloat(this IDataRecord rd, string field) => rd.ReadNullableFloat(field) ?? 0.0F;

        /// <summary> Reads a guid from the <see cref="IDataRecord"/>. </summary>
        public static Guid ReadGuid(this IDataRecord rd, string field) => rd.ReadNullableGuid(field) ?? Guid.Empty;

        /// <summary> Reads a date time from the <see cref="IDataRecord"/>. </summary>
        public static DateTime ReadDateTime(this IDataRecord rd, string field) => rd.ReadNullableDateTime(field) ?? DateTime.MinValue;

        /// <summary> Reads a byte array from the <see cref="IDataRecord"/>. </summary>
        public static byte[] ReadBytes(this IDataRecord rd, string field) => rd.ReadRefValueByField(field, rd.StreamBytes) ?? Array.Empty<byte>();

        /// <summary> Reads a nullable string from the <see cref="IDataRecord"/>. </summary>
        public static string? ReadNullableString(this IDataRecord rd, string field) => rd.ReadRefValueByField(field, rd.GetString);

        /// <summary> Reads a nullable char from the <see cref="IDataRecord"/>. </summary>
        public static char? ReadNullableChar(this IDataRecord rd, string field) => rd.ReadRefValueByField(field, rd.GetChar);

        /// <summary> Reads a nullable boolean from the <see cref="IDataRecord"/>. </summary>
        public static bool? ReadNullableBoolean(this IDataRecord rd, string field) => rd.ReadValueByField(field, rd.GetBoolean);

        /// <summary> Reads a nullable boolean from the <see cref="IDataRecord"/>. </summary>
        public static bool? ReadNullableBool(this IDataRecord rd, string field) => rd.ReadNullableBoolean(field);

        /// <summary> Reads a nullable byte from the <see cref="IDataRecord"/>. </summary>
        public static byte? ReadNullableByte(this IDataRecord rd, string field) => rd.ReadValueByField(field, rd.GetByte);

        /// <summary> Reads a nullable short from the <see cref="IDataRecord"/>. </summary>
        public static short? ReadNullableInt16(this IDataRecord rd, string field) => rd.ReadValueByField(field, rd.GetInt16);

        /// <summary> Reads a nullable short from the <see cref="IDataRecord"/>. </summary>
        public static short? ReadNullableShort(this IDataRecord rd, string field) => rd.ReadNullableInt16(field);

        /// <summary> Reads a nullable int from the <see cref="IDataRecord"/>. </summary>
        public static int? ReadNullableInt32(this IDataRecord rd, string field) => rd.ReadValueByField(field, rd.GetInt32);

        /// <summary> Reads a nullable int from the <see cref="IDataRecord"/>. </summary>
        public static int? ReadNullableInt(this IDataRecord rd, string field) => rd.ReadNullableInt32(field);

        /// <summary> Reads a nullable long from the <see cref="IDataRecord"/>. </summary>
        public static long? ReadNullableInt64(this IDataRecord rd, string field) => rd.ReadValueByField(field, rd.GetInt64);

        /// <summary> Reads a nullable long from the <see cref="IDataRecord"/>. </summary>
        public static long? ReadNullableLong(this IDataRecord rd, string field) => rd.ReadNullableInt64(field);

        /// <summary> Reads a nullable decimal from the <see cref="IDataRecord"/>. </summary>
        public static decimal? ReadNullableDecimal(this IDataRecord rd, string field) => rd.ReadValueByField(field, rd.GetDecimal);

        /// <summary> Reads a nullable double from the <see cref="IDataRecord"/>. </summary>
        public static double? ReadNullableDouble(this IDataRecord rd, string field) => rd.ReadValueByField(field, rd.GetDouble);

        /// <summary> Reads a nullable float from the <see cref="IDataRecord"/>. </summary>
        public static float? ReadNullableFloat(this IDataRecord rd, string field) => rd.ReadValueByField(field, rd.GetFloat);

        /// <summary> Reads a nullable guid from the <see cref="IDataRecord"/>. </summary>
        public static Guid? ReadNullableGuid(this IDataRecord rd, string field) => rd.ReadValueByField(field, rd.GetGuid);

        /// <summary> Reads a nullable date time from the <see cref="IDataRecord"/>. </summary>
        public static DateTime? ReadNullableDateTime(this IDataRecord rd, string field) => rd.ReadValueByField(field, rd.GetDateTime);

        //
        // Index-based reading

        /// <summary> Reads a string from the <see cref="IDataRecord"/>. </summary>
        public static string ReadString(this IDataRecord rd, int i) => rd.ReadNullableString(i) ?? string.Empty;

        /// <summary> Reads a char from the <see cref="IDataRecord"/>. </summary>
        public static char ReadChar(this IDataRecord rd, int i) => rd.ReadNullableChar(i) ?? char.MinValue;

        /// <summary> Reads a boolean from the <see cref="IDataRecord"/>. </summary>
        public static bool ReadBoolean(this IDataRecord rd, int i) => rd.ReadNullableBoolean(i) ?? false;

        /// <summary> Reads a boolean from the <see cref="IDataRecord"/>. </summary>
        public static bool ReadBool(this IDataRecord rd, int i) => rd.ReadBoolean(i);

        /// <summary> Reads a byte from the <see cref="IDataRecord"/>. </summary>
        public static byte ReadByte(this IDataRecord rd, int i) => rd.ReadNullableByte(i) ?? 0;

        /// <summary> Reads a short from the <see cref="IDataRecord"/>. </summary>
        public static short ReadInt16(this IDataRecord rd, int i) => rd.ReadNullableInt16(i) ?? 0;

        /// <summary> Reads a short from the <see cref="IDataRecord"/>. </summary>
        public static short ReadShort(this IDataRecord rd, int i) => rd.ReadInt16(i);

        /// <summary> Reads an int from the <see cref="IDataRecord"/>. </summary>
        public static int ReadInt32(this IDataRecord rd, int i) => rd.ReadNullableInt32(i) ?? 0;

        /// <summary> Reads an int from the <see cref="IDataRecord"/>. </summary>
        public static int ReadInt(this IDataRecord rd, int i) => rd.ReadInt32(i);

        /// <summary> Reads a long from the <see cref="IDataRecord"/>. </summary>
        public static long ReadInt64(this IDataRecord rd, int i) => rd.ReadNullableInt64(i) ?? 0L;

        /// <summary> Reads a long from the <see cref="IDataRecord"/>. </summary>
        public static long ReadLong(this IDataRecord rd, int i) => rd.ReadInt64(i);

        /// <summary> Reads a decimal from the <see cref="IDataRecord"/>. </summary>
        public static decimal ReadDecimal(this IDataRecord rd, int i) => rd.ReadNullableDecimal(i) ?? 0.0M;

        /// <summary> Reads a double from the <see cref="IDataRecord"/>. </summary>
        public static double ReadDouble(this IDataRecord rd, int i) => rd.ReadNullableDouble(i) ?? 0.0;

        /// <summary> Reads a float from the <see cref="IDataRecord"/>. </summary>
        public static float ReadFloat(this IDataRecord rd, int i) => rd.ReadNullableFloat(i) ?? 0.0F;

        /// <summary> Reads a guid from the <see cref="IDataRecord"/>. </summary>
        public static Guid ReadGuid(this IDataRecord rd, int i) => rd.ReadNullableGuid(i) ?? Guid.Empty;

        /// <summary> Reads a date time from the <see cref="IDataRecord"/>. </summary>
        public static DateTime ReadDateTime(this IDataRecord rd, int i) => rd.ReadNullableDateTime(i) ?? DateTime.MinValue;

        /// <summary> Reads a byte array from the <see cref="IDataRecord"/>. </summary>
        public static byte[] ReadBytes(this IDataRecord rd, int i) => rd.ReadRefValueByIndex(i, rd.StreamBytes) ?? Array.Empty<byte>();

        /// <summary> Reads a nullable string from the <see cref="IDataRecord"/>. </summary>
        public static string? ReadNullableString(this IDataRecord rd, int i) => rd.ReadRefValueByIndex(i, rd.GetString);

        /// <summary> Reads a nullable char from the <see cref="IDataRecord"/>. </summary>
        public static char? ReadNullableChar(this IDataRecord rd, int i) => rd.ReadRefValueByIndex(i, rd.GetChar);

        /// <summary> Reads a nullable boolean from the <see cref="IDataRecord"/>. </summary>
        public static bool? ReadNullableBoolean(this IDataRecord rd, int i) => rd.ReadValueByIndex(i, rd.GetBoolean);

        /// <summary> Reads a nullable boolean from the <see cref="IDataRecord"/>. </summary>
        public static bool? ReadNullableBool(this IDataRecord rd, int i) => rd.ReadNullableBoolean(i);

        /// <summary> Reads a nullable byte from the <see cref="IDataRecord"/>. </summary>
        public static byte? ReadNullableByte(this IDataRecord rd, int i) => rd.ReadValueByIndex(i, rd.GetByte);

        /// <summary> Reads a nullable short from the <see cref="IDataRecord"/>. </summary>
        public static short? ReadNullableInt16(this IDataRecord rd, int i) => rd.ReadValueByIndex(i, rd.GetInt16);

        /// <summary> Reads a nullable short from the <see cref="IDataRecord"/>. </summary>
        public static short? ReadNullableShort(this IDataRecord rd, int i) => rd.ReadNullableInt16(i);

        /// <summary> Reads a nullable int from the <see cref="IDataRecord"/>. </summary>
        public static int? ReadNullableInt32(this IDataRecord rd, int i) => rd.ReadValueByIndex(i, rd.GetInt32);

        /// <summary> Reads a nullable int from the <see cref="IDataRecord"/>. </summary>
        public static int? ReadNullableInt(this IDataRecord rd, int i) => rd.ReadNullableInt32(i);

        /// <summary> Reads a nullable long from the <see cref="IDataRecord"/>. </summary>
        public static long? ReadNullableInt64(this IDataRecord rd, int i) => rd.ReadValueByIndex(i, rd.GetInt64);

        /// <summary> Reads a nullable long from the <see cref="IDataRecord"/>. </summary>
        public static long? ReadNullableLong(this IDataRecord rd, int i) => rd.ReadNullableInt64(i);

        /// <summary> Reads a nullable decimal from the <see cref="IDataRecord"/>. </summary>
        public static decimal? ReadNullableDecimal(this IDataRecord rd, int i) => rd.ReadValueByIndex(i, rd.GetDecimal);

        /// <summary> Reads a nullable double from the <see cref="IDataRecord"/>. </summary>
        public static double? ReadNullableDouble(this IDataRecord rd, int i) => rd.ReadValueByIndex(i, rd.GetDouble);

        /// <summary> Reads a nullable float from the <see cref="IDataRecord"/>. </summary>
        public static float? ReadNullableFloat(this IDataRecord rd, int i) => rd.ReadValueByIndex(i, rd.GetFloat);

        /// <summary> Reads a nullable guid from the <see cref="IDataRecord"/>. </summary>
        public static Guid? ReadNullableGuid(this IDataRecord rd, int i) => rd.ReadValueByIndex(i, rd.GetGuid);

        /// <summary> Reads a nullable date time from the <see cref="IDataRecord"/>. </summary>
        public static DateTime? ReadNullableDateTime(this IDataRecord rd, int i) => rd.ReadValueByIndex(i, rd.GetDateTime);

#if NET6_0_OR_GREATER
        /// <summary> Reads a date from the <see cref="IDataRecord"/>. </summary>
        public static DateOnly ReadDateOnly(this IDataRecord rd, string field) => rd.ReadNullableDateOnly(field) ?? DateOnly.MinValue;

        /// <summary> Reads a time from the <see cref="IDataRecord"/>. </summary>
        public static TimeOnly ReadTimeOnly(this IDataRecord rd, string field) => rd.ReadNullableTimeOnly(field) ?? TimeOnly.MinValue;

        /// <summary> Reads a nullable date from the <see cref="IDataRecord"/>. </summary>
        public static DateOnly? ReadNullableDateOnly(this IDataRecord rd, string field) => rd.ReadValueByField(field, i => DateOnly.FromDateTime(rd.GetDateTime(i)));

        /// <summary> Reads a nullable time from the <see cref="IDataRecord"/>. </summary>
        public static TimeOnly? ReadNullableTimeOnly(this IDataRecord rd, string field) => rd.ReadValueByField(field, i => TimeOnly.FromDateTime(rd.GetDateTime(i)));

        /// <summary> Reads a date from the <see cref="IDataRecord"/>. </summary>
        public static DateOnly ReadDateOnly(this IDataRecord rd, int i) => rd.ReadNullableDateOnly(i) ?? DateOnly.MinValue;

        /// <summary> Reads a time from the <see cref="IDataRecord"/>. </summary>
        public static TimeOnly ReadTimeOnly(this IDataRecord rd, int i) => rd.ReadNullableTimeOnly(i) ?? TimeOnly.MinValue;

        /// <summary> Reads a nullable date from the <see cref="IDataRecord"/>. </summary>
        public static DateOnly? ReadNullableDateOnly(this IDataRecord rd, int i) => rd.ReadValueByIndex(i, idx => DateOnly.FromDateTime(rd.GetDateTime(idx)));

        /// <summary> Reads a nullable time from the <see cref="IDataRecord"/>. </summary>
        public static TimeOnly? ReadNullableTimeOnly(this IDataRecord rd, int i) => rd.ReadValueByIndex(i, idx => TimeOnly.FromDateTime(rd.GetDateTime(idx)));
#endif

        private static T ReadRefValueByField<T>(
            this IDataRecord rd,
            string fieldName,
            Func<int, T> map) {

            try {
                var i = rd.GetOrdinal(fieldName);
                return ReadRefValueByIndex(rd, i, map);
            }
            catch (IndexOutOfRangeException ex) {
                throw new DatabaseReadFieldException(DatabaseErrorCode.FieldNotFound, fieldName, ex);
            }
            catch (ArgumentOutOfRangeException ex) {
                throw new DatabaseReadFieldException(DatabaseErrorCode.FieldNotFound, fieldName, ex);
            }
            catch (Exception ex) {
                if (ex is DatabaseReadFieldException) {
                    throw;
                }
                throw new DatabaseReadFieldException(DatabaseErrorCode.CouldNotCastValue, fieldName, ex);
            }
        }

        private static T? ReadValueByField<T>(
            this IDataRecord rd,
            string fieldName,
            Func<int, T> map) where T : struct {

            try {
                var i = rd.GetOrdinal(fieldName);
                return ReadValueByIndex(rd, i, map);
            }
            catch (IndexOutOfRangeException ex) {
                throw new DatabaseReadFieldException(DatabaseErrorCode.FieldNotFound, fieldName, ex);
            }
            catch (ArgumentOutOfRangeException ex) {
                throw new DatabaseReadFieldException(DatabaseErrorCode.FieldNotFound, fieldName, ex);
            }
            catch (Exception ex) {
                if (ex is DatabaseReadFieldException) {
                    throw;
                }

                throw new DatabaseReadFieldException(DatabaseErrorCode.CouldNotCastValue, fieldName, ex);
            }
        }

        private static T ReadRefValueByIndex<T>(
            this IDataRecord rd,
            int i,
            Func<int, T> map) {

            try {
                if (rd.IsDBNull(i)) {
                    return default!;
                }

                return map(i);
            }
            catch (IndexOutOfRangeException ex) {
                throw new DatabaseReadFieldException(DatabaseErrorCode.FieldNotFound, $"Field at index: {i}", ex);
            }
            catch (ArgumentOutOfRangeException ex) {
                throw new DatabaseReadFieldException(DatabaseErrorCode.FieldNotFound, $"Field at index: {i}", ex);
            }
        }

        private static T? ReadValueByIndex<T>(
            this IDataRecord rd,
            int i,
            Func<int, T> map) where T : struct {

            try {
                if (rd.IsDBNull(i)) {
                    return default;
                }

                return map(i);
            }
            catch (IndexOutOfRangeException ex) {
                throw new DatabaseReadFieldException(DatabaseErrorCode.FieldNotFound, $"Field at index: {i}", ex);
            }
            catch (ArgumentOutOfRangeException ex) {
                throw new DatabaseReadFieldException(DatabaseErrorCode.FieldNotFound, $"Field at index: {i}", ex);
            }
        }


        private static byte[] StreamBytes(this IDataRecord rd, int ordinal) {
            var bufferSize = 1024;
            var buffer = new byte[bufferSize];
            long bytesReturned;
            var startIndex = 0;

            using (var ms = new MemoryStream()) {
                bytesReturned = rd.GetBytes(ordinal, startIndex, buffer, 0, bufferSize);
                while (bytesReturned == bufferSize) {
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
