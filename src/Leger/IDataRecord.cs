namespace Leger;

using System.Data;

public static class IDataRecordExtensions
{
    public static string ReadString(this IDataRecord rd, string field) =>
        rd.ReadValueByField(field, rd.GetString) ?? string.Empty;

    public static char ReadChar(this IDataRecord rd, string field) =>
        rd.ReadValueByField(field, rd.GetChar);

    public static bool ReadBoolean(this IDataRecord rd, string field) =>
        rd.ReadValueByField(field, rd.GetBoolean);

    public static bool ReadBool(this IDataRecord rd, string field) =>
        rd.ReadBoolean(field);

    public static byte ReadByte(this IDataRecord rd, string field) =>
        rd.ReadValueByField(field, rd.GetByte);

    public static short ReadInt16(this IDataRecord rd, string field) =>
        rd.ReadValueByField(field, rd.GetInt16);

    public static short ReadShort(this IDataRecord rd, string field) =>
        rd.ReadInt16(field);

    public static int ReadInt32(this IDataRecord rd, string field) =>
        rd.ReadValueByField(field, rd.GetInt32);

    public static int ReadInt(this IDataRecord rd, string field) =>
        rd.ReadInt32(field);

    public static long ReadInt64(this IDataRecord rd, string field) =>
        rd.ReadValueByField(field, rd.GetInt64);

    public static long ReadLong(this IDataRecord rd, string field) =>
        rd.ReadInt64(field);

    public static decimal ReadDecimal(this IDataRecord rd, string field) =>
        rd.ReadValueByField(field, rd.GetDecimal);

    public static double ReadDouble(this IDataRecord rd, string field) =>
        rd.ReadValueByField(field, rd.GetDouble);

    public static float ReadFloat(this IDataRecord rd, string field) =>
        rd.ReadValueByField(field, rd.GetFloat);

    public static Guid ReadGuid(this IDataRecord rd, string field) =>
        rd.ReadValueByField(field, rd.GetGuid);

    public static DateTime ReadDateTime(this IDataRecord rd, string field) =>
        rd.ReadValueByField(field, rd.GetDateTime);

    public static byte[] ReadBytes(this IDataRecord rd, string field) =>
        rd.ReadValueByField(field, rd.StreamBytes) ?? [];

    public static string? ReadNullableString(this IDataRecord rd, string field) =>
        rd.ReadValueByField(field, rd.GetString);

    public static char? ReadNullableChar(this IDataRecord rd, string field) =>
        rd.ReadValueByField(field, rd.GetChar);

    public static bool? ReadNullableBoolean(this IDataRecord rd, string field) =>
        rd.ReadNullableValueByField(field, rd.GetBoolean);

    public static bool? ReadNullableBool(this IDataRecord rd, string field) =>
        rd.ReadNullableBoolean(field);

    public static byte? ReadNullableByte(this IDataRecord rd, string field) =>
        rd.ReadNullableValueByField(field, rd.GetByte);

    public static short? ReadNullableInt16(this IDataRecord rd, string field) =>
        rd.ReadNullableValueByField(field, rd.GetInt16);

    public static short? ReadNullableShort(this IDataRecord rd, string field) =>
        rd.ReadNullableInt16(field);

    public static int? ReadNullableInt32(this IDataRecord rd, string field) =>
        rd.ReadNullableValueByField(field, rd.GetInt32);

    public static int? ReadNullableInt(this IDataRecord rd, string field) =>
        rd.ReadNullableInt32(field);

    public static long? ReadNullableInt64(this IDataRecord rd, string field) =>
        rd.ReadNullableValueByField(field, rd.GetInt64);

    public static long? ReadNullableLong(this IDataRecord rd, string field) =>
        rd.ReadNullableInt64(field);

    public static decimal? ReadNullableDecimal(this IDataRecord rd, string field) =>
        rd.ReadNullableValueByField(field, rd.GetDecimal);

    public static double? ReadNullableDouble(this IDataRecord rd, string field) =>
        rd.ReadNullableValueByField(field, rd.GetDouble);

    public static float? ReadNullableFloat(this IDataRecord rd, string field) =>
        rd.ReadNullableValueByField(field, rd.GetFloat);

    public static Guid? ReadNullableGuid(this IDataRecord rd, string field) =>
        rd.ReadNullableValueByField(field, rd.GetGuid);

    public static DateTime? ReadNullableDateTime(this IDataRecord rd, string field) =>
        rd.ReadNullableValueByField(field, rd.GetDateTime);

    private static T? ReadValueByField<T>(this IDataRecord rd, string fieldName, Func<int, T?> map)
    {
        var i = rd.GetOrdinal(fieldName);

        if (rd.IsDBNull(i))
        {
            return default;
        }

        try
        {
            return map(i);
        }
        catch (InvalidCastException ex)
        {
            throw new DatabaseExecutionException(DatabaseErrorCode.CouldNotCastValue, fieldName, ex);
        }
    }

    private static T? ReadNullableValueByField<T>(
        this IDataRecord rd,
        string fieldName,
        Func<int, T> map) where T : struct
    {
        var i = rd.GetOrdinal(fieldName);

        if (rd.IsDBNull(i))
        {
            return null;
        }

        try
        {
            return map(i);
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

        using var ms = new MemoryStream();
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
