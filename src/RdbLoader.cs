using System.Collections.Concurrent;
using System.Text;

/// <summary>
/// Deserializes a Redis Database (RDB) snapshot file and populates the in-memory data store.
/// </summary>
static class RdbLoader
{
    /// <summary>
    /// Reads the RDB file at <paramref name="filePath"/> and populates <paramref name="dataStore"/>
    /// with all persisted key-value pairs and their expiry timestamps.
    /// Does nothing if the file does not exist.
    /// </summary>
    /// <param name="filePath">Absolute path to the RDB file.</param>
    /// <param name="dataStore">The data store to populate.</param>
    public static void Load(string filePath, ConcurrentDictionary<string, StoredValue> dataStore)
    {
        if (!File.Exists(filePath))
            return;

        byte[] fileBytes = File.ReadAllBytes(filePath);
        int offset = 0;

        string magic = Encoding.ASCII.GetString(fileBytes, offset, 5);
        offset += 5;
        if (magic != "REDIS")
            return;

        offset += 4; // skip version string (e.g. "0009")

        while (offset < fileBytes.Length)
        {
            byte opcode = fileBytes[offset++];

            if (opcode == 0xFF)
                break;

            if (opcode == 0xFE)
            {
                var (_, n) = ReadLength(fileBytes, offset);
                offset += n;
            }
            else if (opcode == 0xFD)
            {
                uint expirySeconds = BitConverter.ToUInt32(fileBytes, offset);
                offset += 4;
                long expiryMs = expirySeconds * 1000L;

                offset++; // value type
                var (key, kb) = ReadString(fileBytes, offset); offset += kb;
                var (value, vb) = ReadString(fileBytes, offset); offset += vb;
                dataStore[key] = new StoredValue(value, expiryMs);
            }
            else if (opcode == 0xFC)
            {
                ulong expiryMs = BitConverter.ToUInt64(fileBytes, offset);
                offset += 8;

                offset++; // value type
                var (key, kb) = ReadString(fileBytes, offset); offset += kb;
                var (value, vb) = ReadString(fileBytes, offset); offset += vb;
                dataStore[key] = new StoredValue(value, (long)expiryMs);
            }
            else if (opcode == 0xFB)
            {
                var (_, b1) = ReadLength(fileBytes, offset); offset += b1;
                var (_, b2) = ReadLength(fileBytes, offset); offset += b2;
            }
            else if (opcode == 0xFA)
            {
                var (_, kb) = ReadString(fileBytes, offset); offset += kb;
                var (_, vb) = ReadString(fileBytes, offset); offset += vb;
            }
            else
            {
                byte valueType = opcode;
                var (key, kb) = ReadString(fileBytes, offset); offset += kb;

                if (valueType == 0)
                {
                    var (value, vb) = ReadString(fileBytes, offset); offset += vb;
                    dataStore[key] = new StoredValue(value);
                }
                else
                {
                    break;
                }
            }
        }
    }

    /// <summary>
    /// Reads a length-encoded integer from <paramref name="data"/> at <paramref name="offset"/>.
    /// </summary>
    /// <returns>The decoded length and the number of bytes consumed.</returns>
    private static (int length, int bytesRead) ReadLength(byte[] data, int offset)
    {
        byte firstByte = data[offset];
        int type = (firstByte & 0xC0) >> 6;

        return type switch
        {
            0 => (firstByte & 0x3F, 1),
            1 => (((firstByte & 0x3F) << 8) | data[offset + 1], 2),
            2 => ((data[offset + 1] << 24) | (data[offset + 2] << 16) | (data[offset + 3] << 8) | data[offset + 4], 5),
            _ => (firstByte & 0x3F, 1)
        };
    }

    /// <summary>
    /// Reads a length-prefixed or specially-encoded string from <paramref name="data"/> at <paramref name="offset"/>.
    /// </summary>
    /// <returns>The decoded string and the total number of bytes consumed.</returns>
    private static (string str, int bytesRead) ReadString(byte[] data, int offset)
    {
        var (length, lengthBytes) = ReadLength(data, offset);
        byte firstByte = data[offset];
        int type = (firstByte & 0xC0) >> 6;

        if (type == 3)
        {
            int encodingType = firstByte & 0x3F;
            return encodingType switch
            {
                0 => (((sbyte)data[offset + 1]).ToString(), 2),
                1 => (BitConverter.ToInt16(data, offset + 1).ToString(), 3),
                2 => (BitConverter.ToInt32(data, offset + 1).ToString(), 5),
                _ => ("", lengthBytes)
            };
        }

        string str = Encoding.UTF8.GetString(data, offset + lengthBytes, length);
        return (str, lengthBytes + length);
    }
}
