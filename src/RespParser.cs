namespace codecrafters_redis;

/// <summary>
/// Stateless utilities for parsing the Redis Serialization Protocol (RESP).
/// </summary>
static class RespParser
{
    /// <summary>
    /// Parses a RESP array from a raw UTF-8 string and returns the bulk-string elements.
    /// Returns an empty array when the input is not a valid RESP array.
    /// </summary>
    /// <param name="input">Raw RESP-encoded input.</param>
    public static string[] ParseArray(string input)
    {
        var parts = new List<string>();
        var lines = input.Split(new[] { "\r\n" }, StringSplitOptions.None);

        if (lines.Length == 0 || !lines[0].StartsWith('*'))
            return parts.ToArray();

        int i = 1;
        while (i < lines.Length)
        {
            if (lines[i].StartsWith('$'))
            {
                i++;
                if (i < lines.Length)
                {
                    parts.Add(lines[i]);
                    i++;
                }
            }
            else
            {
                i++;
            }
        }

        return parts.ToArray();
    }

    /// <summary>
    /// Attempts to parse one complete RESP command from the start of <paramref name="data"/>.
    /// </summary>
    /// <param name="data">Buffered input that may contain a partial or complete RESP command.</param>
    /// <returns>
    /// The parsed command parts and the number of characters consumed, or
    /// <c>(null, 0)</c> when <paramref name="data"/> does not yet hold a complete command.
    /// </returns>
    public static (string[]? parts, int bytesConsumed) TryParseCommand(string data)
    {
        if (string.IsNullOrEmpty(data) || !data.StartsWith('*'))
            return (null, 0);

        var lines = data.Split(new[] { "\r\n" }, StringSplitOptions.None);
        if (lines.Length < 2)
            return (null, 0);

        if (!int.TryParse(lines[0].Substring(1), out int arrayLength))
            return (null, 0);

        var parts = new List<string>();
        int lineIndex = 1;
        int bytesConsumed = lines[0].Length + 2;

        for (int i = 0; i < arrayLength; i++)
        {
            if (lineIndex >= lines.Length)
                return (null, 0);

            string lengthLine = lines[lineIndex];
            if (!lengthLine.StartsWith('$') || !int.TryParse(lengthLine.Substring(1), out int bulkLength))
                return (null, 0);

            bytesConsumed += lengthLine.Length + 2;
            lineIndex++;

            if (lineIndex >= lines.Length)
                return (null, 0);

            string value = lines[lineIndex];
            if (value.Length != bulkLength)
            {
                bool isLastOrSecondLast = lineIndex == lines.Length - 1
                    || (lineIndex == lines.Length - 2 && lines[lineIndex + 1] == "");
                if (isLastOrSecondLast)
                    return (null, 0);
            }

            parts.Add(value);
            bytesConsumed += value.Length + 2;
            lineIndex++;
        }

        return (parts.ToArray(), bytesConsumed);
    }

    /// <summary>
    /// Parses a Redis stream ID string into its millisecond timestamp and sequence-number components.
    /// </summary>
    /// <param name="id">
    /// A stream entry ID such as <c>"1234-0"</c>, the special token <c>"-"</c> (minimum),
    /// or <c>"+"</c> (maximum).
    /// </param>
    /// <param name="isStart">
    /// When <c>true</c>, a partial ID without a sequence part defaults to sequence 0 (lower bound);
    /// otherwise it defaults to <see cref="long.MaxValue"/> (upper bound).
    /// </param>
    /// <returns>A tuple of <c>(milliseconds, sequenceNumber)</c>.</returns>
    public static (long millis, long seq) ParseStreamId(string id, bool isStart)
    {
        if (id == "-") return isStart ? (0L, 0L) : (long.MaxValue, long.MaxValue);
        if (id == "+") return (long.MaxValue, long.MaxValue);

        string[] parts = id.Split('-');
        long millis = long.Parse(parts[0]);
        long seq = parts.Length == 1
            ? (isStart ? 0L : long.MaxValue)
            : long.Parse(parts[1]);

        return (millis, seq);
    }
}
