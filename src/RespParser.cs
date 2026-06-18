using System.Text;

namespace codecrafters_redis;

/// <summary>
/// Stateless utilities for parsing the Redis Serialization Protocol (RESP).
/// </summary>
static class RespParser
{
    // ── Byte-based parser ──────────────────────────────────────────────────────

    /// <summary>
    /// Parses one complete RESP array command from the raw byte buffer
    /// <paramref name="data"/>, returning the decoded command parts and the number of
    /// <em>bytes</em> consumed.  Byte-accurate consumption means replica offset tracking
    /// and binary-safe framing work correctly for non-ASCII payloads.
    /// </summary>
    /// <param name="data">Raw receive buffer.  May contain one or more complete commands
    /// followed by a partial command.</param>
    /// <param name="malformed"><c>true</c> when the data is recognisably invalid RESP
    /// (e.g. a non-numeric array count) rather than merely incomplete.</param>
    /// <returns>The parsed command parts and the number of bytes consumed, or
    /// <c>(null, 0)</c> when <paramref name="data"/> does not yet hold a complete command.</returns>
    public static (string[]? parts, int bytesConsumed) TryParseCommandFromBytes(
        ReadOnlySpan<byte> data, out bool malformed)
    {
        malformed = false;

        if (data.IsEmpty || data[0] != (byte)'*')
            return (null, 0);

        int arrayHeaderEnd = IndexOfCrLf(data);
        if (arrayHeaderEnd < 0)
            return (null, 0);

        if (!TryParseAsciiInt(data.Slice(1, arrayHeaderEnd - 1), out int arrayLength) || arrayLength < 0)
        {
            malformed = true;
            return (null, 0);
        }

        int offset = arrayHeaderEnd + 2;
        var parts = new string[arrayLength];

        for (int i = 0; i < arrayLength; i++)
        {
            if (offset >= data.Length)
                return (null, 0);

            if (data[offset] != (byte)'$')
            {
                malformed = offset < data.Length - 1;
                return (null, 0);
            }

            int bulkHeaderEnd = IndexOfCrLf(data.Slice(offset));
            if (bulkHeaderEnd < 0)
                return (null, 0);

            if (!TryParseAsciiInt(data.Slice(offset + 1, bulkHeaderEnd - 1), out int bulkLength) || bulkLength < 0)
            {
                malformed = true;
                return (null, 0);
            }

            offset += bulkHeaderEnd + 2;

            if (offset + bulkLength + 2 > data.Length)
                return (null, 0);

            parts[i] = Encoding.UTF8.GetString(data.Slice(offset, bulkLength));
            offset += bulkLength + 2; // skip payload + \r\n
        }

        return (parts, offset);
    }

    /// <summary>
    /// Overload without the <c>malformed</c> out parameter (for convenience callers).
    /// </summary>
    public static (string[]? parts, int bytesConsumed) TryParseCommandFromBytes(ReadOnlySpan<byte> data) =>
        TryParseCommandFromBytes(data, out _);

    /// <summary>
    /// Returns the byte offset of the first <c>\r\n</c> sequence in <paramref name="span"/>,
    /// or <c>-1</c> if none is present.
    /// </summary>
    private static int IndexOfCrLf(ReadOnlySpan<byte> span)
    {
        for (int i = 0; i < span.Length - 1; i++)
            if (span[i] == '\r' && span[i + 1] == '\n')
                return i;
        return -1;
    }

    /// <summary>
    /// Parses a decimal integer encoded as ASCII bytes.  Accepts an optional leading
    /// <c>'-'</c> for negative values (e.g. <c>$-1</c> nil markers).
    /// </summary>
    private static bool TryParseAsciiInt(ReadOnlySpan<byte> span, out int value)
    {
        value = 0;
        if (span.IsEmpty) return false;
        bool negative = span[0] == (byte)'-';
        int start = negative ? 1 : 0;
        for (int i = start; i < span.Length; i++)
        {
            byte b = span[i];
            if (b < (byte)'0' || b > (byte)'9') return false;
            value = value * 10 + (b - '0');
        }
        if (negative) value = -value;
        return span.Length > start;
    }

    // ── String-based parser (used by ReplayAof for text-encoded AOF files) ────

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
    public static (string[]? parts, int bytesConsumed) TryParseCommand(string data) =>
        TryParseCommand(data, out _);

    /// <summary>
    /// As <see cref="TryParseCommand(string)"/>, but additionally reports whether a failed
    /// parse is a protocol violation (<paramref name="malformed"/> is <c>true</c>) rather
    /// than data that may still become valid once more bytes arrive.
    /// </summary>
    public static (string[]? parts, int bytesConsumed) TryParseCommand(string data, out bool malformed)
    {
        malformed = false;
        if (string.IsNullOrEmpty(data) || !data.StartsWith('*'))
            return (null, 0);

        var lines = data.Split(new[] { "\r\n" }, StringSplitOptions.None);
        if (lines.Length < 2)
            return (null, 0);

        if (!int.TryParse(lines[0].Substring(1), out int arrayLength))
        {
            // The header line is complete (terminated by \r\n) yet its count is invalid.
            malformed = true;
            return (null, 0);
        }

        var parts = new List<string>();
        int lineIndex = 1;
        int bytesConsumed = lines[0].Length + 2;

        for (int i = 0; i < arrayLength; i++)
        {
            if (!TryParseElement(lines, ref lineIndex, ref bytesConsumed, parts, ref malformed))
                return (null, 0);
        }

        return (parts.ToArray(), bytesConsumed);
    }

    private static bool TryParseElement(string[] lines, ref int lineIndex, ref int bytesConsumed, List<string> parts, ref bool malformed)
    {
        if (lineIndex >= lines.Length)
            return false;

        string lengthLine = lines[lineIndex];
        if (!lengthLine.StartsWith('$') || !int.TryParse(lengthLine.Substring(1), out int bulkLength))
        {
            // A terminated line where a $<length> header belongs is a protocol violation;
            // an unterminated trailing fragment may simply be incomplete.
            malformed = lineIndex < lines.Length - 1;
            return false;
        }

        bytesConsumed += lengthLine.Length + 2;
        lineIndex++;

        if (lineIndex >= lines.Length)
            return false;

        // Bulk lengths are byte counts; the buffered data is decoded text, so compare
        // against the value's UTF-8 byte count or non-ASCII values never complete.
        string value = lines[lineIndex];
        if (System.Text.Encoding.UTF8.GetByteCount(value) != bulkLength)
        {
            bool isLastOrSecondLast = lineIndex == lines.Length - 1
                || (lineIndex == lines.Length - 2 && lines[lineIndex + 1] == "");
            if (isLastOrSecondLast)
                return false;
        }

        parts.Add(value);
        bytesConsumed += value.Length + 2;
        lineIndex++;
        return true;
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
        long defaultSeq = isStart ? 0L : long.MaxValue;
        long seq = parts.Length == 1 ? defaultSeq : long.Parse(parts[1]);

        return (millis, seq);
    }
}
