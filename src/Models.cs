namespace codecrafters_redis;

/// <summary>
/// Represents a client blocked on a BLPOP operation, waiting for a list element.
/// </summary>
record BlockedClient(string Key, TaskCompletionSource<string?> TaskCompletionSource);

/// <summary>
/// Represents a client blocked on an XREAD BLOCK operation, waiting for stream entries.
/// </summary>
record BlockedStreamReader(
    string[] Keys,
    string[] Ids,
    TaskCompletionSource<List<(string key, List<StreamEntry> entries)>?> TaskCompletionSource);

/// <summary>
/// A single entry in a Redis stream, identified by a millisecond-precision timestamp and sequence number.
/// </summary>
record StreamEntry(string Id, Dictionary<string, string> Fields);

/// <summary>
/// A member–score pair in a Redis sorted set.
/// Members are ordered by ascending score, then lexicographically by member name on ties.
/// </summary>
record SortedSetEntry(string Member, double Score) : IComparable<SortedSetEntry>
{
    /// <inheritdoc/>
    public int CompareTo(SortedSetEntry? other)
    {
        if (other == null) return 1;
        int cmp = Score.CompareTo(other.Score);
        return cmp != 0 ? cmp : string.Compare(Member, other.Member, StringComparison.Ordinal);
    }

    public static bool operator <(SortedSetEntry left, SortedSetEntry right) => left.CompareTo(right) < 0;
    public static bool operator <=(SortedSetEntry left, SortedSetEntry right) => left.CompareTo(right) <= 0;
    public static bool operator >(SortedSetEntry left, SortedSetEntry right) => left.CompareTo(right) > 0;
    public static bool operator >=(SortedSetEntry left, SortedSetEntry right) => left.CompareTo(right) >= 0;
}

/// <summary>
/// Holds the value and optional expiry timestamp for a Redis key.
/// Exactly one of <see cref="Value"/>, <see cref="List"/>, <see cref="Stream"/>,
/// or <see cref="SortedSet"/> is non-null, reflecting the Redis type of the key.
/// </summary>
record StoredValue
{
    /// <summary>The string value for a string key.</summary>
    public string? Value { get; init; }

    /// <summary>The list of elements for a list key.</summary>
    public List<string>? List { get; init; }

    /// <summary>The ordered entries for a stream key.</summary>
    public List<StreamEntry>? Stream { get; init; }

    /// <summary>The scored members for a sorted set key.</summary>
    public List<SortedSetEntry>? SortedSet { get; init; }

    /// <summary>Unix millisecond timestamp at which this key expires, or <c>null</c> for no expiry.</summary>
    public long? ExpiryMs { get; init; }

    /// <summary>Creates a string value with an optional expiry.</summary>
    public StoredValue(string value, long? expiryMs = null) { Value = value; ExpiryMs = expiryMs; }

    /// <summary>Creates a list value with an optional expiry.</summary>
    public StoredValue(List<string> list, long? expiryMs = null) { List = list; ExpiryMs = expiryMs; }

    /// <summary>Creates a stream value with an optional expiry.</summary>
    public StoredValue(List<StreamEntry> stream, long? expiryMs = null) { Stream = stream; ExpiryMs = expiryMs; }

    /// <summary>Creates a sorted set value with an optional expiry.</summary>
    public StoredValue(List<SortedSetEntry> sortedSet, long? expiryMs = null) { SortedSet = sortedSet; ExpiryMs = expiryMs; }
}
