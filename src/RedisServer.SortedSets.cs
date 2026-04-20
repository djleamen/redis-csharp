using System.Text;

partial class RedisServer
{
    /// <summary>
    /// Adds or updates a member in the sorted set stored at <paramref name="key"/>.
    /// Creates the sorted set if it does not already exist.
    /// </summary>
    private string ZAdd(string key, string scoreStr, string member)
    {
        if (!double.TryParse(scoreStr, System.Globalization.NumberStyles.Float,
            System.Globalization.CultureInfo.InvariantCulture, out double score))
        {
            return "-ERR value is not a valid float\r\n";
        }

        if (!_dataStore.ContainsKey(key))
        {
            _dataStore[key] = new StoredValue(new List<SortedSetEntry> { new(member, score) });
            return ":1\r\n";
        }

        if (!_dataStore.TryGetValue(key, out StoredValue? sv) || sv.SortedSet == null)
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

        var existing = sv.SortedSet.FirstOrDefault(e => e.Member == member);
        if (existing != null)
        {
            sv.SortedSet.Remove(existing);
            sv.SortedSet.Add(new SortedSetEntry(member, score));
            sv.SortedSet.Sort();
            return ":0\r\n";
        }

        sv.SortedSet.Add(new SortedSetEntry(member, score));
        sv.SortedSet.Sort();
        return ":1\r\n";
    }

    /// <summary>
    /// Returns the rank (zero-based index) of <paramref name="member"/> in the sorted set
    /// at <paramref name="key"/>, ordered by ascending score.
    /// </summary>
    private string ZRank(string key, string member)
    {
        if (!_dataStore.TryGetValue(key, out StoredValue? sv))
            return "$-1\r\n";

        if (sv.SortedSet == null)
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

        for (int i = 0; i < sv.SortedSet.Count; i++)
            if (sv.SortedSet[i].Member == member)
                return $":{i}\r\n";

        return "$-1\r\n";
    }

    /// <summary>
    /// Returns the members in the sorted set at <paramref name="key"/> within the
    /// specified inclusive index range, ordered by ascending score.
    /// </summary>
    private string ZRange(string key, string startStr, string stopStr)
    {
        if (!int.TryParse(startStr, out int start) || !int.TryParse(stopStr, out int stop))
            return "-ERR value is not an integer or out of range\r\n";

        if (!_dataStore.TryGetValue(key, out StoredValue? sv))
            return "*0\r\n";

        if (sv.SortedSet == null)
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

        int count = sv.SortedSet.Count;
        if (start < 0) start = Math.Max(0, count + start);
        if (stop < 0) stop = Math.Max(0, count + stop);

        if (start >= count || start > stop)
            return "*0\r\n";

        stop = Math.Min(stop, count - 1);

        var sb = new StringBuilder();
        sb.Append($"*{stop - start + 1}\r\n");
        for (int i = start; i <= stop; i++)
            sb.Append($"${sv.SortedSet[i].Member.Length}\r\n{sv.SortedSet[i].Member}\r\n");

        return sb.ToString();
    }

    /// <summary>
    /// Returns the number of members in the sorted set at <paramref name="key"/>.
    /// </summary>
    private string ZCard(string key)
    {
        if (!_dataStore.TryGetValue(key, out StoredValue? sv))
            return ":0\r\n";

        return sv.SortedSet == null
            ? "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
            : $":{sv.SortedSet.Count}\r\n";
    }

    /// <summary>
    /// Returns the score of <paramref name="member"/> in the sorted set at <paramref name="key"/>.
    /// </summary>
    private string ZScore(string key, string member)
    {
        if (!_dataStore.TryGetValue(key, out StoredValue? sv))
            return "$-1\r\n";

        if (sv.SortedSet == null)
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

        var entry = sv.SortedSet.FirstOrDefault(e => e.Member == member);
        if (entry == null) return "$-1\r\n";

        string scoreStr = entry.Score.ToString(System.Globalization.CultureInfo.InvariantCulture);
        return $"${scoreStr.Length}\r\n{scoreStr}\r\n";
    }

    /// <summary>
    /// Removes <paramref name="member"/> from the sorted set at <paramref name="key"/>.
    /// Returns 1 if the member was removed, 0 if it did not exist.
    /// </summary>
    private string ZRem(string key, string member)
    {
        if (!_dataStore.TryGetValue(key, out StoredValue? sv))
            return ":0\r\n";

        if (sv.SortedSet == null)
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

        var entry = sv.SortedSet.FirstOrDefault(e => e.Member == member);
        if (entry == null) return ":0\r\n";

        sv.SortedSet.Remove(entry);
        return ":1\r\n";
    }
}
