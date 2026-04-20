using System.Text;

partial class RedisServer
{
    /// <summary>
    /// Appends a new entry to the stream stored at the key specified in <paramref name="parts"/>.
    /// Auto-generates the entry ID when <c>*</c> is provided.
    /// </summary>
    private string XAdd(string[] parts)
    {
        string key = parts[1];
        string entryId = parts[2];
        int fieldCount = parts.Length - 3;

        if (fieldCount % 2 != 0)
            return "-ERR wrong number of arguments for XADD\r\n";

        long millisTime = 0;
        long seqNum = 0;
        string? errorResponse = ResolveStreamId(key, entryId, ref millisTime, ref seqNum, ref entryId);

        if (errorResponse != null)
            return errorResponse;

        if (millisTime == 0 && seqNum == 0)
            return "-ERR The ID specified in XADD must be greater than 0-0\r\n";

        if (_dataStore.TryGetValue(key, out StoredValue? existing) && existing.Stream?.Count > 0)
        {
            var last = existing.Stream[^1];
            string[] lp = last.Id.Split('-');
            long lm = long.Parse(lp[0]), ls = long.Parse(lp[1]);

            if (millisTime < lm || (millisTime == lm && seqNum <= ls))
                return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
        }

        var fields = new Dictionary<string, string>();
        for (int i = 3; i < parts.Length; i += 2)
            fields[parts[i]] = parts[i + 1];

        var entry = new StreamEntry(entryId, fields);

        if (!_dataStore.ContainsKey(key))
        {
            _dataStore[key] = new StoredValue(new List<StreamEntry> { entry });
        }
        else if (_dataStore.TryGetValue(key, out StoredValue? sv) && sv.Stream != null)
        {
            sv.Stream.Add(entry);
        }
        else
        {
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }

        UnblockWaitingStreamReaders(key);
        return $"${entryId.Length}\r\n{entryId}\r\n";
    }

    private async Task<string> XRead(string[] parts)
    {
        int blockTimeout = -1;
        int streamsIndex = 1;

        if (parts[1].ToUpper() == "BLOCK")
        {
            if (parts.Length < 6)
                return "-ERR wrong number of arguments for XREAD\r\n";

            if (!int.TryParse(parts[2], out blockTimeout))
                return "-ERR timeout is not an integer or out of range\r\n";

            streamsIndex = 3;
        }

        if (parts[streamsIndex].ToUpper() != "STREAMS")
            return "-ERR wrong number of arguments for XREAD\r\n";

        int argsAfterStreams = parts.Length - streamsIndex - 1;
        if (argsAfterStreams % 2 != 0)
            return "-ERR wrong number of arguments for XREAD\r\n";

        int streamCount = argsAfterStreams / 2;
        var keys = new string[streamCount];
        var ids = new string[streamCount];

        for (int i = 0; i < streamCount; i++)
        {
            keys[i] = parts[streamsIndex + 1 + i];
            ids[i] = parts[streamsIndex + 1 + streamCount + i];

            if (ids[i] == "$")
            {
                ids[i] = (_dataStore.TryGetValue(keys[i], out StoredValue? sv) &&
                          sv.Stream?.Count > 0)
                    ? sv.Stream[^1].Id
                    : "0-0";
            }
        }

        var results = CollectStreamResults(keys, ids, streamCount);

        if (results.Count == 0 && blockTimeout >= 0)
        {
            var tcs = new TaskCompletionSource<List<(string key, List<StreamEntry> entries)>?>(
                TaskCreationOptions.RunContinuationsAsynchronously);

            lock (_blockedStreamReadersLock)
            {
                for (int i = 0; i < streamCount; i++)
                {
                    if (!_blockedStreamReaders.ContainsKey(keys[i]))
                        _blockedStreamReaders[keys[i]] = new Queue<BlockedStreamReader>();
                    _blockedStreamReaders[keys[i]].Enqueue(new BlockedStreamReader(keys, ids, tcs));
                }
            }

            Task<List<(string, List<StreamEntry>)>?> entriesTask = tcs.Task;
            Task completed = blockTimeout > 0
                ? await Task.WhenAny(entriesTask, Task.Delay(blockTimeout))
                : (await Task.WhenAny(entriesTask), entriesTask).Item1;

            lock (_blockedStreamReadersLock)
            {
                for (int i = 0; i < streamCount; i++)
                {
                    if (_blockedStreamReaders.TryGetValue(keys[i], out var q))
                    {
                        var temp = new Queue<BlockedStreamReader>();
                        while (q.Count > 0)
                        {
                            var r = q.Dequeue();
                            if (r.TaskCompletionSource != tcs) temp.Enqueue(r);
                        }
                        if (temp.Count > 0) _blockedStreamReaders[keys[i]] = temp;
                        else _blockedStreamReaders.TryRemove(keys[i], out _);
                    }
                }
            }

            if (entriesTask.IsCompletedSuccessfully && entriesTask.Result != null)
                results = entriesTask.Result;
            else
                return "*-1\r\n";
        }

        if (results.Count == 0)
            return "*-1\r\n";

        return BuildXReadResponse(results);
    }

    /// <summary>
    /// Returns the entries in the stream at <paramref name="key"/> whose IDs fall within
    /// the inclusive range from <paramref name="startId"/> to <paramref name="endId"/>.
    /// </summary>
    private string XRange(string key, string startId, string endId)
    {
        if (!_dataStore.TryGetValue(key, out StoredValue? sv))
            return "*0\r\n";

        if (sv.Stream == null)
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

        var (startMs, startSeq) = RespParser.ParseStreamId(startId, true);
        var (endMs, endSeq) = RespParser.ParseStreamId(endId, false);

        var matching = new List<StreamEntry>();
        foreach (var entry in sv.Stream)
        {
            string[] p = entry.Id.Split('-');
            long em = long.Parse(p[0]), es = long.Parse(p[1]);

            bool inRange = (em > startMs && em < endMs)
                || (em == startMs && em == endMs && es >= startSeq && es <= endSeq)
                || (em == startMs && em != endMs && es >= startSeq)
                || (em == endMs && em != startMs && es <= endSeq);

            if (inRange) matching.Add(entry);
        }

        return BuildStreamEntryArray(matching);
    }

    /// <summary>
    /// Wakes up blocked XREAD clients waiting on <paramref name="key"/> with newly available entries.
    /// </summary>
    private void UnblockWaitingStreamReaders(string key)
    {
        lock (_blockedStreamReadersLock)
        {
            if (!_blockedStreamReaders.TryGetValue(key, out var queue) || queue.Count == 0)
                return;

            var toUnblock = new List<BlockedStreamReader>();
            while (queue.Count > 0)
                toUnblock.Add(queue.Dequeue());
            _blockedStreamReaders.TryRemove(key, out _);

            foreach (var reader in toUnblock)
            {
                var results = CollectStreamResults(reader.Keys, reader.Ids, reader.Keys.Length);
                if (results.Count > 0)
                    reader.TaskCompletionSource.TrySetResult(results);
            }
        }
    }

    /// <summary>
    /// Queries each specified stream for entries newer than the given IDs and returns the results.
    /// </summary>
    private List<(string key, List<StreamEntry> entries)> CollectStreamResults(
        string[] keys, string[] ids, int count)
    {
        var results = new List<(string, List<StreamEntry>)>();

        for (int i = 0; i < count; i++)
        {
            if (!_dataStore.TryGetValue(keys[i], out StoredValue? sv) || sv.Stream == null)
                continue;

            var (startMs, startSeq) = RespParser.ParseStreamId(ids[i], true);
            var matching = new List<StreamEntry>();

            foreach (var entry in sv.Stream)
            {
                string[] p = entry.Id.Split('-');
                long em = long.Parse(p[0]), es = long.Parse(p[1]);
                if (em > startMs || (em == startMs && es > startSeq))
                    matching.Add(entry);
            }

            if (matching.Count > 0)
                results.Add((keys[i], matching));
        }

        return results;
    }

    /// <summary>
    /// Builds the nested RESP array response for XREAD results.
    /// </summary>
    private static string BuildXReadResponse(List<(string key, List<StreamEntry> entries)> results)
    {
        var sb = new StringBuilder();
        sb.Append($"*{results.Count}\r\n");

        foreach (var (key, entries) in results)
        {
            sb.Append("*2\r\n");
            sb.Append($"${key.Length}\r\n{key}\r\n");
            sb.Append(BuildStreamEntryArray(entries));
        }

        return sb.ToString();
    }

    /// <summary>
    /// Builds a RESP array of stream entries (each entry is a two-element array of ID and field map).
    /// </summary>
    private static string BuildStreamEntryArray(List<StreamEntry> entries)
    {
        var sb = new StringBuilder();
        sb.Append($"*{entries.Count}\r\n");

        foreach (var entry in entries)
        {
            sb.Append("*2\r\n");
            sb.Append($"${entry.Id.Length}\r\n{entry.Id}\r\n");
            sb.Append($"*{entry.Fields.Count * 2}\r\n");

            foreach (var kvp in entry.Fields)
            {
                sb.Append($"${kvp.Key.Length}\r\n{kvp.Key}\r\n");
                sb.Append($"${kvp.Value.Length}\r\n{kvp.Value}\r\n");
            }
        }

        return sb.ToString();
    }

    /// <summary>
    /// Resolves the stream entry ID (or wildcard) for XADD, computing the actual milliseconds
    /// and sequence number and updating <paramref name="resolvedId"/>.
    /// </summary>
    /// <returns>An error RESP string on failure, or <c>null</c> on success.</returns>
    private string? ResolveStreamId(string key, string rawId,
        ref long millisTime, ref long seqNum, ref string resolvedId)
    {
        if (rawId == "*")
        {
            millisTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            seqNum = 0;

            if (_dataStore.TryGetValue(key, out StoredValue? sv) && sv.Stream?.Count > 0)
            {
                var last = sv.Stream[^1];
                string[] lp = last.Id.Split('-');
                long lm = long.Parse(lp[0]), ls = long.Parse(lp[1]);

                if (millisTime == lm) seqNum = ls + 1;
                else if (millisTime <= lm) { millisTime = lm; seqNum = ls + 1; }
            }

            resolvedId = $"{millisTime}-{seqNum}";
            return null;
        }

        string[] idParts = rawId.Split('-');
        if (idParts.Length != 2 || !long.TryParse(idParts[0], out millisTime))
            return "-ERR Invalid stream ID specified as stream command argument\r\n";

        if (idParts[1] == "*")
        {
            seqNum = 0;
            if (_dataStore.TryGetValue(key, out StoredValue? sv) && sv.Stream?.Count > 0)
            {
                var last = sv.Stream[^1];
                string[] lp = last.Id.Split('-');
                long lm = long.Parse(lp[0]), ls = long.Parse(lp[1]);

                if (millisTime == lm) seqNum = ls + 1;
                else if (millisTime == 0) seqNum = 1;
            }
            else if (millisTime == 0)
            {
                seqNum = 1;
            }

            resolvedId = $"{millisTime}-{seqNum}";
            return null;
        }

        if (!long.TryParse(idParts[1], out seqNum))
            return "-ERR Invalid stream ID specified as stream command argument\r\n";

        return null;
    }
}
