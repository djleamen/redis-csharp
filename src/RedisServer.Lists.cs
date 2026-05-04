using System.Text;

namespace codecrafters_redis;

partial class RedisServer
{
    /// <summary>
    /// Returns the elements in the list at <paramref name="key"/> within the specified
    /// inclusive index range.
    /// </summary>
    private string LRange(string key, string startStr, string stopStr)
    {
        if (!int.TryParse(startStr, out int start) || !int.TryParse(stopStr, out int stop))
            return "-ERR value is not an integer or out of range\r\n";

        if (!_dataStore.TryGetValue(key, out StoredValue? sv))
            return "*0\r\n";

        if (sv.List == null)
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

        var list = sv.List;
        if (start < 0) start = Math.Max(0, list.Count + start);
        if (stop < 0) stop = Math.Max(0, list.Count + stop);

        if (start >= list.Count || start > stop)
            return "*0\r\n";

        int actualStop = Math.Min(stop, list.Count - 1);
        var sb = new StringBuilder();
        sb.Append($"*{actualStop - start + 1}\r\n");
        for (int i = start; i <= actualStop; i++)
            sb.Append($"${list[i].Length}\r\n{list[i]}\r\n");

        return sb.ToString();
    }

    /// <summary>
    /// Returns the length of the list stored at <paramref name="key"/>.
    /// </summary>
    private string LLen(string key)
    {
        if (!_dataStore.TryGetValue(key, out StoredValue? sv))
            return ":0\r\n";

        return sv.List == null
            ? "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
            : $":{sv.List.Count}\r\n";
    }

    /// <summary>
    /// Removes and returns one or more elements from the head of the list.
    /// Supports the optional count argument for multi-element pops.
    /// </summary>
    private string LPop(string[] parts)
    {
        string key = parts[1];
        int count = 1;

        if (parts.Length >= 3 && (!int.TryParse(parts[2], out count) || count < 1))
            return "-ERR value is not an integer or out of range\r\n";

        if (!_dataStore.TryGetValue(key, out StoredValue? sv))
            return "$-1\r\n";

        if (sv.List == null)
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

        if (sv.List.Count == 0)
            return "$-1\r\n";

        if (parts.Length >= 3)
        {
            int toRemove = Math.Min(count, sv.List.Count);
            var removed = new List<string>();
            for (int i = 0; i < toRemove; i++)
            {
                removed.Add(sv.List[0]);
                sv.List.RemoveAt(0);
            }
            var sb = new StringBuilder();
            sb.Append($"*{removed.Count}\r\n");
            foreach (var el in removed)
                sb.Append($"${el.Length}\r\n{el}\r\n");
            return sb.ToString();
        }

        string element = sv.List[0];
        sv.List.RemoveAt(0);
        return $"${element.Length}\r\n{element}\r\n";
    }

    /// <summary>
    /// Blocking variant of LPOP. Waits up to <paramref name="timeoutStr"/> seconds for
    /// an element to appear at the head of the list at <paramref name="key"/>.
    /// </summary>
    private async Task<string> BLPop(string key, string timeoutStr)
    {
        if (!double.TryParse(timeoutStr, out double timeout))
            return "-ERR timeout is not a float or out of range\r\n";

        if (_dataStore.TryGetValue(key, out StoredValue? sv) && sv.List != null && sv.List.Count > 0)
        {
            string element = sv.List[0];
            sv.List.RemoveAt(0);
            return $"*2\r\n${key.Length}\r\n{key}\r\n${element.Length}\r\n{element}\r\n";
        }

        var tcs = new TaskCompletionSource<string?>();
        lock (_blockedClientsLock)
        {
            if (!_blockedClients.ContainsKey(key))
                _blockedClients[key] = new Queue<BlockedClient>();
            _blockedClients[key].Enqueue(new BlockedClient(key, tcs));
        }

        Task<string?> elementTask = tcs.Task;
        Task completed = timeout > 0
            ? await Task.WhenAny(elementTask, Task.Delay((int)(timeout * 1000)))
            : (await Task.WhenAny(elementTask), elementTask).Item1;

        string? popped = null;
        if (completed == elementTask && elementTask.IsCompletedSuccessfully)
        {
            popped = elementTask.Result;
        }
        else
        {
            lock (_blockedClientsLock)
            {
                if (_blockedClients.TryGetValue(key, out Queue<BlockedClient>? queue))
                {
                    var temp = new Queue<BlockedClient>();
                    while (queue.Count > 0)
                    {
                        var bc = queue.Dequeue();
                        if (bc.TaskCompletionSource != tcs) temp.Enqueue(bc);
                    }
                    if (temp.Count > 0) _blockedClients[key] = temp;
                    else _blockedClients.TryRemove(key, out _);
                }
            }
            tcs.TrySetResult(null);
        }

        return popped != null
            ? $"*2\r\n${key.Length}\r\n{key}\r\n${popped.Length}\r\n{popped}\r\n"
            : "*-1\r\n";
    }

    /// <summary>
    /// Wakes up blocked BLPOP clients waiting on <paramref name="key"/>,
    /// delivering the first available list element to each one in order.
    /// </summary>
    private void UnblockWaitingClients(string key)
    {
        lock (_blockedClientsLock)
        {
            while (_blockedClients.TryGetValue(key, out var queue) && queue.Count > 0)
            {
                if (!_dataStore.TryGetValue(key, out StoredValue? sv) || sv.List == null || sv.List.Count == 0)
                    break;

                var blocked = queue.Dequeue();
                string element = sv.List[0];
                sv.List.RemoveAt(0);
                blocked.TaskCompletionSource.SetResult(element);

                if (queue.Count == 0)
                    _blockedClients.TryRemove(key, out _);
            }
        }
    }
}
