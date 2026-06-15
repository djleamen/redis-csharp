using System.Globalization;
using System.Text;

namespace codecrafters_redis;

partial class RedisServer
{
    /// <summary>
    /// Returns the elements in the list at <paramref name="key"/> within the specified
    /// inclusive index range.
    /// Acquires the per-key lock so that concurrent mutations cannot corrupt the read.
    /// </summary>
    private async Task<string> LRange(string key, string startStr, string stopStr)
    {
        if (!int.TryParse(startStr, out int start) || !int.TryParse(stopStr, out int stop))
            return "-ERR value is not an integer or out of range\r\n";

        var keyLock = GetKeyLock(key);
        await keyLock.WaitAsync();
        try
        {
            if (!_dataStore.TryGetValue(key, out StoredValue? sv))
                return "*0\r\n";

            if (sv.List == null)
                return WrongTypeError;

            var list = sv.List;
            if (start < 0) start = Math.Max(0, list.Count + start);
            if (stop < 0) stop = Math.Max(0, list.Count + stop);

            if (start >= list.Count || start > stop)
                return "*0\r\n";

            int actualStop = Math.Min(stop, list.Count - 1);
            var sb = new StringBuilder();
            sb.Append($"*{actualStop - start + 1}\r\n");
            for (int i = start; i <= actualStop; i++)
                sb.Append($"${Encoding.UTF8.GetByteCount(list[i])}\r\n{list[i]}\r\n");

            return sb.ToString();
        }
        finally
        {
            keyLock.Release();
        }
    }

    /// <summary>
    /// Returns the length of the list stored at <paramref name="key"/>.
    /// Acquires the per-key lock so that concurrent mutations cannot corrupt the read.
    /// </summary>
    private async Task<string> LLen(string key)
    {
        var keyLock = GetKeyLock(key);
        await keyLock.WaitAsync();
        try
        {
            if (!_dataStore.TryGetValue(key, out StoredValue? sv))
                return ":0\r\n";

            return sv.List == null
                ? WrongTypeError
                : $":{sv.List.Count}\r\n";
        }
        finally
        {
            keyLock.Release();
        }
    }

    /// <summary>
    /// Removes and returns one or more elements from the head of the list.
    /// Supports the optional count argument for multi-element pops.
    /// Acquires the per-key lock to prevent races with concurrent push/pop operations.
    /// </summary>
    private async Task<string> LPop(string[] parts)
    {
        string key = parts[1];
        int count = 1;

        if (parts.Length >= 3 && (!int.TryParse(parts[2], out count) || count < 1))
            return "-ERR value is not an integer or out of range\r\n";

        var keyLock = GetKeyLock(key);
        await keyLock.WaitAsync();
        try
        {
            if (!_dataStore.TryGetValue(key, out StoredValue? sv))
                return "$-1\r\n";

            if (sv.List == null)
                return WrongTypeError;

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
                    sb.Append($"${Encoding.UTF8.GetByteCount(el)}\r\n{el}\r\n");
                return sb.ToString();
            }

            string element = sv.List[0];
            sv.List.RemoveAt(0);
            return $"${Encoding.UTF8.GetByteCount(element)}\r\n{element}\r\n";
        }
        finally
        {
            keyLock.Release();
        }
    }

    /// <summary>
    /// Blocking variant of LPOP. Waits up to <paramref name="timeoutStr"/> seconds for
    /// an element to appear at the head of the list at <paramref name="key"/>.
    /// <para>
    /// The per-key lock guards the fast-path pop. For the slow (blocking) path,
    /// <see cref="TaskCompletionSource{T}.TrySetResult"/> is used as an atomic arbiter:
    /// the first of (timeout, delivery) to call TrySetResult wins, so an element can
    /// never be removed from the list and then silently dropped.
    /// </para>
    /// </summary>
    private async Task<string> BLPop(string key, string timeoutStr)
    {
        if (!double.TryParse(timeoutStr, NumberStyles.Float, CultureInfo.InvariantCulture, out double timeout)
            || timeout < 0)
            return "-ERR timeout is not a float or out of range\r\n";

        // Fast path: element already available — take it under the per-key lock.
        var keyLock = GetKeyLock(key);
        await keyLock.WaitAsync();
        try
        {
            if (_dataStore.TryGetValue(key, out StoredValue? sv) && sv.List != null && sv.List.Count > 0)
            {
                string element = sv.List[0];
                sv.List.RemoveAt(0);
                return $"*2\r\n${Encoding.UTF8.GetByteCount(key)}\r\n{key}\r\n${Encoding.UTF8.GetByteCount(element)}\r\n{element}\r\n";
            }
        }
        finally
        {
            keyLock.Release();
        }

        // Slow path: register as a blocked waiter and wait. RunContinuationsAsynchronously
        // ensures the continuation does not run inline on the delivering thread (which
        // holds the per-key lock inside UnblockWaitingClientsAsync), keeping lock hold
        // times short and avoiding re-entrancy.
        var tcs = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_blockedClientsLock)
        {
            if (!_blockedClients.ContainsKey(key))
                _blockedClients[key] = new Queue<BlockedClient>();
            _blockedClients[key].Enqueue(new BlockedClient(key, tcs));
        }

        // Close the lost-wakeup window: between releasing the key lock on the fast
        // path above and enqueuing this waiter, a concurrent push could have added an
        // element and found the waiter queue empty, leaving the element sitting in the
        // list. Now that this waiter is registered, drain any such element so a BLPOP
        // with data already available is served immediately instead of blocking until
        // timeout. This is a no-op when no element is waiting.
        await UnblockWaitingClientsAsync(key);

        Task<string?> elementTask = tcs.Task;
        if (timeout > 0)
            await Task.WhenAny(elementTask, Task.Delay((int)(timeout * 1000)));
        else
            await elementTask;

        // Use TrySetResult(null) as the atomic claim for the timeout path.
        // • If it returns true  → we won the race; no element was (or will be) delivered.
        // • If it returns false → UnblockWaitingClients already delivered an element.
        bool cancelWon = tcs.TrySetResult(null);
        string? popped = cancelWon ? null : elementTask.Result;

        if (cancelWon)
            RemoveFromBlockedQueue(key, tcs);

        return popped != null
            ? $"*2\r\n${Encoding.UTF8.GetByteCount(key)}\r\n{key}\r\n${Encoding.UTF8.GetByteCount(popped)}\r\n{popped}\r\n"
            : "*-1\r\n";
    }

    /// <summary>
    /// Removes <paramref name="tcs"/> from the blocked-client queue for <paramref name="key"/>
    /// without signalling it (the caller is responsible for setting the result first).
    /// </summary>
    private void RemoveFromBlockedQueue(string key, TaskCompletionSource<string?> tcs)
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
    }

    /// <summary>
    /// Wakes up blocked BLPOP clients waiting on <paramref name="key"/>,
    /// delivering the first available list element to each waiter in order.
    /// <para>
    /// Acquires the per-key lock asynchronously first (no thread blocking), then
    /// takes <see cref="_blockedClientsLock"/> only briefly to dequeue one waiter at
    /// a time. Uses <see cref="TaskCompletionSource{T}.TrySetResult"/> so that if a
    /// waiter's timeout already won the race, the element is placed back and the next
    /// waiter is tried rather than silently dropping the element.
    /// </para>
    /// </summary>
    private async Task UnblockWaitingClientsAsync(string key)
    {
        var keyLock = GetKeyLock(key);
        await keyLock.WaitAsync();
        try
        {
            while (true)
            {
                // Check list availability while holding the key lock.
                if (!_dataStore.TryGetValue(key, out StoredValue? sv) || sv.List == null || sv.List.Count == 0)
                    break;

                // Dequeue the next waiter under the blocked-clients lock (brief, sync).
                BlockedClient? blocked;
                lock (_blockedClientsLock)
                {
                    if (!_blockedClients.TryGetValue(key, out var queue) || queue.Count == 0)
                        break;
                    blocked = queue.Dequeue();
                }

                // Pop the element and try to deliver it.  keyLock ensures the list
                // cannot be mutated concurrently, so sv.List is still non-empty here.
                string element = sv.List[0];
                sv.List.RemoveAt(0);

                if (!blocked.TaskCompletionSource.TrySetResult(element))
                {
                    // The waiter's timeout already claimed the TCS.  The waiter was
                    // permanently dequeued above, so the blocked-clients queue shrinks
                    // by one each iteration regardless; the loop will always terminate.
                    // Put the element back and try the next waiter.
                    sv.List.Insert(0, element);
                }
            }

            lock (_blockedClientsLock)
            {
                if (_blockedClients.TryGetValue(key, out var q) && q.Count == 0)
                    _blockedClients.TryRemove(key, out _);
            }
        }
        finally
        {
            keyLock.Release();
        }
    }
}
