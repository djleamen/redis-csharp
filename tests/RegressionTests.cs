using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace RedisServer.Tests;

/// <summary>
/// Socket-level regression tests covering:
///   1. Per-key locking – concurrent RPUSH operations must not lose or corrupt data.
///   2. BLPOP delivery race – an element pushed at the instant a BLPOP times out must
///      not be silently dropped.
///   3. AOF expiry replay – keys persisted with a relative PX expiry must not be
///      resurrected after a server restart.
/// </summary>
public class RegressionTests
{
    // ──────────────────────────────────────────────────────────────────────────
    // 1. Per-key locking: concurrent RPUSHes must not lose or corrupt elements.
    // ──────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ConcurrentRPush_NoDataLoss()
    {
        await using var server = await RedisServerFixture.StartAsync();

        const int clients = 10;
        const int pushesPerClient = 20;
        const string key = "mylist";

        // All clients push concurrently.
        var tasks = Enumerable.Range(0, clients).Select(async _ =>
        {
            await using var c = await RedisClient.ConnectAsync(server.Port);
            for (int i = 0; i < pushesPerClient; i++)
                await c.SendCommandAsync("RPUSH", key, $"v{i}");
        });
        await Task.WhenAll(tasks);

        // The final list length must equal the total number of pushes.
        await using var verifier = await RedisClient.ConnectAsync(server.Port);
        string llenResp = await verifier.SendCommandAsync("LLEN", key);
        int length = ParseInteger(llenResp);
        Assert.Equal(clients * pushesPerClient, length);
    }

    [Fact]
    public async Task ConcurrentLPushAndLPop_NoLostOrDuplicated()
    {
        await using var server = await RedisServerFixture.StartAsync();

        const string key = "listkey";
        const int producers = 5;
        const int itemsEach = 10;

        // Producers push concurrently.
        var pushTasks = Enumerable.Range(0, producers).Select(async producerId =>
        {
            await using var c = await RedisClient.ConnectAsync(server.Port);
            for (int i = 0; i < itemsEach; i++)
                await c.SendCommandAsync("LPUSH", key, $"p{producerId}-{i}");
        });
        await Task.WhenAll(pushTasks);

        // Total length must be producers * itemsEach.
        await using var verifier = await RedisClient.ConnectAsync(server.Port);
        string llenResp = await verifier.SendCommandAsync("LLEN", key);
        int total = producers * itemsEach;
        Assert.Equal(total, ParseInteger(llenResp));

        // Now drain the whole list with LPOP and assert every pushed element comes back
        // exactly once — nothing lost, nothing duplicated.
        var popped = new List<string>();
        for (int i = 0; i < total; i++)
        {
            string resp = await verifier.SendCommandAsync("LPOP", key);
            popped.Add(ParseBulkString(resp));
        }

        var expected = new HashSet<string>();
        for (int p = 0; p < producers; p++)
            for (int i = 0; i < itemsEach; i++)
                expected.Add($"p{p}-{i}");

        Assert.Equal(total, popped.Count);
        Assert.Equal(expected.Count, popped.Distinct().Count());
        Assert.True(expected.SetEquals(popped), "Popped elements did not match the pushed set exactly.");

        // List is now empty.
        Assert.Equal(0, ParseInteger(await verifier.SendCommandAsync("LLEN", key)));
    }

    // ──────────────────────────────────────────────────────────────────────────
    // 2. BLPOP delivery race: element pushed at timeout boundary must not drop.
    // ──────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task BLPop_ElementDelivered_WhenPushedBeforeTimeout()
    {
        await using var server = await RedisServerFixture.StartAsync();
        const string key = "blkey";

        // Client A blocks with a 2-second timeout.
        await using var consumer = await RedisClient.ConnectAsync(server.Port);
        await consumer.SendRawAsync(BuildResp("BLPOP", key, "2"));

        // Wait a moment so the server has queued the blocked client.
        await Task.Delay(100);

        // Client B pushes an element well within the timeout.
        await using var producer = await RedisClient.ConnectAsync(server.Port);
        await producer.SendCommandAsync("RPUSH", key, "hello");

        // Client A should receive the element.
        string resp = await consumer.ReadResponseAsync();
        Assert.Contains("hello", resp);
    }

    [Fact]
    public async Task BLPop_TimesOutCleanly_WhenNoPush()
    {
        await using var server = await RedisServerFixture.StartAsync();
        const string key = "emptykey";

        await using var consumer = await RedisClient.ConnectAsync(server.Port);
        await consumer.SendRawAsync(BuildResp("BLPOP", key, "0.3"));

        // Wait slightly longer than the timeout.
        await Task.Delay(600);
        string resp = await consumer.ReadResponseAsync();

        // Nil multi-bulk reply signals a timeout.
        Assert.Contains("*-1", resp);
    }

    /// <summary>
    /// Stress-tests the delivery race: repeatedly push an element at the exact moment a
    /// short-lived BLPOP times out.  In the old code without the TrySetResult fix, the
    /// element would occasionally be dequeued from the list but the result would never
    /// reach the consumer (the element vanished).  After the fix, either the consumer
    /// receives the element OR the element stays in the list — neither path loses it.
    /// </summary>
    [Fact]
    public async Task BLPop_DeliveryRace_ElementNeverDropped()
    {
        await using var server = await RedisServerFixture.StartAsync();
        const string key = "racekey";

        for (int round = 0; round < 30; round++)
        {
            // Fresh consumers/producers each round so state is clean.
            await using var consumer = await RedisClient.ConnectAsync(server.Port);
            await using var producer = await RedisClient.ConnectAsync(server.Port);

            // 100 ms timeout — we'll push at ~90 ms (before it fires).
            await consumer.SendRawAsync(BuildResp("BLPOP", key, "0.1"));
            await Task.Delay(90);

            // Push exactly one element.
            string pushResp = await producer.SendCommandAsync("RPUSH", key, $"round{round}");

            // Either the consumer gets it, or it stays in the list. Neither should be empty.
            string blResp = await consumer.ReadResponseAsync();

            bool consumerGotIt = blResp.Contains($"round{round}");
            if (!consumerGotIt)
            {
                // Element must still be in the list — not dropped.
                string llen = await producer.SendCommandAsync("LLEN", key);
                int len = ParseInteger(llen);
                Assert.True(len >= 1,
                    $"Round {round}: element was neither delivered to consumer nor left in list (dropped).");
                // Clean up for next round.
                await producer.SendCommandAsync("LPOP", key);
            }
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // 3. AOF expiry replay: relative PX must not be re-applied on restart.
    // ──────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task AofReplay_PxExpiry_NotExtendedOnRestart()
    {
        string dataDir = Path.Combine(Path.GetTempPath(), $"redis-aof-test-{Guid.NewGuid()}");
        Directory.CreateDirectory(dataDir);

        try
        {
            // ── Phase 1: start server with AOF enabled, set a key with PX 8000 (8 s). ──
            // 8 s is long enough to survive server-restart overhead (~1-3 s on slow CI).
            const int ttlMs = 8000;
            long setAt = 0;

            await using (var server1 = await RedisServerFixture.StartAsync(dataDir, appendonly: true))
            {
                await using var c = await RedisClient.ConnectAsync(server1.Port);
                string setResp = await c.SendCommandAsync("SET", "expkey", "value", "PX", ttlMs.ToString());
                Assert.Contains("+OK", setResp);
                // Record the wall-clock time right after the server acknowledged SET so
                // we know when the absolute expiry was anchored.
                setAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            }
            // Server 1 is now stopped; fixture does NOT delete dataDir (caller-owned dir).

            // ── Phase 2: restart with the same AOF directory. ──
            await using (var server2 = await RedisServerFixture.StartAsync(dataDir, appendonly: true))
            {
                await using var c2 = await RedisClient.ConnectAsync(server2.Port);

                // Immediately after restart the key must still exist (AOF stored an
                // absolute PXAT, so the remaining TTL is honoured, not re-applied).
                string getResp = await c2.SendCommandAsync("GET", "expkey");
                Assert.Contains("value", getResp);

                // Wait until well past the original absolute deadline.
                // We measure elapsed time from the post-SET timestamp (setAt) and add
                // 1500 ms of grace to absorb clock drift and task-scheduling delays.
                long elapsed = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - setAt;
                int waitMs = (int)(ttlMs - elapsed) + 1500;
                if (waitMs > 0)
                    await Task.Delay(waitMs);

                string getResp2 = await c2.SendCommandAsync("GET", "expkey");
                Assert.Contains("$-1", getResp2);
            }
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task AofReplay_AlreadyExpiredKey_NotResurrected()
    {
        string dataDir = Path.Combine(Path.GetTempPath(), $"redis-aof-test-{Guid.NewGuid()}");
        Directory.CreateDirectory(dataDir);

        try
        {
            // ── Phase 1: set a key with a very short TTL. ──
            await using (var server1 = await RedisServerFixture.StartAsync(dataDir, appendonly: true))
            {
                await using var c = await RedisClient.ConnectAsync(server1.Port);
                await c.SendCommandAsync("SET", "shortkey", "val", "PX", "200");
            }

            // Wait for the TTL to lapse before restarting.
            await Task.Delay(400);

            // ── Phase 2: restart — the key must NOT come back. ──
            await using (var server2 = await RedisServerFixture.StartAsync(dataDir, appendonly: true))
            {
                await using var c2 = await RedisClient.ConnectAsync(server2.Port);
                string getResp = await c2.SendCommandAsync("GET", "shortkey");
                Assert.Contains("$-1", getResp);
            }
        }
        finally
        {
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // 4. Byte-based RESP: multi-byte UTF-8 payloads must round-trip correctly.
    //    Before the fix, responses used string.Length (char count) for the RESP
    //    bulk-string header, which is wrong for non-ASCII characters — e.g. "é"
    //    is 1 char but 2 bytes.  A compliant client would reject such responses
    //    because the declared length would not match the actual payload bytes.
    // ──────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ByteBasedResp_MultibyteSET_GET_RoundTrips()
    {
        await using var server = await RedisServerFixture.StartAsync();
        await using var client = await RedisClient.ConnectAsync(server.Port);

        // "café" — 4 chars but 5 UTF-8 bytes ('é' is U+00E9, encoded as 0xC3 0xA9).
        const string value = "café";
        string setResp = await client.SendCommandAsync("SET", "k", value);
        Assert.Contains("+OK", setResp);

        string getResp = await client.SendCommandAsync("GET", "k");

        // The response must declare the correct byte length (5, not 4).
        int expectedBytes = Encoding.UTF8.GetByteCount(value);
        Assert.StartsWith($"${expectedBytes}\r\n", getResp);
        Assert.Contains(value, getResp);
    }

    [Fact]
    public async Task ByteBasedResp_MultibyteECHO_CorrectByteCount()
    {
        await using var server = await RedisServerFixture.StartAsync();
        await using var client = await RedisClient.ConnectAsync(server.Port);

        // "日本語" — 3 chars but 9 UTF-8 bytes.
        const string payload = "日本語";
        string resp = await client.SendCommandAsync("ECHO", payload);

        int expectedBytes = Encoding.UTF8.GetByteCount(payload);
        Assert.StartsWith($"${expectedBytes}\r\n", resp);
        Assert.Contains(payload, resp);
    }

    [Fact]
    public async Task ByteBasedResp_MultibyteKey_KeysListCorrect()
    {
        await using var server = await RedisServerFixture.StartAsync();
        await using var client = await RedisClient.ConnectAsync(server.Port);

        // Key name with a multi-byte character.
        const string key = "clé";
        await client.SendCommandAsync("SET", key, "v");

        string keysResp = await client.SendCommandAsync("KEYS", "*");
        int expectedBytes = Encoding.UTF8.GetByteCount(key);
        Assert.Contains($"${expectedBytes}\r\n{key}", keysResp);
    }

    [Fact]
    public async Task ByteBasedResp_MultibyteListElement_LRangeCorrect()
    {
        await using var server = await RedisServerFixture.StartAsync();
        await using var client = await RedisClient.ConnectAsync(server.Port);

        const string element = "über";   // 4 chars, 5 UTF-8 bytes
        await client.SendCommandAsync("RPUSH", "mylist", element);

        string rangeResp = await client.SendCommandAsync("LRANGE", "mylist", "0", "-1");
        int expectedBytes = Encoding.UTF8.GetByteCount(element);
        Assert.Contains($"${expectedBytes}\r\n{element}", rangeResp);
    }



    private static string BuildResp(params string[] args)
    {
        var sb = new StringBuilder();
        sb.Append($"*{args.Length}\r\n");
        foreach (var a in args)
            sb.Append($"${Encoding.UTF8.GetByteCount(a)}\r\n{a}\r\n");
        return sb.ToString();
    }

    private static int ParseInteger(string resp)
    {
        // RESP integer reply: ":N\r\n"
        resp = resp.Trim();
        if (resp.StartsWith(':'))
            return int.Parse(resp[1..]);
        throw new InvalidDataException($"Expected RESP integer, got: {resp}");
    }

    private static string ParseBulkString(string resp)
    {
        // RESP bulk string reply: "$<len>\r\n<data>\r\n" (or "$-1\r\n" for nil).
        if (!resp.StartsWith('$'))
            throw new InvalidDataException($"Expected RESP bulk string, got: {resp}");
        int headerEnd = resp.IndexOf("\r\n", StringComparison.Ordinal);
        if (headerEnd < 0)
            throw new InvalidDataException($"Malformed RESP bulk string: {resp}");
        if (resp[1..headerEnd] == "-1")
            throw new InvalidDataException("Unexpected nil bulk string while draining list.");
        return resp[(headerEnd + 2)..].TrimEnd('\r', '\n');
    }
}
