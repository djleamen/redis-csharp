using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;

namespace codecrafters_redis;

/// <summary>
/// A Redis-compatible in-memory server implementing the RESP protocol.
/// Supports persistence (RDB), replication, transactions (MULTI/EXEC),
/// pub/sub, streams, sorted sets, lists, geospatial commands, and ACL authentication.
/// </summary>
partial class RedisServer
{
    private const string ReplicationId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private const int ReplicationOffset = 0;
    private const string WrongTypeError = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    private const string NoPass = "nopass";

    private readonly int _port;
    private readonly string _dir;
    private readonly string _dbFilename;
    private readonly string? _masterHost;
    private readonly int? _masterPort;
    private readonly bool _isReplica;

    private readonly string _appendonly;
    private readonly string _appenddirname;
    private readonly string _appendfilename;
    private readonly string _appendfsync;
    private string? _aofFilePath;

    private readonly ConcurrentDictionary<string, StoredValue> _dataStore = new();

    private readonly ConcurrentDictionary<string, Queue<BlockedClient>> _blockedClients = new();
    private readonly object _blockedClientsLock = new();

    private readonly ConcurrentDictionary<string, Queue<BlockedStreamReader>> _blockedStreamReaders = new();
    private readonly object _blockedStreamReadersLock = new();

    private readonly ConcurrentDictionary<string, HashSet<Socket>> _channelSubscribers = new();
    private readonly ConcurrentDictionary<Socket, HashSet<string>> _clientSubscriptions = new();
    private readonly object _subscriptionsLock = new();

    private readonly List<Socket> _replicaConnections = new();
    private readonly object _replicaConnectionsLock = new();
    private readonly Dictionary<Socket, long> _replicaAckOffsets = new();
    private long _replicaOffset;
    private long _masterOffset;

    private readonly ConcurrentDictionary<string, HashSet<Socket>> _keyWatchers = new();
    private readonly ConcurrentDictionary<Socket, bool> _watchDirty = new();
    private readonly object _watchLock = new();

    private readonly HashSet<string> _defaultUserFlags = new() { NoPass };
    private readonly List<string> _defaultUserPasswords = new();

    /// <summary>
    /// Initializes a new <see cref="RedisServer"/> with the specified configuration and
    /// loads any existing RDB snapshot from disk.
    /// </summary>
    /// <param name="port">TCP port to listen on.</param>
    /// <param name="dir">Directory containing the RDB file.</param>
    /// <param name="dbFilename">RDB filename.</param>
    /// <param name="masterHost">Master host for replica mode, or <c>null</c> for standalone/master mode.</param>
    /// <param name="masterPort">Master port for replica mode, or <c>null</c> for standalone/master mode.</param>
    public RedisServer(int port, string dir, string dbFilename, string? masterHost, int? masterPort,
        RedisServerOptions? options = null)
    {
        var opts = options ?? new RedisServerOptions();
        _port = port;
        _dir = dir;
        _dbFilename = dbFilename;
        _masterHost = masterHost;
        _masterPort = masterPort;
        _isReplica = masterHost != null && masterPort.HasValue;
        _appendonly = opts.Appendonly;
        _appenddirname = opts.Appenddirname;
        _appendfilename = opts.Appendfilename;
        _appendfsync = opts.Appendfsync;

        RdbLoader.Load(Path.Combine(dir, dbFilename), _dataStore);

        if (_appendonly.Equals("yes", StringComparison.OrdinalIgnoreCase))
        {
            string aofDir = Path.Combine(_dir, _appenddirname);
            Directory.CreateDirectory(aofDir);
            string manifestFile = Path.Combine(aofDir, $"{_appendfilename}.manifest");
            string aofFileName;
            if (File.Exists(manifestFile))
            {
                // Read manifest to find the active incremental AOF file (type i)
                string[] lines = File.ReadAllLines(manifestFile);
                aofFileName = $"{_appendfilename}.1.incr.aof"; // default fallback
                string? aofManifestLine = lines.FirstOrDefault(l => l.Contains("type i"));
                if (aofManifestLine != null)
                {
                    string[] tokens = aofManifestLine.Split(' ');
                    if (tokens.Length >= 2 && tokens[0] == "file")
                        aofFileName = tokens[1];
                }
            }
            else
            {
                aofFileName = $"{_appendfilename}.1.incr.aof";
                File.WriteAllText(manifestFile, $"file {aofFileName} seq 1 type i\n");
            }
            _aofFilePath = Path.Combine(aofDir, aofFileName);
            if (File.Exists(_aofFilePath))
                ReplayAof(_aofFilePath);
            else
                File.Create(_aofFilePath).Dispose();
        }
    }

    /// <summary>
    /// Starts the TCP listener, initiates replication if configured as a replica,
    /// and enters the accept loop.
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        var listener = new TcpListener(IPAddress.Any, _port);
        listener.Start();

        if (_isReplica && _masterHost != null && _masterPort.HasValue)
            _ = Task.Run(() => ConnectToMasterAsync(_masterHost, _masterPort.Value, _port), cancellationToken);

        while (!cancellationToken.IsCancellationRequested)
        {
            Socket client = await listener.AcceptSocketAsync(cancellationToken);
            client.NoDelay = true;
            _ = Task.Run(() => HandleClientAsync(client), cancellationToken);
        }
    }

    /// <summary>
    /// Services a single client connection from initial receive through clean-up on disconnect.
    /// Maintains per-connection state for authentication, transactions, subscriptions,
    /// and whether the connection is a replication stream.
    /// </summary>
    private async Task HandleClientAsync(Socket client)
    {
        var session = new ClientSession(_defaultUserFlags.Contains(NoPass));

        while (true)
        {
            try
            {
                byte[] buffer = new byte[1024];
                int bytesRead = await client.ReceiveAsync(buffer.AsMemory(), SocketFlags.None);
                if (bytesRead == 0) break;

                string input = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                string[] parts = RespParser.ParseArray(input);
                if (parts.Length == 0) continue;

                await ProcessCommandAsync(client, session, parts, input);
            }
            catch { break; }
        }

        CleanupClient(client);
    }

    // ── Per-connection mutable state ─────────────────────────────────────────
    private sealed class ClientSession
    {
        public bool InTransaction { get; set; }
        public List<string[]> TransactionQueue { get; } = new();
        public HashSet<string> WatchedKeys { get; } = new();
        public bool IsReplicationConnection { get; set; }
        public bool IsSubscribedMode { get; set; }
        public bool IsAuthenticated { get; set; }
        public ClientSession(bool isAuthenticated) => IsAuthenticated = isAuthenticated;
    }

    private async Task ProcessCommandAsync(Socket client, ClientSession session, string[] parts, string input)
    {
        string command = parts[0].ToUpper();

        if (session.IsSubscribedMode && !IsAllowedInSubMode(command))
        {
            string err = $"-ERR Can't execute '{command.ToLower()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n";
            await client.SendAsync(Encoding.UTF8.GetBytes(err).AsMemory(), SocketFlags.None);
            return;
        }

        if (!session.IsAuthenticated && !IsAllowedUnauthenticated(command))
        {
            await client.SendAsync(Encoding.UTF8.GetBytes("-NOAUTH Authentication required.\r\n").AsMemory(), SocketFlags.None);
            return;
        }

        string response = await HandleTransactionCommandAsync(client, session, command, parts)
                       ?? await DispatchCommandAsync(client, session, parts, input);

        if (!string.IsNullOrEmpty(response) && !session.IsReplicationConnection)
            await client.SendAsync(Encoding.UTF8.GetBytes(response).AsMemory(), SocketFlags.None);
    }

    private static bool IsAllowedInSubMode(string command) =>
        command is "SUBSCRIBE" or "UNSUBSCRIBE" or "PSUBSCRIBE" or "PUNSUBSCRIBE" or "PING" or "QUIT" or "RESET";

    private static bool IsAllowedUnauthenticated(string command) =>
        command is "AUTH" or "HELLO" or "QUIT" or "RESET";

    private async Task<string?> HandleTransactionCommandAsync(Socket client, ClientSession session, string command, string[] parts)
    {
        if (command == "MULTI")
        {
            session.InTransaction = true;
            session.TransactionQueue.Clear();
            return "+OK\r\n";
        }
        if (command == "EXEC") return await HandleExecAsync(client, session);
        if (command == "DISCARD") return HandleDiscard(client, session);
        if (command == "WATCH") return HandleWatch(client, session, parts);
        if (command == "UNWATCH") { ClearWatchState(client, session.WatchedKeys); return "+OK\r\n"; }
        if (session.InTransaction) { session.TransactionQueue.Add(parts); return "+QUEUED\r\n"; }
        return null;
    }

    private async Task<string> HandleExecAsync(Socket client, ClientSession session)
    {
        if (!session.InTransaction)
            return "-ERR EXEC without MULTI\r\n";

        string result;
        bool dirty = _watchDirty.TryGetValue(client, out var d) && d;
        if (dirty)
        {
            result = "*-1\r\n";
        }
        else
        {
            var responses = new List<string>();
            foreach (var queued in session.TransactionQueue)
                responses.Add(await ExecuteCommandAsync(queued, client));

            var sb = new StringBuilder();
            sb.Append($"*{responses.Count}\r\n");
            foreach (var r in responses)
                sb.Append(r);
            result = sb.ToString();
        }

        session.InTransaction = false;
        session.TransactionQueue.Clear();
        ClearWatchState(client, session.WatchedKeys);
        return result;
    }

    private string HandleDiscard(Socket client, ClientSession session)
    {
        if (!session.InTransaction)
            return "-ERR DISCARD without MULTI\r\n";
        session.InTransaction = false;
        session.TransactionQueue.Clear();
        ClearWatchState(client, session.WatchedKeys);
        return "+OK\r\n";
    }

    private string HandleWatch(Socket client, ClientSession session, string[] parts)
    {
        if (session.InTransaction)
            return "-ERR WATCH inside MULTI is not allowed\r\n";
        lock (_watchLock)
        {
            for (int i = 1; i < parts.Length; i++)
            {
                string watchKey = parts[i];
                session.WatchedKeys.Add(watchKey);
                if (!_keyWatchers.TryGetValue(watchKey, out var watchers))
                {
                    watchers = new HashSet<Socket>();
                    _keyWatchers[watchKey] = watchers;
                }
                watchers.Add(client);
            }
        }
        return "+OK\r\n";
    }

    private async Task<string> DispatchCommandAsync(Socket client, ClientSession session, string[] parts, string input)
    {
        string command = parts[0].ToUpper();
        return command switch
        {
            "PING" => session.IsSubscribedMode ? "*2\r\n$4\r\npong\r\n$0\r\n\r\n" : "+PONG\r\n",
            "ECHO" when parts.Length > 1 => $"${parts[1].Length}\r\n{parts[1]}\r\n",
            "INFO" => HandleInfo(parts),
            "CONFIG" when parts.Length >= 3 && parts[1].ToUpper() == "GET" => HandleConfigGet(parts[2]),
            "KEYS" when parts.Length >= 2 => HandleKeys(parts[1]),
            "REPLCONF" => HandleReplConf(client, parts),
            "PSYNC" when parts.Length >= 3 => await HandlePsyncAsync(client, session),
            "SET" when parts.Length >= 3 => HandleSet(client, session, parts, input),
            "GET" when parts.Length > 1 => GetStringValue(parts[1]),
            "INCR" when parts.Length >= 2 => HandleIncr(client, parts[1]),
            "ZADD" when parts.Length >= 4 => HandleZAdd(client, parts),
            "GEOADD" when parts.Length >= 5 => HandleGeoAdd(client, parts),
            "GEOPOS" when parts.Length >= 3 => GeoPos(parts),
            "GEODIST" when parts.Length >= 4 => GeoDist(parts[1], parts[2], parts[3]),
            "GEOSEARCH" when parts.Length >= 8 => GeoSearch(parts),
            "AUTH" when parts.Length >= 3 => HandleAuth(session, parts),
            "ACL" => HandleAcl(parts),
            "ZRANK" when parts.Length >= 3 => ZRank(parts[1], parts[2]),
            "ZRANGE" when parts.Length >= 4 => ZRange(parts[1], parts[2], parts[3]),
            "ZCARD" when parts.Length >= 2 => ZCard(parts[1]),
            "ZSCORE" when parts.Length >= 3 => ZScore(parts[1], parts[2]),
            "ZREM" when parts.Length >= 3 => HandleZRem(client, parts),
            "WAIT" when parts.Length >= 3 => await WaitForReplicas(parts[1], parts[2]),
            "RPUSH" when parts.Length >= 3 => await HandleRPushAsync(client, parts),
            "LPUSH" when parts.Length >= 3 => await HandleLPushAsync(client, parts),
            "LRANGE" when parts.Length >= 4 => LRange(parts[1], parts[2], parts[3]),
            "LLEN" when parts.Length >= 2 => LLen(parts[1]),
            "LPOP" when parts.Length >= 2 => LPop(parts) ?? string.Empty,
            "BLPOP" when parts.Length >= 3 => await BLPop(parts[1], parts[2]),
            "TYPE" when parts.Length >= 2 => TypeOf(parts[1]),
            "XADD" when parts.Length >= 4 => HandleXAdd(client, parts),
            "XREAD" when parts.Length >= 4 => await XRead(parts),
            "XRANGE" when parts.Length >= 4 => XRange(parts[1], parts[2], parts[3]),
            "PUBLISH" when parts.Length >= 3 => Publish(parts[1], parts[2]),
            "SUBSCRIBE" when parts.Length >= 2 => HandleSubscribe(client, session, parts),
            "UNSUBSCRIBE" when parts.Length >= 2 => HandleUnsubscribe(client, session, parts),
            _ => "-ERR unknown command\r\n"
        };
    }

    private string HandleInfo(string[] parts)
    {
        if (parts.Length == 1 || parts[1].ToUpper() == "REPLICATION")
        {
            string role = _isReplica ? "slave" : "master";
            string info = _isReplica
                ? $"role:{role}"
                : $"role:{role}\r\nmaster_replid:{ReplicationId}\r\nmaster_repl_offset:{ReplicationOffset}";
            return $"${info.Length}\r\n{info}\r\n";
        }
        return "$0\r\n\r\n";
    }

    private string HandleConfigGet(string paramName)
    {
        string parameter = paramName.ToLower();
        string? value = parameter switch
        {
            "dir" => _dir,
            "dbfilename" => _dbFilename,
            "appendonly" => _appendonly,
            "appenddirname" => _appenddirname,
            "appendfilename" => _appendfilename,
            "appendfsync" => _appendfsync,
            _ => null
        };
        return value != null
            ? $"*2\r\n${parameter.Length}\r\n{parameter}\r\n${value.Length}\r\n{value}\r\n"
            : "*0\r\n";
    }

    private string HandleKeys(string pattern)
    {
        var matchingKeys = new List<string>();
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        foreach (var key in _dataStore.Keys)
        {
            if (pattern != "*" && key != pattern) continue;
            if (_dataStore.TryGetValue(key, out StoredValue? sv) &&
                (!sv.ExpiryMs.HasValue || now <= sv.ExpiryMs.Value))
                matchingKeys.Add(key);
        }
        var sb = new StringBuilder();
        sb.Append($"*{matchingKeys.Count}\r\n");
        foreach (var key in matchingKeys)
            sb.Append($"${key.Length}\r\n{key}\r\n");
        return sb.ToString();
    }

    private string HandleReplConf(Socket client, string[] parts)
    {
        if (parts.Length >= 3 && parts[1].ToUpper() == "ACK")
        {
            if (long.TryParse(parts[2], out long ackOffset))
            {
                lock (_replicaConnectionsLock)
                {
                    if (_replicaConnections.Contains(client))
                        _replicaAckOffsets[client] = ackOffset;
                }
            }
            return string.Empty;
        }
        return "+OK\r\n";
    }

    private async Task<string> HandlePsyncAsync(Socket client, ClientSession session)
    {
        string fullresync = $"+FULLRESYNC {ReplicationId} {ReplicationOffset}\r\n";
        byte[] fullresyncBytes = Encoding.UTF8.GetBytes(fullresync);
        byte[] rdbFile = Convert.FromBase64String("UkVESVMwMDA5/2NhMOXkSGD0");
        byte[] rdbHeader = Encoding.UTF8.GetBytes($"${rdbFile.Length}\r\n");
        byte[] responseData = new byte[fullresyncBytes.Length + rdbHeader.Length + rdbFile.Length];
        Array.Copy(fullresyncBytes, 0, responseData, 0, fullresyncBytes.Length);
        Array.Copy(rdbHeader, 0, responseData, fullresyncBytes.Length, rdbHeader.Length);
        Array.Copy(rdbFile, 0, responseData, fullresyncBytes.Length + rdbHeader.Length, rdbFile.Length);
        await client.SendAsync(responseData.AsMemory(), SocketFlags.None);
        lock (_replicaConnectionsLock)
        {
            _replicaConnections.Add(client);
            _replicaAckOffsets[client] = 0;
        }
        session.IsReplicationConnection = true;
        return string.Empty;
    }

    private string HandleSet(Socket client, ClientSession session, string[] parts, string input)
    {
        string key = parts[1];
        long? expiryMs = ParseSetExpiry(parts);
        _dataStore[key] = new StoredValue(parts[2], expiryMs);
        if (_appendonly.Equals("yes", StringComparison.OrdinalIgnoreCase))
            AppendToAof(parts);
        NotifyKeyModified(key, client);
        if (!session.IsReplicationConnection)
            PropagateToReplicas(input);
        return "+OK\r\n";
    }

    private string HandleIncr(Socket client, string key)
    {
        string result = IncrementKey(key);
        NotifyKeyModified(key, client);
        return result;
    }

    private string HandleZAdd(Socket client, string[] parts)
    {
        string result = ZAdd(parts[1], parts[2], parts[3]);
        NotifyKeyModified(parts[1], client);
        return result;
    }

    private string HandleGeoAdd(Socket client, string[] parts)
    {
        string result = GeoAdd(parts);
        NotifyKeyModified(parts[1], client);
        return result;
    }

    private string HandleAuth(ClientSession session, string[] parts)
    {
        var (authenticated, resp) = Authenticate(parts[1], parts[2]);
        session.IsAuthenticated = authenticated;
        return resp;
    }

    private string HandleAcl(string[] parts)
    {
        if (parts.Length >= 2 && parts[1].ToUpper() == "WHOAMI") return "$7\r\ndefault\r\n";
        if (parts.Length >= 3 && parts[1].ToUpper() == "SETUSER") return AclSetUser(parts);
        if (parts.Length >= 3 && parts[1].ToUpper() == "GETUSER") return AclGetUser(parts[2]);
        return "-ERR unknown command\r\n";
    }

    private string HandleZRem(Socket client, string[] parts)
    {
        string result = ZRem(parts[1], parts[2]);
        NotifyKeyModified(parts[1], client);
        return result;
    }

    private async Task<string> HandleRPushAsync(Socket client, string[] parts)
    {
        string key = parts[1];
        var elements = parts.Skip(2).ToArray();
        bool shouldUnblock;
        string response;

        if (!_dataStore.ContainsKey(key))
        {
            _dataStore[key] = new StoredValue(new List<string>(elements));
            response = $":{elements.Length}\r\n";
            shouldUnblock = true;
        }
        else if (_dataStore.TryGetValue(key, out StoredValue? sv) && sv.List != null)
        {
            sv.List.AddRange(elements);
            response = $":{sv.List.Count}\r\n";
            shouldUnblock = true;
        }
        else
        {
            response = WrongTypeError;
            shouldUnblock = false;
        }

        await client.SendAsync(Encoding.UTF8.GetBytes(response).AsMemory(), SocketFlags.None);
        if (shouldUnblock)
        {
            UnblockWaitingClients(key);
            NotifyKeyModified(key, client);
        }
        return string.Empty;
    }

    private async Task<string> HandleLPushAsync(Socket client, string[] parts)
    {
        string key = parts[1];
        var elements = parts.Skip(2).ToArray();
        bool shouldUnblock;
        string response;

        if (!_dataStore.ContainsKey(key))
        {
            _dataStore[key] = new StoredValue(new List<string>(elements.Reverse()));
            response = $":{elements.Length}\r\n";
            shouldUnblock = true;
        }
        else if (_dataStore.TryGetValue(key, out StoredValue? sv) && sv.List != null)
        {
            for (int i = 0; i < elements.Length; i++)
                sv.List.Insert(0, elements[i]);
            response = $":{sv.List.Count}\r\n";
            shouldUnblock = true;
        }
        else
        {
            response = WrongTypeError;
            shouldUnblock = false;
        }

        await client.SendAsync(Encoding.UTF8.GetBytes(response).AsMemory(), SocketFlags.None);
        if (shouldUnblock)
        {
            UnblockWaitingClients(key);
            NotifyKeyModified(key, client);
        }
        return string.Empty;
    }

    private string HandleXAdd(Socket client, string[] parts)
    {
        string result = XAdd(parts);
        NotifyKeyModified(parts[1], client);
        return result;
    }

    private string HandleSubscribe(Socket client, ClientSession session, string[] parts)
    {
        bool isSubscribedMode = session.IsSubscribedMode;
        Subscribe(client, parts.Skip(1).ToArray(), ref isSubscribedMode);
        session.IsSubscribedMode = isSubscribedMode;
        return string.Empty;
    }

    private string HandleUnsubscribe(Socket client, ClientSession session, string[] parts)
    {
        bool isSubscribedMode = session.IsSubscribedMode;
        Unsubscribe(client, parts.Skip(1).ToArray(), ref isSubscribedMode);
        session.IsSubscribedMode = isSubscribedMode;
        return string.Empty;
    }

    private void CleanupClient(Socket client)
    {
        lock (_replicaConnectionsLock)
        {
            _replicaConnections.Remove(client);
            _replicaAckOffsets.Remove(client);
        }

        lock (_subscriptionsLock)
        {
            if (_clientSubscriptions.TryGetValue(client, out HashSet<string>? channels))
            {
                foreach (string channel in channels)
                {
                    if (_channelSubscribers.TryGetValue(channel, out HashSet<Socket>? subs))
                    {
                        subs.Remove(client);
                        if (subs.Count == 0)
                            _channelSubscribers.TryRemove(channel, out _);
                    }
                }
                _clientSubscriptions.TryRemove(client, out _);
            }
        }

        client.Close();
    }

    /// <summary>
    /// Executes a single command, as invoked during EXEC for queued transaction commands.
    /// Returns the RESP-encoded response string.
    /// </summary>
    /// <param name="parts">The parsed command array (command name + arguments).</param>
    /// <param name="client">The client socket, used for contextual operations.</param>
    private async Task<string> ExecuteCommandAsync(string[] parts, Socket client)
    {
        if (parts.Length == 0)
            return "-ERR empty command\r\n";

        string command = parts[0].ToUpper();

        return command switch
        {
            "PING" => "+PONG\r\n",
            "ECHO" when parts.Length > 1 => $"${parts[1].Length}\r\n{parts[1]}\r\n",
            "SET" when parts.Length >= 3 => ExecSet(parts, client),
            "GET" when parts.Length > 1 => GetStringValue(parts[1]),
            "INCR" when parts.Length >= 2 => IncrementKey(parts[1]),
            "ZADD" when parts.Length >= 4 => ZAdd(parts[1], parts[2], parts[3]),
            "GEOADD" when parts.Length >= 5 => GeoAdd(parts),
            "GEOPOS" when parts.Length >= 3 => GeoPos(parts),
            "GEODIST" when parts.Length >= 4 => GeoDist(parts[1], parts[2], parts[3]),
            "GEOSEARCH" when parts.Length >= 8 => GeoSearch(parts),
            "AUTH" when parts.Length >= 3 => Authenticate(parts[1], parts[2]).response,
            "ACL" when parts.Length >= 2 && parts[1].ToUpper() == "WHOAMI" => "$7\r\ndefault\r\n",
            "ACL" when parts.Length >= 3 && parts[1].ToUpper() == "SETUSER" => AclSetUser(parts),
            "ACL" when parts.Length >= 3 && parts[1].ToUpper() == "GETUSER" => AclGetUser(parts[2]),
            "ZRANK" when parts.Length >= 3 => ZRank(parts[1], parts[2]),
            "ZRANGE" when parts.Length >= 4 => ZRange(parts[1], parts[2], parts[3]),
            "ZCARD" when parts.Length >= 2 => ZCard(parts[1]),
            "ZSCORE" when parts.Length >= 3 => ZScore(parts[1], parts[2]),
            "ZREM" when parts.Length >= 3 => ZRem(parts[1], parts[2]),
            "LRANGE" when parts.Length >= 4 => LRange(parts[1], parts[2], parts[3]),
            "LLEN" when parts.Length >= 2 => LLen(parts[1]),
            "LPOP" when parts.Length >= 2 => LPop(parts),
            "TYPE" when parts.Length >= 2 => TypeOf(parts[1]),
            "XRANGE" when parts.Length >= 4 => XRange(parts[1], parts[2], parts[3]),
            _ => "-ERR unknown command\r\n"
        };
    }

    /// <summary>
    /// Executes a SET command within a MULTI/EXEC transaction.
    /// </summary>
    private string ExecSet(string[] parts, Socket client)
    {
        _dataStore[parts[1]] = new StoredValue(parts[2], ParseSetExpiry(parts));
        NotifyKeyModified(parts[1], client);
        return "+OK\r\n";
    }

    /// <summary>
    /// Retrieves the string value for <paramref name="key"/>, returning a RESP bulk string
    /// or null bulk string if the key is missing or expired.
    /// </summary>
    private string GetStringValue(string key)
    {
        if (!_dataStore.TryGetValue(key, out StoredValue? sv))
            return "$-1\r\n";

        if (sv.ExpiryMs.HasValue && DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() > sv.ExpiryMs.Value)
        {
            _dataStore.TryRemove(key, out _);
            return "$-1\r\n";
        }

        return sv.Value != null
            ? $"${sv.Value.Length}\r\n{sv.Value}\r\n"
            : WrongTypeError;
    }

    /// <summary>
    /// Atomically increments the integer value stored at <paramref name="key"/> by one.
    /// Creates the key with value 1 if it does not exist.
    /// </summary>
    private string IncrementKey(string key)
    {
        if (_dataStore.TryGetValue(key, out StoredValue? sv))
        {
            if (sv.ExpiryMs.HasValue && DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() > sv.ExpiryMs.Value)
            {
                _dataStore.TryRemove(key, out _);
                _dataStore[key] = new StoredValue("1");
                return ":1\r\n";
            }

            if (sv.Value == null)
                return WrongTypeError;

            if (!int.TryParse(sv.Value, out int current))
                return "-ERR value is not an integer or out of range\r\n";

            int next = current + 1;
            _dataStore[key] = new StoredValue(next.ToString(), sv.ExpiryMs);
            return $":{next}\r\n";
        }

        _dataStore[key] = new StoredValue("1");
        return ":1\r\n";
    }
    /// <summary>
    /// Validates credentials against the default user's ACL rules.
    /// Returns the updated authentication flag and the RESP response.
    /// </summary>
    private (bool authenticated, string response) Authenticate(string username, string password)
    {
        if (username != "default")
            return (false, "-WRONGPASS invalid username-password pair or user is disabled.\r\n");

        if (_defaultUserFlags.Contains(NoPass))
            return (true, "+OK\r\n");

        byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes(password));
        string hexHash = Convert.ToHexString(hash).ToLower();

        return _defaultUserPasswords.Contains(hexHash)
            ? (true, "+OK\r\n")
            : (false, "-WRONGPASS invalid username-password pair or user is disabled.\r\n");
    }

    /// <summary>
    /// Applies ACL rules to a user, currently supporting password addition via the
    /// <c>&gt;password</c> directive on the default user.
    /// </summary>
    private string AclSetUser(string[] parts)
    {
        string username = parts[2];
        if (username != "default")
            return "-ERR unknown user\r\n";

        for (int i = 3; i < parts.Length; i++)
        {
            string rule = parts[i];
            if (!rule.StartsWith('>')) continue;

            string password = rule.Substring(1);
            byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes(password));
            string hexHash = Convert.ToHexString(hash).ToLower();

            if (!_defaultUserPasswords.Contains(hexHash))
                _defaultUserPasswords.Add(hexHash);

            _defaultUserFlags.Remove(NoPass);
        }

        return "+OK\r\n";
    }

    /// <summary>
    /// Returns the ACL flags and password hashes for the specified user.
    /// </summary>
    private string AclGetUser(string username)
    {
        if (username != "default")
            return "$-1\r\n";

        var flags = _defaultUserFlags.ToList();
        var sb = new StringBuilder();
        sb.Append("*4\r\n");
        sb.Append("$5\r\nflags\r\n");
        sb.Append($"*{flags.Count}\r\n");
        foreach (var f in flags) sb.Append($"${f.Length}\r\n{f}\r\n");
        sb.Append("$9\r\npasswords\r\n");
        sb.Append($"*{_defaultUserPasswords.Count}\r\n");
        foreach (var p in _defaultUserPasswords) sb.Append($"${p.Length}\r\n{p}\r\n");
        return sb.ToString();
    }

    /// <summary>
    /// Returns the Redis type of the value stored at <paramref name="key"/>:
    /// string, list, stream, zset, or none.
    /// </summary>
    private string TypeOf(string key)
    {
        if (!_dataStore.TryGetValue(key, out StoredValue? sv))
            return "+none\r\n";

        if (sv.Value != null)
        {
            if (sv.ExpiryMs.HasValue && DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() > sv.ExpiryMs.Value)
            {
                _dataStore.TryRemove(key, out _);
                return "+none\r\n";
            }
            return "+string\r\n";
        }

        if (sv.List != null) return "+list\r\n";
        if (sv.Stream != null) return "+stream\r\n";
        if (sv.SortedSet != null) return "+zset\r\n";
        return "+none\r\n";
    }

    /// <summary>
    /// Marks all clients watching <paramref name="key"/> (other than <paramref name="modifier"/>)
    /// as dirty so their next EXEC will abort.
    /// </summary>
    private void NotifyKeyModified(string key, Socket modifier)
    {
        lock (_watchLock)
        {
            if (_keyWatchers.TryGetValue(key, out var watchers))
            {
                foreach (var watcher in watchers.Where(w => w != modifier))
                    _watchDirty[watcher] = true;
            }
        }
    }

    /// <summary>
    /// Removes <paramref name="client"/> from all key-watch sets and clears its dirty flag.
    /// </summary>
    private void ClearWatchState(Socket client, HashSet<string> watchedKeys)
    {
        lock (_watchLock)
        {
            foreach (var key in watchedKeys)
            {
                if (_keyWatchers.TryGetValue(key, out var watchers))
                {
                    watchers.Remove(client);
                    if (watchers.Count == 0)
                        _keyWatchers.TryRemove(key, out _);
                }
            }
            _watchDirty.TryRemove(client, out _);
        }
        watchedKeys.Clear();
    }

    /// <summary>
    /// Parses the optional PX or EX expiry argument from a SET command and returns the
    /// absolute Unix millisecond expiry timestamp, or <c>null</c> if no expiry was specified.
    /// </summary>
    private static long? ParseSetExpiry(string[] parts)
    {
        for (int i = 3; i < parts.Length - 1; i++)
        {
            string opt = parts[i].ToUpper();
            if (opt == "PX" && long.TryParse(parts[i + 1], out long px))
                return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + px;
            if (opt == "EX" && long.TryParse(parts[i + 1], out long ex))
                return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + (ex * 1000);
        }
        return null;
    }

    /// <summary>
    /// Appends a command to the append-only file in RESP format, optionally fsyncing
    /// to disk when <c>appendfsync</c> is set to <c>always</c>.
    /// </summary>
    private void AppendToAof(string[] parts)
    {
        if (_aofFilePath == null) return;

        var sb = new StringBuilder();
        sb.Append($"*{parts.Length}\r\n");
        foreach (string part in parts)
            sb.Append($"${part.Length}\r\n{part}\r\n");

        using var fs = new FileStream(_aofFilePath, FileMode.Append, FileAccess.Write, FileShare.Read);
        byte[] data = Encoding.UTF8.GetBytes(sb.ToString());
        fs.Write(data, 0, data.Length);
        if (_appendfsync.Equals("always", StringComparison.OrdinalIgnoreCase))
            fs.Flush(flushToDisk: true);
    }

    /// <summary>
    /// Replays all commands stored in the AOF file at <paramref name="path"/> into the
    /// in-memory data store during startup.
    /// </summary>
    private void ReplayAof(string path)
    {
        string data = File.ReadAllText(path);
        int offset = 0;
        while (offset < data.Length)
        {
            var (parts, consumed) = RespParser.TryParseCommand(data.Substring(offset));
            if (parts == null || consumed == 0)
                break;

            string command = parts[0].ToUpper();
            if (command == "SET" && parts.Length >= 3)
            {
                long? expiryMs = ParseSetExpiry(parts);
                _dataStore[parts[1]] = new StoredValue(parts[2], expiryMs);
            }

            offset += consumed;
        }
    }
}
