using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;

/// <summary>
/// A Redis-compatible in-memory server implementing the RESP protocol.
/// Supports persistence (RDB), replication, transactions (MULTI/EXEC),
/// pub/sub, streams, sorted sets, lists, geospatial commands, and ACL authentication.
/// </summary>
class RedisServer
{
    private const string ReplicationId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    private const int ReplicationOffset = 0;

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

    private readonly HashSet<string> _defaultUserFlags = new() { "nopass" };
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
        string appendonly = "no", string appenddirname = "appendonlydir",
        string appendfilename = "appendonly.aof", string appendfsync = "everysec")
    {
        _port = port;
        _dir = dir;
        _dbFilename = dbFilename;
        _masterHost = masterHost;
        _masterPort = masterPort;
        _isReplica = masterHost != null && masterPort.HasValue;
        _appendonly = appendonly;
        _appenddirname = appenddirname;
        _appendfilename = appendfilename;
        _appendfsync = appendfsync;

        RdbLoader.Load(Path.Combine(dir, dbFilename), _dataStore);

        if (_appendonly.Equals("yes", StringComparison.OrdinalIgnoreCase))
        {
            string aofDir = Path.Combine(_dir, _appenddirname);
            Directory.CreateDirectory(aofDir);
            string aofFile = Path.Combine(aofDir, $"{_appendfilename}.1.incr.aof");
            if (!File.Exists(aofFile))
                File.Create(aofFile).Dispose();
        }
    }

    /// <summary>
    /// Starts the TCP listener, initiates replication if configured as a replica,
    /// and enters the accept loop.
    /// </summary>
    public async Task RunAsync()
    {
        var listener = new TcpListener(IPAddress.Any, _port);
        listener.Start();

        if (_isReplica && _masterHost != null && _masterPort.HasValue)
            _ = Task.Run(() => ConnectToMasterAsync(_masterHost, _masterPort.Value, _port));

        while (true)
        {
            Socket client = listener.AcceptSocket();
            client.NoDelay = true;
            _ = Task.Run(() => HandleClientAsync(client));
        }
    }

    // -------------------------------------------------------------------------
    // Client handling
    // -------------------------------------------------------------------------

    /// <summary>
    /// Services a single client connection from initial receive through clean-up on disconnect.
    /// Maintains per-connection state for authentication, transactions, subscriptions,
    /// and whether the connection is a replication stream.
    /// </summary>
    private async Task HandleClientAsync(Socket client)
    {
        bool inTransaction = false;
        var transactionQueue = new List<string[]>();
        var watchedKeys = new HashSet<string>();
        bool isReplicationConnection = false;
        bool isSubscribedMode = false;
        bool isAuthenticated = _defaultUserFlags.Contains("nopass");

        while (true)
        {
            try
            {
                byte[] buffer = new byte[1024];
                int bytesRead = client.Receive(buffer);

                if (bytesRead == 0)
                    break;

                string input = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                string[] parts = RespParser.ParseArray(input);
                if (parts.Length == 0)
                    continue;

                string command = parts[0].ToUpper();
                string response = string.Empty;

                if (isSubscribedMode)
                {
                    string[] allowedInSubMode = { "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT", "RESET" };
                    if (!allowedInSubMode.Contains(command))
                    {
                        response = $"-ERR Can't execute '{command.ToLower()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n";
                        client.Send(Encoding.UTF8.GetBytes(response));
                        continue;
                    }
                }

                if (!isAuthenticated && command != "AUTH" && command != "HELLO" && command != "QUIT" && command != "RESET")
                {
                    client.Send(Encoding.UTF8.GetBytes("-NOAUTH Authentication required.\r\n"));
                    continue;
                }

                if (command == "MULTI")
                {
                    inTransaction = true;
                    transactionQueue.Clear();
                    response = "+OK\r\n";
                }
                else if (command == "EXEC")
                {
                    if (inTransaction)
                    {
                        bool dirty = _watchDirty.TryGetValue(client, out var d) && d;
                        if (dirty)
                        {
                            response = "*-1\r\n";
                        }
                        else
                        {
                            var responses = new List<string>();
                            foreach (var queued in transactionQueue)
                                responses.Add(await ExecuteCommandAsync(queued, client));

                            var sb = new StringBuilder();
                            sb.Append($"*{responses.Count}\r\n");
                            foreach (var r in responses)
                                sb.Append(r);

                            response = sb.ToString();
                        }
                        inTransaction = false;
                        transactionQueue.Clear();
                        ClearWatchState(client, watchedKeys);
                    }
                    else
                    {
                        response = "-ERR EXEC without MULTI\r\n";
                    }
                }
                else if (command == "DISCARD")
                {
                    if (inTransaction)
                    {
                        inTransaction = false;
                        transactionQueue.Clear();
                        ClearWatchState(client, watchedKeys);
                        response = "+OK\r\n";
                    }
                    else
                    {
                        response = "-ERR DISCARD without MULTI\r\n";
                    }
                }
                else if (command == "WATCH")
                {
                    if (inTransaction)
                    {
                        response = "-ERR WATCH inside MULTI is not allowed\r\n";
                    }
                    else
                    {
                        lock (_watchLock)
                        {
                            for (int i = 1; i < parts.Length; i++)
                            {
                                string watchKey = parts[i];
                                watchedKeys.Add(watchKey);
                                if (!_keyWatchers.TryGetValue(watchKey, out var watchers))
                                {
                                    watchers = new HashSet<Socket>();
                                    _keyWatchers[watchKey] = watchers;
                                }
                                watchers.Add(client);
                            }
                        }
                        response = "+OK\r\n";
                    }
                }
                else if (command == "UNWATCH")
                {
                    ClearWatchState(client, watchedKeys);
                    response = "+OK\r\n";
                }
                else if (inTransaction)
                {
                    transactionQueue.Add(parts);
                    response = "+QUEUED\r\n";
                }
                else if (command == "PING")
                {
                    response = isSubscribedMode ? "*2\r\n$4\r\npong\r\n$0\r\n\r\n" : "+PONG\r\n";
                }
                else if (command == "ECHO" && parts.Length > 1)
                {
                    string message = parts[1];
                    response = $"${message.Length}\r\n{message}\r\n";
                }
                else if (command == "INFO")
                {
                    if (parts.Length == 1 || parts[1].ToUpper() == "REPLICATION")
                    {
                        string role = _isReplica ? "slave" : "master";
                        string info = _isReplica
                            ? $"role:{role}"
                            : $"role:{role}\r\nmaster_replid:{ReplicationId}\r\nmaster_repl_offset:{ReplicationOffset}";
                        response = $"${info.Length}\r\n{info}\r\n";
                    }
                    else
                    {
                        response = "$0\r\n\r\n";
                    }
                }
                else if (command == "CONFIG" && parts.Length >= 3 && parts[1].ToUpper() == "GET")
                {
                    string parameter = parts[2].ToLower();
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

                    response = value != null
                        ? $"*2\r\n${parameter.Length}\r\n{parameter}\r\n${value.Length}\r\n{value}\r\n"
                        : "*0\r\n";
                }
                else if (command == "KEYS" && parts.Length >= 2)
                {
                    string pattern = parts[1];
                    var matchingKeys = new List<string>();
                    long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                    foreach (var key in _dataStore.Keys)
                    {
                        if (pattern != "*" && key != pattern)
                            continue;
                        if (_dataStore.TryGetValue(key, out StoredValue? sv) &&
                            (!sv.ExpiryMs.HasValue || now <= sv.ExpiryMs.Value))
                        {
                            matchingKeys.Add(key);
                        }
                    }

                    var sb = new StringBuilder();
                    sb.Append($"*{matchingKeys.Count}\r\n");
                    foreach (var key in matchingKeys)
                        sb.Append($"${key.Length}\r\n{key}\r\n");

                    response = sb.ToString();
                }
                else if (command == "REPLCONF")
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
                        response = string.Empty;
                    }
                    else
                    {
                        response = "+OK\r\n";
                    }
                }
                else if (command == "PSYNC" && parts.Length >= 3)
                {
                    string fullresync = $"+FULLRESYNC {ReplicationId} {ReplicationOffset}\r\n";
                    byte[] fullresyncBytes = Encoding.UTF8.GetBytes(fullresync);

                    byte[] rdbFile = Convert.FromBase64String("UkVESVMwMDA5/2NhMOXkSGD0");
                    byte[] rdbHeader = Encoding.UTF8.GetBytes($"${rdbFile.Length}\r\n");

                    byte[] responseData = new byte[fullresyncBytes.Length + rdbHeader.Length + rdbFile.Length];
                    Array.Copy(fullresyncBytes, 0, responseData, 0, fullresyncBytes.Length);
                    Array.Copy(rdbHeader, 0, responseData, fullresyncBytes.Length, rdbHeader.Length);
                    Array.Copy(rdbFile, 0, responseData, fullresyncBytes.Length + rdbHeader.Length, rdbFile.Length);

                    client.Send(responseData);

                    lock (_replicaConnectionsLock)
                    {
                        _replicaConnections.Add(client);
                        _replicaAckOffsets[client] = 0;
                    }
                    isReplicationConnection = true;
                    continue;
                }
                else if (command == "SET" && parts.Length >= 3)
                {
                    string key = parts[1];
                    string value = parts[2];
                    long? expiryMs = ParseSetExpiry(parts);

                    _dataStore[key] = new StoredValue(value, expiryMs);
                    response = "+OK\r\n";
                    NotifyKeyModified(key, client);

                    if (!isReplicationConnection)
                        PropagateToReplicas(input);
                }
                else if (command == "GET" && parts.Length > 1)
                {
                    response = GetStringValue(parts[1]);
                }
                else if (command == "INCR" && parts.Length >= 2)
                {
                    response = IncrementKey(parts[1]);
                    NotifyKeyModified(parts[1], client);
                }
                else if (command == "ZADD" && parts.Length >= 4)
                {
                    response = ZAdd(parts[1], parts[2], parts[3]);
                    NotifyKeyModified(parts[1], client);
                }
                else if (command == "GEOADD" && parts.Length >= 5)
                {
                    response = GeoAdd(parts);
                    NotifyKeyModified(parts[1], client);
                }
                else if (command == "GEOPOS" && parts.Length >= 3)
                {
                    response = GeoPos(parts);
                }
                else if (command == "GEODIST" && parts.Length >= 4)
                {
                    response = GeoDist(parts[1], parts[2], parts[3]);
                }
                else if (command == "GEOSEARCH" && parts.Length >= 8)
                {
                    response = GeoSearch(parts);
                }
                else if (command == "AUTH" && parts.Length >= 3)
                {
                    (isAuthenticated, response) = Authenticate(parts[1], parts[2]);
                }
                else if (command == "ACL" && parts.Length >= 2 && parts[1].ToUpper() == "WHOAMI")
                {
                    response = "$7\r\ndefault\r\n";
                }
                else if (command == "ACL" && parts.Length >= 3 && parts[1].ToUpper() == "SETUSER")
                {
                    response = AclSetUser(parts);
                }
                else if (command == "ACL" && parts.Length >= 3 && parts[1].ToUpper() == "GETUSER")
                {
                    response = AclGetUser(parts[2]);
                }
                else if (command == "ZRANK" && parts.Length >= 3)
                {
                    response = ZRank(parts[1], parts[2]);
                }
                else if (command == "ZRANGE" && parts.Length >= 4)
                {
                    response = ZRange(parts[1], parts[2], parts[3]);
                }
                else if (command == "ZCARD" && parts.Length >= 2)
                {
                    response = ZCard(parts[1]);
                }
                else if (command == "ZSCORE" && parts.Length >= 3)
                {
                    response = ZScore(parts[1], parts[2]);
                }
                else if (command == "ZREM" && parts.Length >= 3)
                {
                    response = ZRem(parts[1], parts[2]);
                    NotifyKeyModified(parts[1], client);
                }
                else if (command == "WAIT" && parts.Length >= 3)
                {
                    response = await WaitForReplicas(parts[1], parts[2]);
                }
                else if (command == "RPUSH" && parts.Length >= 3)
                {
                    string key = parts[1];
                    var elements = parts.Skip(2).ToArray();
                    bool shouldUnblock = false;

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
                        response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                    }

                    if (!string.IsNullOrEmpty(response))
                    {
                        client.Send(Encoding.UTF8.GetBytes(response));
                        response = string.Empty;
                    }

                    if (shouldUnblock)
                    {
                        UnblockWaitingClients(key);
                        NotifyKeyModified(key, client);
                    }
                }
                else if (command == "LPUSH" && parts.Length >= 3)
                {
                    string key = parts[1];
                    var elements = parts.Skip(2).ToArray();
                    bool shouldUnblock = false;

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
                        response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                    }

                    if (!string.IsNullOrEmpty(response))
                    {
                        client.Send(Encoding.UTF8.GetBytes(response));
                        response = string.Empty;
                    }

                    if (shouldUnblock)
                    {
                        UnblockWaitingClients(key);
                        NotifyKeyModified(key, client);
                    }
                }
                else if (command == "LRANGE" && parts.Length >= 4)
                {
                    response = LRange(parts[1], parts[2], parts[3]);
                }
                else if (command == "LLEN" && parts.Length >= 2)
                {
                    response = LLen(parts[1]);
                }
                else if (command == "LPOP" && parts.Length >= 2)
                {
                    response = LPop(parts);
                    if (response == null!) goto SkipSend;
                }
                else if (command == "BLPOP" && parts.Length >= 3)
                {
                    response = await BLPop(parts[1], parts[2]);
                }
                else if (command == "TYPE" && parts.Length >= 2)
                {
                    response = TypeOf(parts[1]);
                }
                else if (command == "XADD" && parts.Length >= 4)
                {
                    response = XAdd(parts);
                    NotifyKeyModified(parts[1], client);
                }
                else if (command == "XREAD" && parts.Length >= 4)
                {
                    response = await XRead(parts);
                }
                else if (command == "XRANGE" && parts.Length >= 4)
                {
                    response = XRange(parts[1], parts[2], parts[3]);
                }
                else if (command == "PUBLISH" && parts.Length >= 3)
                {
                    response = Publish(parts[1], parts[2]);
                }
                else if (command == "SUBSCRIBE" && parts.Length >= 2)
                {
                    Subscribe(client, parts.Skip(1).ToArray(), ref isSubscribedMode);
                    response = string.Empty;
                }
                else if (command == "UNSUBSCRIBE" && parts.Length >= 2)
                {
                    Unsubscribe(client, parts.Skip(1).ToArray(), ref isSubscribedMode);
                    response = string.Empty;
                }
                else
                {
                    response = "-ERR unknown command\r\n";
                }

                SkipSend:
                if (!string.IsNullOrEmpty(response) && !isReplicationConnection)
                    client.Send(Encoding.UTF8.GetBytes(response));
            }
            catch
            {
                break;
            }
        }

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

    // -------------------------------------------------------------------------
    // Transaction command execution
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // Individual command implementations
    // -------------------------------------------------------------------------

    private string ExecSet(string[] parts, Socket client)
    {
        _dataStore[parts[1]] = new StoredValue(parts[2], ParseSetExpiry(parts));
        NotifyKeyModified(parts[1], client);
        return "+OK\r\n";
    }

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
            : "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    }

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
                return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

            if (!int.TryParse(sv.Value, out int current))
                return "-ERR value is not an integer or out of range\r\n";

            int next = current + 1;
            _dataStore[key] = new StoredValue(next.ToString(), sv.ExpiryMs);
            return $":{next}\r\n";
        }

        _dataStore[key] = new StoredValue("1");
        return ":1\r\n";
    }

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

    private string GeoAdd(string[] parts)
    {
        if (!double.TryParse(parts[2], System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out double lon) ||
            !double.TryParse(parts[3], System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out double lat))
        {
            return "-ERR invalid longitude,latitude pair\r\n";
        }

        if (lon < -180.0 || lon > 180.0)
            return $"-ERR invalid longitude value {lon:F6}\r\n";

        if (lat < -85.05112878 || lat > 85.05112878)
            return $"-ERR invalid latitude value {lat:F6}\r\n";

        string key = parts[1];
        string member = parts[4];
        double score = GeoUtils.EncodeGeoHash(lon, lat);

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

    private string GeoPos(string[] parts)
    {
        string key = parts[1];
        int memberCount = parts.Length - 2;
        var sb = new StringBuilder();
        sb.Append($"*{memberCount}\r\n");

        for (int i = 2; i < parts.Length; i++)
        {
            string member = parts[i];
            if (_dataStore.TryGetValue(key, out StoredValue? sv) && sv.SortedSet != null)
            {
                var entry = sv.SortedSet.FirstOrDefault(e => e.Member == member);
                if (entry != null)
                {
                    var (decLon, decLat) = GeoUtils.DecodeGeoHash((long)entry.Score);
                    string lonStr = decLon.ToString("R", System.Globalization.CultureInfo.InvariantCulture);
                    string latStr = decLat.ToString("R", System.Globalization.CultureInfo.InvariantCulture);
                    sb.Append($"*2\r\n${lonStr.Length}\r\n{lonStr}\r\n${latStr.Length}\r\n{latStr}\r\n");
                    continue;
                }
            }
            sb.Append("*-1\r\n");
        }

        return sb.ToString();
    }

    private string GeoDist(string key, string member1, string member2)
    {
        if (!_dataStore.TryGetValue(key, out StoredValue? sv) || sv.SortedSet == null)
            return "$-1\r\n";

        var e1 = sv.SortedSet.FirstOrDefault(e => e.Member == member1);
        var e2 = sv.SortedSet.FirstOrDefault(e => e.Member == member2);
        if (e1 == null || e2 == null)
            return "$-1\r\n";

        var (lon1, lat1) = GeoUtils.DecodeGeoHash((long)e1.Score);
        var (lon2, lat2) = GeoUtils.DecodeGeoHash((long)e2.Score);
        double dist = GeoUtils.DistanceMeters(lat1, lon1, lat2, lon2);
        string distStr = dist.ToString("F4", System.Globalization.CultureInfo.InvariantCulture);
        return $"${distStr.Length}\r\n{distStr}\r\n";
    }

    private string GeoSearch(string[] parts)
    {
        string key = parts[1];

        if (parts[2].ToUpper() != "FROMLONLAT" || parts[5].ToUpper() != "BYRADIUS")
            return "-ERR unsupported GEOSEARCH options\r\n";

        if (!double.TryParse(parts[3], System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out double centerLon) ||
            !double.TryParse(parts[4], System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out double centerLat) ||
            !double.TryParse(parts[6], System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out double radius))
        {
            return "-ERR invalid arguments\r\n";
        }

        double unitMultiplier = parts[7].ToLower() switch
        {
            "km" => 1000.0,
            "mi" => 1609.344,
            "ft" => 0.3048,
            _ => 1.0
        };
        double radiusMeters = radius * unitMultiplier;

        var matches = new List<string>();
        if (_dataStore.TryGetValue(key, out StoredValue? sv) && sv.SortedSet != null)
        {
            foreach (var entry in sv.SortedSet)
            {
                var (mLon, mLat) = GeoUtils.DecodeGeoHash((long)entry.Score);
                if (GeoUtils.DistanceMeters(centerLat, centerLon, mLat, mLon) <= radiusMeters)
                    matches.Add(entry.Member);
            }
        }

        var sb = new StringBuilder();
        sb.Append($"*{matches.Count}\r\n");
        foreach (var m in matches)
            sb.Append($"${m.Length}\r\n{m}\r\n");
        return sb.ToString();
    }

    /// <summary>
    /// Validates credentials against the default user's ACL rules.
    /// Returns the updated authentication flag and the RESP response.
    /// </summary>
    private (bool authenticated, string response) Authenticate(string username, string password)
    {
        if (username != "default")
            return (false, "-WRONGPASS invalid username-password pair or user is disabled.\r\n");

        if (_defaultUserFlags.Contains("nopass"))
            return (true, "+OK\r\n");

        byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes(password));
        string hexHash = Convert.ToHexString(hash).ToLower();

        return _defaultUserPasswords.Contains(hexHash)
            ? (true, "+OK\r\n")
            : (false, "-WRONGPASS invalid username-password pair or user is disabled.\r\n");
    }

    private string AclSetUser(string[] parts)
    {
        string username = parts[2];
        if (username != "default")
            return "-ERR unknown user\r\n";

        for (int i = 3; i < parts.Length; i++)
        {
            string rule = parts[i];
            if (!rule.StartsWith(">")) continue;

            string password = rule.Substring(1);
            byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes(password));
            string hexHash = Convert.ToHexString(hash).ToLower();

            if (!_defaultUserPasswords.Contains(hexHash))
                _defaultUserPasswords.Add(hexHash);

            _defaultUserFlags.Remove("nopass");
        }

        return "+OK\r\n";
    }

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

    private string ZCard(string key)
    {
        if (!_dataStore.TryGetValue(key, out StoredValue? sv))
            return ":0\r\n";

        return sv.SortedSet == null
            ? "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
            : $":{sv.SortedSet.Count}\r\n";
    }

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

    private async Task<string> WaitForReplicas(string numReplicasStr, string timeoutStr)
    {
        if (!int.TryParse(numReplicasStr, out int numReplicas))
            return "-ERR value is not an integer or out of range\r\n";

        if (!int.TryParse(timeoutStr, out int timeout))
            return "-ERR timeout is not an integer or out of range\r\n";

        List<Socket> replicas;
        long currentOffset;
        lock (_replicaConnectionsLock)
        {
            replicas = new List<Socket>(_replicaConnections);
            currentOffset = _masterOffset;
        }

        if (replicas.Count == 0)
            return ":0\r\n";

        if (currentOffset == 0)
            return $":{replicas.Count}\r\n";

        RequestReplicaAcks(replicas);
        long deadline = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + timeout;
        int acked = 0;

        while (true)
        {
            lock (_replicaConnectionsLock)
            {
                acked = replicas.Count(r =>
                    _replicaAckOffsets.TryGetValue(r, out long off) && off >= currentOffset);
            }

            if (acked >= numReplicas || DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() >= deadline)
                break;

            await Task.Delay(10);
        }

        return $":{acked}\r\n";
    }

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

    private string LLen(string key)
    {
        if (!_dataStore.TryGetValue(key, out StoredValue? sv))
            return ":0\r\n";

        return sv.List == null
            ? "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
            : $":{sv.List.Count}\r\n";
    }

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

    private string Publish(string channel, string message)
    {
        int count = 0;
        lock (_subscriptionsLock)
        {
            if (_channelSubscribers.TryGetValue(channel, out HashSet<Socket>? subs))
            {
                count = subs.Count;
                string msg = $"*3\r\n$7\r\nmessage\r\n${channel.Length}\r\n{channel}\r\n${message.Length}\r\n{message}\r\n";
                byte[] msgBytes = Encoding.UTF8.GetBytes(msg);

                foreach (Socket sub in subs.ToList())
                {
                    try { sub.Send(msgBytes); }
                    catch { /* subscriber disconnected */ }
                }
            }
        }
        return $":{count}\r\n";
    }

    private void Subscribe(Socket client, string[] channels, ref bool isSubscribedMode)
    {
        lock (_subscriptionsLock)
        {
            if (!_clientSubscriptions.ContainsKey(client))
                _clientSubscriptions[client] = new HashSet<string>();

            foreach (string channel in channels)
            {
                if (!_channelSubscribers.ContainsKey(channel))
                    _channelSubscribers[channel] = new HashSet<Socket>();

                _channelSubscribers[channel].Add(client);
                _clientSubscriptions[client].Add(channel);

                int subCount = _clientSubscriptions[client].Count;
                string resp = $"*3\r\n$9\r\nsubscribe\r\n${channel.Length}\r\n{channel}\r\n:{subCount}\r\n";
                client.Send(Encoding.UTF8.GetBytes(resp));
            }

            isSubscribedMode = true;
        }
    }

    private void Unsubscribe(Socket client, string[] channels, ref bool isSubscribedMode)
    {
        lock (_subscriptionsLock)
        {
            foreach (string channel in channels)
            {
                if (_channelSubscribers.TryGetValue(channel, out HashSet<Socket>? subs))
                {
                    subs.Remove(client);
                    if (subs.Count == 0) _channelSubscribers.TryRemove(channel, out _);
                }

                _clientSubscriptions.TryGetValue(client, out HashSet<string>? clientChans);
                clientChans?.Remove(channel);

                int remaining = _clientSubscriptions.TryGetValue(client, out HashSet<string>? c) ? c.Count : 0;
                string resp = $"*3\r\n$11\r\nunsubscribe\r\n${channel.Length}\r\n{channel}\r\n:{remaining}\r\n";
                client.Send(Encoding.UTF8.GetBytes(resp));
            }

            if (!_clientSubscriptions.TryGetValue(client, out var remaining2) || remaining2.Count == 0)
            {
                isSubscribedMode = false;
                _clientSubscriptions.TryRemove(client, out _);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Replication
    // -------------------------------------------------------------------------

    /// <summary>
    /// Connects to the master server, performs the replication handshake (PING, REPLCONF, PSYNC),
    /// receives the initial RDB snapshot, and then continuously processes propagated commands.
    /// </summary>
    private async Task ConnectToMasterAsync(string host, int masterPort, int replicaPort)
    {
        try
        {
            var masterClient = new TcpClient();
            await masterClient.ConnectAsync(host, masterPort);
            NetworkStream stream = masterClient.GetStream();
            byte[] buffer = new byte[4096];

            await SendAndReceiveAsync(stream, buffer, "*1\r\n$4\r\nPING\r\n");

            string portStr = replicaPort.ToString();
            await SendAndReceiveAsync(stream, buffer,
                $"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${portStr.Length}\r\n{portStr}\r\n");

            await SendAndReceiveAsync(stream, buffer,
                "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");

            await stream.WriteAsync(Encoding.UTF8.GetBytes("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"));

            int bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
            string fullResponse = Encoding.UTF8.GetString(buffer, 0, bytesRead);

            int fullresyncEnd = fullResponse.IndexOf("\r\n");
            if (fullresyncEnd == -1) return;

            int rdbStart = fullresyncEnd + 2;

            while (rdbStart >= fullResponse.Length || fullResponse[rdbStart] != '$')
            {
                int n = await stream.ReadAsync(buffer, bytesRead, buffer.Length - bytesRead);
                if (n == 0) return;
                bytesRead += n;
                fullResponse = Encoding.UTF8.GetString(buffer, 0, bytesRead);
            }

            int rdbLenEnd = fullResponse.IndexOf("\r\n", rdbStart);
            while (rdbLenEnd == -1)
            {
                int n = await stream.ReadAsync(buffer, bytesRead, buffer.Length - bytesRead);
                if (n == 0) return;
                bytesRead += n;
                fullResponse = Encoding.UTF8.GetString(buffer, 0, bytesRead);
                rdbLenEnd = fullResponse.IndexOf("\r\n", rdbStart);
            }

            string rdbLenStr = fullResponse.Substring(rdbStart + 1, rdbLenEnd - rdbStart - 1);
            if (!int.TryParse(rdbLenStr, out int rdbLength)) return;

            int rdbDataStart = Encoding.UTF8.GetByteCount(fullResponse.Substring(0, rdbLenEnd)) + 2;
            int rdbDataEnd = rdbDataStart + rdbLength;

            while (bytesRead < rdbDataEnd)
            {
                int n = await stream.ReadAsync(buffer, bytesRead, buffer.Length - bytesRead);
                if (n == 0) return;
                bytesRead += n;
            }

            var commandBuffer = new StringBuilder();
            if (bytesRead > rdbDataEnd)
                commandBuffer.Append(Encoding.UTF8.GetString(buffer, rdbDataEnd, bytesRead - rdbDataEnd));

            if (commandBuffer.Length > 0)
                await ProcessBufferedCommandsAsync(commandBuffer, stream);

            while (true)
            {
                bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
                if (bytesRead == 0) break;

                commandBuffer.Append(Encoding.UTF8.GetString(buffer, 0, bytesRead));
                await ProcessBufferedCommandsAsync(commandBuffer, stream);
            }
        }
        catch { }
    }

    /// <summary>
    /// Writes a RESP command to the stream and reads (and discards) a single response frame.
    /// Used during the replication handshake sequence.
    /// </summary>
    private static async Task SendAndReceiveAsync(NetworkStream stream, byte[] buffer, string command)
    {
        await stream.WriteAsync(Encoding.UTF8.GetBytes(command));
        int bytesRead = await stream.ReadAsync(buffer.AsMemory(0, buffer.Length));
        if (bytesRead == 0)
            throw new IOException("Connection closed by remote host");
    }

    /// <summary>
    /// Drains all complete RESP commands from <paramref name="buffer"/> and processes each one.
    /// Partial data at the end of the buffer is left for the next read.
    /// </summary>
    private async Task ProcessBufferedCommandsAsync(StringBuilder buffer, NetworkStream stream)
    {
        string data = buffer.ToString();
        int processed = 0;

        while (true)
        {
            string remaining = data.Substring(processed);
            if (remaining.Length == 0) break;

            var (cmd, length) = RespParser.TryParseCommand(remaining);
            if (cmd == null || length == 0) break;

            await ProcessReplicatedCommandAsync(cmd, stream, length);
            processed += length;
        }

        if (processed > 0)
            buffer.Remove(0, processed);
    }

    /// <summary>
    /// Applies a single command propagated from the master and advances the replica offset.
    /// Responds to REPLCONF GETACK with the offset captured <em>before</em> processing the command,
    /// matching Redis's protocol where GETACK itself is counted only after the reply is sent.
    /// </summary>
    private async Task ProcessReplicatedCommandAsync(string[] parts, NetworkStream stream, int commandLength)
    {
        if (parts.Length == 0) return;

        string command = parts[0].ToUpper();
        long offsetBefore = _replicaOffset;

        if (command == "REPLCONF" && parts.Length >= 3 && parts[1].ToUpper() == "GETACK")
        {
            string ack = $"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${offsetBefore.ToString().Length}\r\n{offsetBefore}\r\n";
            await stream.WriteAsync(Encoding.UTF8.GetBytes(ack));
            await stream.FlushAsync();
        }

        if (command == "SET" && parts.Length >= 3)
        {
            long? expiry = ParseSetExpiry(parts);
            _dataStore[parts[1]] = new StoredValue(parts[2], expiry);
        }

        _replicaOffset += commandLength;
    }

    /// <summary>
    /// Sends the current raw command bytes to all connected replica sockets,
    /// removes disconnected replicas, and advances the master replication offset.
    /// </summary>
    private void PropagateToReplicas(string command)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(command);
        lock (_replicaConnectionsLock)
        {
            var disconnected = new List<Socket>();
            foreach (var replica in _replicaConnections)
            {
                try { replica.Send(bytes); }
                catch { disconnected.Add(replica); }
            }

            foreach (var r in disconnected)
            {
                _replicaConnections.Remove(r);
                _replicaAckOffsets.Remove(r);
            }

            if (_replicaConnections.Count > 0)
                _masterOffset += bytes.Length;
        }
    }

    /// <summary>
    /// Sends a REPLCONF GETACK * command to all specified replicas,
    /// removing any that have disconnected.
    /// </summary>
    private void RequestReplicaAcks(IEnumerable<Socket> replicas)
    {
        byte[] getack = Encoding.UTF8.GetBytes("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n");
        lock (_replicaConnectionsLock)
        {
            var disconnected = new List<Socket>();
            foreach (var r in replicas)
            {
                try { r.Send(getack); }
                catch { disconnected.Add(r); }
            }

            foreach (var r in disconnected)
            {
                _replicaConnections.Remove(r);
                _replicaAckOffsets.Remove(r);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Watch helpers
    // -------------------------------------------------------------------------

    private void NotifyKeyModified(string key, Socket modifier)
    {
        lock (_watchLock)
        {
            if (_keyWatchers.TryGetValue(key, out var watchers))
            {
                foreach (var watcher in watchers)
                {
                    if (watcher != modifier)
                        _watchDirty[watcher] = true;
                }
            }
        }
    }

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

    // -------------------------------------------------------------------------
    // Blocking command helpers
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // Stream helpers
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // Utility helpers
    // -------------------------------------------------------------------------

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
}
