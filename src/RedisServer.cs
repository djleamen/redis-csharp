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
            string manifestFile = Path.Combine(aofDir, $"{_appendfilename}.manifest");
            string aofFileName;
            if (File.Exists(manifestFile))
            {
                // Read manifest to find the active incremental AOF file (type i)
                string[] lines = File.ReadAllLines(manifestFile);
                aofFileName = $"{_appendfilename}.1.incr.aof"; // default fallback
                foreach (string line in lines)
                {
                    if (line.Contains("type i"))
                    {
                        string[] tokens = line.Split(' ');
                        if (tokens.Length >= 2 && tokens[0] == "file")
                            aofFileName = tokens[1];
                        break;
                    }
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

                    if (_appendonly.Equals("yes", StringComparison.OrdinalIgnoreCase))
                        AppendToAof(parts);

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
                        response = WrongTypeError;
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

        if (_defaultUserFlags.Contains("nopass"))
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

    // -------------------------------------------------------------------------
    // Watch helpers
    // -------------------------------------------------------------------------

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
                foreach (var watcher in watchers)
                {
                    if (watcher != modifier)
                        _watchDirty[watcher] = true;
                }
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
