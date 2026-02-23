/**
 * Redis - A simple Redis server implementation in C#
 * From CodeCrafters.io build-your-own-redis (C#)
 */

using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;

int port = 6379; // Default port
string? masterHost = null;
int? masterPort = null;
string dir = "/tmp/redis-files"; // Default directory for RDB file
string dbfilename = "dump.rdb"; // Default RDB filename

for (int i = 0; i < args.Length; i++)
{
    if (args[i] == "--port" && i + 1 < args.Length)
    {
        if (int.TryParse(args[i + 1], out int parsedPort))
        {
            port = parsedPort;
        }
    }
    else if (args[i] == "--replicaof" && i + 1 < args.Length)
    {
        string[] parts = args[i + 1].Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 2 && int.TryParse(parts[1], out int parsedMasterPort))
        {
            masterHost = parts[0];
            masterPort = parsedMasterPort;
        }
    }
    else if (args[i] == "--dir" && i + 1 < args.Length)
    {
        dir = args[i + 1];
    }
    else if (args[i] == "--dbfilename" && i + 1 < args.Length)
    {
        dbfilename = args[i + 1];
    }
}

bool isReplica = masterHost != null && masterPort.HasValue;

const string replicationId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
const int replicationOffset = 0;

var dataStore = new ConcurrentDictionary<string, StoredValue>();

var blockedClients = new ConcurrentDictionary<string, Queue<BlockedClient>>();
var blockedClientsLock = new object();

var blockedStreamReaders = new ConcurrentDictionary<string, Queue<BlockedStreamReader>>();
var blockedStreamReadersLock = new object();

var channelSubscribers = new ConcurrentDictionary<string, HashSet<Socket>>();
var clientSubscriptions = new ConcurrentDictionary<Socket, HashSet<string>>();
var subscriptionsLock = new object();

var replicaConnections = new List<Socket>();
var replicaConnectionsLock = new object();
var replicaAckOffsets = new Dictionary<Socket, long>();

long replicaOffset = 0;
long masterOffset = 0;

// Default user ACL state
var defaultUserFlags = new HashSet<string> { "nopass" };
var defaultUserPasswords = new List<string>();

// Load RDB file if it exists
LoadRdbFile(Path.Combine(dir, dbfilename));

TcpListener server = new TcpListener(IPAddress.Any, port);
server.Start();

if (isReplica && masterHost != null && masterPort.HasValue)
{
    Task.Run(() => ConnectToMaster(masterHost, masterPort.Value, port));
}

while (true)
{
    Socket client = server.AcceptSocket();
    client.NoDelay = true;  // Disable Nagle's algorithm
    Task.Run(() => HandleClient(client));
}

/* Execute a single command and return the response */
async Task<string> ExecuteCommand(string[] parts, Socket client)
{
    if (parts.Length == 0)
        return "-ERR empty command\r\n";
    
    string command = parts[0].ToUpper();
    string response = string.Empty;
    
    // PING and ECHO
    if (command == "PING")
    {
        response = "+PONG\r\n";
    }
    else if (command == "ECHO" && parts.Length > 1)
    {
        string message = parts[1];
        response = $"${message.Length}\r\n{message}\r\n";
    }
    // SET and GET
    else if (command == "SET" && parts.Length >= 3)
    {
        string key = parts[1];
        string value = parts[2];
        long? expiryMs = null;
        
        for (int i = 3; i < parts.Length - 1; i++)
        {
            string option = parts[i].ToUpper();
            if (option == "PX")
            {
                if (long.TryParse(parts[i + 1], out long px))
                {
                    expiryMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + px;
                }
                break;
            }
            else if (option == "EX")
            {
                if (long.TryParse(parts[i + 1], out long ex))
                {
                    expiryMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + (ex * 1000);
                }
                break;
            }
        }
        
        dataStore[key] = new StoredValue(value, expiryMs);
        response = "+OK\r\n";
    }
    else if (command == "GET" && parts.Length > 1)
    {
        string key = parts[1];
        if (dataStore.TryGetValue(key, out StoredValue? storedValue))
        {
            if (storedValue.ExpiryMs.HasValue && 
                DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() > storedValue.ExpiryMs.Value)
            {
                dataStore.TryRemove(key, out _);
                response = "$-1\r\n";
            }
            else if (storedValue.Value != null)
            {
                response = $"${storedValue.Value.Length}\r\n{storedValue.Value}\r\n";
            }
            else
            {
                response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
            }
        }
        else
        {
            response = "$-1\r\n";
        }
    }
    // INCR - Increment the value of a key by 1
    else if (command == "INCR" && parts.Length >= 2)
    {
        string key = parts[1];
        
        if (dataStore.TryGetValue(key, out StoredValue? storedValue))
        {
            if (storedValue.ExpiryMs.HasValue && 
                DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() > storedValue.ExpiryMs.Value)
            {
                dataStore.TryRemove(key, out _);
                dataStore[key] = new StoredValue("1");
                response = ":1\r\n";
            }
            else if (storedValue.Value != null)
            {
                if (int.TryParse(storedValue.Value, out int currentValue))
                {
                    int newValue = currentValue + 1;
                    dataStore[key] = new StoredValue(newValue.ToString(), storedValue.ExpiryMs);
                    response = $":{newValue}\r\n";
                }
                else
                {
                    response = "-ERR value is not an integer or out of range\r\n";
                }
            }
            else
            {
                response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
            }
        }
        else
        {
            dataStore[key] = new StoredValue("1");
            response = ":1\r\n";
        }
    }
    // ZADD - Add member to a sorted set
    else if (command == "ZADD" && parts.Length >= 4)
    {
        string key = parts[1];
        
        if (!double.TryParse(parts[2], System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out double score))
        {
            response = "-ERR value is not a valid float\r\n";
        }
        else
        {
            string member = parts[3];
            
            if (!dataStore.ContainsKey(key))
            {
                var sortedSet = new List<SortedSetEntry> { new SortedSetEntry(member, score) };
                dataStore[key] = new StoredValue(sortedSet);
                response = ":1\r\n";
            }
            else
            {
                if (dataStore.TryGetValue(key, out StoredValue? storedValue) && storedValue.SortedSet != null)
                {
                    // Check if member already exists
                    var existingEntry = storedValue.SortedSet.FirstOrDefault(e => e.Member == member);
                    if (existingEntry != null)
                    {
                        // Member already exists, update score
                        storedValue.SortedSet.Remove(existingEntry);
                        storedValue.SortedSet.Add(new SortedSetEntry(member, score));
                        storedValue.SortedSet.Sort();
                        response = ":0\r\n";  // No new members added
                    }
                    else
                    {
                        // New member
                        storedValue.SortedSet.Add(new SortedSetEntry(member, score));
                        storedValue.SortedSet.Sort();
                        response = ":1\r\n";
                    }
                }
                else
                {
                    response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                }
            }
        }
    }
    // GEOADD - Add geospatial item
    else if (command == "GEOADD" && parts.Length >= 5)
    {
        if (!double.TryParse(parts[2], System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out double lon) ||
            !double.TryParse(parts[3], System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out double lat))
        {
            response = "-ERR invalid longitude,latitude pair\r\n";
        }
        else if (lon < -180.0 || lon > 180.0)
        {
            response = $"-ERR invalid longitude value {lon:F6}\r\n";
        }
        else if (lat < -85.05112878 || lat > 85.05112878)
        {
            response = $"-ERR invalid latitude value {lat:F6}\r\n";
        }
        else
        {
            string key = parts[1];
            string member = parts[4];
            double score = (double)EncodeGeoHash(lon, lat);
            
            if (!dataStore.ContainsKey(key))
            {
                var sortedSet = new List<SortedSetEntry> { new SortedSetEntry(member, score) };
                dataStore[key] = new StoredValue(sortedSet);
                response = ":1\r\n";
            }
            else if (dataStore.TryGetValue(key, out StoredValue? storedValue) && storedValue.SortedSet != null)
            {
                var existingEntry = storedValue.SortedSet.FirstOrDefault(e => e.Member == member);
                if (existingEntry != null)
                {
                    storedValue.SortedSet.Remove(existingEntry);
                    storedValue.SortedSet.Add(new SortedSetEntry(member, score));
                    storedValue.SortedSet.Sort();
                    response = ":0\r\n";
                }
                else
                {
                    storedValue.SortedSet.Add(new SortedSetEntry(member, score));
                    storedValue.SortedSet.Sort();
                    response = ":1\r\n";
                }
            }
            else
            {
                response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
            }
        }
    }
    // GEOPOS - Get longitude and latitude of locations
    else if (command == "GEOPOS" && parts.Length >= 3)
    {
        string key = parts[1];
        int memberCount = parts.Length - 2;
        var sb = new StringBuilder();
        sb.Append($"*{memberCount}\r\n");

        for (int i = 2; i < parts.Length; i++)
        {
            string member = parts[i];
            bool found = false;
            if (dataStore.TryGetValue(key, out StoredValue? geoVal) && geoVal.SortedSet != null)
            {
                var entry = geoVal.SortedSet.FirstOrDefault(e => e.Member == member);
                if (entry != null)
                {
                    var (decLon, decLat) = DecodeGeoHash((long)entry.Score);
                    string lonStr = decLon.ToString("R", System.Globalization.CultureInfo.InvariantCulture);
                    string latStr = decLat.ToString("R", System.Globalization.CultureInfo.InvariantCulture);
                    sb.Append($"*2\r\n${lonStr.Length}\r\n{lonStr}\r\n${latStr.Length}\r\n{latStr}\r\n");
                    found = true;
                }
            }
            if (!found)
                sb.Append("*-1\r\n");
        }
        response = sb.ToString();
    }
    // GEODIST - Get distance between two locations
    else if (command == "GEODIST" && parts.Length >= 4)
    {
        string key = parts[1];
        string member1 = parts[2];
        string member2 = parts[3];

        if (!dataStore.TryGetValue(key, out StoredValue? geoDistVal) || geoDistVal.SortedSet == null)
        {
            response = "$-1\r\n";
        }
        else
        {
            var e1 = geoDistVal.SortedSet.FirstOrDefault(e => e.Member == member1);
            var e2 = geoDistVal.SortedSet.FirstOrDefault(e => e.Member == member2);
            if (e1 == null || e2 == null)
            {
                response = "$-1\r\n";
            }
            else
            {
                var (lon1, lat1) = DecodeGeoHash((long)e1.Score);
                var (lon2, lat2) = DecodeGeoHash((long)e2.Score);
                double dist = GeoDistMeters(lat1, lon1, lat2, lon2);
                string distStr = dist.ToString("F4", System.Globalization.CultureInfo.InvariantCulture);
                response = $"${distStr.Length}\r\n{distStr}\r\n";
            }
        }
    }
    // GEOSEARCH - Search for locations within a radius
    else if (command == "GEOSEARCH" && parts.Length >= 8)
    {
        string key = parts[1];
        // Only support FROMLONLAT ... BYRADIUS ... form
        if (parts[2].ToUpper() != "FROMLONLAT" || parts[5].ToUpper() != "BYRADIUS")
        {
            response = "-ERR unsupported GEOSEARCH options\r\n";
        }
        else if (!double.TryParse(parts[3], System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out double centerLon) ||
                 !double.TryParse(parts[4], System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out double centerLat) ||
                 !double.TryParse(parts[6], System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out double radius))
        {
            response = "-ERR invalid arguments\r\n";
        }
        else
        {
            double unitMultiplier = parts[7].ToLower() switch
            {
                "km" => 1000.0,
                "mi" => 1609.344,
                "ft" => 0.3048,
                _    => 1.0   // m (default)
            };
            double radiusMeters = radius * unitMultiplier;

            var matches = new List<string>();
            if (dataStore.TryGetValue(key, out StoredValue? geoSearchVal) && geoSearchVal.SortedSet != null)
            {
                foreach (var entry in geoSearchVal.SortedSet)
                {
                    var (mLon, mLat) = DecodeGeoHash((long)entry.Score);
                    double dist = GeoDistMeters(centerLat, centerLon, mLat, mLon);
                    if (dist <= radiusMeters)
                        matches.Add(entry.Member);
                }
            }

            var sbGs = new StringBuilder();
            sbGs.Append($"*{matches.Count}\r\n");
            foreach (var m in matches)
                sbGs.Append($"${m.Length}\r\n{m}\r\n");
            response = sbGs.ToString();
        }
    }
    // ACL WHOAMI - Return the username of the current connection
    else if (command == "ACL" && parts.Length >= 2 && parts[1].ToUpper() == "WHOAMI")
    {
        response = "$7\r\ndefault\r\n";
    }
    // ACL SETUSER - Set properties of a user
    else if (command == "ACL" && parts.Length >= 3 && parts[1].ToUpper() == "SETUSER")
    {
        string username = parts[2];
        if (username == "default")
        {
            for (int i = 3; i < parts.Length; i++)
            {
                string rule = parts[i];
                if (rule.StartsWith(">"))
                {
                    string password = rule.Substring(1);
                    byte[] hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(password));
                    string hash = Convert.ToHexString(hashBytes).ToLower();
                    if (!defaultUserPasswords.Contains(hash))
                        defaultUserPasswords.Add(hash);
                    defaultUserFlags.Remove("nopass");
                }
            }
            response = "+OK\r\n";
        }
        else
        {
            response = "-ERR unknown user\r\n";
        }
    }
    // ACL GETUSER - Get properties of a user
    else if (command == "ACL" && parts.Length >= 3 && parts[1].ToUpper() == "GETUSER")
    {
        string username = parts[2];
        if (username == "default")
        {
            var flagsList = defaultUserFlags.ToList();
            var sbAcl = new StringBuilder();
            sbAcl.Append("*4\r\n");
            sbAcl.Append("$5\r\nflags\r\n");
            sbAcl.Append($"*{flagsList.Count}\r\n");
            foreach (var f in flagsList) sbAcl.Append($"${f.Length}\r\n{f}\r\n");
            sbAcl.Append("$9\r\npasswords\r\n");
            sbAcl.Append($"*{defaultUserPasswords.Count}\r\n");
            foreach (var p in defaultUserPasswords) sbAcl.Append($"${p.Length}\r\n{p}\r\n");
            response = sbAcl.ToString();
        }
        else
        {
            response = "$-1\r\n";
        }
    }
    // ZRANK - Get the rank of a member in a sorted set
    else if (command == "ZRANK" && parts.Length >= 3)
    {
        string key = parts[1];
        string member = parts[2];
        
        if (!dataStore.TryGetValue(key, out StoredValue? storedValue))
        {
            response = "$-1\r\n";  // Key doesn't exist
        }
        else if (storedValue.SortedSet == null)
        {
            response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }
        else
        {
            // Find the index of the member in the sorted set
            int rank = -1;
            for (int i = 0; i < storedValue.SortedSet.Count; i++)
            {
                if (storedValue.SortedSet[i].Member == member)
                {
                    rank = i;
                    break;
                }
            }
            
            if (rank >= 0)
            {
                response = $":{rank}\r\n";
            }
            else
            {
                response = "$-1\r\n";  // Member doesn't exist
            }
        }
    }
    // ZRANGE - List members from a sorted set by index range
    else if (command == "ZRANGE" && parts.Length >= 4)
    {
        string key = parts[1];
        
        if (!int.TryParse(parts[2], out int start) || !int.TryParse(parts[3], out int stop))
        {
            response = "-ERR value is not an integer or out of range\r\n";
        }
        else if (!dataStore.TryGetValue(key, out StoredValue? storedValue))
        {
            response = "*0\r\n";  // Key doesn't exist, return empty array
        }
        else if (storedValue.SortedSet == null)
        {
            response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }
        else
        {
            var sortedSet = storedValue.SortedSet;
            int count = sortedSet.Count;
            
            // Convert negative indexes to positive
            if (start < 0)
            {
                start = Math.Max(0, count + start);
            }
            if (stop < 0)
            {
                stop = Math.Max(0, count + stop);
            }
            
            // Handle edge cases
            if (start >= count || start > stop)
            {
                response = "*0\r\n";  // Empty array
            }
            else
            {
                // Adjust stop index if it exceeds the cardinality
                if (stop >= count)
                {
                    stop = count - 1;
                }
                
                // Build the RESP array response
                var sb = new StringBuilder();
                int resultCount = stop - start + 1;
                sb.Append($"*{resultCount}\r\n");
                
                for (int i = start; i <= stop; i++)
                {
                    string member = sortedSet[i].Member;
                    sb.Append($"${member.Length}\r\n{member}\r\n");
                }
                
                response = sb.ToString();
            }
        }
    }
    // ZCARD - Get the cardinality of a sorted set
    else if (command == "ZCARD" && parts.Length >= 2)
    {
        string key = parts[1];
        
        if (!dataStore.TryGetValue(key, out StoredValue? storedValue))
        {
            response = ":0\r\n";  // Key doesn't exist
        }
        else if (storedValue.SortedSet == null)
        {
            response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }
        else
        {
            response = $":{storedValue.SortedSet.Count}\r\n";
        }
    }
    // ZSCORE - Get the score of a member in a sorted set
    else if (command == "ZSCORE" && parts.Length >= 3)
    {
        string key = parts[1];
        string member = parts[2];
        
        if (!dataStore.TryGetValue(key, out StoredValue? storedValue))
        {
            response = "$-1\r\n";  // Key doesn't exist
        }
        else if (storedValue.SortedSet == null)
        {
            response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }
        else
        {
            // Find the member in the sorted set
            var entry = storedValue.SortedSet.FirstOrDefault(e => e.Member == member);
            if (entry != null)
            {
                string scoreStr = entry.Score.ToString(System.Globalization.CultureInfo.InvariantCulture);
                response = $"${scoreStr.Length}\r\n{scoreStr}\r\n";
            }
            else
            {
                response = "$-1\r\n";  // Member doesn't exist
            }
        }
    }
    // ZREM - Remove a member from a sorted set
    else if (command == "ZREM" && parts.Length >= 3)
    {
        string key = parts[1];
        string member = parts[2];
        
        if (!dataStore.TryGetValue(key, out StoredValue? storedValue))
        {
            response = ":0\r\n";  // Key doesn't exist
        }
        else if (storedValue.SortedSet == null)
        {
            response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }
        else
        {
            // Find and remove the member from the sorted set
            var entry = storedValue.SortedSet.FirstOrDefault(e => e.Member == member);
            if (entry != null)
            {
                storedValue.SortedSet.Remove(entry);
                response = ":1\r\n";  // Member removed
            }
            else
            {
                response = ":0\r\n";  // Member doesn't exist
            }
        }
    }
    else
    {
        response = "-ERR unknown command\r\n";
    }
    
    return response;
}

/* Connect to master server and perform replication handshake */
async Task ConnectToMaster(string host, int masterPort, int replicaPort)
{
    try
    {
        var masterClient = new TcpClient();
        await masterClient.ConnectAsync(host, masterPort);
        
        NetworkStream stream = masterClient.GetStream();
        byte[] buffer = new byte[4096];
        
        // Step 1: Send PING
        string pingCommand = "*1\r\n$4\r\nPING\r\n";
        byte[] pingBytes = Encoding.UTF8.GetBytes(pingCommand);
        await stream.WriteAsync(pingBytes, 0, pingBytes.Length);

        int bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
        string response = Encoding.UTF8.GetString(buffer, 0, bytesRead);
        
        // Step 2: Send REPLCONF listening-port
        string portStr = replicaPort.ToString();
        string replconfPort = $"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${portStr.Length}\r\n{portStr}\r\n";
        byte[] replconfPortBytes = Encoding.UTF8.GetBytes(replconfPort);
        await stream.WriteAsync(replconfPortBytes, 0, replconfPortBytes.Length);
        
        bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
        response = Encoding.UTF8.GetString(buffer, 0, bytesRead);
        
        // Step 3: Send REPLCONF capa
        string replconfCapa = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
        byte[] replconfCapaBytes = Encoding.UTF8.GetBytes(replconfCapa);
        await stream.WriteAsync(replconfCapaBytes, 0, replconfCapaBytes.Length);
        
        bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
        response = Encoding.UTF8.GetString(buffer, 0, bytesRead);
        
        // Step 4: Send PSYNC
        string psyncCommand = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        byte[] psyncBytes = Encoding.UTF8.GetBytes(psyncCommand);
        await stream.WriteAsync(psyncBytes, 0, psyncBytes.Length);
        
        // Step 5: Receive FULLRESYNC and RDB file
        bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
        
        string fullResponse = Encoding.UTF8.GetString(buffer, 0, bytesRead);
        
        int fullresyncEnd = fullResponse.IndexOf("\r\n");
        if (fullresyncEnd == -1)
        {
            Console.WriteLine("[Replica] Error: Invalid PSYNC response");
            return;
        }
        
        int rdbStart = fullresyncEnd + 2;
        
        // Read more data if we don't have the RDB bulk string header yet
        while (rdbStart >= fullResponse.Length || fullResponse[rdbStart] != '$')
        {
            int additionalBytesRead = await stream.ReadAsync(buffer, bytesRead, buffer.Length - bytesRead);
            if (additionalBytesRead == 0)
            {
                Console.WriteLine("[Replica] Error: Connection closed while waiting for RDB header");
                return;
            }
            bytesRead += additionalBytesRead;
            fullResponse = Encoding.UTF8.GetString(buffer, 0, bytesRead);
        }
        
        int rdbLenEnd = fullResponse.IndexOf("\r\n", rdbStart);
        
        // Read more data if we don't have the complete RDB length line yet
        while (rdbLenEnd == -1)
        {
            int additionalBytesRead = await stream.ReadAsync(buffer, bytesRead, buffer.Length - bytesRead);
            if (additionalBytesRead == 0)
            {
                Console.WriteLine("[Replica] Error: Connection closed while reading RDB length");
                return;
            }
            bytesRead += additionalBytesRead;
            fullResponse = Encoding.UTF8.GetString(buffer, 0, bytesRead);
            rdbLenEnd = fullResponse.IndexOf("\r\n", rdbStart);
        }
        
        string rdbLenStr = fullResponse.Substring(rdbStart + 1, rdbLenEnd - rdbStart - 1);
        if (!int.TryParse(rdbLenStr, out int rdbLength))
        {
            Console.WriteLine("[Replica] Error: Cannot parse RDB length");
            return;
        }
        
        int rdbDataStartInBytes = Encoding.UTF8.GetByteCount(fullResponse.Substring(0, rdbLenEnd)) + 2;
        int rdbDataEndInBytes = rdbDataStartInBytes + rdbLength;
        
        while (bytesRead < rdbDataEndInBytes)
        {
            int additionalBytesRead = await stream.ReadAsync(buffer, bytesRead, buffer.Length - bytesRead);
            if (additionalBytesRead == 0)
            {
                Console.WriteLine("[Replica] Error: Connection closed while reading RDB");
                return;
            }
            bytesRead += additionalBytesRead;
        }
        
        Console.WriteLine($"[Replica] Handshake complete. RDB file received ({rdbLength} bytes). Now processing commands from master...");
        
        // Step 6: Continue processing commands propagated from master
        var commandBuffer = new StringBuilder();
        if (bytesRead > rdbDataEndInBytes)
        {
            string leftoverData = Encoding.UTF8.GetString(buffer, rdbDataEndInBytes, bytesRead - rdbDataEndInBytes);
            commandBuffer.Append(leftoverData);
            Console.WriteLine($"[Replica] Found {bytesRead - rdbDataEndInBytes} bytes of command data after RDB");
            Console.WriteLine($"[Replica] Leftover data (first 100 chars): {leftoverData.Substring(0, Math.Min(100, leftoverData.Length))}");
        }
        else
        {
            Console.WriteLine($"[Replica] No leftover data after RDB (bytesRead={bytesRead}, rdbDataEndInBytes={rdbDataEndInBytes})");
        }
        
        if (commandBuffer.Length > 0)
        {
            Console.WriteLine($"[Replica] Processing initial buffer with {commandBuffer.Length} characters");
            await ProcessBufferedCommands(commandBuffer, stream);
        }
        
        while (true)
        {
            bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
            if (bytesRead == 0)
            {
                Console.WriteLine("[Replica] Master connection closed.");
                break;
            }
            
            string data = Encoding.UTF8.GetString(buffer, 0, bytesRead);
            commandBuffer.Append(data);
            Console.WriteLine($"[Replica] Received {bytesRead} bytes");
            
            await ProcessBufferedCommands(commandBuffer, stream);
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[Replica] Error in master connection: {ex.Message}");
        Console.WriteLine($"[Replica] Stack trace: {ex.StackTrace}");
    }
}

/* Process all complete commands from the buffer */
async Task ProcessBufferedCommands(StringBuilder commandBuffer, NetworkStream stream)
{
    string bufferedData = commandBuffer.ToString();
    int processedLength = 0;
    
    Console.WriteLine($"[Replica] ProcessBufferedCommands: buffer length = {bufferedData.Length}");
    if (bufferedData.Length > 0)
    {
        Console.WriteLine($"[Replica] Buffer content (first 200 chars): {bufferedData.Substring(0, Math.Min(200, bufferedData.Length)).Replace("\r", "\\r").Replace("\n", "\\n")}");
    }
    
    while (true)
    {
        string remainingData = bufferedData.Substring(processedLength);
        
        if (remainingData.Length == 0)
        {
            break;
        }
        
        // Try to parse a complete RESP array command
        var (command, commandLength) = TryParseRespCommand(remainingData);
        
        if (command == null || commandLength == 0)
        {
            // No complete command available, wait for more data
            Console.WriteLine($"[Replica] No complete command found, remaining {remainingData.Length} chars in buffer");
            break;
        }
        
        // Process the command (may send response for REPLCONF GETACK)
        await ProcessReplicatedCommand(command, stream, commandLength);
        
        processedLength += commandLength;
    }
    
    // Remove processed data from buffer
    if (processedLength > 0)
    {
        Console.WriteLine($"[Replica] Removing {processedLength} characters from buffer");
        commandBuffer.Remove(0, processedLength);
    }
}

/* Try to parse a complete RESP command from buffered data */
(string[]?, int) TryParseRespCommand(string data)
{
    if (string.IsNullOrEmpty(data) || !data.StartsWith('*'))
        return (null, 0);
    
    var lines = data.Split(new[] { "\r\n" }, StringSplitOptions.None);
    
    if (lines.Length < 2)
        return (null, 0);
    
    // Parse array length
    if (!int.TryParse(lines[0].Substring(1), out int arrayLength))
        return (null, 0);
    
    var parts = new List<string>();
    int lineIndex = 1;
    int bytesConsumed = lines[0].Length + 2; // +2 for \r\n
    
    for (int i = 0; i < arrayLength; i++)
    {
        // Check if we have enough lines for bulk string header and value
        if (lineIndex >= lines.Length)
            return (null, 0);
        
        // Parse bulk string length
        string lengthLine = lines[lineIndex];
        if (!lengthLine.StartsWith('$') || !int.TryParse(lengthLine.Substring(1), out int bulkLength))
            return (null, 0);
        
        bytesConsumed += lengthLine.Length + 2; // +2 for \r\n
        lineIndex++;
        
        // Check if we have the bulk string value
        if (lineIndex >= lines.Length)
            return (null, 0);
        
        string value = lines[lineIndex];
        
        // Verify the value length matches (important for incomplete data)
        if (value.Length != bulkLength)
        {
            // Check if this is the last line and might be incomplete
            if (lineIndex == lines.Length - 1 || (lineIndex == lines.Length - 2 && lines[lineIndex + 1] == ""))
            {
                return (null, 0); // Incomplete data
            }
        }
        
        parts.Add(value);
        bytesConsumed += value.Length + 2; // +2 for \r\n
        lineIndex++;
    }
    
    Console.WriteLine($"[Replica] Parsed command: [{string.Join(", ", parts)}]");
    return (parts.ToArray(), bytesConsumed);
}

/* Process a command replicated from master */
async Task ProcessReplicatedCommand(string[] parts, NetworkStream stream, int commandLength)
{
    if (parts.Length == 0)
        return;
    
    string command = parts[0].ToUpper();
    long offsetBeforeCommand = replicaOffset;
    
    Console.WriteLine($"[Replica] Processing replicated command: {command}");
    
    // Handle REPLCONF GETACK - respond with the offset before processing this GETACK.
    // Redis includes this GETACK in the stream offset only after replying.
    if (command == "REPLCONF" && parts.Length >= 3)
    {
        string subCommand = parts[1].ToUpper();
        if (subCommand == "GETACK")
        {
            string offsetStr = offsetBeforeCommand.ToString();
            string ackResponse = $"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${offsetStr.Length}\r\n{offsetStr}\r\n";
            byte[] ackBytes = Encoding.UTF8.GetBytes(ackResponse);
            await stream.WriteAsync(ackBytes, 0, ackBytes.Length);
            await stream.FlushAsync();
            Console.WriteLine($"[Replica] Sent ACK response with offset {offsetBeforeCommand}");
        }
    }
    
    // Process write commands that modify state
    if (command == "SET" && parts.Length >= 3)
    {
        string key = parts[1];
        string value = parts[2];
        long? expiryMs = null;
        
        for (int i = 3; i < parts.Length - 1; i++)
        {
            string option = parts[i].ToUpper();
            if (option == "PX")
            {
                if (long.TryParse(parts[i + 1], out long px))
                {
                    expiryMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + px;
                }
                break;
            }
            else if (option == "EX")
            {
                if (long.TryParse(parts[i + 1], out long ex))
                {
                    expiryMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + (ex * 1000);
                }
                break;
            }
        }
        
        dataStore[key] = new StoredValue(value, expiryMs);
        Console.WriteLine($"[Replica] SET {key} = {value}");
    }
    // Add other write commands as needed (INCR, RPUSH, LPUSH, XADD, etc.)
    
    // Every command from the master replication stream advances the replica offset,
    // including REPLCONF GETACK.
    replicaOffset += commandLength;
    Console.WriteLine($"[Replica] Updated offset from {offsetBeforeCommand} to {replicaOffset} after {command} ({commandLength} bytes)");
}

/* Handle client connection */
async Task HandleClient(Socket client)
{
    bool inTransaction = false;
    var transactionQueue = new List<string[]>();
    bool isReplicationConnection = false;
    bool isSubscribedMode = false;
    
    while (true)
    {
        try
        {
            byte[] buffer = new byte[1024];
            int bytesRead = client.Receive(buffer);
            
            if (bytesRead == 0)
                break;
            
            // Parse RESP command
            string input = Encoding.UTF8.GetString(buffer, 0, bytesRead);
            string[] parts = ParseRespArray(input);
            if (parts.Length == 0)
                continue;
            
            string command = parts[0].ToUpper();
            string response = string.Empty;
            
            if (isSubscribedMode)
            {
                string[] allowedCommands = { "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT", "RESET" };
                if (!allowedCommands.Contains(command))
                {
                    response = $"-ERR Can't execute '{command.ToLower()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n";
                    byte[] errorBytes = Encoding.UTF8.GetBytes(response);
                    client.Send(errorBytes);
                    continue;
                }
            }
            
            // MULTI - Start a transaction
            if (command == "MULTI")
            {
                inTransaction = true;
                transactionQueue.Clear();
                response = "+OK\r\n";
            }
            // EXEC - Execute a transaction
            else if (command == "EXEC")
            {
                if (inTransaction)
                {
                    var responses = new List<string>();
                    
                    foreach (var queuedCommand in transactionQueue)
                    {
                        string cmdResponse = await ExecuteCommand(queuedCommand, client);
                        responses.Add(cmdResponse);
                    }
                    
                    var sb = new StringBuilder();
                    sb.Append($"*{responses.Count}\r\n");
                    foreach (var resp in responses)
                    {
                        sb.Append(resp);
                    }
                    response = sb.ToString();
                    
                    inTransaction = false;
                    transactionQueue.Clear();
                }
                else
                {
                    response = "-ERR EXEC without MULTI\r\n";
                }
            }
            // DISCARD - Abort a transaction
            else if (command == "DISCARD")
            {
                if (inTransaction)
                {
                    inTransaction = false;
                    transactionQueue.Clear();
                    response = "+OK\r\n";
                }
                else
                {
                    response = "-ERR DISCARD without MULTI\r\n";
                }
            }
            else if (inTransaction)
            {
                transactionQueue.Add(parts);
                response = "+QUEUED\r\n";
            }
            // PING and ECHO
            else if (command == "PING")
            {
                if (isSubscribedMode)
                {
                    // In subscribed mode, PING responds with ["pong", ""] as a RESP array
                    response = "*2\r\n$4\r\npong\r\n$0\r\n\r\n";
                }
                else
                {
                    response = "+PONG\r\n";
                }
            }
            else if (command == "ECHO" && parts.Length > 1)
            {
                string message = parts[1];
                response = $"${message.Length}\r\n{message}\r\n";
            }
            // INFO - Get server information
            else if (command == "INFO")
            {
                if (parts.Length == 1 || parts[1].ToUpper() == "REPLICATION")
                {
                    string role = isReplica ? "slave" : "master";
                    string info;
                    
                    if (isReplica)
                    {
                        info = $"role:{role}";
                    }
                    else
                    {
                        info = $"role:{role}\r\nmaster_replid:{replicationId}\r\nmaster_repl_offset:{replicationOffset}";
                    }
                    
                    response = $"${info.Length}\r\n{info}\r\n";
                }
                else
                {
                    response = "$0\r\n\r\n";
                }
            }
            // CONFIG GET - Get configuration parameter value
            else if (command == "CONFIG" && parts.Length >= 3 && parts[1].ToUpper() == "GET")
            {
                string parameter = parts[2].ToLower();
                string? value = null;
                
                if (parameter == "dir")
                {
                    value = dir;
                }
                else if (parameter == "dbfilename")
                {
                    value = dbfilename;
                }
                
                if (value != null)
                {
                    response = $"*2\r\n${parameter.Length}\r\n{parameter}\r\n${value.Length}\r\n{value}\r\n";
                }
                else
                {
                    response = "*0\r\n";
                }
            }
            // KEYS - Get all keys matching a pattern
            else if (command == "KEYS" && parts.Length >= 2)
            {
                string pattern = parts[1];
                var matchingKeys = new List<string>();
                
                foreach (var key in dataStore.Keys)
                {
                    // Simple pattern matching - only "*" wildcard supported for now
                    if (pattern == "*" || key == pattern)
                    {
                        // Check if key has expired
                        if (dataStore.TryGetValue(key, out StoredValue? storedValue))
                        {
                            if (!storedValue.ExpiryMs.HasValue ||
                                DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() <= storedValue.ExpiryMs.Value)
                            {
                                matchingKeys.Add(key);
                            }
                        }
                    }
                }
                
                var sb = new StringBuilder();
                sb.Append($"*{matchingKeys.Count}\r\n");
                foreach (var key in matchingKeys)
                {
                    sb.Append($"${key.Length}\r\n{key}\r\n");
                }
                response = sb.ToString();
            }
            // REPLCONF - Replication configuration (used during handshake)
            else if (command == "REPLCONF")
            {
                if (parts.Length >= 3 && parts[1].ToUpper() == "ACK")
                {
                    if (long.TryParse(parts[2], out long ackOffset))
                    {
                        lock (replicaConnectionsLock)
                        {
                            if (replicaConnections.Contains(client))
                            {
                                replicaAckOffsets[client] = ackOffset;
                            }
                        }
                    }
                    response = string.Empty;
                }
                else
                {
                    response = "+OK\r\n";
                }
            }
            // PSYNC - Synchronize replica with master
            else if (command == "PSYNC" && parts.Length >= 3)
            {
                Console.WriteLine("[PSYNC] Starting RDB transfer...");
                
                string fullresyncResponse = $"+FULLRESYNC {replicationId} {replicationOffset}\r\n";
                byte[] fullresyncBytes = Encoding.UTF8.GetBytes(fullresyncResponse);
                
                string emptyRdbBase64 = "UkVESVMwMDA5/2NhMOXkSGD0";
                byte[] emptyRdbFile = Convert.FromBase64String(emptyRdbBase64);
                string rdbHeader = $"${emptyRdbFile.Length}\r\n";
                byte[] rdbHeaderBytes = Encoding.UTF8.GetBytes(rdbHeader);
                byte[] responseData = new byte[
                    fullresyncBytes.Length +
                    rdbHeaderBytes.Length +
                    emptyRdbFile.Length
                ];
                
                Array.Copy(fullresyncBytes, 0, responseData, 0, fullresyncBytes.Length);
                Array.Copy(rdbHeaderBytes, 0, responseData, fullresyncBytes.Length, rdbHeaderBytes.Length);
                Array.Copy(emptyRdbFile, 0, responseData, fullresyncBytes.Length + rdbHeaderBytes.Length, emptyRdbFile.Length);
                
                client.Send(responseData);
                Console.WriteLine($"[PSYNC] Sent {responseData.Length} bytes.");

                // Mark this connection as a replica
                lock (replicaConnectionsLock)
                {
                    replicaConnections.Add(client);
                    replicaAckOffsets[client] = 0;
                }
                isReplicationConnection = true;
                continue;
            }
            // SET and GET
            else if (command == "SET" && parts.Length >= 3)
            {
                string key = parts[1];
                string value = parts[2];
                long? expiryMs = null;
                
                for (int i = 3; i < parts.Length - 1; i++)
                {
                    string option = parts[i].ToUpper();
                    if (option == "PX")
                    {
                        if (long.TryParse(parts[i + 1], out long px))
                        {
                            expiryMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + px;
                        }
                        break;
                    }
                    else if (option == "EX")
                    {
                        if (long.TryParse(parts[i + 1], out long ex))
                        {
                            expiryMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + (ex * 1000);
                        }
                        break;
                    }
                }
                
                dataStore[key] = new StoredValue(value, expiryMs);
                response = "+OK\r\n";
                
                // Propagate to replicas
                if (!isReplicationConnection)
                {
                    PropagateToReplicas(input);
                }
            }
            else if (command == "GET" && parts.Length > 1)
            {
                string key = parts[1];
                if (dataStore.TryGetValue(key, out StoredValue? storedValue))
                {
                    if (storedValue.ExpiryMs.HasValue && 
                        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() > storedValue.ExpiryMs.Value)
                    {
                        dataStore.TryRemove(key, out _);
                        response = "$-1\r\n";
                    }
                    else if (storedValue.Value != null)
                    {
                        response = $"${storedValue.Value.Length}\r\n{storedValue.Value}\r\n";
                    }
                    else
                    {
                        response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                    }
                }
                else
                {
                    response = "$-1\r\n";
                }
            }
            // INCR - Increment the value of a key by 1
            else if (command == "INCR" && parts.Length >= 2)
            {
                string key = parts[1];
                
                if (dataStore.TryGetValue(key, out StoredValue? storedValue))
                {
                    // Check if key has expired
                    if (storedValue.ExpiryMs.HasValue && 
                        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() > storedValue.ExpiryMs.Value)
                    {
                        dataStore.TryRemove(key, out _);
                        response = "-ERR key expired\r\n";
                    }
                    else if (storedValue.Value != null)
                    {
                        if (int.TryParse(storedValue.Value, out int currentValue))
                        {
                            int newValue = currentValue + 1;
                            dataStore[key] = new StoredValue(newValue.ToString(), storedValue.ExpiryMs);
                            response = $":{newValue}\r\n";
                        }
                        else
                        {
                            response = "-ERR value is not an integer or out of range\r\n";
                        }
                    }
                    else
                    {
                        response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                    }
                }
                else
                {
                    dataStore[key] = new StoredValue("1");
                    response = ":1\r\n";
                }
            }
            // ZADD - Add member to a sorted set
            else if (command == "ZADD" && parts.Length >= 4)
            {
                string key = parts[1];
                
                if (!double.TryParse(parts[2], System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out double score))
                {
                    response = "-ERR value is not a valid float\r\n";
                }
                else
                {
                    string member = parts[3];
                    
                    if (!dataStore.ContainsKey(key))
                    {
                        var sortedSet = new List<SortedSetEntry> { new SortedSetEntry(member, score) };
                        dataStore[key] = new StoredValue(sortedSet);
                        response = ":1\r\n";
                    }
                    else
                    {
                        if (dataStore.TryGetValue(key, out StoredValue? storedValue) && storedValue.SortedSet != null)
                        {
                            // Check if member already exists
                            var existingEntry = storedValue.SortedSet.FirstOrDefault(e => e.Member == member);
                            if (existingEntry != null)
                            {
                                // Member already exists, update score
                                storedValue.SortedSet.Remove(existingEntry);
                                storedValue.SortedSet.Add(new SortedSetEntry(member, score));
                                storedValue.SortedSet.Sort();
                                response = ":0\r\n";  // No new members added
                            }
                            else
                            {
                                // New member
                                storedValue.SortedSet.Add(new SortedSetEntry(member, score));
                                storedValue.SortedSet.Sort();
                                response = ":1\r\n";
                            }
                        }
                        else
                        {
                            response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                        }
                    }
                }
            }
            // GEOADD - Add geospatial item
            else if (command == "GEOADD" && parts.Length >= 5)
            {
                if (!double.TryParse(parts[2], System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out double lon) ||
                    !double.TryParse(parts[3], System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out double lat))
                {
                    response = "-ERR invalid longitude,latitude pair\r\n";
                }
                else if (lon < -180.0 || lon > 180.0)
                {
                    response = $"-ERR invalid longitude value {lon:F6}\r\n";
                }
                else if (lat < -85.05112878 || lat > 85.05112878)
                {
                    response = $"-ERR invalid latitude value {lat:F6}\r\n";
                }
                else
                {
                    string key = parts[1];
                    string member = parts[4];
                    double score = (double)EncodeGeoHash(lon, lat);
                    
                    if (!dataStore.ContainsKey(key))
                    {
                        var sortedSet = new List<SortedSetEntry> { new SortedSetEntry(member, score) };
                        dataStore[key] = new StoredValue(sortedSet);
                        response = ":1\r\n";
                    }
                    else if (dataStore.TryGetValue(key, out StoredValue? storedVal) && storedVal.SortedSet != null)
                    {
                        var existingEntry = storedVal.SortedSet.FirstOrDefault(e => e.Member == member);
                        if (existingEntry != null)
                        {
                            storedVal.SortedSet.Remove(existingEntry);
                            storedVal.SortedSet.Add(new SortedSetEntry(member, score));
                            storedVal.SortedSet.Sort();
                            response = ":0\r\n";
                        }
                        else
                        {
                            storedVal.SortedSet.Add(new SortedSetEntry(member, score));
                            storedVal.SortedSet.Sort();
                            response = ":1\r\n";
                        }
                    }
                    else
                    {
                        response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                    }
                }
            }
            // GEOPOS - Get longitude and latitude of locations
            else if (command == "GEOPOS" && parts.Length >= 3)
            {
                string key = parts[1];
                int memberCount = parts.Length - 2;
                var sbGeo = new StringBuilder();
                sbGeo.Append($"*{memberCount}\r\n");

                for (int gi = 2; gi < parts.Length; gi++)
                {
                    string member = parts[gi];
                    bool found = false;
                    if (dataStore.TryGetValue(key, out StoredValue? geoVal) && geoVal.SortedSet != null)
                    {
                        var entry = geoVal.SortedSet.FirstOrDefault(e => e.Member == member);
                        if (entry != null)
                        {
                            var (decLon, decLat) = DecodeGeoHash((long)entry.Score);
                            string lonStr = decLon.ToString("R", System.Globalization.CultureInfo.InvariantCulture);
                            string latStr = decLat.ToString("R", System.Globalization.CultureInfo.InvariantCulture);
                            sbGeo.Append($"*2\r\n${lonStr.Length}\r\n{lonStr}\r\n${latStr.Length}\r\n{latStr}\r\n");
                            found = true;
                        }
                    }
                    if (!found)
                        sbGeo.Append("*-1\r\n");
                }
                response = sbGeo.ToString();
            }
            // GEODIST - Get distance between two locations
            else if (command == "GEODIST" && parts.Length >= 4)
            {
                string key = parts[1];
                string member1 = parts[2];
                string member2 = parts[3];

                if (!dataStore.TryGetValue(key, out StoredValue? geoDistVal) || geoDistVal.SortedSet == null)
                {
                    response = "$-1\r\n";
                }
                else
                {
                    var e1 = geoDistVal.SortedSet.FirstOrDefault(e => e.Member == member1);
                    var e2 = geoDistVal.SortedSet.FirstOrDefault(e => e.Member == member2);
                    if (e1 == null || e2 == null)
                    {
                        response = "$-1\r\n";
                    }
                    else
                    {
                        var (lon1, lat1) = DecodeGeoHash((long)e1.Score);
                        var (lon2, lat2) = DecodeGeoHash((long)e2.Score);
                        double dist = GeoDistMeters(lat1, lon1, lat2, lon2);
                        string distStr = dist.ToString("F4", System.Globalization.CultureInfo.InvariantCulture);
                        response = $"${distStr.Length}\r\n{distStr}\r\n";
                    }
                }
            }
            // GEOSEARCH - Search for locations within a radius
            else if (command == "GEOSEARCH" && parts.Length >= 8)
            {
                string key = parts[1];
                if (parts[2].ToUpper() != "FROMLONLAT" || parts[5].ToUpper() != "BYRADIUS")
                {
                    response = "-ERR unsupported GEOSEARCH options\r\n";
                }
                else if (!double.TryParse(parts[3], System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out double centerLon) ||
                         !double.TryParse(parts[4], System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out double centerLat) ||
                         !double.TryParse(parts[6], System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out double radius))
                {
                    response = "-ERR invalid arguments\r\n";
                }
                else
                {
                    double unitMultiplier = parts[7].ToLower() switch
                    {
                        "km" => 1000.0,
                        "mi" => 1609.344,
                        "ft" => 0.3048,
                        _    => 1.0
                    };
                    double radiusMeters = radius * unitMultiplier;

                    var matches = new List<string>();
                    if (dataStore.TryGetValue(key, out StoredValue? geoSearchVal) && geoSearchVal.SortedSet != null)
                    {
                        foreach (var entry in geoSearchVal.SortedSet)
                        {
                            var (mLon, mLat) = DecodeGeoHash((long)entry.Score);
                            double dist = GeoDistMeters(centerLat, centerLon, mLat, mLon);
                            if (dist <= radiusMeters)
                                matches.Add(entry.Member);
                        }
                    }

                    var sbGs = new StringBuilder();
                    sbGs.Append($"*{matches.Count}\r\n");
                    foreach (var m in matches)
                        sbGs.Append($"${m.Length}\r\n{m}\r\n");
                    response = sbGs.ToString();
                }
            }
            // ACL WHOAMI - Return the username of the current connection
            else if (command == "ACL" && parts.Length >= 2 && parts[1].ToUpper() == "WHOAMI")
            {
                response = "$7\r\ndefault\r\n";
            }
            // ACL SETUSER - Set properties of a user
            else if (command == "ACL" && parts.Length >= 3 && parts[1].ToUpper() == "SETUSER")
            {
                string username = parts[2];
                if (username == "default")
                {
                    for (int si = 3; si < parts.Length; si++)
                    {
                        string rule = parts[si];
                        if (rule.StartsWith(">"))
                        {
                            string password = rule.Substring(1);
                            byte[] hashBytes = SHA256.HashData(Encoding.UTF8.GetBytes(password));
                            string hash = Convert.ToHexString(hashBytes).ToLower();
                            if (!defaultUserPasswords.Contains(hash))
                                defaultUserPasswords.Add(hash);
                            defaultUserFlags.Remove("nopass");
                        }
                    }
                    response = "+OK\r\n";
                }
                else
                {
                    response = "-ERR unknown user\r\n";
                }
            }
            // ACL GETUSER - Get properties of a user
            else if (command == "ACL" && parts.Length >= 3 && parts[1].ToUpper() == "GETUSER")
            {
                string username = parts[2];
                if (username == "default")
                {
                    var flagsList = defaultUserFlags.ToList();
                    var sbAcl = new StringBuilder();
                    sbAcl.Append("*4\r\n");
                    sbAcl.Append("$5\r\nflags\r\n");
                    sbAcl.Append($"*{flagsList.Count}\r\n");
                    foreach (var f in flagsList) sbAcl.Append($"${f.Length}\r\n{f}\r\n");
                    sbAcl.Append("$9\r\npasswords\r\n");
                    sbAcl.Append($"*{defaultUserPasswords.Count}\r\n");
                    foreach (var p in defaultUserPasswords) sbAcl.Append($"${p.Length}\r\n{p}\r\n");
                    response = sbAcl.ToString();
                }
                else
                {
                    response = "$-1\r\n";
                }
            }
            // ZRANK - Get the rank of a member in a sorted set
            else if (command == "ZRANK" && parts.Length >= 3)
            {
                string key = parts[1];
                string member = parts[2];
                
                if (!dataStore.TryGetValue(key, out StoredValue? storedValue))
                {
                    response = "$-1\r\n";  // Key doesn't exist
                }
                else if (storedValue.SortedSet == null)
                {
                    response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                }
                else
                {
                    // Find the index of the member in the sorted set
                    int rank = -1;
                    for (int i = 0; i < storedValue.SortedSet.Count; i++)
                    {
                        if (storedValue.SortedSet[i].Member == member)
                        {
                            rank = i;
                            break;
                        }
                    }
                    
                    if (rank >= 0)
                    {
                        response = $":{rank}\r\n";
                    }
                    else
                    {
                        response = "$-1\r\n";  // Member doesn't exist
                    }
                }
            }
            // ZRANGE - List members from a sorted set by index range
            else if (command == "ZRANGE" && parts.Length >= 4)
            {
                string key = parts[1];
                
                if (!int.TryParse(parts[2], out int start) || !int.TryParse(parts[3], out int stop))
                {
                    response = "-ERR value is not an integer or out of range\r\n";
                }
                else if (!dataStore.TryGetValue(key, out StoredValue? storedValue))
                {
                    response = "*0\r\n";  // Key doesn't exist, return empty array
                }
                else if (storedValue.SortedSet == null)
                {
                    response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                }
                else
                {
                    var sortedSet = storedValue.SortedSet;
                    int count = sortedSet.Count;
                    
                    if (start < 0)
                    {
                        start = Math.Max(0, count + start);
                    }
                    if (stop < 0)
                    {
                        stop = Math.Max(0, count + stop);
                    }
                    
                    // Handle edge cases
                    if (start >= count || start > stop)
                    {
                        response = "*0\r\n";  // Empty array
                    }
                    else
                    {
                        // Adjust stop index if it exceeds the cardinality
                        if (stop >= count)
                        {
                            stop = count - 1;
                        }
                        
                        // Build the RESP array response
                        var sb = new StringBuilder();
                        int resultCount = stop - start + 1;
                        sb.Append($"*{resultCount}\r\n");
                        
                        for (int i = start; i <= stop; i++)
                        {
                            string member = sortedSet[i].Member;
                            sb.Append($"${member.Length}\r\n{member}\r\n");
                        }
                        
                        response = sb.ToString();
                    }
                }
            }
            // ZCARD - Get the cardinality of a sorted set
            else if (command == "ZCARD" && parts.Length >= 2)
            {
                string key = parts[1];
                
                if (!dataStore.TryGetValue(key, out StoredValue? storedValue))
                {
                    response = ":0\r\n";  // Key doesn't exist
                }
                else if (storedValue.SortedSet == null)
                {
                    response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                }
                else
                {
                    response = $":{storedValue.SortedSet.Count}\r\n";
                }
            }
            // ZSCORE - Get the score of a member in a sorted set
            else if (command == "ZSCORE" && parts.Length >= 3)
            {
                string key = parts[1];
                string member = parts[2];
                
                if (!dataStore.TryGetValue(key, out StoredValue? storedValue))
                {
                    response = "$-1\r\n";
                }
                else if (storedValue.SortedSet == null)
                {
                    response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                }
                else
                {
                    // Find the member in the sorted set
                    var entry = storedValue.SortedSet.FirstOrDefault(e => e.Member == member);
                    if (entry != null)
                    {
                        string scoreStr = entry.Score.ToString(System.Globalization.CultureInfo.InvariantCulture);
                        response = $"${scoreStr.Length}\r\n{scoreStr}\r\n";
                    }
                    else
                    {
                        response = "$-1\r\n";
                    }
                }
            }
            // ZREM - Remove a member from a sorted set
            else if (command == "ZREM" && parts.Length >= 3)
            {
                string key = parts[1];
                string member = parts[2];
                
                if (!dataStore.TryGetValue(key, out StoredValue? storedValue))
                {
                    response = ":0\r\n";  // Key doesn't exist
                }
                else if (storedValue.SortedSet == null)
                {
                    response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                }
                else
                {
                    var entry = storedValue.SortedSet.FirstOrDefault(e => e.Member == member);
                    if (entry != null)
                    {
                        storedValue.SortedSet.Remove(entry);
                        response = ":1\r\n";
                    }
                    else
                    {
                        response = ":0\r\n";
                    }
                }
            }
            // WAIT - Wait for acknowledgements from replicas
            else if (command == "WAIT" && parts.Length >= 3)
            {
                if (!int.TryParse(parts[1], out int numReplicas))
                {
                    response = "-ERR value is not an integer or out of range\r\n";
                }
                else if (!int.TryParse(parts[2], out int timeout))
                {
                    response = "-ERR timeout is not an integer or out of range\r\n";
                }
                else
                {
                    List<Socket> replicas;
                    long currentOffset;
                    lock (replicaConnectionsLock)
                    {
                        replicas = new List<Socket>(replicaConnections);
                        currentOffset = masterOffset;
                    }
                    
                    if (replicas.Count == 0)
                    {
                        response = ":0\r\n";
                    }
                    else if (currentOffset == 0)
                    {
                        response = $":{replicas.Count}\r\n";
                    }
                    else
                    {
                        RequestReplicaAcks(replicas);
                        long deadline = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + timeout;
                        int ackedReplicas = 0;

                        while (true)
                        {
                            lock (replicaConnectionsLock)
                            {
                                ackedReplicas = replicas.Count(replica =>
                                    replicaAckOffsets.TryGetValue(replica, out long ackOffset) &&
                                    ackOffset >= currentOffset);
                            }

                            if (ackedReplicas >= numReplicas ||
                                DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() >= deadline)
                            {
                                break;
                            }

                            await Task.Delay(10);
                        }

                        response = $":{ackedReplicas}\r\n";
                    }
                }
            }
            // RPUSH - Append elements to a list
            else if (command == "RPUSH" && parts.Length >= 3)
            {
                string key = parts[1];
                var elements = parts.Skip(2).ToArray();
                bool shouldUnblock = false;
                
                if (!dataStore.ContainsKey(key))
                {
                    var list = new List<string>(elements);
                    dataStore[key] = new StoredValue(list);
                    response = $":{list.Count}\r\n";
                    shouldUnblock = true;
                }
                else
                {
                    if (dataStore.TryGetValue(key, out StoredValue? storedValue) && storedValue.List != null)
                    {
                        storedValue.List.AddRange(elements);
                        int count = storedValue.List.Count;
                        response = $":{count}\r\n";
                        shouldUnblock = true;
                    }
                    else
                    {
                        response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                    }
                }
                
                if (!string.IsNullOrEmpty(response))
                {
                    byte[] responseBytes = Encoding.UTF8.GetBytes(response);
                    client.Send(responseBytes);
                    response = string.Empty;
                }
                
                if (shouldUnblock)
                {
                    UnblockWaitingClients(key);
                }
            }
            // LPUSH - Prepend elements to a list
            else if (command == "LPUSH" && parts.Length >= 3)
            {
                string key = parts[1];
                var elements = parts.Skip(2).ToArray();
                bool shouldUnblock = false;
                
                if (!dataStore.ContainsKey(key))
                {
                    var list = new List<string>(elements.Reverse());
                    dataStore[key] = new StoredValue(list);
                    response = $":{list.Count}\r\n";
                    shouldUnblock = true;
                }
                else
                {
                    if (dataStore.TryGetValue(key, out StoredValue? storedValue) && storedValue.List != null)
                    {
                        for (int i = 0; i < elements.Length; i++)
                        {
                            storedValue.List.Insert(0, elements[i]);
                        }
                        int count = storedValue.List.Count;
                        response = $":{count}\r\n";
                        shouldUnblock = true;
                    }
                    else
                    {
                        response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                    }
                }
                
                if (!string.IsNullOrEmpty(response))
                {
                    byte[] responseBytes = Encoding.UTF8.GetBytes(response);
                    client.Send(responseBytes);
                    response = string.Empty;
                }
                
                if (shouldUnblock)
                {
                    UnblockWaitingClients(key);
                }
            }
            // LRANGE - Retrieve elements from a list by range
            else if (command == "LRANGE" && parts.Length >= 4)
            {
                string key = parts[1];

                if (!int.TryParse(parts[2], out int start) || !int.TryParse(parts[3], out int stop))
                {
                    response = "-ERR value is not an integer or out of range\r\n";
                }
                else if (!dataStore.TryGetValue(key, out StoredValue? storedValue))
                {
                    response = "*0\r\n";
                }
                else if (storedValue.List == null)
                {
                    response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                }
                else
                {
                    var list = storedValue.List;
                    
                    if (start < 0)
                    {
                        start = Math.Max(0, list.Count + start);
                    }
                    if (stop < 0)
                    {
                        stop = Math.Max(0, list.Count + stop);
                    }
                    
                    if (start >= list.Count || start > stop)
                    {
                        response = "*0\r\n";
                    }
                    else
                    {
                        int actualStop = Math.Min(stop, list.Count - 1);
                        
                        var rangeElements = new List<string>();
                        for (int i = start; i <= actualStop; i++)
                        {
                            rangeElements.Add(list[i]);
                        }
                        
                        var sb = new StringBuilder();
                        sb.Append($"*{rangeElements.Count}\r\n");
                        foreach (var element in rangeElements)
                        {
                            sb.Append($"${element.Length}\r\n{element}\r\n");
                        }
                        response = sb.ToString();
                    }
                }
            }
            // LLEN - Get the length of a list
            else if (command == "LLEN" && parts.Length >= 2)
            {
                string key = parts[1];
                
                if (!dataStore.TryGetValue(key, out StoredValue? storedValue))
                {
                    response = ":0\r\n";
                }
                else if (storedValue.List == null)
                {
                    response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                }
                else
                {
                    response = $":{storedValue.List.Count}\r\n";
                }
            }
            // LPOP - Remove and return the first element(s) of a list
            else if (command == "LPOP" && parts.Length >= 2)
            {
                string key = parts[1];
                int count = 1;
                
                if (parts.Length >= 3)
                {
                    if (!int.TryParse(parts[2], out count) || count < 1)
                    {
                        response = "-ERR value is not an integer or out of range\r\n";
                        goto SendResponse;
                    }
                }
                
                if (!dataStore.TryGetValue(key, out StoredValue? storedValue))
                {
                    response = "$-1\r\n";
                }
                else if (storedValue.List == null)
                {
                    response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                }
                else if (storedValue.List.Count == 0)
                {
                    response = "$-1\r\n";
                }
                else
                {
                    int elementsToRemove = Math.Min(count, storedValue.List.Count);
                    
                    if (parts.Length >= 3)
                    {
                        var removedElements = new List<string>();
                        for (int i = 0; i < elementsToRemove; i++)
                        {
                            removedElements.Add(storedValue.List[0]);
                            storedValue.List.RemoveAt(0);
                        }
                        
                        var sb = new StringBuilder();
                        sb.Append($"*{removedElements.Count}\r\n");
                        foreach (var element in removedElements)
                        {
                            sb.Append($"${element.Length}\r\n{element}\r\n");
                        }
                        response = sb.ToString();
                    }
                    else
                    {
                        string element = storedValue.List[0];
                        storedValue.List.RemoveAt(0);
                        response = $"${element.Length}\r\n{element}\r\n";
                    }
                }
                
                SendResponse:;
            }
            // BLPOP - Blocking pop from list
            else if (command == "BLPOP" && parts.Length >= 3)
            {
                string key = parts[1];
                
                if (!double.TryParse(parts[2], out double timeout))
                {
                    response = "-ERR timeout is not a float or out of range\r\n";
                }
                else if (dataStore.TryGetValue(key, out StoredValue? storedValue) && storedValue.List != null && storedValue.List.Count > 0)
                {
                    string element = storedValue.List[0];
                    storedValue.List.RemoveAt(0);
                    
                    var sb = new StringBuilder();
                    sb.Append("*2\r\n");
                    sb.Append($"${key.Length}\r\n{key}\r\n");
                    sb.Append($"${element.Length}\r\n{element}\r\n");
                    response = sb.ToString();
                }
                else
                {
                    var tcs = new TaskCompletionSource<string?>();
                    
                    lock (blockedClientsLock)
                    {
                        if (!blockedClients.ContainsKey(key))
                        {
                            blockedClients[key] = new Queue<BlockedClient>();
                        }
                        blockedClients[key].Enqueue(new BlockedClient(key, tcs));
                    }
                    
                    Task<string?> elementTask = tcs.Task;
                    Task completedTask;
                    
                    if (timeout > 0)
                    {
                        int timeoutMs = (int)(timeout * 1000);
                        Task delayTask = Task.Delay(timeoutMs);
                        completedTask = await Task.WhenAny(elementTask, delayTask);
                    }
                    else
                    {
                        await elementTask;
                        completedTask = elementTask;
                    }
                    
                    string? poppedElement = null;
                    
                    if (completedTask == elementTask && elementTask.IsCompletedSuccessfully)
                    {
                        poppedElement = elementTask.Result;
                    }
                    else
                    {
                        lock (blockedClientsLock)
                        {
                            if (blockedClients.TryGetValue(key, out Queue<BlockedClient>? queue))
                            {
                                var tempQueue = new Queue<BlockedClient>();
                                while (queue.Count > 0)
                                {
                                    var bc = queue.Dequeue();
                                    if (bc.TaskCompletionSource != tcs)
                                    {
                                        tempQueue.Enqueue(bc);
                                    }
                                }
                                
                                if (tempQueue.Count > 0)
                                {
                                    blockedClients[key] = tempQueue;
                                }
                                else
                                {
                                    blockedClients.TryRemove(key, out _);
                                }
                            }
                        }
                        
                        tcs.TrySetResult(null);
                    }
                    
                    if (poppedElement != null)
                    {
                        var sb = new StringBuilder();
                        sb.Append("*2\r\n");
                        sb.Append($"${key.Length}\r\n{key}\r\n");
                        sb.Append($"${poppedElement.Length}\r\n{poppedElement}\r\n");
                        response = sb.ToString();
                    }
                    else
                    {
                        response = "*-1\r\n";
                    }
                }
            }
            // TYPE - Get the type of value stored at a key
            else if (command == "TYPE" && parts.Length >= 2)
            {
                string key = parts[1];
                
                if (!dataStore.TryGetValue(key, out StoredValue? storedValue))
                {
                    response = "+none\r\n";
                }
                else if (storedValue.Value != null)
                {
                    if (storedValue.ExpiryMs.HasValue && 
                        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() > storedValue.ExpiryMs.Value)
                    {
                        dataStore.TryRemove(key, out _);
                        response = "+none\r\n";
                    }
                    else
                    {
                        response = "+string\r\n";
                    }
                }
                else if (storedValue.List != null)
                {
                    response = "+list\r\n";
                }
                else if (storedValue.Stream != null)
                {
                    response = "+stream\r\n";
                }
                else
                {
                    response = "+none\r\n";
                }
            }
            // XADD - Add entry to a stream
            else if (command == "XADD" && parts.Length >= 4)
            {
                string key = parts[1];
                string entryId = parts[2];
                
                int fieldCount = parts.Length - 3;
                if (fieldCount % 2 != 0)
                {
                    response = "-ERR wrong number of arguments for XADD\r\n";
                }
                else
                {
                    long millisTime = 0;
                    long seqNum = 0;
                    
                    if (entryId == "*")
                    {
                        millisTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                        
                        if (dataStore.TryGetValue(key, out StoredValue? storedValue) && storedValue.Stream != null && storedValue.Stream.Count > 0)
                        {
                            var lastEntry = storedValue.Stream[storedValue.Stream.Count - 1];
                            string[] lastIdParts = lastEntry.Id.Split('-');
                            long lastMillisTime = long.Parse(lastIdParts[0]);
                            long lastSeqNum = long.Parse(lastIdParts[1]);
                            
                            if (millisTime == lastMillisTime)
                            {
                                seqNum = lastSeqNum + 1;
                            }
                            else if (millisTime <= lastMillisTime)
                            {
                                millisTime = lastMillisTime;
                                seqNum = lastSeqNum + 1;
                            }
                            else
                            {
                                seqNum = 0;
                            }
                        }
                        else
                        {
                            seqNum = 0;
                        }
                        
                        entryId = $"{millisTime}-{seqNum}";
                    }
                    else
                    {
                        string[] idParts = entryId.Split('-');
                        if (idParts.Length != 2 || !long.TryParse(idParts[0], out millisTime))
                        {
                            response = "-ERR Invalid stream ID specified as stream command argument\r\n";
                        }
                        else if (idParts[1] == "*")
                        {
                            if (dataStore.TryGetValue(key, out StoredValue? storedValue) && storedValue.Stream != null && storedValue.Stream.Count > 0)
                            {
                                var lastEntry = storedValue.Stream[storedValue.Stream.Count - 1];
                                string[] lastIdParts = lastEntry.Id.Split('-');
                                long lastMillisTime = long.Parse(lastIdParts[0]);
                                long lastSeqNum = long.Parse(lastIdParts[1]);
                                
                                if (millisTime == lastMillisTime)
                                {
                                    seqNum = lastSeqNum + 1;
                                }
                                else
                                {
                                    if (millisTime == 0)
                                    {
                                        seqNum = 1;
                                    }
                                    else
                                    {
                                        seqNum = 0;
                                    }
                                }
                            }
                            else
                            {
                                if (millisTime == 0)
                                {
                                    seqNum = 1;
                                }
                                else
                                {
                                    seqNum = 0;
                                }
                            }
                            
                            entryId = $"{millisTime}-{seqNum}";
                        }
                        else if (!long.TryParse(idParts[1], out seqNum))
                        {
                            response = "-ERR Invalid stream ID specified as stream command argument\r\n";
                        }
                    }
                    
                    if (string.IsNullOrEmpty(response))
                    {
                        if (millisTime == 0 && seqNum == 0)
                        {
                            response = "-ERR The ID specified in XADD must be greater than 0-0\r\n";
                        }
                        else
                        {
                            bool isValid = true;
                            if (dataStore.TryGetValue(key, out StoredValue? storedValue) && storedValue.Stream != null && storedValue.Stream.Count > 0)
                            {
                                var lastEntry = storedValue.Stream[storedValue.Stream.Count - 1];
                                string[] lastIdParts = lastEntry.Id.Split('-');
                                long lastMillisTime = long.Parse(lastIdParts[0]);
                                long lastSeqNum = long.Parse(lastIdParts[1]);
                                
                                if (millisTime < lastMillisTime)
                                {
                                    isValid = false;
                                }
                                else if (millisTime == lastMillisTime && seqNum <= lastSeqNum)
                                {
                                    isValid = false;
                                }
                                
                                if (!isValid)
                                {
                                    response = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                                }
                            }
                            
                            if (isValid)
                            {
                                var fields = new Dictionary<string, string>();
                                for (int i = 3; i < parts.Length; i += 2)
                                {
                                    fields[parts[i]] = parts[i + 1];
                                }
                                
                                var entry = new StreamEntry(entryId, fields);
                                
                                if (!dataStore.ContainsKey(key))
                                {
                                    var stream = new List<StreamEntry> { entry };
                                    dataStore[key] = new StoredValue(stream);
                                    response = $"${entryId.Length}\r\n{entryId}\r\n";
                                }
                                else
                                {
                                    if (dataStore.TryGetValue(key, out StoredValue? existingValue) && existingValue.Stream != null)
                                    {
                                        existingValue.Stream.Add(entry);
                                        response = $"${entryId.Length}\r\n{entryId}\r\n";
                                    }
                                    else
                                    {
                                        response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                                    }
                                }
                                
                                UnblockWaitingStreamReaders(key);
                            }
                        }
                    }
                }
            }
            // XREAD - Read data from streams starting from a specified ID (exclusive)
            else if (command == "XREAD" && parts.Length >= 4)
            {
                int blockTimeout = -1;
                int streamsIndex = 1;
                
                if (parts[1].ToUpper() == "BLOCK")
                {
                    if (parts.Length < 6)
                    {
                        response = "-ERR wrong number of arguments for XREAD\r\n";
                        goto SkipXRead;
                    }
                    if (!int.TryParse(parts[2], out blockTimeout))
                    {
                        response = "-ERR timeout is not an integer or out of range\r\n";
                        goto SkipXRead;
                    }
                    streamsIndex = 3;
                }
                
                if (parts[streamsIndex].ToUpper() != "STREAMS")
                {
                    response = "-ERR wrong number of arguments for XREAD\r\n";
                }
                else if (parts.Length < streamsIndex + 3)
                {
                    response = "-ERR wrong number of arguments for XREAD\r\n";
                }
                else
                {
                    int argsAfterStreams = parts.Length - streamsIndex - 1;
                    if (argsAfterStreams % 2 != 0)
                    {
                        response = "-ERR wrong number of arguments for XREAD\r\n";
                    }
                    else
                    {
                        int streamCount = argsAfterStreams / 2;
                        var keys = new string[streamCount];
                        var ids = new string[streamCount];
                        
                        for (int i = 0; i < streamCount; i++)
                        {
                            keys[i] = parts[streamsIndex + 1 + i];
                            ids[i] = parts[streamsIndex + 1 + streamCount + i];
                            
                            if (ids[i] == "$")
                            {
                                if (dataStore.TryGetValue(keys[i], out StoredValue? storedValue) && storedValue.Stream != null && storedValue.Stream.Count > 0)
                                {
                                    var lastEntry = storedValue.Stream[storedValue.Stream.Count - 1];
                                    ids[i] = lastEntry.Id;
                                }
                                else
                                {
                                    ids[i] = "0-0";
                                }
                            }
                        }
                        
                        // Query each stream and collect results
                        var streamResults = new List<(string key, List<StreamEntry> entries)>();
                        
                        for (int i = 0; i < streamCount; i++)
                        {
                            string key = keys[i];
                            string startId = ids[i];
                            
                            if (dataStore.TryGetValue(key, out StoredValue? storedValue) && storedValue.Stream != null)
                            {
                                var (startMillis, startSeq) = ParseStreamId(startId, true);
                                
                                var matchingEntries = new List<StreamEntry>();
                                foreach (var entry in storedValue.Stream)
                                {
                                    string[] idParts = entry.Id.Split('-');
                                    long entryMillis = long.Parse(idParts[0]);
                                    long entrySeq = long.Parse(idParts[1]);
                                    
                                    bool isGreater = false;
                                    if (entryMillis > startMillis)
                                    {
                                        isGreater = true;
                                    }
                                    else if (entryMillis == startMillis && entrySeq > startSeq)
                                    {
                                        isGreater = true;
                                    }
                                    
                                    if (isGreater)
                                    {
                                        matchingEntries.Add(entry);
                                    }
                                }
                                
                                if (matchingEntries.Count > 0)
                                {
                                    streamResults.Add((key, matchingEntries));
                                }
                            }
                        }
                        
                        // Build RESP response
                        if (streamResults.Count == 0 && blockTimeout >= 0)
                        {
                            var tcs = new TaskCompletionSource<List<(string key, List<StreamEntry> entries)>?>(TaskCreationOptions.RunContinuationsAsynchronously);
                            
                            lock (blockedStreamReadersLock)
                            {
                                for (int i = 0; i < streamCount; i++)
                                {
                                    string key = keys[i];
                                    if (!blockedStreamReaders.ContainsKey(key))
                                    {
                                        blockedStreamReaders[key] = new Queue<BlockedStreamReader>();
                                    }
                                    blockedStreamReaders[key].Enqueue(new BlockedStreamReader(keys, ids, tcs));
                                }
                            }
                            
                            Task<List<(string key, List<StreamEntry> entries)>?> entriesTask = tcs.Task;
                            Task completedTask;
                            
                            if (blockTimeout > 0)
                            {
                                Task delayTask = Task.Delay(blockTimeout);
                                completedTask = await Task.WhenAny(entriesTask, delayTask);
                            }
                            else
                            {
                                await entriesTask;
                                completedTask = entriesTask;
                            }
                            
                            lock (blockedStreamReadersLock)
                            {
                                for (int i = 0; i < streamCount; i++)
                                {
                                    string key = keys[i];
                                    if (blockedStreamReaders.TryGetValue(key, out Queue<BlockedStreamReader>? queue))
                                    {
                                        var tempQueue = new Queue<BlockedStreamReader>();
                                        while (queue.Count > 0)
                                        {
                                            var reader = queue.Dequeue();
                                            if (reader.TaskCompletionSource != tcs)
                                            {
                                                tempQueue.Enqueue(reader);
                                            }
                                        }
                                        
                                        if (tempQueue.Count > 0)
                                        {
                                            blockedStreamReaders[key] = tempQueue;
                                        }
                                        else
                                        {
                                            blockedStreamReaders.TryRemove(key, out _);
                                        }
                                    }
                                }
                            }
                            
                            if (entriesTask.IsCompletedSuccessfully && entriesTask.Result != null)
                            {
                                streamResults = entriesTask.Result;
                            }
                            else if (completedTask == entriesTask)
                            {
                                response = "*-1\r\n";
                            }
                            else
                            {
                                response = "*-1\r\n";
                            }
                        }
                        
                        if (string.IsNullOrEmpty(response))
                        {
                            if (streamResults.Count == 0)
                            {
                                response = "*-1\r\n";
                            }
                            else
                            {
                                var sb = new StringBuilder();
                                
                                sb.Append($"*{streamResults.Count}\r\n");
                                
                                foreach (var (key, matchingEntries) in streamResults)
                                {
                                    sb.Append("*2\r\n");
                                    
                                    sb.Append($"${key.Length}\r\n{key}\r\n");
                                    
                                    sb.Append($"*{matchingEntries.Count}\r\n");
                                    
                                    foreach (var entry in matchingEntries)
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
                                }
                                
                                response = sb.ToString();
                            }
                        }
                    }
                }
                
                SkipXRead:;
            }
            // XRANGE - Query range of entries from stream
            else if (command == "XRANGE" && parts.Length >= 4)
            {
                string key = parts[1];
                string startId = parts[2];
                string endId = parts[3];
                
                if (!dataStore.TryGetValue(key, out StoredValue? storedValue))
                {
                    response = "*0\r\n";
                }
                else if (storedValue.Stream == null)
                {
                    response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                }
                else
                {
                    var (startMillis, startSeq) = ParseStreamId(startId, true);
                    var (endMillis, endSeq) = ParseStreamId(endId, false);
                    
                    var matchingEntries = new List<StreamEntry>();
                    foreach (var entry in storedValue.Stream)
                    {
                        string[] idParts = entry.Id.Split('-');
                        long entryMillis = long.Parse(idParts[0]);
                        long entrySeq = long.Parse(idParts[1]);
                        
                        bool isInRange = false;
                        if (entryMillis > startMillis && entryMillis < endMillis)
                        {
                            isInRange = true;
                        }
                        else if (entryMillis == startMillis && entryMillis == endMillis)
                        {
                            isInRange = entrySeq >= startSeq && entrySeq <= endSeq;
                        }
                        else if (entryMillis == startMillis)
                        {
                            isInRange = entrySeq >= startSeq;
                        }
                        else if (entryMillis == endMillis)
                        {
                            isInRange = entrySeq <= endSeq;
                        }
                        
                        if (isInRange)
                        {
                            matchingEntries.Add(entry);
                        }
                    }
                    
                    var sb = new StringBuilder();
                    sb.Append($"*{matchingEntries.Count}\r\n");
                    
                    foreach (var entry in matchingEntries)
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
                    
                    response = sb.ToString();
                }
            }
            // PUBLISH - Publish a message to a channel
            else if (command == "PUBLISH" && parts.Length >= 3)
            {
                string channel = parts[1];
                string message = parts[2];
                
                int subscriberCount = 0;
                lock (subscriptionsLock)
                {
                    if (channelSubscribers.TryGetValue(channel, out HashSet<Socket>? subscribers))
                    {
                        subscriberCount = subscribers.Count;
                        
                        string messageResponse = $"*3\r\n$7\r\nmessage\r\n${channel.Length}\r\n{channel}\r\n${message.Length}\r\n{message}\r\n";
                        byte[] messageBytes = Encoding.UTF8.GetBytes(messageResponse);
                        
                        foreach (Socket subscriber in subscribers.ToList())
                        {
                            try
                            {
                                subscriber.Send(messageBytes);
                            }
                            catch (Exception)
                            {
                                // Subscriber disconnected or error sending, skip
                            }
                        }
                    }
                }
                
                response = $":{subscriberCount}\r\n";
            }
            // SUBSCRIBE - Subscribe to one or more channels
            else if (command == "SUBSCRIBE" && parts.Length >= 2)
            {
                lock (subscriptionsLock)
                {
                    if (!clientSubscriptions.ContainsKey(client))
                    {
                        clientSubscriptions[client] = new HashSet<string>();
                    }
                    
                    for (int i = 1; i < parts.Length; i++)
                    {
                        string channel = parts[i];
                        
                        if (!channelSubscribers.ContainsKey(channel))
                        {
                            channelSubscribers[channel] = new HashSet<Socket>();
                        }
                        channelSubscribers[channel].Add(client);
                        
                        clientSubscriptions[client].Add(channel);
                        
                        int subscriptionCount = clientSubscriptions[client].Count;
                        string subResponse = $"*3\r\n$9\r\nsubscribe\r\n${channel.Length}\r\n{channel}\r\n:{subscriptionCount}\r\n";
                        byte[] subResponseBytes = Encoding.UTF8.GetBytes(subResponse);
                        client.Send(subResponseBytes);
                    }
                    
                    // Enter subscribed mode
                    isSubscribedMode = true;
                }
                
                response = string.Empty;
            }
            // UNSUBSCRIBE - Unsubscribe from one or more channels
            else if (command == "UNSUBSCRIBE" && parts.Length >= 2)
            {
                lock (subscriptionsLock)
                {
                    for (int i = 1; i < parts.Length; i++)
                    {
                        string channel = parts[i];
                        
                        if (channelSubscribers.TryGetValue(channel, out HashSet<Socket>? subscribers))
                        {
                            subscribers.Remove(client);
                            if (subscribers.Count == 0)
                            {
                                channelSubscribers.TryRemove(channel, out _);
                            }
                        }
                        
                        if (clientSubscriptions.TryGetValue(client, out HashSet<string>? channels))
                        {
                            channels.Remove(channel);
                        }
                        
                        int subscriptionCount = clientSubscriptions.ContainsKey(client) ? clientSubscriptions[client].Count : 0;
                        string unsubResponse = $"*3\r\n$11\r\nunsubscribe\r\n${channel.Length}\r\n{channel}\r\n:{subscriptionCount}\r\n";
                        byte[] unsubResponseBytes = Encoding.UTF8.GetBytes(unsubResponse);
                        client.Send(unsubResponseBytes);
                    }
                    
                    if (!clientSubscriptions.ContainsKey(client) || clientSubscriptions[client].Count == 0)
                    {
                        isSubscribedMode = false;
                        if (clientSubscriptions.ContainsKey(client))
                        {
                            clientSubscriptions.TryRemove(client, out _);
                        }
                    }
                }
                
                response = string.Empty;
            }
            else
            {
                response = "-ERR unknown command\r\n";
            }
            
            if (!string.IsNullOrEmpty(response) && !isReplicationConnection)
            {
                byte[] responseBytes = Encoding.UTF8.GetBytes(response);
                client.Send(responseBytes);
            }
        }
        catch
        {
            break;
        }
    }
    
    lock (replicaConnectionsLock)
    {
        replicaConnections.Remove(client);
        replicaAckOffsets.Remove(client);
    }
    
    // Clean up subscriptions
    lock (subscriptionsLock)
    {
        if (clientSubscriptions.TryGetValue(client, out HashSet<string>? channels))
        {
            foreach (string channel in channels)
            {
                if (channelSubscribers.TryGetValue(channel, out HashSet<Socket>? subscribers))
                {
                    subscribers.Remove(client);
                    if (subscribers.Count == 0)
                    {
                        channelSubscribers.TryRemove(channel, out _);
                    }
                }
            }
            clientSubscriptions.TryRemove(client, out _);
        }
    }
    
    client.Close();
}

/* Propagate command to all connected replicas */
void PropagateToReplicas(string command)
{
    byte[] commandBytes = Encoding.UTF8.GetBytes(command);
    
    lock (replicaConnectionsLock)
    {
        var disconnectedReplicas = new List<Socket>();
        
        foreach (var replica in replicaConnections)
        {
            try
            {
                replica.Send(commandBytes);
            }
            catch
            {
                disconnectedReplicas.Add(replica);
            }
        }
        
        // Remove disconnected replicas
        foreach (var replica in disconnectedReplicas)
        {
            replicaConnections.Remove(replica);
            replicaAckOffsets.Remove(replica);
        }
        
        // Update master offset
        if (replicaConnections.Count > 0)
        {
            masterOffset += commandBytes.Length;
        }
    }
}

/* Request ACKs from connected replicas */
void RequestReplicaAcks(IEnumerable<Socket> replicas)
{
    string getackCmd = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
    byte[] getackBytes = Encoding.UTF8.GetBytes(getackCmd);

    lock (replicaConnectionsLock)
    {
        var disconnectedReplicas = new List<Socket>();

        foreach (var replica in replicas)
        {
            try
            {
                replica.Send(getackBytes);
            }
            catch
            {
                disconnectedReplicas.Add(replica);
            }
        }

        foreach (var replica in disconnectedReplicas)
        {
            replicaConnections.Remove(replica);
            replicaAckOffsets.Remove(replica);
        }
    }
}

/* Unblock waiting clients for a given key */
void UnblockWaitingClients(string key)
{
    lock (blockedClientsLock)
    {
        while (blockedClients.TryGetValue(key, out Queue<BlockedClient>? queue) && queue.Count > 0)
        {
            if (dataStore.TryGetValue(key, out StoredValue? storedValue) && storedValue.List != null && storedValue.List.Count > 0)
            {
                var blockedClient = queue.Dequeue();
                string element = storedValue.List[0];
                storedValue.List.RemoveAt(0);
                
                blockedClient.TaskCompletionSource.SetResult(element);
                
                if (queue.Count == 0)
                {
                    blockedClients.TryRemove(key, out _);
                }
            }
            else
            {
                break;
            }
        }
    }
}

/* Parse stream ID into milliseconds and sequence number */
(long, long) ParseStreamId(string id, bool isStart)
{
    if (id == "-")
    {
        return isStart ? (0, 0) : (long.MaxValue, long.MaxValue);
    }
    
    if (id == "+")
    {
        return (long.MaxValue, long.MaxValue);
    }
    
    string[] parts = id.Split('-');
    long millis = long.Parse(parts[0]);
    long seq;
    
    if (parts.Length == 1)
    {
        seq = isStart ? 0 : long.MaxValue;
    }
    else
    {
        seq = long.Parse(parts[1]);
    }
    
    return (millis, seq);
}

/* Parse RESP array from input string */
string[] ParseRespArray(string input)
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

/* Unblock waiting stream readers for a given key */
void UnblockWaitingStreamReaders(string key)
{
    lock (blockedStreamReadersLock)
    {
        if (blockedStreamReaders.TryGetValue(key, out Queue<BlockedStreamReader>? queue) && queue.Count > 0)
        {
            var readersToUnblock = new List<BlockedStreamReader>();
            while (queue.Count > 0)
            {
                readersToUnblock.Add(queue.Dequeue());
            }
            blockedStreamReaders.TryRemove(key, out _);
            
            foreach (var reader in readersToUnblock)
            {
                var results = new List<(string key, List<StreamEntry> entries)>();
                
                for (int i = 0; i < reader.Keys.Length; i++)
                {
                    string streamKey = reader.Keys[i];
                    string startId = reader.Ids[i];
                    
                    if (dataStore.TryGetValue(streamKey, out StoredValue? storedValue) && storedValue.Stream != null)
                    {
                        var (startMillis, startSeq) = ParseStreamId(startId, true);
                        var matchingEntries = new List<StreamEntry>();
                        
                        foreach (var entry in storedValue.Stream)
                        {
                            string[] idParts = entry.Id.Split('-');
                            long entryMillis = long.Parse(idParts[0]);
                            long entrySeq = long.Parse(idParts[1]);
                            
                            bool isGreater = false;
                            if (entryMillis > startMillis)
                            {
                                isGreater = true;
                            }
                            else if (entryMillis == startMillis && entrySeq > startSeq)
                            {
                                isGreater = true;
                            }
                            
                            if (isGreater)
                            {
                                matchingEntries.Add(entry);
                            }
                        }
                        
                        if (matchingEntries.Count > 0)
                        {
                            results.Add((streamKey, matchingEntries));
                        }
                    }
                }
                
                if (results.Count > 0)
                {
                    reader.TaskCompletionSource.TrySetResult(results);
                }
            }
        }
    }
}

/* Load RDB file and populate dataStore */
void LoadRdbFile(string filePath)
{
    if (!File.Exists(filePath))
    {
        Console.WriteLine($"[RDB] File not found: {filePath}");
        return;
    }
    
    try
    {
        byte[] fileBytes = File.ReadAllBytes(filePath);
        int offset = 0;
        
        string magic = Encoding.ASCII.GetString(fileBytes, offset, 5);
        offset += 5;
        
        if (magic != "REDIS")
        {
            Console.WriteLine("[RDB] Invalid RDB file format");
            return;
        }
        
        string version = Encoding.ASCII.GetString(fileBytes, offset, 4);
        offset += 4;
        Console.WriteLine($"[RDB] Loading RDB version {version}");
        
        while (offset < fileBytes.Length)
        {
            byte opcode = fileBytes[offset];
            offset++;
            
            if (opcode == 0xFF)
            {
                // End of RDB file
                Console.WriteLine("[RDB] Reached end of file");
                break;
            }
            else if (opcode == 0xFE)
            {
                // Database selector
                var (dbNumber, bytesRead) = ReadLength(fileBytes, offset);
                offset += bytesRead;
                Console.WriteLine($"[RDB] Selecting database {dbNumber}");
            }
            else if (opcode == 0xFD)
            {
                // Expiry time in seconds
                uint expirySeconds = BitConverter.ToUInt32(fileBytes, offset);
                offset += 4;
                long expiryMs = expirySeconds * 1000L;
                
                // Read value type and key-value pair
                byte valueType = fileBytes[offset];
                offset++;
                
                var (key, keyBytesRead) = ReadString(fileBytes, offset);
                offset += keyBytesRead;
                
                var (value, valueBytesRead) = ReadString(fileBytes, offset);
                offset += valueBytesRead;
                
                dataStore[key] = new StoredValue(value, expiryMs);
                Console.WriteLine($"[RDB] Loaded key '{key}' with expiry {expiryMs}ms");
            }
            else if (opcode == 0xFC)
            {
                // Expiry time in milliseconds
                ulong expiryMs = BitConverter.ToUInt64(fileBytes, offset);
                offset += 8;
                
                // Read value type and key-value pair
                byte valueType = fileBytes[offset];
                offset++;
                
                var (key, keyBytesRead) = ReadString(fileBytes, offset);
                offset += keyBytesRead;
                
                var (value, valueBytesRead) = ReadString(fileBytes, offset);
                offset += valueBytesRead;
                
                dataStore[key] = new StoredValue(value, (long)expiryMs);
                Console.WriteLine($"[RDB] Loaded key '{key}' with expiry {expiryMs}ms");
            }
            else if (opcode == 0xFB)
            {
                // Resizedb - hash table size information
                var (dbHashTableSize, bytesRead1) = ReadLength(fileBytes, offset);
                offset += bytesRead1;
                var (expiryHashTableSize, bytesRead2) = ReadLength(fileBytes, offset);
                offset += bytesRead2;
                Console.WriteLine($"[RDB] Resize DB: hash table size={dbHashTableSize}, expiry hash table size={expiryHashTableSize}");
            }
            else if (opcode == 0xFA)
            {
                // Auxiliary field
                var (auxKey, keyBytesRead) = ReadString(fileBytes, offset);
                offset += keyBytesRead;
                var (auxValue, valueBytesRead) = ReadString(fileBytes, offset);
                offset += valueBytesRead;
                Console.WriteLine($"[RDB] Auxiliary field: {auxKey}={auxValue}");
            }
            else
            {
                // Value type - this is a key-value pair without expiry
                byte valueType = opcode;
                
                var (key, keyBytesRead) = ReadString(fileBytes, offset);
                offset += keyBytesRead;
                
                if (valueType == 0)
                {
                    // String encoding
                    var (value, valueBytesRead) = ReadString(fileBytes, offset);
                    offset += valueBytesRead;
                    
                    dataStore[key] = new StoredValue(value);
                    Console.WriteLine($"[RDB] Loaded key '{key}' = '{value}'");
                }
                else
                {
                    Console.WriteLine($"[RDB] Unsupported value type: {valueType}");
                    break;
                }
            }
        }
        
        Console.WriteLine($"[RDB] Loaded {dataStore.Count} keys from RDB file");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[RDB] Error loading RDB file: {ex.Message}");
    }
}

/* Read length-encoded value from RDB file */
(int length, int bytesRead) ReadLength(byte[] data, int offset)
{
    byte firstByte = data[offset];
    int type = (firstByte & 0xC0) >> 6;
    
    if (type == 0)
    {
        // 6-bit length
        return (firstByte & 0x3F, 1);
    }
    else if (type == 1)
    {
        // 14-bit length
        int length = ((firstByte & 0x3F) << 8) | data[offset + 1];
        return (length, 2);
    }
    else if (type == 2)
    {
        // 32-bit length (big-endian)
        int length = (data[offset + 1] << 24) | (data[offset + 2] << 16) | 
                     (data[offset + 3] << 8) | data[offset + 4];
        return (length, 5);
    }
    else
    {
        // Special encoding
        return (firstByte & 0x3F, 1);
    }
}

/* Read length-prefixed string from RDB file */
(string str, int bytesRead) ReadString(byte[] data, int offset)
{
    var (length, lengthBytes) = ReadLength(data, offset);
    int totalBytesRead = lengthBytes;
    
    // Check for special encoding
    byte firstByte = data[offset];
    int type = (firstByte & 0xC0) >> 6;
    
    if (type == 3)
    {
        // Special encoding - handle integer encoding
        int encodingType = firstByte & 0x3F;
        if (encodingType == 0)
        {
            // 8-bit integer
            int value = (sbyte)data[offset + 1];
            return (value.ToString(), 2);
        }
        else if (encodingType == 1)
        {
            // 16-bit integer (little-endian)
            short value = BitConverter.ToInt16(data, offset + 1);
            return (value.ToString(), 3);
        }
        else if (encodingType == 2)
        {
            // 32-bit integer (little-endian)
            int value = BitConverter.ToInt32(data, offset + 1);
            return (value.ToString(), 5);
        }
    }
    
    string str = Encoding.UTF8.GetString(data, offset + lengthBytes, length);
    totalBytesRead += length;
    return (str, totalBytesRead);
}

/* Encode latitude/longitude into a 52-bit Redis geohash integer score.
 * Longitude bits occupy even positions (0,2,4,...) and latitude bits
 * occupy odd positions (1,3,5,...). */
long EncodeGeoHash(double longitude, double latitude)
{
    // Normalise to [0, 1]
    double normLon = (longitude + 180.0) / 360.0;
    double normLat = (latitude + 85.05112878) / 170.10225756;

    // Scale to 26-bit integers
    long lonBits = (long)(normLon * (1L << 26));
    long latBits = (long)(normLat * (1L << 26));

    // Clamp to [0, 2^26 - 1]
    lonBits = Math.Max(0, Math.Min((1L << 26) - 1, lonBits));
    latBits = Math.Max(0, Math.Min((1L << 26) - 1, latBits));

    // Redis interleave64(lat, lon): latitude occupies even bit positions (0,2,4,...),
    // longitude occupies odd bit positions (1,3,5,...).
    long result = 0;
    for (int i = 0; i < 26; i++)
    {
        result |= ((latBits >> i) & 1L) << (2 * i);
        result |= ((lonBits >> i) & 1L) << (2 * i + 1);
    }

    return result;
}

/* Calculate distance in meters between two lat/lon points using Haversine formula.
 * Earth radius matches Redis: 6372797.560856 meters. */
double GeoDistMeters(double lat1Deg, double lon1Deg, double lat2Deg, double lon2Deg)
{
    const double earthRadius = 6372797.560856;
    double lat1 = lat1Deg * Math.PI / 180.0;
    double lat2 = lat2Deg * Math.PI / 180.0;
    double dLat = (lat2Deg - lat1Deg) * Math.PI / 180.0;
    double dLon = (lon2Deg - lon1Deg) * Math.PI / 180.0;
    double a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2)
             + Math.Cos(lat1) * Math.Cos(lat2)
             * Math.Sin(dLon / 2) * Math.Sin(dLon / 2);
    double c = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
    return earthRadius * c;
}

/* Decode a Redis geohash score back to (longitude, latitude). */
(double lon, double lat) DecodeGeoHash(long score)
{
    long latBits = 0;
    long lonBits = 0;
    for (int i = 0; i < 26; i++)
    {
        latBits |= ((score >> (2 * i)) & 1L) << i;
        lonBits |= ((score >> (2 * i + 1)) & 1L) << i;
    }
    double lon = (lonBits + 0.5) / (double)(1L << 26) * 360.0 - 180.0;
    double lat = (latBits + 0.5) / (double)(1L << 26) * 170.10225756 - 85.05112878;
    return (lon, lat);
}

/* Blocked client waiting for an element from a list */
record BlockedClient(string Key, TaskCompletionSource<string?> TaskCompletionSource);

/* Blocked stream reader waiting for new entries */
record BlockedStreamReader(string[] Keys, string[] Ids, TaskCompletionSource<List<(string key, List<StreamEntry> entries)>?> TaskCompletionSource);

/* Stream entry with ID and key-value pairs */
record StreamEntry(string Id, Dictionary<string, string> Fields);

/* Sorted set entry with member and score */
record SortedSetEntry(string Member, double Score) : IComparable<SortedSetEntry>
{
    public int CompareTo(SortedSetEntry? other)
    {
        if (other == null) return 1;
        
        // First compare by score
        int scoreComparison = Score.CompareTo(other.Score);
        if (scoreComparison != 0) return scoreComparison;
        
        // If scores are equal, compare by member lexicographically
        return string.Compare(Member, other.Member, StringComparison.Ordinal);
    }
}

/* Store value and expiry time */
record StoredValue
{
    public string? Value { get; init; }
    public List<string>? List { get; init; }
    public List<StreamEntry>? Stream { get; init; }
    public List<SortedSetEntry>? SortedSet { get; init; }
    public long? ExpiryMs { get; init; }
    
    public StoredValue(string value, long? expiryMs = null)
    {
        Value = value;
        ExpiryMs = expiryMs;
    }
    
    public StoredValue(List<string> list, long? expiryMs = null)
    {
        List = list;
        ExpiryMs = expiryMs;
    }
    
    public StoredValue(List<StreamEntry> stream, long? expiryMs = null)
    {
        Stream = stream;
        ExpiryMs = expiryMs;
    }
    
    public StoredValue(List<SortedSetEntry> sortedSet, long? expiryMs = null)
    {
        SortedSet = sortedSet;
        ExpiryMs = expiryMs;
    }
}
