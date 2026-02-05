/**
 * Redis - A simple Redis server implementation in C#
 * From CodeCrafters.io build-your-own-redis (C#)
 */

using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

int port = 6379; // Default port
string? masterHost = null;
int? masterPort = null;

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
}

bool isReplica = masterHost != null && masterPort.HasValue;

const string replicationId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
const int replicationOffset = 0;

var dataStore = new ConcurrentDictionary<string, StoredValue>();

var blockedClients = new ConcurrentDictionary<string, Queue<BlockedClient>>();
var blockedClientsLock = new object();

var blockedStreamReaders = new ConcurrentDictionary<string, Queue<BlockedStreamReader>>();
var blockedStreamReadersLock = new object();

var replicaConnections = new List<Socket>();
var replicaConnectionsLock = new object();

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
        byte[] buffer = new byte[1024];
        
        string pingCommand = "*1\r\n$4\r\nPING\r\n";
        byte[] pingBytes = Encoding.UTF8.GetBytes(pingCommand);
        await stream.WriteAsync(pingBytes, 0, pingBytes.Length);

        int bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
        string response = Encoding.UTF8.GetString(buffer, 0, bytesRead);
        
        string portStr = replicaPort.ToString();
        string replconfPort = $"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${portStr.Length}\r\n{portStr}\r\n";
        byte[] replconfPortBytes = Encoding.UTF8.GetBytes(replconfPort);
        await stream.WriteAsync(replconfPortBytes, 0, replconfPortBytes.Length);
        
        bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
        response = Encoding.UTF8.GetString(buffer, 0, bytesRead);
        
        string replconfCapa = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
        byte[] replconfCapaBytes = Encoding.UTF8.GetBytes(replconfCapa);
        await stream.WriteAsync(replconfCapaBytes, 0, replconfCapaBytes.Length);
        
        bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
        response = Encoding.UTF8.GetString(buffer, 0, bytesRead);
        
        string psyncCommand = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        byte[] psyncBytes = Encoding.UTF8.GetBytes(psyncCommand);
        await stream.WriteAsync(psyncBytes, 0, psyncBytes.Length);
        
        bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length);
        response = Encoding.UTF8.GetString(buffer, 0, bytesRead);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error connecting to master: {ex.Message}");
    }
}

/* Handle client connection */
async Task HandleClient(Socket client)
{
    bool inTransaction = false;
    var transactionQueue = new List<string[]>();
    bool isReplicationConnection = false;
    
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
                response = "+PONG\r\n";
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
            // REPLCONF - Replication configuration (used during handshake)
            else if (command == "REPLCONF")
            {
                response = "+OK\r\n";
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
                }
                isReplicationConnection = true;
                response = string.Empty;
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

/* Blocked client waiting for an element from a list */
record BlockedClient(string Key, TaskCompletionSource<string?> TaskCompletionSource);

/* Blocked stream reader waiting for new entries */
record BlockedStreamReader(string[] Keys, string[] Ids, TaskCompletionSource<List<(string key, List<StreamEntry> entries)>?> TaskCompletionSource);

/* Stream entry with ID and key-value pairs */
record StreamEntry(string Id, Dictionary<string, string> Fields);

/* Store value and expiry time */
record StoredValue
{
    public string? Value { get; init; }
    public List<string>? List { get; init; }
    public List<StreamEntry>? Stream { get; init; }
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
}
