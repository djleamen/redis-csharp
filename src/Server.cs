/**
 * Redis - A simple Redis server implementation in C#
 * From CodeCrafters.io build-your-own-redis (C#)
 */

using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

// Thread-safe data store
var dataStore = new ConcurrentDictionary<string, StoredValue>();

// Track blocked clients per key
var blockedClients = new ConcurrentDictionary<string, Queue<BlockedClient>>();
var blockedClientsLock = new object();

TcpListener server = new TcpListener(IPAddress.Any, 6379);
server.Start();

while (true)
{
    Socket client = server.AcceptSocket();
    Task.Run(() => HandleClient(client));
}

async Task HandleClient(Socket client)
{
    while (true)
    {
        try
        {
            // Read incoming command
            byte[] buffer = new byte[1024];
            int bytesRead = client.Receive(buffer);
            
            // Client disconnected
            if (bytesRead == 0)
                break;
            
            // Parse RESP command
            string input = Encoding.UTF8.GetString(buffer, 0, bytesRead);
            string[] parts = ParseRespArray(input);
            if (parts.Length == 0)
                continue;
            
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
                    // Check if key has expired
                    if (storedValue.ExpiryMs.HasValue && 
                        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() > storedValue.ExpiryMs.Value)
                    {
                        // Key expired, remove it and return null
                        dataStore.TryRemove(key, out _);
                        response = "$-1\r\n";
                    }
                    else
                    {
                        response = $"${storedValue.Value.Length}\r\n{storedValue.Value}\r\n";
                    }
                }
                else
                {
                    response = "$-1\r\n"; // Null bulk string
                }
            }
            // RPUSH - Append elements to a list
            else if (command == "RPUSH" && parts.Length >= 3)
            {
                string key = parts[1];
                // Get all elements to append (from index 2 onwards)
                var elements = parts.Skip(2).ToArray();
                bool shouldUnblock = false;
                
                if (!dataStore.ContainsKey(key))
                {
                    // Create new list with all elements
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
                
                // Send response first
                if (!string.IsNullOrEmpty(response))
                {
                    byte[] responseBytes = Encoding.UTF8.GetBytes(response);
                    client.Send(responseBytes);
                    response = string.Empty; // Mark as sent
                }
                
                // Then check if there are blocked clients waiting for this key
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
                
                // Send response first
                if (!string.IsNullOrEmpty(response))
                {
                    byte[] responseBytes = Encoding.UTF8.GetBytes(response);
                    client.Send(responseBytes);
                    response = string.Empty; // Mark as sent
                }
                
                // Then check if there are blocked clients waiting for this key
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
                    // Key doesn't exist, return 0
                    response = ":0\r\n";
                }
                else if (storedValue.List == null)
                {
                    // Key exists but is not a list
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
                int count = 1; // Default to 1 element
                
                // Check if count parameter is provided
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
                    // Key doesn't exist, return null
                    response = "$-1\r\n";
                }
                else if (storedValue.List == null)
                {
                    // Key exists but is not a list
                    response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                }
                else if (storedValue.List.Count == 0)
                {
                    // List is empty
                    response = "$-1\r\n";
                }
                else
                {
                    // Determine how many elements to remove
                    int elementsToRemove = Math.Min(count, storedValue.List.Count);
                    
                    if (parts.Length >= 3)
                    {
                        // Return multiple elements as RESP array
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
                        // Return single element as bulk string
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
                    // List has elements, pop immediately
                    string element = storedValue.List[0];
                    storedValue.List.RemoveAt(0);
                    
                    // Return array: [key, element]
                    var sb = new StringBuilder();
                    sb.Append("*2\r\n");
                    sb.Append($"${key.Length}\r\n{key}\r\n");
                    sb.Append($"${element.Length}\r\n{element}\r\n");
                    response = sb.ToString();
                }
                else
                {
                    // List is empty or doesn't exist, block
                    var tcs = new TaskCompletionSource<string?>();
                    
                    lock (blockedClientsLock)
                    {
                        if (!blockedClients.ContainsKey(key))
                        {
                            blockedClients[key] = new Queue<BlockedClient>();
                        }
                        blockedClients[key].Enqueue(new BlockedClient(key, tcs));
                    }
                    
                    // Wait for element to become available or timeout
                    Task<string?> elementTask = tcs.Task;
                    Task completedTask;
                    
                    if (timeout > 0)
                    {
                        // Wait with timeout
                        int timeoutMs = (int)(timeout * 1000);
                        Task delayTask = Task.Delay(timeoutMs);
                        completedTask = await Task.WhenAny(elementTask, delayTask);
                    }
                    else
                    {
                        // Wait indefinitely (timeout = 0)
                        await elementTask;
                        completedTask = elementTask;
                    }
                    
                    string? poppedElement = null;
                    
                    if (completedTask == elementTask && elementTask.IsCompletedSuccessfully)
                    {
                        // Element became available
                        poppedElement = elementTask.Result;
                    }
                    else
                    {
                        // Timeout reached - need to remove this client from the blocked queue
                        lock (blockedClientsLock)
                        {
                            if (blockedClients.TryGetValue(key, out Queue<BlockedClient>? queue))
                            {
                                // Remove this client from the queue if it's still there
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
                        
                        // Set the TCS to cancelled state if not already completed
                        tcs.TrySetResult(null);
                    }
                    
                    if (poppedElement != null)
                    {
                        // Return array: [key, element]
                        var sb = new StringBuilder();
                        sb.Append("*2\r\n");
                        sb.Append($"${key.Length}\r\n{key}\r\n");
                        sb.Append($"${poppedElement.Length}\r\n{poppedElement}\r\n");
                        response = sb.ToString();
                    }
                    else
                    {
                        // Timeout reached
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
                    // Key doesn't exist
                    response = "+none\r\n";
                }
                else if (storedValue.Value != null)
                {
                    // Check if expired
                    if (storedValue.ExpiryMs.HasValue && 
                        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() > storedValue.ExpiryMs.Value)
                    {
                        // Key expired, treat as non-existent
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
                    // Unknown type
                    response = "+none\r\n";
                }
            }
            // XADD - Add entry to a stream
            else if (command == "XADD" && parts.Length >= 4)
            {
                string key = parts[1];
                string entryId = parts[2];
                
                // Parse key-value pairs (must be even number of arguments after entry ID)
                int fieldCount = parts.Length - 3;
                if (fieldCount % 2 != 0)
                {
                    response = "-ERR wrong number of arguments for XADD\r\n";
                }
                else
                {
                    var fields = new Dictionary<string, string>();
                    for (int i = 3; i < parts.Length; i += 2)
                    {
                        fields[parts[i]] = parts[i + 1];
                    }
                    
                    var entry = new StreamEntry(entryId, fields);
                    
                    if (!dataStore.ContainsKey(key))
                    {
                        // Create new stream
                        var stream = new List<StreamEntry> { entry };
                        dataStore[key] = new StoredValue(stream);
                        response = $"${entryId.Length}\r\n{entryId}\r\n";
                    }
                    else
                    {
                        if (dataStore.TryGetValue(key, out StoredValue? storedValue) && storedValue.Stream != null)
                        {
                            // Append to existing stream
                            storedValue.Stream.Add(entry);
                            response = $"${entryId.Length}\r\n{entryId}\r\n";
                        }
                        else
                        {
                            response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                        }
                    }
                }
            }
            else
            {
                response = "-ERR unknown command\r\n";
            }
            
            // Send response if we have one
            if (!string.IsNullOrEmpty(response))
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

void UnblockWaitingClients(string key)
{
    lock (blockedClientsLock)
    {
        // Keep unblocking clients while there are both blocked clients and elements
        while (blockedClients.TryGetValue(key, out Queue<BlockedClient>? queue) && queue.Count > 0)
        {
            // Check if there are elements in the list
            if (dataStore.TryGetValue(key, out StoredValue? storedValue) && storedValue.List != null && storedValue.List.Count > 0)
            {
                var blockedClient = queue.Dequeue();
                string element = storedValue.List[0];
                storedValue.List.RemoveAt(0);
                
                // Unblock the client by completing the TaskCompletionSource
                blockedClient.TaskCompletionSource.SetResult(element);
                
                // Clean up empty queue
                if (queue.Count == 0)
                {
                    blockedClients.TryRemove(key, out _);
                }
            }
            else
            {
                // No more elements, stop unblocking
                break;
            }
        }
    }
}

// Simple RESP array parser
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
            i++; // Skip the length line
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

// Blocked client waiting for list element
record BlockedClient(string Key, TaskCompletionSource<string?> TaskCompletionSource);

// Stream entry with ID and key-value pairs
record StreamEntry(string Id, Dictionary<string, string> Fields);

// Store value and expiry time
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
