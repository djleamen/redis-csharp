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

TcpListener server = new TcpListener(IPAddress.Any, 6379);
server.Start();

while (true)
{
    Socket client = server.AcceptSocket();
    Task.Run(() => HandleClient(client));
}

void HandleClient(Socket client)
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
            string response;
            
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
                
                if (!dataStore.ContainsKey(key))
                {
                    // Create new list with all elements
                    var list = new List<string>(elements);
                    dataStore[key] = new StoredValue(list);
                    response = $":{list.Count}\r\n";
                }
                else
                {
                    if (dataStore.TryGetValue(key, out StoredValue? storedValue) && storedValue.List != null)
                    {
                        storedValue.List.AddRange(elements);
                        int count = storedValue.List.Count;
                        response = $":{count}\r\n";
                    }
                    else
                    {
                        response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                    }
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
            else
            {
                response = "-ERR unknown command\r\n";
            }
            
            byte[] responseBytes = Encoding.UTF8.GetBytes(response);
            client.Send(responseBytes);
        }
        catch
        {
            break;
        }
    }
    
    client.Close();
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

// Store value and expiry time
record StoredValue
{
    public string? Value { get; init; }
    public List<string>? List { get; init; }
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
}
