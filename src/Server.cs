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
    Socket client = server.AcceptSocket(); // wait for client
    
    Task.Run(() => HandleClient(client));
}

void HandleClient(Socket client)
{
    // Handle multiple commands on the same connection
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
                
                // Parse options (PX, EX)
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
                string element = parts[2];
                
                if (!dataStore.ContainsKey(key))
                {
                    var list = new List<string> { element };
                    dataStore[key] = new StoredValue(list);
                    response = ":1\r\n";
                }
                else
                {
                    response = "-ERR list operations not fully implemented\r\n";
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
