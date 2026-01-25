/**
 * Redis - A simple Redis server implementation in C#
 * From CodeCrafters.io build-your-own-redis (C#)
 */

using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

// Thread-safe data store
var dataStore = new ConcurrentDictionary<string, string>();

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
                dataStore[key] = value;
                response = "+OK\r\n";
            }
            else if (command == "GET" && parts.Length > 1)
            {
                string key = parts[1];
                if (dataStore.TryGetValue(key, out string? value))
                {
                    response = $"${value.Length}\r\n{value}\r\n";
                }
                else
                {
                    response = "$-1\r\n"; // Null bulk string
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
