/**
 * Redis - A simple Redis server implementation in C#
 * From CodeCrafters.io build-your-own-redis (C#)
 */

using System.Net;
using System.Net.Sockets;
using System.Text;

TcpListener server = new TcpListener(IPAddress.Any, 6379);
server.Start();

while (true)
{
    Socket client = server.AcceptSocket(); // wait for client
    
    // Read incoming 
    byte[] buffer = new byte[1024];
    client.Receive(buffer);
    
    // Send PONG response
    string response = "+PONG\r\n";
    byte[] responseBytes = Encoding.UTF8.GetBytes(response);
    client.Send(responseBytes);
    
    client.Close();
}
