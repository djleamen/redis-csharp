using System.Net.Sockets;
using System.Text;

partial class RedisServer
{
    /// <summary>
    /// Publishes <paramref name="message"/> to all subscribers of <paramref name="channel"/>
    /// and returns the number of clients that received it.
    /// </summary>
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
}
