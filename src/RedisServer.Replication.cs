using System.Net.Sockets;
using System.Text;

namespace codecrafters_redis;

partial class RedisServer
{
    /// <summary>
    /// Blocks until <paramref name="numReplicasStr"/> replicas have acknowledged all
    /// preceding write commands, or until <paramref name="timeoutStr"/> milliseconds elapse.
    /// </summary>
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

    /// <summary>
    /// Connects to the master server, performs the replication handshake (PING, REPLCONF, PSYNC),
    /// receives the initial RDB snapshot, and then continuously processes propagated commands.
    /// </summary>
    private async Task ConnectToMasterAsync(string host, int masterPort, int replicaPort)
    {
        try
        {
            using var masterClient = new TcpClient();
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
        catch
        {
            // Connection to master failed or was lost; replica will remain disconnected.
        }
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
}
