/**
 * Redis - A simple Redis server implementation in C#
 * From CodeCrafters.io build-your-own-redis (C#)
 *
 * Entry point: parses command-line arguments and starts the server.
 * All server logic lives in RedisServer.cs; data models in Models.cs;
 * protocol parsing in RespParser.cs; RDB loading in RdbLoader.cs;
 * and geo utilities in GeoUtils.cs.
 */

using codecrafters_redis;

int port = 6379;
string? masterHost = null;
int? masterPort = null;
string dir = Directory.GetCurrentDirectory();
string dbfilename = "dump.rdb";
string appendonly = "no";
string appenddirname = "appendonlydir";
string appendfilename = "appendonly.aof";
string appendfsync = "everysec";

for (int i = 0; i < args.Length - 1; i++)
{
    string val = args[i + 1];
    switch (args[i])
    {
        case "--port" when int.TryParse(val, out int p): port = p; break;
        case "--replicaof": (masterHost, masterPort) = ParseReplicaOf(val); break;
        case "--dir": dir = val; break;
        case "--dbfilename": dbfilename = val; break;
        case "--appendonly": appendonly = val; break;
        case "--appenddirname": appenddirname = val; break;
        case "--appendfilename": appendfilename = val; break;
        case "--appendfsync": appendfsync = val; break;
    }
}

static (string? host, int? port) ParseReplicaOf(string value)
{
    string[] parts = value.Split(' ', StringSplitOptions.RemoveEmptyEntries);
    if (parts.Length == 2 && int.TryParse(parts[1], out int mp))
        return (parts[0], mp);
    return (null, null);
}

var server = new RedisServer(port, dir, dbfilename, masterHost, masterPort,
    new RedisServerOptions(appendonly, appenddirname, appendfilename, appendfsync));
await server.RunAsync();
