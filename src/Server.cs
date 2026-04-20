/**
 * Redis - A simple Redis server implementation in C#
 * From CodeCrafters.io build-your-own-redis (C#)
 *
 * Entry point: parses command-line arguments and starts the server.
 * All server logic lives in RedisServer.cs; data models in Models.cs;
 * protocol parsing in RespParser.cs; RDB loading in RdbLoader.cs;
 * and geo utilities in GeoUtils.cs.
 */

int port = 6379;
string? masterHost = null;
int? masterPort = null;
string dir = Directory.GetCurrentDirectory();
string dbfilename = "dump.rdb";

for (int i = 0; i < args.Length; i++)
{
    if (args[i] == "--port" && i + 1 < args.Length && int.TryParse(args[i + 1], out int p))
    {
        port = p;
    }
    else if (args[i] == "--replicaof" && i + 1 < args.Length)
    {
        string[] replicaParts = args[i + 1].Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (replicaParts.Length == 2 && int.TryParse(replicaParts[1], out int mp))
        {
            masterHost = replicaParts[0];
            masterPort = mp;
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

var server = new RedisServer(port, dir, dbfilename, masterHost, masterPort);
await server.RunAsync();
