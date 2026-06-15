using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace RedisServer.Tests;

/// <summary>
/// Starts the codecrafters-redis server binary on a free port, provides a raw TCP
/// helper for sending RESP commands and reading responses, and cleans up on dispose.
/// </summary>
sealed class RedisServerFixture : IAsyncDisposable
{
    private readonly Process _process;
    private readonly string _dataDir;
    /// <summary>True when this fixture created the data directory and must delete it on dispose.</summary>
    private readonly bool _ownsDataDir;

    public int Port { get; }

    private RedisServerFixture(Process process, int port, string dataDir, bool ownsDataDir)
    {
        _process = process;
        Port = port;
        _dataDir = dataDir;
        _ownsDataDir = ownsDataDir;
    }

    /// <summary>
    /// Builds (if necessary) and starts a server instance on a randomly chosen free
    /// TCP port.  When <paramref name="appendonly"/> is <c>true</c> the server uses AOF
    /// persistence inside <paramref name="dataDir"/> (created when <c>null</c>).
    /// The fixture only deletes the data directory on dispose when it created it (i.e.
    /// when <paramref name="dataDir"/> was <c>null</c>), so callers that share a
    /// directory across two server instances retain control of its lifetime.
    /// </summary>
    public static async Task<RedisServerFixture> StartAsync(string? dataDir = null, bool appendonly = false)
    {
        // The test assembly lives at tests/bin/Debug/net9.0/ — walk 4 levels up to the
        // repo root that contains codecrafters-redis.csproj.
        string testBin = AppContext.BaseDirectory;
        string repoRoot = Path.GetFullPath(Path.Combine(testBin, "..", "..", "..", ".."));
        string serverProject = Path.Combine(repoRoot, "codecrafters-redis.csproj");

        // Build the server once per test run so the suite works under a plain
        // `dotnet test` invocation. The test project deliberately has no
        // ProjectReference to the server (it is launched as a subprocess), so nothing
        // else builds it; per-test starts below then use --no-build for fast startup.
        EnsureServerBuilt(repoRoot, serverProject);

        int port = GetFreePort();
        bool ownsDir = dataDir == null;
        string effectiveDataDir = dataDir ?? Path.Combine(Path.GetTempPath(), $"redis-test-{Guid.NewGuid()}");
        Directory.CreateDirectory(effectiveDataDir);

        var argParts = new List<string>
        {
            "run", "--project", $"\"{serverProject}\"", "--no-build", "--",
            "--port", port.ToString(),
            "--dir", $"\"{effectiveDataDir}\""
        };
        if (appendonly)
            argParts.AddRange(new[] { "--appendonly", "yes", "--appendfsync", "always" });

        var psi = new ProcessStartInfo("dotnet", string.Join(" ", argParts))
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            WorkingDirectory = repoRoot
        };
        var proc = new Process { StartInfo = psi };
        proc.Start();

        // Wait until the server is accepting connections (up to 8 s).
        var deadline = DateTime.UtcNow.AddSeconds(8);
        bool ready = false;
        while (DateTime.UtcNow < deadline)
        {
            try
            {
                using var probe = new TcpClient();
                await probe.ConnectAsync(IPAddress.Loopback, port);
                ready = true;
                break;
            }
            catch
            {
                await Task.Delay(80);
            }
        }

        if (!ready)
            throw new InvalidOperationException(
                $"Server did not start within 8 seconds on port {port}.\n" +
                $"Stderr: {proc.StandardError.ReadToEnd()}");

        return new RedisServerFixture(proc, port, effectiveDataDir, ownsDir);
    }

    private static readonly object _buildLock = new();
    private static bool _serverBuilt;

    /// <summary>
    /// Builds the server project a single time across the whole test run. Guarded by a
    /// lock so concurrent fixtures cannot trigger overlapping builds.
    /// </summary>
    private static void EnsureServerBuilt(string repoRoot, string serverProject)
    {
        lock (_buildLock)
        {
            if (_serverBuilt) return;

            using var build = Process.Start(new ProcessStartInfo(
                "dotnet", $"build \"{serverProject}\" -c Debug --nologo")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                WorkingDirectory = repoRoot
            })!;

            // Drain both pipes concurrently *before* waiting for exit: a build that
            // produces enough output to fill a redirected pipe buffer would otherwise
            // deadlock against WaitForExit.
            Task<string> stdoutTask = build.StandardOutput.ReadToEndAsync();
            Task<string> stderrTask = build.StandardError.ReadToEndAsync();
            build.WaitForExit();
            if (build.ExitCode != 0)
                throw new InvalidOperationException(
                    "Failed to build the redis server for tests:\n" +
                    stdoutTask.GetAwaiter().GetResult() + stderrTask.GetAwaiter().GetResult());

            _serverBuilt = true;
        }
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    public async ValueTask DisposeAsync()
    {
        try { _process.Kill(entireProcessTree: true); } catch { }
        try { await _process.WaitForExitAsync(); } catch { }
        _process.Dispose();
        // Only delete the directory when this fixture created it — callers that share
        // a dataDir across two fixture instances manage the lifetime themselves.
        if (_ownsDataDir)
            try { Directory.Delete(_dataDir, recursive: true); } catch { }
    }
}


/// <summary>
/// A minimal RESP client used in tests — sends raw RESP commands and reads the full
/// response line(s) from the server.
/// </summary>
sealed class RedisClient : IAsyncDisposable
{
    private readonly TcpClient _tcp;
    private readonly NetworkStream _stream;
    private readonly byte[] _readBuf = new byte[65536];

    private RedisClient(TcpClient tcp)
    {
        _tcp = tcp;
        _stream = tcp.GetStream();
    }

    public static async Task<RedisClient> ConnectAsync(int port)
    {
        var tcp = new TcpClient();
        await tcp.ConnectAsync(IPAddress.Loopback, port);
        return new RedisClient(tcp);
    }

    /// <summary>Sends a RESP array command and returns the raw response string.</summary>
    public async Task<string> SendCommandAsync(params string[] args)
    {
        var sb = new StringBuilder();
        sb.Append($"*{args.Length}\r\n");
        foreach (var a in args)
            sb.Append($"${Encoding.UTF8.GetByteCount(a)}\r\n{a}\r\n");
        await _stream.WriteAsync(Encoding.UTF8.GetBytes(sb.ToString()));
        await _stream.FlushAsync();
        return await ReadResponseAsync();
    }

    /// <summary>Sends a raw RESP string directly (useful for pipelining / blocking tests).</summary>
    public async Task SendRawAsync(string raw)
    {
        await _stream.WriteAsync(Encoding.UTF8.GetBytes(raw));
        await _stream.FlushAsync();
    }

    public async Task<string> ReadResponseAsync()
    {
        // Read until we have at least one complete RESP response. NetworkStream.ReadTimeout
        // does not apply to ReadAsync, so guard each read with a CancellationToken timeout
        // to avoid hanging indefinitely on a stalled server.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(6));
        int total = 0;
        do
        {
            int n = await _stream.ReadAsync(_readBuf.AsMemory(total), cts.Token);
            if (n == 0) break;
            total += n;
        }
        while (_stream.DataAvailable);

        return Encoding.UTF8.GetString(_readBuf, 0, total);
    }

    public async ValueTask DisposeAsync()
    {
        _stream.Dispose();
        _tcp.Dispose();
        await Task.CompletedTask;
    }
}

