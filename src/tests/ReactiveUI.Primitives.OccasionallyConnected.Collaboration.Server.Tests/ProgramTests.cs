// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.DependencyInjection;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests;

/// <summary>Tests for the collaboration server runner and executable entry point.</summary>
public sealed class ProgramTests
{
    /// <summary>The test bearer token accepted by the development credential store.</summary>
    private const string Token = "process-token";

    /// <summary>The test tenant identifier accepted by the development credential store.</summary>
    private const string Tenant = "process-tenant";

    /// <summary>The test client identifier accepted by the development credential store.</summary>
    private const string Client = "process-client";

    /// <summary>The mounted route used to verify command-line path base startup.</summary>
    private const string PathBase = "process-main";

    /// <summary>The expected plain text health response.</summary>
    private const string HealthBody = "ok";

    /// <summary>The executable assembly name produced by the example project.</summary>
    private const string ServerAssemblyFileName = "ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.dll";

    /// <summary>The readiness timeout in seconds for the host startup path.</summary>
    private const int ReadyTimeoutSeconds = 10;

    /// <summary>The cleanup timeout in seconds for the host shutdown path.</summary>
    private const int StopTimeoutSeconds = 5;

    /// <summary>The readiness poll interval in milliseconds.</summary>
    private const int ReadyPollMilliseconds = 50;

    /// <summary>Verifies the public runner owns startup, cancellation shutdown and journal disposal.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RunAsyncStopsHostAndReleasesJournalWhenCancellationIsRequested()
    {
        using var lease = new ProcessDatabaseLease();
        using var cancellation = new CancellationTokenSource();
        var address = $"http://127.0.0.1:{GetAvailableLoopbackPort()}";
        var options = CreateOptions(address, lease.Path);
        var runTask = CollaborationServerExample.RunAsync(options, cancellation.Token);
        try
        {
            var body = await WaitForRunnerHealthAsync(runTask, CreateMountedAddress(address), cancellation.Token).ConfigureAwait(false);

            await Assert.That(body).IsEqualTo(HealthBody);
            await Assert.That(File.Exists(lease.Path)).IsTrue();
        }
        finally
        {
            await cancellation.CancelAsync().ConfigureAwait(false);
            await AwaitRunnerCompletionAsync(runTask).ConfigureAwait(false);
        }

        await using var journal = new FileStream(lease.Path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        await Assert.That(journal.CanWrite).IsTrue();
    }

    /// <summary>Verifies Program.Main starts the real ASP.NET/SQLite server from process arguments.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ProgramMainStartsHostProcessFromCommandLineArguments()
    {
        using var lease = new ProcessDatabaseLease();
        var address = $"http://127.0.0.1:{GetAvailableLoopbackPort()}";
        using var server = StartServerProcess(address, lease.Path);
        try
        {
            var body = await WaitForProcessHealthAsync(server, CreateMountedAddress(address), CancellationToken.None).ConfigureAwait(false);

            await Assert.That(body).IsEqualTo(HealthBody);
            await Assert.That(File.Exists(lease.Path)).IsTrue();
        }
        finally
        {
            await server.StopAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Creates command-line-equivalent runner options.</summary>
    /// <param name="address">The loopback address to bind.</param>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <returns>The configured server options.</returns>
    private static CollaborationServerOptions CreateOptions(string address, string databasePath) =>
        new() { ListenUri = new(address), DatabasePath = databasePath, PathBase = PathBase, Credentials = [new(Token, Tenant, Client)] };

    /// <summary>Starts the server executable with command-line options.</summary>
    /// <param name="address">The loopback address to bind.</param>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <returns>The started child process.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the server process cannot be started.</exception>
    private static CapturedServerProcess StartServerProcess(string address, string databasePath)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName = FindDotNetCliPath(),
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            WorkingDirectory = AppContext.BaseDirectory,
        };
        startInfo.ArgumentList.Add(FindServerAssemblyPath());
        startInfo.ArgumentList.Add("--url");
        startInfo.ArgumentList.Add(address);
        startInfo.ArgumentList.Add("--database");
        startInfo.ArgumentList.Add(databasePath);
        startInfo.ArgumentList.Add("--credentials");
        startInfo.ArgumentList.Add($"{Token}:{Tenant}:{Client}");
        startInfo.ArgumentList.Add("--path-base");
        startInfo.ArgumentList.Add(PathBase);

        startInfo.Environment["DOTNET_NOLOGO"] = "1";
        var process = Process.Start(startInfo)
            ?? throw new InvalidOperationException("The example server process did not start.");
        return new(process);
    }

    /// <summary>Waits until the public runner serves the mounted health endpoint.</summary>
    /// <param name="runTask">The active runner task.</param>
    /// <param name="address">The mounted loopback base address.</param>
    /// <param name="cancellationToken">The external cancellation token.</param>
    /// <returns>The health response body.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the runner exits before becoming healthy.</exception>
    /// <exception cref="OperationCanceledException">Thrown when the runner does not become healthy before the bounded timeout.</exception>
    private static async ValueTask<string> WaitForRunnerHealthAsync(
        Task runTask,
        string address,
        CancellationToken cancellationToken)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(ReadyTimeoutSeconds));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        await using var httpClients = CreateHttpClientServices();
        using var httpClient = CreateHttpClient(httpClients, address);
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(ReadyPollMilliseconds));
        while (!linked.Token.IsCancellationRequested)
        {
            if (runTask.IsCompleted)
            {
                await runTask.ConfigureAwait(false);
                throw new InvalidOperationException("The example server runner completed before it became healthy.");
            }

            var result = await TryReadHealthAsync(httpClient, linked.Token).ConfigureAwait(false);
            if (result is not null)
            {
                return result;
            }

            if (!await WaitForNextPollAsync(timer, linked.Token).ConfigureAwait(false))
            {
                break;
            }
        }

        throw new OperationCanceledException("The example server runner did not become healthy before the bounded readiness timeout.", linked.Token);
    }

    /// <summary>Waits until the child process serves the mounted health endpoint.</summary>
    /// <param name="server">The started child process.</param>
    /// <param name="address">The mounted loopback base address.</param>
    /// <param name="cancellationToken">The external cancellation token.</param>
    /// <returns>The health response body.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the server exits before becoming healthy.</exception>
    /// <exception cref="OperationCanceledException">Thrown when the process does not become healthy before the bounded timeout.</exception>
    private static async ValueTask<string> WaitForProcessHealthAsync(
        CapturedServerProcess server,
        string address,
        CancellationToken cancellationToken)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(ReadyTimeoutSeconds));
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        await using var httpClients = CreateHttpClientServices();
        using var httpClient = CreateHttpClient(httpClients, address);
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(ReadyPollMilliseconds));
        while (!linked.Token.IsCancellationRequested)
        {
            if (server.Process.HasExited)
            {
                throw new InvalidOperationException(await server.CreateExitMessageAsync().ConfigureAwait(false));
            }

            var result = await TryReadHealthAsync(httpClient, linked.Token).ConfigureAwait(false);
            if (result is not null)
            {
                return result;
            }

            if (!await WaitForNextPollAsync(timer, linked.Token).ConfigureAwait(false))
            {
                break;
            }
        }

        await server.StopAsync().ConfigureAwait(false);
        var output = await server.ReadOutputAsync().ConfigureAwait(false);
        throw new OperationCanceledException(
            $"The example server process did not become healthy before the bounded readiness timeout.{output}",
            linked.Token);
    }

    /// <summary>Creates the HTTP client provider used by readiness probes.</summary>
    /// <returns>The service provider containing the HTTP client factory.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ServiceProvider CreateHttpClientServices() =>
        new ServiceCollection().AddHttpClient(nameof(ProgramTests), static (_, _) => { }).Services.BuildServiceProvider();

    /// <summary>Creates a mounted HTTP client for readiness probes.</summary>
    /// <param name="services">The HTTP client provider.</param>
    /// <param name="address">The mounted loopback base address.</param>
    /// <returns>The configured HTTP client.</returns>
    private static HttpClient CreateHttpClient(IServiceProvider services, string address)
    {
        var client = services.GetRequiredService<IHttpClientFactory>().CreateClient(nameof(ProgramTests));
        client.BaseAddress = new(address);
        return client;
    }

    /// <summary>Reads the health endpoint when it is available.</summary>
    /// <param name="httpClient">The mounted HTTP client.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The response body, or <see langword="null"/> when the endpoint is not ready.</returns>
    private static async ValueTask<string?> TryReadHealthAsync(HttpClient httpClient, CancellationToken cancellationToken)
    {
        try
        {
            using var response = await httpClient.GetAsync(new Uri("healthz", UriKind.Relative), cancellationToken).ConfigureAwait(false);
            return response.StatusCode == HttpStatusCode.OK
                ? await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false)
                : null;
        }
        catch (HttpRequestException)
        {
            return null;
        }
        catch (OperationCanceledException)
        {
            return null;
        }
    }

    /// <summary>Waits for the next readiness poll without surfacing expected timeout cancellation.</summary>
    /// <param name="timer">The readiness timer.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns><see langword="true"/> when another poll should run.</returns>
    private static async ValueTask<bool> WaitForNextPollAsync(PeriodicTimer timer, CancellationToken cancellationToken)
    {
        try
        {
            return await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return false;
        }
    }

    /// <summary>Waits for the public runner to finish after cancellation.</summary>
    /// <param name="runTask">The runner task.</param>
    /// <returns>The assertion task.</returns>
    private static async ValueTask AwaitRunnerCompletionAsync(Task runTask)
    {
        try
        {
            await runTask.WaitAsync(TimeSpan.FromSeconds(StopTimeoutSeconds)).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
    }

    /// <summary>Finds the example server assembly copied next to the test host or under the source output tree.</summary>
    /// <returns>The server assembly path.</returns>
    /// <exception cref="FileNotFoundException">Thrown when the built server assembly cannot be found.</exception>
    private static string FindServerAssemblyPath()
    {
        var direct = System.IO.Path.Combine(AppContext.BaseDirectory, ServerAssemblyFileName);
        if (File.Exists(direct))
        {
            return direct;
        }

        var targetFramework = new DirectoryInfo(AppContext.BaseDirectory.TrimEnd(
            System.IO.Path.DirectorySeparatorChar,
            System.IO.Path.AltDirectorySeparatorChar)).Name;
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
        {
            if (!string.Equals(directory.Name, "src", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            foreach (var configuration in new[] { "Release", "Debug" })
            {
                var candidate = System.IO.Path.Combine(
                    directory.FullName,
                    "examples",
                    "OccasionallyConnected.Collaboration.Server",
                    "bin",
                    configuration,
                    targetFramework,
                    ServerAssemblyFileName);
                if (File.Exists(candidate))
                {
                    return candidate;
                }
            }
        }

        throw new FileNotFoundException("The built example server assembly was not found.", ServerAssemblyFileName);
    }

    /// <summary>Finds the dotnet host used to execute the example server assembly.</summary>
    /// <returns>The dotnet executable path or command name.</returns>
    private static string FindDotNetCliPath()
    {
        var processPath = Environment.ProcessPath;
        if (!string.IsNullOrWhiteSpace(processPath))
        {
            var processFile = new FileInfo(processPath);
            var sibling = System.IO.Path.Combine(processFile.DirectoryName ?? string.Empty, DotNetFileName());
            if (File.Exists(sibling))
            {
                return sibling;
            }
        }

        var dotnetRoot = Environment.GetEnvironmentVariable("DOTNET_ROOT");
        if (!string.IsNullOrWhiteSpace(dotnetRoot))
        {
            var rooted = System.IO.Path.Combine(dotnetRoot, DotNetFileName());
            if (File.Exists(rooted))
            {
                return rooted;
            }
        }

        return "dotnet";
    }

    /// <summary>Gets the platform-specific dotnet executable file name.</summary>
    /// <returns>The dotnet executable file name.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string DotNetFileName() =>
        OperatingSystem.IsWindows() ? "dotnet.exe" : "dotnet";

    /// <summary>Creates a mounted loopback base address.</summary>
    /// <param name="address">The loopback address.</param>
    /// <returns>The mounted loopback address.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string CreateMountedAddress(string address) =>
        new Uri(new Uri(EnsureTrailingSlash(address)), $"{PathBase}/").ToString();

    /// <summary>Ensures a base address ends with a slash before relative URI composition.</summary>
    /// <param name="address">The address to inspect.</param>
    /// <returns>The slash-terminated address.</returns>
    private static string EnsureTrailingSlash(string address) =>
        address.EndsWith('/') ? address : $"{address}/";

    /// <summary>Reserves and releases an available loopback TCP port for the child server process.</summary>
    /// <returns>The selected port.</returns>
    private static int GetAvailableLoopbackPort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    /// <summary>Owns a started server process and its drained output streams.</summary>
    private sealed class CapturedServerProcess : IDisposable
    {
        /// <summary>The standard output read task.</summary>
        private readonly Task<string> _standardOutput;

        /// <summary>The standard error read task.</summary>
        private readonly Task<string> _standardError;

        /// <summary>Initializes a new instance of the <see cref="CapturedServerProcess"/> class.</summary>
        /// <param name="process">The started child process.</param>
        internal CapturedServerProcess(Process process)
        {
            Process = process;
            _standardOutput = process.StandardOutput.ReadToEndAsync();
            _standardError = process.StandardError.ReadToEndAsync();
        }

        /// <summary>Gets the captured process.</summary>
        internal Process Process { get; }

        /// <summary>Creates a diagnostic process-exit message.</summary>
        /// <returns>The diagnostic message.</returns>
        internal async ValueTask<string> CreateExitMessageAsync() =>
            $"The example server process exited before it became healthy. Exit code: {Process.ExitCode}.{await ReadOutputAsync().ConfigureAwait(false)}";

        /// <summary>Stops the child process and waits for its exit with a bounded timeout.</summary>
        /// <returns>The cleanup task.</returns>
        internal async ValueTask StopAsync()
        {
            try
            {
                if (!Process.HasExited)
                {
                    Process.Kill(entireProcessTree: true);
                }
            }
            catch (InvalidOperationException)
            {
            }

            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(StopTimeoutSeconds));
            try
            {
                await Process.WaitForExitAsync(timeout.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        /// <summary>Reads the drained child process output.</summary>
        /// <returns>The diagnostic output text.</returns>
        internal async ValueTask<string> ReadOutputAsync()
        {
            var standardOutput = await ReadOutputStreamAsync(_standardOutput).ConfigureAwait(false);
            var standardError = await ReadOutputStreamAsync(_standardError).ConfigureAwait(false);
            return $"{Environment.NewLine}stdout:{Environment.NewLine}{standardOutput}{Environment.NewLine}stderr:{Environment.NewLine}{standardError}";

            static async ValueTask<string> ReadOutputStreamAsync(Task<string> output)
            {
                try
                {
                    return await output.WaitAsync(TimeSpan.FromSeconds(StopTimeoutSeconds)).ConfigureAwait(false);
                }
                catch (TimeoutException)
                {
                    return "<output stream did not complete before the cleanup timeout>";
                }
            }
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void IDisposable.Dispose() => Process.Dispose();
    }

    /// <summary>Owns a temporary SQLite database path for the process test.</summary>
    private sealed class ProcessDatabaseLease : IDisposable
    {
        /// <summary>The temporary directory containing the database file.</summary>
        private readonly string _directory;

        /// <summary>Initializes a new instance of the <see cref="ProcessDatabaseLease"/> class.</summary>
        internal ProcessDatabaseLease()
        {
            _directory = OwnedTempDirectory.Create("rxui-oc-server-process-");
            Path = System.IO.Path.Combine(_directory, "journal.db");
        }

        /// <summary>Gets the leased database file path.</summary>
        internal string Path { get; }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void IDisposable.Dispose() => Directory.Delete(_directory, recursive: true);
    }
}
