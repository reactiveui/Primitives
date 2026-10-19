// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Text.Json;

namespace ReactiveUI.Primitives.OccasionallyConnected.Web.Tests;

/// <summary>Tests the shipped JS event bridge using Node's DOM event targets.</summary>
public sealed partial class BrowserLifecycleAdapterTests
{
    /// <summary>The maximum time allowed for the JS event bridge test.</summary>
    private const int JavaScriptTimeoutMilliseconds = 15_000;

    /// <summary>Checks all browser event mappings, bounded delivery, recovery, and listener removal.</summary>
    /// <returns>The test task.</returns>
    /// <exception cref="InvalidOperationException">Node could not be started.</exception>
    /// <exception cref="TimeoutException">Node did not exit within the bounded test deadline.</exception>
    [Test]
    [NotInParallel]
    public async Task JavaScriptListenersAreBoundedAndRemoved()
    {
        var startInfo = new ProcessStartInfo("node")
        { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true };
        startInfo.ArgumentList.Add(Path.Combine(AppContext.BaseDirectory, "browserLifecycleTests.mjs"));
        using var process = Process.Start(startInfo) ?? throw new InvalidOperationException("Node did not start.");
        using var timeout = new CancellationTokenSource(JavaScriptTimeoutMilliseconds);
        var output = process.StandardOutput.ReadToEndAsync();
        var error = process.StandardError.ReadToEndAsync();
        int exitCode;
        try
        {
            exitCode = await WaitForJavaScriptExitAsync(process, timeout.Token);
        }
        catch (OperationCanceledException exception) when (timeout.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Node did not exit within {JavaScriptTimeoutMilliseconds}ms. "
                + $"HasExited={process.HasExited}; stdout={output.Status}; stderr={error.Status}.",
                exception);
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
            }

            await Task.WhenAll(output, error).WaitAsync(TimeSpan.FromMilliseconds(JavaScriptTimeoutMilliseconds));
        }

        await Assert.That(exitCode).IsEqualTo(0);
        await Assert.That(await error).IsEmpty();
        using var results = JsonDocument.Parse(await output);
        await Assert.That(results.RootElement.GetArrayLength()).IsGreaterThan(0);
        foreach (var result in results.RootElement.EnumerateArray())
        {
            await Assert.That(result.GetBoolean()).IsTrue();
        }
    }

    /// <summary>Waits for Node to exit without treating signal termination as a normal exit.</summary>
    /// <param name="process">The owned Node process.</param>
    /// <param name="cancellationToken">The test timeout token.</param>
    /// <returns>The normal exit code.</returns>
    /// <exception cref="OperationCanceledException">The process wait was canceled.</exception>
    /// <exception cref="InvalidOperationException">Node was terminated by a signal.</exception>
    private static async Task<int> WaitForJavaScriptExitAsync(Process process, CancellationToken cancellationToken)
    {
#if NET11_0_OR_GREATER
        var status = await process.WaitForExitStatusAsync(cancellationToken).ConfigureAwait(false);
        if (status.Canceled)
        {
            throw new OperationCanceledException(cancellationToken);
        }

        if (status.Signal is not null)
        {
            throw new InvalidOperationException("The browser event bridge test process was terminated by a signal.");
        }

        return status.ExitCode;
#else
        await process.WaitForExitAsync(cancellationToken).ConfigureAwait(false);
        return process.ExitCode;
#endif
    }
}
