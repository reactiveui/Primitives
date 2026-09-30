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
    [Test]
    public async Task JavaScriptListenersAreBoundedAndRemoved()
    {
        var startInfo = new ProcessStartInfo("node")
        { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true };
        startInfo.ArgumentList.Add(Path.Combine(AppContext.BaseDirectory, "browserLifecycleTests.mjs"));
        using var process = Process.Start(startInfo) ?? throw new InvalidOperationException("Node did not start.");
        using var timeout = new CancellationTokenSource(JavaScriptTimeoutMilliseconds);
        var output = process.StandardOutput.ReadToEndAsync(timeout.Token);
        var error = process.StandardError.ReadToEndAsync(timeout.Token);
        try
        {
            await process.WaitForExitAsync(timeout.Token);
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
            }
        }

        await Assert.That(process.ExitCode).IsEqualTo(0);
        await Assert.That(await error).IsEmpty();
        using var results = JsonDocument.Parse(await output);
        await Assert.That(results.RootElement.GetArrayLength()).IsGreaterThan(0);
        foreach (var result in results.RootElement.EnumerateArray())
        {
            await Assert.That(result.GetBoolean()).IsTrue();
        }
    }
}
