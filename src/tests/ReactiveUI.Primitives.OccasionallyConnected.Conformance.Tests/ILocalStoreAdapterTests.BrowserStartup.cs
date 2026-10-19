// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if NET10_0_OR_GREATER
using System.Diagnostics;
using System.Text.Json;

namespace ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests;

/// <content>Deterministic browser startup polling assertions.</content>
public sealed partial class ILocalStoreAdapterTests
{
    /// <summary>Checks only supported startup races are polled and the original deadline and exit guard remain strict.</summary>
    /// <param name="code">The injected read failure.</param>
    /// <param name="platform">The injected operating system.</param>
    /// <param name="mode">The injected deadline or process exit.</param>
    /// <param name="expected">The exact result and operation counts.</param>
    /// <returns>The assertions.</returns>
    /// <exception cref="InvalidOperationException">Node could not start.</exception>
    [Test]
    [Arguments("EBUSY", "win32", "ready", "43123:2:1")]
    [Arguments("ENOENT", "linux", "ready", "43123:2:1")]
    [Arguments("EACCES", "win32", "ready", "EACCES:1:0")]
    [Arguments("EBUSY", "linux", "ready", "EBUSY:1:0")]
    [Arguments("EBUSY", "win32", "deadline", "AbortError:1:1")]
    [Arguments("EBUSY", "win32", "exit", "exit:0:0")]
    public async Task IndexedDbBrowserStartupPreservesStrictReadAndDeadlineGuards(
        string code,
        string platform,
        string mode,
        string expected)
    {
        var module = new Uri(Path.Combine(AppContext.BaseDirectory, "indexedDbBrowserStartup.mjs")).AbsoluteUri;
        var script = $$"""
            import { waitForDebuggingPort } from "{{JsonEncodedText.Encode(module)}}";
            const code = "{{JsonEncodedText.Encode(code)}}";
            const mode = "{{JsonEncodedText.Encode(mode)}}";
            const controller = new AbortController();
            let reads = 0, waits = 0, result;
            const read = async () => {
                if (++reads === 1) throw Object.assign(new Error(code), { code });
                return "43123\n/browser";
            };
            const wait = async (milliseconds, value, options) => {
                ++waits;
                if (milliseconds !== 25 || options.signal !== controller.signal) throw new Error("Invalid polling guard");
                if (mode === "deadline") controller.abort();
            };
            try {
                result = await waitForDebuggingPort("port", { exitCode: mode === "exit" ? 1 : null },
                    () => "diagnostic", controller.signal, read, wait, "{{JsonEncodedText.Encode(platform)}}");
            } catch (error) {
                result = error.name === "AbortError" ? error.name : error.code ??
                    (error.message.includes("exit 1") && error.message.includes("diagnostic") ? "exit" : "unexpected");
            }
            console.log(`${result}:${reads}:${waits}`);
            """;
        await AssertBrowserStartupResultAsync(script, expected);
    }

    /// <summary>Runs the actual JavaScript startup helper through its TUnit assertions.</summary>
    /// <param name="script">The injected startup driver.</param>
    /// <param name="expected">The exact startup result.</param>
    /// <returns>The assertions.</returns>
    /// <exception cref="InvalidOperationException">Node could not start.</exception>
    private static async Task AssertBrowserStartupResultAsync(string script, string expected)
    {
        ProcessStartInfo start = new("node") { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true };
        start.ArgumentList.Add("--input-type=module");
        start.ArgumentList.Add("--eval");
        start.ArgumentList.Add(script);
        using var process = Process.Start(start) ?? throw new InvalidOperationException("The startup regression driver did not start.");
        var output = process.StandardOutput.ReadToEndAsync();
        var error = process.StandardError.ReadToEndAsync();
        using var timeout = new CancellationTokenSource(BrowserGuard);
        try
        {
            await WaitForChildExitAsync(process, timeout.Token);
            await Assert.That(process.ExitCode).IsEqualTo(0);
            await Assert.That(await error).IsEmpty();
            await Assert.That((await output).Trim()).IsEqualTo(expected);
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
            }

            using var join = new CancellationTokenSource(ChildGuard);
            await WaitForChildExitAsync(process, join.Token);
            await Task.WhenAll(output, error).WaitAsync(ChildGuard);
        }
    }
}
#endif
