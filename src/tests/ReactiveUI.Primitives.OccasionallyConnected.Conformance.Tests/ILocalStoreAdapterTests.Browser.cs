// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if NET10_0_OR_GREATER
using System.Diagnostics;
using System.Text.Json;

namespace ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests;

/// <content>Real browser transaction and process-restart evidence for IndexedDB.</content>
public sealed partial class ILocalStoreAdapterTests
{
    /// <summary>The actual browser transaction assertions returned by the driver.</summary>
    private const int BrowserAssertionCount = 10;

    /// <summary>The maximum browser startup, transaction and restart interval.</summary>
    private static readonly TimeSpan BrowserGuard = TimeSpan.FromSeconds(60);

    /// <summary>Checks a missing persisted identity or generation is corruption, not empty recovery.</summary>
    /// <param name="json">The corrupted persisted document.</param>
    /// <returns>The assertions.</returns>
    [Test]
    [Arguments("null")]
    [Arguments("{}")]
    [Arguments("{\"Generation\":1,\"StoreIdentity\":\"other\"}")]
    public async Task IndexedDbInvalidDocumentFailsClosed(string json)
    {
        await using var fixture = new StoreFixture(IndexedDbProvider);
        await using (var store = await fixture.OpenAsync())
        {
            _ = await Testing.LocalStoreConformance.SeedDurableAsync(store);
        }

        fixture.CorruptBrowserDocument(json);
        Func<Task> reopen = async () =>
        {
            await using var store = await fixture.OpenAsync();
        };
        await Assert.That(reopen).ThrowsExactly<InvalidDataException>();
    }

    /// <summary>Checks the shipped module commits atomically across pages and a browser process restart.</summary>
    /// <returns>The assertions.</returns>
    /// <exception cref="InvalidOperationException">Node cannot start the browser driver.</exception>
    [Test]
    public async Task IndexedDbTransactionsSurviveBrowserProcessRestart()
    {
        ProcessStartInfo start = new("node") { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true };
        start.ArgumentList.Add(Path.Combine(AppContext.BaseDirectory, "indexedDbBrowserConformance.mjs"));
        using var process = Process.Start(start) ?? throw new InvalidOperationException("The real browser driver did not start.");
        var output = process.StandardOutput.ReadToEndAsync();
        var error = process.StandardError.ReadToEndAsync();
        using var timeout = new CancellationTokenSource(BrowserGuard);
        try
        {
            await WaitForChildExitAsync(process, timeout.Token);
            var errors = await error;
            await Assert.That(errors).IsEmpty();
            using var results = JsonDocument.Parse(await output);
            await Assert.That(results.RootElement.GetArrayLength()).IsEqualTo(BrowserAssertionCount);
            foreach (var result in results.RootElement.EnumerateArray())
            {
                await Assert.That(result.GetBoolean()).IsTrue();
            }
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
