// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using OccasionallyConnected.PackedSample;

namespace ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests;

/// <summary>Tests diagnostic snapshot formatting for <see cref="SampleCounterLog"/>.</summary>
public sealed class SampleCounterLogTests
{
    /// <summary>The number of distinct keys added while snapshots are formatted.</summary>
    private const int ConcurrentKeyCount = 2048;

    /// <summary>The number of summaries formatted while the writer is active.</summary>
    private const int SnapshotCount = 512;

    /// <summary>The maximum interval for the concurrent callbacks.</summary>
    private const int GuardSeconds = 10;

    /// <summary>Checks empty output, ordinal sorting and repeated counts.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DescribePreservesEmptyPolicyOrderingAndCounts()
    {
        var log = new SampleCounterLog();
        await Assert.That(log.Describe()).IsEmpty();
        await Assert.That(log.Describe("none")).IsEqualTo("none");
        log.Record("beta");
        log.Record("alpha");
        log.Record("beta");
        await Assert.That(log.Describe()).IsEqualTo("alpha x1, beta x2");
    }

    /// <summary>Checks formatting while another callback grows the dictionary.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DescribeSnapshotsConcurrentKeyGrowth()
    {
        var log = new SampleCounterLog();
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var writer = Task.Run(async () =>
        {
            await start.Task;
            for (var index = 0; index < ConcurrentKeyCount; index++)
            {
                log.Record(index.ToString(CultureInfo.InvariantCulture));
                await Task.Yield();
            }
        });
        var reader = Task.Run(async () =>
        {
            await start.Task;
            for (var index = 0; index < SnapshotCount; index++)
            {
                _ = log.Describe();
                await Task.Yield();
            }
        });
        start.SetResult();
        await Task.WhenAll(writer, reader).WaitAsync(TimeSpan.FromSeconds(GuardSeconds));
        var entries = log.Describe().Split(", ");
        await Assert.That(entries.Length).IsEqualTo(ConcurrentKeyCount);
        await Assert.That(Array.TrueForAll(entries, static entry => entry.EndsWith(" x1", StringComparison.Ordinal))).IsTrue();
    }
}
