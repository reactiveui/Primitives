// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests ownership of resources returned by scheduled actions.</summary>
public class ScheduledItemTests
{
    /// <summary>Cancellation after publication leaves no resource for final cleanup to release again.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CancellationAfterPublication_ReleasesResultOnce()
    {
        RecordingDisposable result = new();
        ScheduledItem<long> item = new(0, Comparer<long>.Default, _ => result);
        item.Invoke();
        item.Dispose();
        item.ReleaseCanceledResult();
        await Assert.That(result.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Cancellation inside the action releases the result returned afterward.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CancellationBeforePublication_ReleasesResultOnce()
    {
        RecordingDisposable result = new();
        ScheduledItem<long> item = new(0, Comparer<long>.Default, pending =>
        {
            pending.Dispose();
            return result;
        });
        item.Invoke();
        item.Dispose();
        await Assert.That(result.DisposeCount).IsEqualTo(1);
    }
}
