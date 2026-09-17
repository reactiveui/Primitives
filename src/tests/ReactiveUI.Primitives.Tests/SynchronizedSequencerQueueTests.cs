// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests cancellation-aware queue traversal.</summary>
public sealed class SynchronizedSequencerQueueTests
{
    /// <summary>Cancelled head entries are discarded while live work remains queued.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task GetNextLive_WhenHeadIsCanceled_ThenSkipsItWithoutRemovingLiveWork()
    {
        SynchronizedSequencerQueue<int> queue = new();
        var canceled = ScheduledItem.Create(Sequencer.Immediate, 0, static (_, _) => EmptyDisposable.Instance, 0);
        var live = ScheduledItem.Create(Sequencer.Immediate, 0, static (_, _) => EmptyDisposable.Instance, 1);
        queue.Enqueue(canceled);
        queue.Enqueue(live);
        canceled.Cancel();

        await Assert.That(queue.GetNextLive()).IsSameReferenceAs(live);
        await Assert.That(queue.GetNextLive()).IsSameReferenceAs(live);
        queue.Remove(live);
        await Assert.That(queue.GetNextLive()).IsNull();
    }
}
