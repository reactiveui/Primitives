// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="Signal"/> collect windowing contracts.</summary>
public sealed class SignalCollectTests
{
    /// <summary>Verifies the Collect method covers immediate, scheduled, terminal, and error paths.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CollectCoversImmediateScheduledCompletionErrorAndDisposePaths()
    {
        const int First = 1;
        const int Second = 2;
        const int Third = 3;
        const int ExpectedBatchCount = 2;
        List<int[]> immediateBatches = [];
        _ = Signal.FromEnumerable([First, Second]).Collect(TimeSpan.Zero)
            .Subscribe(batch => immediateBatches.Add([.. batch]));
        await Assert.That(immediateBatches.Count).IsEqualTo(ExpectedBatchCount);
        await Assert.That(immediateBatches[0].SequenceEqual([First])).IsTrue();
        await Assert.That(immediateBatches[1].SequenceEqual([Second])).IsTrue();
        VirtualClock clock = new();
        Signal<int> source = new();
        List<int[]> scheduledBatches = [];
        var completed = 0;
        var subscription = source.Collect(TimeSpan.FromTicks(Second), clock)
            .Subscribe(batch => scheduledBatches.Add([.. batch]), static ex => throw ex, () => completed++);
        source.OnNext(First);
        source.OnNext(Second);
        clock.AdvanceBy(TimeSpan.FromTicks(Second));
        source.OnNext(Third);
        source.OnCompleted();
        clock.AdvanceBy(TimeSpan.FromTicks(Second));
        subscription.Dispose();
        await Assert.That(scheduledBatches.Count).IsEqualTo(ExpectedBatchCount);
        await Assert.That(scheduledBatches[0].SequenceEqual([First, Second])).IsTrue();
        await Assert.That(scheduledBatches[1].SequenceEqual([Third])).IsTrue();
        await Assert.That(completed).IsEqualTo(1);
        VirtualClock errorClock = new();
        Signal<int> errorSource = new();
        InvalidOperationException expected = new("collect");
        Exception? observed = null;
        _ = errorSource.Collect(TimeSpan.FromTicks(First), errorClock).Subscribe(static _ => { }, ex => observed = ex);
        errorSource.OnNext(First);
        errorSource.OnError(expected);
        errorClock.AdvanceBy(TimeSpan.FromTicks(First));
        await Assert.That(observed!).IsSameReferenceAs(expected);
        _ = Assert.Throws<ArgumentNullException>(static () => ((IObservable<int>)null!).Collect(TimeSpan.FromTicks(First)));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Emit(First).Collect(TimeSpan.FromTicks(First), null!));
        var stoppedGuardCompleted = 0;
        _ = new ScriptedObservable<int>(static observer =>
            {
                observer.OnCompleted();
                observer.OnNext(First);
            }).Collect(TimeSpan.FromTicks(First), new VirtualClock())
            .Subscribe(static _ => { }, static ex => throw ex, () => stoppedGuardCompleted++);
        await Assert.That(stoppedGuardCompleted).IsEqualTo(1);
    }
}
