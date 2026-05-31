// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests for <c>SynchronizeAsyncObservable</c> — covers the after-terminal guards
/// on the sink that only fire when the upstream pushes events past its own completion.</summary>
public class SynchronizeAsyncObservableTests
{
    /// <summary>Settle delay to confirm nothing fires.</summary>
    private const int SettleDelayMilliseconds = 50;

    /// <summary>Verifies that <c>OnNext</c>, <c>OnError</c> and a duplicate <c>OnCompleted</c>
    /// arriving after the source has already completed are silently dropped.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenEventsAfterCompleted_ThenDropped()
    {
        var source = new SyncDirectSource<int>();
        var values = new List<int>();
        Exception? caught = null;
        var completedCount = 0;

        using var sub = source.SynchronizeAsync()
            .Subscribe(t => values.Add(t.Value), ex => caught = ex, () => completedCount++);

        source.Observer.OnCompleted();
        source.Observer.OnNext(1);
        source.Observer.OnError(new InvalidOperationException("late"));
        source.Observer.OnCompleted();

        await Task.Delay(SettleDelayMilliseconds);
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsEmpty();
        await Assert.That(caught).IsNull();
    }

    /// <summary>Verifies that an <c>OnCompleted</c> arriving after a prior <c>OnError</c> is silently dropped.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnCompletedAfterError_ThenDropped()
    {
        var source = new SyncDirectSource<int>();
        Exception? caught = null;
        var completedCount = 0;
        var expected = new InvalidOperationException("first");

        using var sub = source.SynchronizeAsync()
            .Subscribe(static _ => { }, ex => caught = ex, () => completedCount++);

        source.Observer.OnError(expected);
        source.Observer.OnCompleted();

        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(completedCount).IsEqualTo(0);
    }

    /// <summary>Verifies the per-emission <c>Sync</c> signal latches on first dispose so a second
    /// dispose by the consumer is a silent no-op.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncSignalDisposedTwice_ThenSecondDisposeIsNoOp()
    {
        var source = new SyncDirectSource<int>();
        var processed = 0;

        using var sub = source.SynchronizeAsync()
            .Subscribe(t =>
            {
                t.Sync.Dispose();
                t.Sync.Dispose();
                processed++;
            });

        source.Observer.OnNext(1);

        await Task.Delay(SettleDelayMilliseconds);
        await Assert.That(processed).IsEqualTo(1);
    }
}
