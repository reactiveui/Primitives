// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions.Operators;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests synchronization acknowledgements and notification handling after termination.</summary>
public class SynchronizeAsyncObservableTests
{
    /// <summary>Verifies disposal completes an acknowledgement published after the disposal step.</summary>
    /// <returns>A task representing the asynchronous test operation.</returns>
    [Test]
    public async Task AcknowledgementPublishedAfterDisposalCompletes()
    {
        SynchronizeAsyncObservable<int>.SynchronizeAsyncSink.SyncSignal signal = new();
        TaskCompletionSource<bool> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        signal.Dispose();
        signal.CompleteIfDisposedRaced(completion);
        signal.CompleteIfDisposedRaced(completion);
        await Assert.That(completion.Task.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>Verifies a live acknowledgement remains pending until disposal.</summary>
    /// <returns>A task representing the asynchronous test operation.</returns>
    [Test]
    public async Task LiveAcknowledgementWaitsForDisposal()
    {
        SynchronizeAsyncObservable<int>.SynchronizeAsyncSink.SyncSignal signal = new();
        var acknowledgement = signal.WaitForDisposeAsync();
        await Assert.That(acknowledgement.IsCompleted).IsFalse();
        signal.Dispose();
        await acknowledgement;
        await Assert.That(acknowledgement.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>Verifies that <c>OnNext</c>, <c>OnError</c> and a duplicate <c>OnCompleted</c>
    /// arriving after the source has already completed are silently dropped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenEventsAfterCompleted_ThenDropped()
    {
        SyncDirectSource<int> source = new();
        List<int> values = [];
        Exception? caught = null;
        var completedCount = 0;
        using var sub = source.SynchronizeAsync()
            .Subscribe(t => values.Add(t.Value), ex => caught = ex, () => completedCount++);
        source.Observer.OnCompleted();
        source.Observer.OnNext(1);
        source.Observer.OnError(new InvalidOperationException("late"));
        source.Observer.OnCompleted();
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsEmpty();
        await Assert.That(caught).IsNull();
    }

    /// <summary>Verifies that an <c>OnCompleted</c> arriving after a prior <c>OnError</c> is silently dropped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnCompletedAfterError_ThenDropped()
    {
        SyncDirectSource<int> source = new();
        Exception? caught = null;
        var completedCount = 0;
        InvalidOperationException expected = new("first");
        using var sub = source.SynchronizeAsync().Subscribe(
            static _ => { },
            ex => caught = ex,
            () => completedCount++);
        source.Observer.OnError(expected);
        source.Observer.OnCompleted();
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(completedCount).IsEqualTo(0);
    }

    /// <summary>Verifies the per-emission <c>Sync</c> signal latches on first dispose so a second dispose by the consumer is a silent no-op.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncSignalDisposedTwice_ThenSecondDisposeIsNoOp()
    {
        SyncDirectSource<int> source = new();
        var processed = 0;
        using var sub = source.SynchronizeAsync().Subscribe(t =>
        {
            t.Sync.Dispose();
            t.Sync.Dispose();
            processed++;
        });
        source.Observer.OnNext(1);
        await Assert.That(processed).IsEqualTo(1);
    }

    /// <summary>Verifies an observer that marshals to another thread which completes the source does not deadlock the paired value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task WhenObserverMarshalsCompletionDuringValue_ThenNoDeadlock() =>
        SerializedDeliveryAssertions.ObserverMarshallingCompletionDoesNotDeadlock<(int Value, IDisposable Sync)>(
            static (source, observer) => source.SynchronizeAsync().Subscribe(observer),
            static observer => observer.OnNext(1));
}
