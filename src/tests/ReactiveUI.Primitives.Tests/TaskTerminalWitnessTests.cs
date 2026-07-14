// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Verifies the task-producing terminal witnesses (<see cref="TaskAnyWitness{T}"/> and
/// <see cref="TaskCountWitness{T}"/>): each stops exactly once, releases its source subscription, and
/// ignores every notification that arrives after it has stopped.
/// </summary>
public sealed class TaskTerminalWitnessTests
{
    /// <summary>The first observed value.</summary>
    private const int First = 1;

    /// <summary>The second observed value.</summary>
    private const int Second = 2;

    /// <summary>The number of matching values expected from the predicate count witness.</summary>
    private const int ExpectedMatches = 2;

    /// <summary>Disposing the any-witness releases its source subscription once and leaves the task pending.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AnyWitnessDisposalReleasesTheSourceSubscriptionOnce()
    {
        RecordingDisposable upstream = new();
        TaskAnyWitness<int> witness = new(CancellationToken.None);
        witness.RegisterCancellation();
        witness.SetSubscription(upstream);

        witness.Dispose();
        witness.Dispose();

        await Assert.That(upstream.DisposeCount).IsEqualTo(1);
        await Assert.That(witness.Task.IsCompleted).IsFalse();
    }

    /// <summary>The any-witness keeps its terminal result when an error arrives after it has stopped.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AnyWitnessIgnoresAnErrorRaisedAfterItsResult()
    {
        RecordingDisposable upstream = new();
        TaskAnyWitness<int> witness = new(CancellationToken.None);
        witness.RegisterCancellation();
        witness.SetSubscription(upstream);

        witness.OnNext(First);
        witness.OnError(new InvalidOperationException("late"));

        await Assert.That(await witness.Task).IsTrue();
        await Assert.That(upstream.DisposeCount).IsEqualTo(1);
    }

    /// <summary>An any-witness that observes no value completes with <see langword="false"/>.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AnyWitnessCompletesFalseWhenNoValueIsObserved()
    {
        RecordingDisposable upstream = new();
        TaskAnyWitness<int> witness = new(static value => value == Second, CancellationToken.None);
        witness.RegisterCancellation();
        witness.SetSubscription(upstream);

        witness.OnNext(First);
        witness.OnCompleted();

        await Assert.That(await witness.Task).IsFalse();
        await Assert.That(upstream.DisposeCount).IsEqualTo(1);
    }

    /// <summary>The count witness counts only the values matching its predicate.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CountWitnessCountsOnlyMatchingValues()
    {
        RecordingDisposable upstream = new();
        TaskCountWitness<int> witness = new(static value => value % Second == 0, CancellationToken.None);
        witness.RegisterCancellation();
        witness.SetSubscription(upstream);

        witness.OnNext(First);
        witness.OnNext(Second);
        witness.OnNext(Second);
        witness.OnCompleted();

        await Assert.That(await witness.Task).IsEqualTo(ExpectedMatches);
        await Assert.That(upstream.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Cancelling the token while the count witness is running cancels its task and releases the source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CountWitnessCancelsItsTaskWhenTheTokenIsCancelledWhileRunning()
    {
        using CancellationTokenSource cts = new();
        RecordingDisposable upstream = new();
        TaskCountWitness<int> witness = new(cts.Token);
        witness.RegisterCancellation();
        witness.SetSubscription(upstream);

        witness.OnNext(First);
        await cts.CancelAsync();

        await Assert.That(() => witness.Task).Throws<TaskCanceledException>();
        await Assert.That(upstream.DisposeCount).IsEqualTo(1);
    }

    /// <summary>A count witness built on an already-cancelled token cancels inline and drops a late subscription.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CountWitnessOnAnAlreadyCancelledTokenCancelsInlineAndDropsTheSubscription()
    {
        using CancellationTokenSource cts = new();
        await cts.CancelAsync();
        RecordingDisposable upstream = new();
        TaskCountWitness<int> witness = new(cts.Token);

        witness.RegisterCancellation();
        witness.SetSubscription(upstream);

        await Assert.That(() => witness.Task).Throws<TaskCanceledException>();
        await Assert.That(upstream.DisposeCount).IsEqualTo(1);
    }

    /// <summary>A stopped count witness publishes no result when completion arrives late.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CountWitnessIgnoresCompletionRaisedAfterItStopped()
    {
        RecordingDisposable upstream = new();
        TaskCountWitness<int> witness = new(CancellationToken.None);
        witness.RegisterCancellation();
        witness.SetSubscription(upstream);

        witness.OnNext(First);
        witness.Dispose();
        witness.OnCompleted();

        await Assert.That(witness.Task.IsCompleted).IsFalse();
        await Assert.That(upstream.DisposeCount).IsEqualTo(1);
    }
}
