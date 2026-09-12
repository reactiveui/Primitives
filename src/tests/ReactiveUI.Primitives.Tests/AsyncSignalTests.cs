// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests asynchronous signal behavior.</summary>
public class AsyncSignalTests
{
    /// <summary>Defines the integer value observed by asynchronous tests.</summary>
    private const int ExpectedValue = 42;

    /// <summary>The first value emitted before completion.</summary>
    private const int FirstEmittedValue = 5;

    /// <summary>A value emitted after completion that must be ignored.</summary>
    private const int IgnoredAfterCompletion = 6;

    /// <summary>The values expected after the first emission.</summary>
    private static readonly int[] FirstEmittedValues = [FirstEmittedValue];

    /// <summary>Subscribing with a null observer is rejected.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void Subscribe_ArgumentChecking() =>
        Assert.Throws<ArgumentNullException>(static () => new AsyncSignal<int>().Subscribe(null!));

    /// <summary>Faulting with a null error is rejected.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void OnError_ArgumentChecking() =>
        Assert.Throws<ArgumentNullException>(static () => new AsyncSignal<int>().OnError(null!));

    /// <summary>The signal is its own awaiter and reports completion once a value arrives.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Await_Blocking()
    {
        AsyncSignal<int> s = new();
        await GetResult_BlockingImpl(s);
        await Assert.That(s.GetAwaiter()).IsSameReferenceAs(s);
        await Assert.That(s.IsCompleted).IsTrue();
    }

    /// <summary>The signal is its own awaiter and reports completion once a fault arrives.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Await_Throw()
    {
        AsyncSignal<int> s = new();
        await GetResult_Blocking_ThrowImpl(s);
        await Assert.That(s.GetAwaiter()).IsSameReferenceAs(s);
        await Assert.That(s.IsCompleted).IsTrue();
    }

    /// <summary>Reading the result of a signal that completed without a value throws.</summary>
    [Test]
    public void GetResult_Empty()
    {
        AsyncSignal<int> s = new();
        s.OnCompleted();
        _ = Assert.Throws<InvalidOperationException>(() => s.GetResult());
    }

    /// <summary>A pending wait delivers a value and leaves the signal completed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetResult_Blocking()
    {
        AsyncSignal<int> s = new();
        await GetResult_BlockingImpl(s);
        await Assert.That(s.IsCompleted).IsTrue();
    }

    /// <summary>A pending wait delivers a fault and leaves the signal completed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetResult_Blocking_Throw()
    {
        AsyncSignal<int> s = new();
        await GetResult_Blocking_ThrowImpl(s);
        await Assert.That(s.IsCompleted).IsTrue();
    }

    /// <summary>A continuation registered under a synchronization context is posted through that context.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetResult_Context()
{
        AsyncSignal<int> signal = new();
        MyContext context = new();
        var completed = false;
        var previous = SynchronizationContext.Current;
        try
        {
            SynchronizationContext.SetSynchronizationContext(context);
            signal.GetAwaiter().OnCompleted(() => completed = true);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
        }

        signal.OnNext(ExpectedValue);
        signal.OnCompleted();
        await Assert.That(completed).IsTrue();
        await Assert.That(context.Ran).IsTrue();
    }

    /// <summary>Observer presence tracks subscriptions as they are added and disposed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers()
    {
        AsyncSignal<int> s = new();
        await Assert.That(s.HasObservers).IsFalse();
        var d1 = s.Subscribe(static _ => { });
        await Assert.That(s.HasObservers).IsTrue();
        d1.Dispose();
        await Assert.That(s.HasObservers).IsFalse();
        var d2 = s.Subscribe(static _ => { });
        await Assert.That(s.HasObservers).IsTrue();
        var d3 = s.Subscribe(static _ => { });
        await Assert.That(s.HasObservers).IsTrue();
        d2.Dispose();
        await Assert.That(s.HasObservers).IsTrue();
        d3.Dispose();
        await Assert.That(s.HasObservers).IsFalse();
    }

    /// <summary>Disposing the signal drops its observers, and disposing a live subscription afterwards is safe.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers_Dispose1()
    {
        AsyncSignal<int> s = new();
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsFalse();
        var d = s.Subscribe(static _ => { });
        await Assert.That(s.HasObservers).IsTrue();
        await Assert.That(s.IsDisposed).IsFalse();
        s.Dispose();
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsTrue();
        d.Dispose();
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsTrue();
    }

    /// <summary>Disposing the last subscription clears the observers and leaves the signal undisposed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers_Dispose2()
    {
        AsyncSignal<int> s = new();
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsFalse();
        var d = s.Subscribe(static _ => { });
        await Assert.That(s.HasObservers).IsTrue();
        await Assert.That(s.IsDisposed).IsFalse();
        d.Dispose();
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsFalse();
        s.Dispose();
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsTrue();
    }

    /// <summary>Disposing a signal that was never subscribed to reports no observers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers_Dispose3()
    {
        AsyncSignal<int> s = new();
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsFalse();
        s.Dispose();
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsTrue();
    }

    /// <summary>Completing the signal releases its observers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers_OnCompleted()
    {
        AsyncSignal<int> s = new();
        await Assert.That(s.HasObservers).IsFalse();
        var d = s.Subscribe(static _ => { });
        await Assert.That(s.HasObservers).IsTrue();
        s.OnNext(ExpectedValue);
        await Assert.That(s.HasObservers).IsTrue();
        s.OnCompleted();
        await Assert.That(s.HasObservers).IsFalse();
        d.Dispose();
    }

    /// <summary>Faulting the signal releases its observers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers_OnError()
    {
        AsyncSignal<int> s = new();
        await Assert.That(s.HasObservers).IsFalse();
        var d = s.Subscribe(
            static _ => { },
            static _ => { });
        await Assert.That(s.HasObservers).IsTrue();
        s.OnNext(ExpectedValue);
        await Assert.That(s.HasObservers).IsTrue();
        s.OnError(new InvalidOperationException());
        await Assert.That(s.HasObservers).IsFalse();
        d.Dispose();
    }

    /// <summary>A signal that completes without producing a value only completes its late subscribers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompletingWithoutAValueOnlyCompletesLateSubscribers()
    {
        AsyncSignal<int> signal = new();
        signal.OnCompleted();

        RecordingWitness<int> late = new();
        signal.Subscribe(late).Dispose();

        await Assert.That(late.Values.Count).IsEqualTo(0);
        await Assert.That(late.Completed).IsEqualTo(1);
        await Assert.That(late.Errors.Count).IsEqualTo(0);
        await Assert.That(signal.IsCompleted).IsTrue();
        await Assert.That(signal.HasObservers).IsFalse();
    }

    /// <summary>Subscriber churn, late subscriptions, repeated terminals, and disposal all hold on an async signal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AsyncSignalSubscriberChurnLateTerminalsAndDisposalCoverBranches()
    {
        var completionFaults = 0;
        AsyncSignal<int> asyncSignal = new();
        _ = Assert.Throws<InvalidOperationException>(() => _ = asyncSignal.Value);
        _ = Assert.Throws<ArgumentNullException>(() => asyncSignal.OnCompleted(null!));
        _ = Assert.Throws<ArgumentNullException>(() => asyncSignal.OnError(null!));
        RecordingWitness<int> asyncFirst = new();
        RecordingWitness<int> asyncSecond = new();
        using var asyncSubscription = asyncSignal.Subscribe(asyncFirst);
        var asyncSecondSubscription = asyncSignal.Subscribe(asyncSecond);
        asyncSecondSubscription.Dispose();
        asyncSignal.OnNext(FirstEmittedValue);
        asyncSignal.OnCompleted(() => completionFaults++);
        asyncSignal.OnCompleted();
        asyncSignal.OnCompleted();
        asyncSignal.OnNext(IgnoredAfterCompletion);
        RecordingWitness<int> asyncLate = new();
        asyncSignal.Subscribe(asyncLate).Dispose();
        await Assert.That(asyncSignal.Value).IsEqualTo(FirstEmittedValue);
        await Assert.That(asyncSignal.GetResult()).IsEqualTo(FirstEmittedValue);
        await Assert.That(asyncFirst.Values.SequenceEqual(FirstEmittedValues)).IsTrue();
        await Assert.That(asyncSecond.Values.Count).IsEqualTo(0);
        await Assert.That(asyncLate.Values.SequenceEqual(FirstEmittedValues)).IsTrue();
        await Assert.That(asyncLate.Completed).IsEqualTo(1);
        AsyncSignal<int> asyncError = new();
        RecordingWitness<int> asyncErrorObserver = new();
        asyncError.Subscribe(asyncErrorObserver).Dispose();
        InvalidOperationException asyncFault = new("async-fault");
        asyncError.OnError(asyncFault);
        asyncError.OnError(new InvalidOperationException("late"));
        _ = Assert.Throws<InvalidOperationException>(() => asyncError.GetResult());
        RecordingWitness<int> asyncErrorLate = new();
        asyncError.Subscribe(asyncErrorLate).Dispose();
        await Assert.That(asyncErrorLate.Errors[0]).IsSameReferenceAs(asyncFault);
        AsyncSignal<int> disposedAsync = new();
        disposedAsync.Dispose();
        disposedAsync.Dispose();
        _ = Assert.Throws<ObjectDisposedException>(() => disposedAsync.OnNext(1));
        _ = Assert.Throws<ObjectDisposedException>(() => disposedAsync.Subscribe(new RecordingWitness<int>()));
        await Assert.That(completionFaults).IsEqualTo(1);
    }

    /// <summary>A pending wait observes completion and returns its last value.</summary>
    /// <param name="s">The pending signal.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task GetResult_BlockingImpl(AsyncSignal<int> s)
    {
        await Assert.That(s.IsCompleted).IsFalse();
        var waits = 0;
        s.WaitIfPending(signal =>
        {
            waits++;
            signal.OnNext(ExpectedValue);
            signal.OnCompleted();
        });
        s.WaitIfPending(_ => waits++);
        await Assert.That(waits).IsEqualTo(1);
        await Assert.That(s.GetResult()).IsEqualTo(ExpectedValue);
        await Assert.That(s.IsCompleted).IsTrue();
    }

    /// <summary>A pending wait observes failure and rethrows the same error.</summary>
    /// <param name="s">The pending signal.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task GetResult_Blocking_ThrowImpl(AsyncSignal<int> s)
    {
        await Assert.That(s.IsCompleted).IsFalse();
        InvalidOperationException expectedException = new();
        var waits = 0;
        s.WaitIfPending(signal =>
        {
            waits++;
            signal.OnError(expectedException);
        });
        s.WaitIfPending(_ => waits++);
        await Assert.That(waits).IsEqualTo(1);
        var caughtException = Assert.Throws<InvalidOperationException>(() => s.GetResult());
        await Assert.That(caughtException).IsSameReferenceAs(expectedException);
        await Assert.That(s.IsCompleted).IsTrue();
    }

    /// <summary>Captures whether a continuation was posted through the synchronization context.</summary>
    private sealed class MyContext : SynchronizationContext
    {
        /// <summary>Gets a value indicating whether a continuation was posted.</summary>
        public bool Ran { get; private set; }

        /// <inheritdoc/>
        public override void Post(SendOrPostCallback d, object? state)
        {
            ArgumentNullException.ThrowIfNull(d);

            Ran = true;
            d(state);
        }
    }
}
