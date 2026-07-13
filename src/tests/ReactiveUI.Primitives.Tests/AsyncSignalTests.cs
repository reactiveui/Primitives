// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests asynchronous signal behavior.</summary>
public class AsyncSignalTests
{
    /// <summary>Defines the integer value observed by asynchronous tests.</summary>
    private const int ExpectedValue = 42;

    /// <summary>The first value emitted before completion in churn coverage.</summary>
    private const int FirstEmittedValue = 5;

    /// <summary>A value emitted after completion that must be ignored.</summary>
    private const int IgnoredAfterCompletion = 6;

    /// <summary>Defines the maximum time to wait for cross-thread test work.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>The values expected after the first emission.</summary>
    private static readonly int[] FirstEmittedValues = [FirstEmittedValue];

    /// <summary>Subscribes the argument checking.</summary>
    [Test]
    public void Subscribe_ArgumentChecking() =>
        Assert.Throws<ArgumentNullException>(static () => new AsyncSignal<int>().Subscribe(null!));

    /// <summary>Called when [error argument checking].</summary>
    [Test]
    public void OnError_ArgumentChecking() =>
        Assert.Throws<ArgumentNullException>(static () => new AsyncSignal<int>().OnError(null!));

    /// <summary>Awaits the blocking.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Await_Blocking()
    {
        AsyncSignal<int> s = new();
        await GetResult_BlockingImpl(s.GetAwaiter());
        await Assert.That(s.IsCompleted).IsTrue();
    }

    /// <summary>Awaits the throw.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Await_Throw()
    {
        AsyncSignal<int> s = new();
        await GetResult_Blocking_ThrowImpl(s.GetAwaiter());
        await Assert.That(s.IsCompleted).IsTrue();
    }

    /// <summary>Gets the result empty.</summary>
    [Test]
    public void GetResult_Empty()
    {
        AsyncSignal<int> s = new();
        s.OnCompleted();
        _ = Assert.Throws<InvalidOperationException>(() => s.GetResult());
    }

    /// <summary>Gets the result blocking.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetResult_Blocking()
    {
        AsyncSignal<int> s = new();
        await GetResult_BlockingImpl(s);
        await Assert.That(s.IsCompleted).IsTrue();
    }

    /// <summary>Gets the result blocking throw.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetResult_Blocking_Throw()
    {
        AsyncSignal<int> s = new();
        await GetResult_Blocking_ThrowImpl(s);
        await Assert.That(s.IsCompleted).IsTrue();
    }

    /// <summary>Gets the result context.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetResult_Context()
    {
        AsyncSignal<int> x = new();
        MyContext ctx = new();
        using ManualResetEventSlim registered = new();
        using ManualResetEventSlim completed = new();
        Thread registrationThread = new(() =>
        {
            SynchronizationContext.SetSynchronizationContext(ctx);
            var a = x.GetAwaiter();
            a.OnCompleted(() => completed.Set());
            registered.Set();
        });
        registrationThread.Start();
        await Assert.That(registered.Wait(WaitTimeout)).IsTrue();
        await Assert.That(registrationThread.Join(WaitTimeout)).IsTrue();
        x.OnNext(ExpectedValue);
        x.OnCompleted();
        await Assert.That(completed.Wait(WaitTimeout)).IsTrue();
        await Assert.That(ctx.Ran).IsTrue();
    }

    /// <summary>Determines whether this instance has observers.</summary>
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

    /// <summary>Determines whether [has observers dispose1].</summary>
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

    /// <summary>Determines whether [has observers dispose2].</summary>
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

    /// <summary>Determines whether [has observers dispose3].</summary>
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

    /// <summary>Determines whether [has observers on completed].</summary>
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

    /// <summary>Determines whether [has observers on error].</summary>
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

    /// <summary>Covers async-signal subscriber churn, late subscriptions, disposal, and terminal no-op branches.</summary>
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
        using var asyncSecondSubscription = asyncSignal.Subscribe(asyncSecond);
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

    /// <summary>Gets the result blocking implementation.</summary>
    /// <param name = "s">The s.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task GetResult_BlockingImpl(IAwaitSignal<int> s)
    {
        await Assert.That(s.IsCompleted).IsFalse();
        using ManualResetEventSlim release = new();
        using ManualResetEventSlim started = new();
        Thread producer = new(() =>
        {
            if (!release.Wait(WaitTimeout))
            {
                return;
            }

            s.OnNext(ExpectedValue);
            s.OnCompleted();
        });
        var y = 0;
        Thread consumer = new(() =>
        {
            started.Set();
            y = s.GetResult();
        });
        producer.Start();
        consumer.Start();
        await Assert.That(started.Wait(WaitTimeout)).IsTrue();
        release.Set();
        await Assert.That(consumer.Join(WaitTimeout)).IsTrue();
        await Assert.That(producer.Join(WaitTimeout)).IsTrue();
        await Assert.That(y).IsEqualTo(ExpectedValue);
        await Assert.That(s.IsCompleted).IsTrue();
    }

    /// <summary>Gets the result blocking throw implementation.</summary>
    /// <param name = "s">The s.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task GetResult_Blocking_ThrowImpl(IAwaitSignal<int> s)
    {
        await Assert.That(s.IsCompleted).IsFalse();
        using ManualResetEventSlim release = new();
        using ManualResetEventSlim started = new();
        InvalidOperationException expectedException = new();
        Thread producer = new(() =>
        {
            if (!release.Wait(WaitTimeout))
            {
                return;
            }

            s.OnError(expectedException);
        });
        Exception? caughtException = null;
        Thread consumer = new(() =>
        {
            started.Set();
            try
            {
                _ = s.GetResult();
            }
            catch (Exception exception)
            {
                caughtException = exception;
            }
        });
        producer.Start();
        consumer.Start();
        await Assert.That(started.Wait(WaitTimeout)).IsTrue();
        release.Set();
        await Assert.That(consumer.Join(WaitTimeout)).IsTrue();
        await Assert.That(producer.Join(WaitTimeout)).IsTrue();
        await Assert.That(caughtException).IsNotNull();
        await Assert.That(caughtException!).IsSameReferenceAs(expectedException);
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
