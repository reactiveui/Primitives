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

    /// <summary>Defines the maximum time to wait for cross-thread test work.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Subscribes the argument checking.</summary>
    [Test]
    public void Subscribe_ArgumentChecking() => Assert.Throws<ArgumentNullException>(() => new AsyncSignal<int>().Subscribe(null!));

    /// <summary>Called when [error argument checking].</summary>
    [Test]
    public void OnError_ArgumentChecking() => Assert.Throws<ArgumentNullException>(() => new AsyncSignal<int>().OnError(null!));

    /// <summary>Awaits the blocking.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Await_Blocking()
    {
        var s = new AsyncSignal<int>();
        await GetResult_BlockingImpl(s.GetAwaiter());
        await Assert.That(s.IsCompleted).IsTrue();
    }

    /// <summary>Awaits the throw.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Await_Throw()
    {
        var s = new AsyncSignal<int>();
        await GetResult_Blocking_ThrowImpl(s.GetAwaiter());
        await Assert.That(s.IsCompleted).IsTrue();
    }

    /// <summary>Gets the result empty.</summary>
    [Test]
    public void GetResult_Empty()
    {
        var s = new AsyncSignal<int>();
        s.OnCompleted();
        Assert.Throws<InvalidOperationException>(() => s.GetResult());
    }

    /// <summary>Gets the result blocking.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetResult_Blocking()
    {
        var s = new AsyncSignal<int>();
        await GetResult_BlockingImpl(s);
        await Assert.That(s.IsCompleted).IsTrue();
    }

    /// <summary>Gets the result blocking throw.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetResult_Blocking_Throw()
    {
        var s = new AsyncSignal<int>();
        await GetResult_Blocking_ThrowImpl(s);
        await Assert.That(s.IsCompleted).IsTrue();
    }

    /// <summary>Gets the result context.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetResult_Context()
    {
        var x = new AsyncSignal<int>();
        var ctx = new MyContext();
        using var registered = new ManualResetEventSlim();
        using var completed = new ManualResetEventSlim();
        var registrationThread = new Thread(() =>
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
        var s = new AsyncSignal<int>();
        await Assert.That(s.HasObservers).IsFalse();
        var d1 = s.Subscribe(_ =>
        {
        });
        await Assert.That(s.HasObservers).IsTrue();
        d1.Dispose();
        await Assert.That(s.HasObservers).IsFalse();
        var d2 = s.Subscribe(_ =>
        {
        });
        await Assert.That(s.HasObservers).IsTrue();
        var d3 = s.Subscribe(_ =>
        {
        });
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
        var s = new AsyncSignal<int>();
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsFalse();
        var d = s.Subscribe(_ =>
        {
        });
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
        var s = new AsyncSignal<int>();
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsFalse();
        var d = s.Subscribe(_ =>
        {
        });
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
        var s = new AsyncSignal<int>();
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
        var s = new AsyncSignal<int>();
        await Assert.That(s.HasObservers).IsFalse();
        var d = s.Subscribe(_ =>
        {
        });
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
        var s = new AsyncSignal<int>();
        await Assert.That(s.HasObservers).IsFalse();
        var d = s.Subscribe(
            _ =>
        {
        },
            _ =>
        {
        });
        await Assert.That(s.HasObservers).IsTrue();
        s.OnNext(ExpectedValue);
        await Assert.That(s.HasObservers).IsTrue();
        s.OnError(new InvalidOperationException());
        await Assert.That(s.HasObservers).IsFalse();
        d.Dispose();
    }

    /// <summary>Gets the result blocking implementation.</summary>
    /// <param name = "s">The s.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task GetResult_BlockingImpl(IAwaitSignal<int> s)
    {
        await Assert.That(s.IsCompleted).IsFalse();
        using var release = new ManualResetEventSlim();
        using var started = new ManualResetEventSlim();
        var producer = new Thread(() =>
        {
            if (!release.Wait(WaitTimeout))
            {
                return;
            }

            s.OnNext(ExpectedValue);
            s.OnCompleted();
        });
        var y = 0;
        var consumer = new Thread(() =>
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
        using var release = new ManualResetEventSlim();
        using var started = new ManualResetEventSlim();
        var expectedException = new InvalidOperationException();
        var producer = new Thread(() =>
        {
            if (!release.Wait(WaitTimeout))
            {
                return;
            }

            s.OnError(expectedException);
        });
        Exception? caughtException = null;
        var consumer = new Thread(() =>
        {
            started.Set();
            try
            {
                s.GetResult();
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
