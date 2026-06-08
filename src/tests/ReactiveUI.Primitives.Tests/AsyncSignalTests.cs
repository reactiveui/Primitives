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
    public void Subscribe_ArgumentChecking() =>
        Assert.Throws<ArgumentNullException>(() => new AsyncSignal<int>().Subscribe(null!));

    /// <summary>Called when [error argument checking].</summary>
    [Test]
    public void OnError_ArgumentChecking() =>
        Assert.Throws<ArgumentNullException>(() => new AsyncSignal<int>().OnError(null!));

    /// <summary>Awaits the blocking.</summary>
    [Test]
    public void Await_Blocking()
    {
        var s = new AsyncSignal<int>();
        GetResult_BlockingImpl(s.GetAwaiter());

        Assert.True(s.IsCompleted);
    }

    /// <summary>Awaits the throw.</summary>
    [Test]
    public void Await_Throw()
    {
        var s = new AsyncSignal<int>();
        GetResult_Blocking_ThrowImpl(s.GetAwaiter());

        Assert.True(s.IsCompleted);
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
    [Test]
    public void GetResult_Blocking()
    {
        var s = new AsyncSignal<int>();
        GetResult_BlockingImpl(s);

        Assert.True(s.IsCompleted);
    }

    /// <summary>Gets the result blocking throw.</summary>
    [Test]
    public void GetResult_Blocking_Throw()
    {
        var s = new AsyncSignal<int>();
        GetResult_Blocking_ThrowImpl(s);

        Assert.True(s.IsCompleted);
    }

    /// <summary>Gets the result context.</summary>
    [Test]
    public void GetResult_Context()
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

        Assert.True(registered.Wait(WaitTimeout));
        Assert.True(registrationThread.Join(WaitTimeout));

        x.OnNext(ExpectedValue);
        x.OnCompleted();

        Assert.True(completed.Wait(WaitTimeout));

        Assert.True(ctx.Ran);
    }

    /// <summary>Determines whether this instance has observers.</summary>
    [Test]
    public void HasObservers()
    {
        var s = new AsyncSignal<int>();
        Assert.False(s.HasObservers);

        var d1 = s.Subscribe(_ => { });
        Assert.True(s.HasObservers);

        d1.Dispose();
        Assert.False(s.HasObservers);

        var d2 = s.Subscribe(_ => { });
        Assert.True(s.HasObservers);

        var d3 = s.Subscribe(_ => { });
        Assert.True(s.HasObservers);

        d2.Dispose();
        Assert.True(s.HasObservers);

        d3.Dispose();
        Assert.False(s.HasObservers);
    }

    /// <summary>Determines whether [has observers dispose1].</summary>
    [Test]
    public void HasObservers_Dispose1()
    {
        var s = new AsyncSignal<int>();
        Assert.False(s.HasObservers);
        Assert.False(s.IsDisposed);

        var d = s.Subscribe(_ => { });
        Assert.True(s.HasObservers);
        Assert.False(s.IsDisposed);

        s.Dispose();
        Assert.False(s.HasObservers);
        Assert.True(s.IsDisposed);

        d.Dispose();
        Assert.False(s.HasObservers);
        Assert.True(s.IsDisposed);
    }

    /// <summary>Determines whether [has observers dispose2].</summary>
    [Test]
    public void HasObservers_Dispose2()
    {
        var s = new AsyncSignal<int>();
        Assert.False(s.HasObservers);
        Assert.False(s.IsDisposed);

        var d = s.Subscribe(_ => { });
        Assert.True(s.HasObservers);
        Assert.False(s.IsDisposed);

        d.Dispose();
        Assert.False(s.HasObservers);
        Assert.False(s.IsDisposed);

        s.Dispose();
        Assert.False(s.HasObservers);
        Assert.True(s.IsDisposed);
    }

    /// <summary>Determines whether [has observers dispose3].</summary>
    [Test]
    public void HasObservers_Dispose3()
    {
        var s = new AsyncSignal<int>();
        Assert.False(s.HasObservers);
        Assert.False(s.IsDisposed);

        s.Dispose();
        Assert.False(s.HasObservers);
        Assert.True(s.IsDisposed);
    }

    /// <summary>Determines whether [has observers on completed].</summary>
    [Test]
    public void HasObservers_OnCompleted()
    {
        var s = new AsyncSignal<int>();
        Assert.False(s.HasObservers);

        var d = s.Subscribe(_ => { });
        Assert.True(s.HasObservers);

        s.OnNext(ExpectedValue);
        Assert.True(s.HasObservers);

        s.OnCompleted();
        Assert.False(s.HasObservers);

        d.Dispose();
    }

    /// <summary>Determines whether [has observers on error].</summary>
    [Test]
    public void HasObservers_OnError()
    {
        var s = new AsyncSignal<int>();
        Assert.False(s.HasObservers);

        var d = s.Subscribe(_ => { }, _ => { });
        Assert.True(s.HasObservers);

        s.OnNext(ExpectedValue);
        Assert.True(s.HasObservers);

        s.OnError(new InvalidOperationException());
        Assert.False(s.HasObservers);

        d.Dispose();
    }

    /// <summary>Gets the result blocking implementation.</summary>
    /// <param name="s">The s.</param>
    private static void GetResult_BlockingImpl(IAwaitSignal<int> s)
    {
        Assert.False(s.IsCompleted);

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

        Assert.True(started.Wait(WaitTimeout));
        release.Set();
        Assert.True(consumer.Join(WaitTimeout));
        Assert.True(producer.Join(WaitTimeout));

        Assert.Equal(ExpectedValue, y);
        Assert.True(s.IsCompleted);
    }

    /// <summary>Gets the result blocking throw implementation.</summary>
    /// <param name="s">The s.</param>
    private static void GetResult_Blocking_ThrowImpl(IAwaitSignal<int> s)
    {
        Assert.False(s.IsCompleted);

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

        Assert.True(started.Wait(WaitTimeout));
        release.Set();
        Assert.True(consumer.Join(WaitTimeout));
        Assert.True(producer.Join(WaitTimeout));

        Assert.NotNull(caughtException);
        Assert.Same(expectedException, caughtException!);
        Assert.True(s.IsCompleted);
    }

    /// <summary>Captures whether a continuation was posted through the synchronization context.</summary>
    private sealed class MyContext : SynchronizationContext
    {
        /// <summary>Gets a value indicating whether a continuation was posted.</summary>
        public bool Ran { get; private set; }

        /// <inheritdoc/>
        public override void Post(SendOrPostCallback d, object? state)
        {
            if (d is null)
            {
                throw new ArgumentNullException(nameof(d));
            }

            Ran = true;
            d(state);
        }
    }
}
