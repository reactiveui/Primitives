// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Threading;
using System.Threading.Tasks;
using ReactiveUI.Primitives.Signals;
using TUnit.Core;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// AsyncSignalTests.
/// </summary>
public class AsyncSignalTests
{
    /// <summary>
    /// Subscribes the argument checking.
    /// </summary>
    [Test]
    public void Subscribe_ArgumentChecking() =>
        Assert.Throws<ArgumentNullException>(() => new AsyncSignal<int>().Subscribe(null!));

    /// <summary>
    /// Called when [error argument checking].
    /// </summary>
    [Test]
    public void OnError_ArgumentChecking() =>
        Assert.Throws<ArgumentNullException>(() => new AsyncSignal<int>().OnError(null!));

    /// <summary>
    /// Awaits the blocking.
    /// </summary>
    [Test]
    public void Await_Blocking()
    {
        var s = new AsyncSignal<int>();
        GetResult_BlockingImpl(s.GetAwaiter());
    }

    /// <summary>
    /// Awaits the throw.
    /// </summary>
    [Test]
    public void Await_Throw()
    {
        var s = new AsyncSignal<int>();
        GetResult_Blocking_ThrowImpl(s.GetAwaiter());
    }

    /// <summary>
    /// Gets the result empty.
    /// </summary>
    [Test]
    public void GetResult_Empty()
    {
        var s = new AsyncSignal<int>();
        s.OnCompleted();
        Assert.Throws<InvalidOperationException>(() => s.GetResult());
    }

    /// <summary>
    /// Gets the result blocking.
    /// </summary>
    [Test]
    public void GetResult_Blocking() => GetResult_BlockingImpl(new AsyncSignal<int>());

    /// <summary>
    /// Gets the result blocking throw.
    /// </summary>
    [Test]
    public void GetResult_Blocking_Throw() => GetResult_Blocking_ThrowImpl(new AsyncSignal<int>());

    /// <summary>
    /// Gets the result context.
    /// </summary>
    [Test]
    public void GetResult_Context()
    {
        var x = new AsyncSignal<int>();

        var ctx = new MyContext();
        var e = new ManualResetEvent(false);

        Task.Run(() =>
        {
            SynchronizationContext.SetSynchronizationContext(ctx);

            var a = x.GetAwaiter();
            a.OnCompleted(() => e.Set());
        });

        x.OnNext(42);
        x.OnCompleted();

        e.WaitOne();

        Assert.True(ctx.Ran);
    }

    /// <summary>
    /// Determines whether this instance has observers.
    /// </summary>
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

    /// <summary>
    /// Determines whether [has observers dispose1].
    /// </summary>
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

    /// <summary>
    /// Determines whether [has observers dispose2].
    /// </summary>
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

    /// <summary>
    /// Determines whether [has observers dispose3].
    /// </summary>
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

    /// <summary>
    /// Determines whether [has observers on completed].
    /// </summary>
    [Test]
    public void HasObservers_OnCompleted()
    {
        var s = new AsyncSignal<int>();
        Assert.False(s.HasObservers);

        var d = s.Subscribe(_ => { });
        Assert.True(s.HasObservers);

        s.OnNext(42);
        Assert.True(s.HasObservers);

        s.OnCompleted();
        Assert.False(s.HasObservers);
    }

    /// <summary>
    /// Determines whether [has observers on error].
    /// </summary>
    [Test]
    public void HasObservers_OnError()
    {
        var s = new AsyncSignal<int>();
        Assert.False(s.HasObservers);

        var d = s.Subscribe(_ => { }, _ => { });
        Assert.True(s.HasObservers);

        s.OnNext(42);
        Assert.True(s.HasObservers);

        s.OnError(new Exception());
        Assert.False(s.HasObservers);
    }

    /// <summary>
    /// Gets the result blocking implementation.
    /// </summary>
    /// <param name="s">The s.</param>
    private static void GetResult_BlockingImpl(IAwaitSignal<int> s)
    {
        Assert.False(s.IsCompleted);

        var e = new ManualResetEvent(false);

        new Thread(() =>
        {
            e.WaitOne();
            s.OnNext(42);
            s.OnCompleted();
        }).Start();

        var y = default(int);
        var t = new Thread(() => y = s.GetResult());
        t.Start();

        while (t.ThreadState != ThreadState.WaitSleepJoin)
        {
        }

        e.Set();
        t.Join();

        Assert.Equal(42, y);
        Assert.True(s.IsCompleted);
    }

    /// <summary>
    /// Gets the result blocking throw implementation.
    /// </summary>
    /// <param name="s">The s.</param>
    private static void GetResult_Blocking_ThrowImpl(IAwaitSignal<int> s)
    {
        Assert.False(s.IsCompleted);

        var e = new ManualResetEvent(false);

        var ex = new Exception();

        new Thread(() =>
        {
            e.WaitOne();
            s.OnError(ex);
        }).Start();

        var y = default(Exception);
        var t = new Thread(() =>
        {
            try
            {
                s.GetResult();
            }
            catch (Exception ex_)
            {
                y = ex_;
            }
        });
        t.Start();

        while (t.ThreadState != ThreadState.WaitSleepJoin)
        {
        }

        e.Set();
        t.Join();

        Assert.Same(ex, y);
        Assert.True(s.IsCompleted);
    }

    private class MyContext : SynchronizationContext
    {
        public bool Ran { get; set; }

        public override void Post(SendOrPostCallback d, object? state)
        {
            Ran = true;
            d(state);
        }
    }
}
