// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;
using TUnit.Core;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// ReplaySignalTests.
/// </summary>
public class ReplaySignalTests
{
    /// <summary>
    /// Constructors the argument checking.
    /// </summary>
    [Test]
    public void Constructor_ArgumentChecking()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new ReplaySignal<int>(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ReplaySignal<int>(-1, EmptySequencer.Instance));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ReplaySignal<int>(-1, TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ReplaySignal<int>(-1, TimeSpan.Zero, EmptySequencer.Instance));

        Assert.Throws<ArgumentOutOfRangeException>(() => new ReplaySignal<int>(TimeSpan.FromTicks(-1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ReplaySignal<int>(TimeSpan.FromTicks(-1), EmptySequencer.Instance));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ReplaySignal<int>(0, TimeSpan.FromTicks(-1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ReplaySignal<int>(0, TimeSpan.FromTicks(-1), EmptySequencer.Instance));

        Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(null!));
        Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(0, null!));
        Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(TimeSpan.Zero, null!));
        Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(0, TimeSpan.Zero, null!));

        // zero allowed
        new ReplaySignal<int>(0);
        new ReplaySignal<int>(TimeSpan.Zero);
        new ReplaySignal<int>(0, TimeSpan.Zero);
        new ReplaySignal<int>(0, EmptySequencer.Instance);
        new ReplaySignal<int>(TimeSpan.Zero, EmptySequencer.Instance);
        new ReplaySignal<int>(0, TimeSpan.Zero, EmptySequencer.Instance);
    }

    /// <summary>
    /// Determines whether this instance has observers.
    /// </summary>
    [Test]
    public void HasObservers()
    {
        HasObserversImpl(new ReplaySignal<int>());
        HasObserversImpl(new ReplaySignal<int>(1));
        HasObserversImpl(new ReplaySignal<int>(3));
        HasObserversImpl(new ReplaySignal<int>(TimeSpan.FromSeconds(1)));
    }

    /// <summary>
    /// Determines whether [has observers dispose1].
    /// </summary>
    [Test]
    public void HasObservers_Dispose1()
    {
        HasObservers_Dispose1Impl(new ReplaySignal<int>());
        HasObservers_Dispose1Impl(new ReplaySignal<int>(1));
        HasObservers_Dispose1Impl(new ReplaySignal<int>(3));
        HasObservers_Dispose1Impl(new ReplaySignal<int>(TimeSpan.FromSeconds(1)));
    }

    /// <summary>
    /// Determines whether [has observers dispose2].
    /// </summary>
    [Test]
    public void HasObservers_Dispose2()
    {
        HasObservers_Dispose2Impl(new ReplaySignal<int>());
        HasObservers_Dispose2Impl(new ReplaySignal<int>(1));
        HasObservers_Dispose2Impl(new ReplaySignal<int>(3));
        HasObservers_Dispose2Impl(new ReplaySignal<int>(TimeSpan.FromSeconds(1)));
    }

    /// <summary>
    /// Determines whether [has observers dispose3].
    /// </summary>
    [Test]
    public void HasObservers_Dispose3()
    {
        HasObservers_Dispose3Impl(new ReplaySignal<int>());
        HasObservers_Dispose3Impl(new ReplaySignal<int>(1));
        HasObservers_Dispose3Impl(new ReplaySignal<int>(3));
        HasObservers_Dispose3Impl(new ReplaySignal<int>(TimeSpan.FromSeconds(1)));
    }

    /// <summary>
    /// Determines whether [has observers on completed].
    /// </summary>
    [Test]
    public void HasObservers_OnCompleted()
    {
        HasObservers_OnCompletedImpl(new ReplaySignal<int>());
        HasObservers_OnCompletedImpl(new ReplaySignal<int>(1));
        HasObservers_OnCompletedImpl(new ReplaySignal<int>(3));
        HasObservers_OnCompletedImpl(new ReplaySignal<int>(TimeSpan.FromSeconds(1)));
    }

    /// <summary>
    /// Determines whether [has observers on error].
    /// </summary>
    [Test]
    public void HasObservers_OnError()
    {
        HasObservers_OnErrorImpl(new ReplaySignal<int>());
        HasObservers_OnErrorImpl(new ReplaySignal<int>(1));
        HasObservers_OnErrorImpl(new ReplaySignal<int>(3));
        HasObservers_OnErrorImpl(new ReplaySignal<int>(TimeSpan.FromSeconds(1)));
    }

    /// <summary>
    /// Called when [error argument checking].
    /// </summary>
    [Test]
    public void OnError_ArgumentChecking()
    {
        Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>().OnError(null!));
        Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(1).OnError(null!));
        Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(2).OnError(null!));
        Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(EmptySequencer.Instance).OnError(null!));
    }

    /// <summary>
    /// Subscribes the argument checking.
    /// </summary>
    [Test]
    public void Subscribe_ArgumentChecking()
    {
        Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>().Subscribe(null!));
        Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(1).Subscribe(null!));
        Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(2).Subscribe(null!));
        Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(EmptySequencer.Instance).Subscribe(null!));
    }

    private static void HasObservers_Dispose1Impl(ReplaySignal<int> s)
    {
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

    private static void HasObservers_Dispose2Impl(ReplaySignal<int> s)
    {
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

    private static void HasObservers_Dispose3Impl(ReplaySignal<int> s)
    {
        Assert.False(s.HasObservers);
        Assert.False(s.IsDisposed);

        s.Dispose();
        Assert.False(s.HasObservers);
        Assert.True(s.IsDisposed);
    }

    private static void HasObservers_OnCompletedImpl(ReplaySignal<int> s)
    {
        Assert.False(s.HasObservers);

        var d = s.Subscribe(_ => { });
        Assert.True(s.HasObservers);

        s.OnNext(42);
        Assert.True(s.HasObservers);

        s.OnCompleted();
        Assert.False(s.HasObservers);
    }

    private static void HasObservers_OnErrorImpl(ReplaySignal<int> s)
    {
        Assert.False(s.HasObservers);

        var d = s.Subscribe(_ => { }, _ => { });
        Assert.True(s.HasObservers);

        s.OnNext(42);
        Assert.True(s.HasObservers);

        s.OnError(new Exception());
        Assert.False(s.HasObservers);
    }

    private static void HasObserversImpl(ReplaySignal<int> s)
    {
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
}
