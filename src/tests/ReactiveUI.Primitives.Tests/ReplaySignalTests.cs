// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>ReplaySignalTests.</summary>
public class ReplaySignalTests
{
    /// <summary>Value emitted while checking observer state.</summary>
    private const int ReplayValue = 42;

    /// <summary>Buffer size of two used across replay signal tests.</summary>
    private const int Two = 2;

    /// <summary>Buffer size of three used across replay signal tests.</summary>
    private const int Three = 3;

    /// <summary>Constructors the argument checking.</summary>
    [Test]
    public void Constructor_ArgumentChecking()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => CreateAndDispose(() => new(-1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => CreateAndDispose(() => new(-1, EmptySequencer.Instance)));
        Assert.Throws<ArgumentOutOfRangeException>(() => CreateAndDispose(() => new(-1, TimeSpan.Zero)));
        Assert.Throws<ArgumentOutOfRangeException>(() => CreateAndDispose(() => new(-1, TimeSpan.Zero, EmptySequencer.Instance)));
        Assert.Throws<ArgumentOutOfRangeException>(() => CreateAndDispose(() => new(TimeSpan.FromTicks(-1))));
        Assert.Throws<ArgumentOutOfRangeException>(() => CreateAndDispose(() => new(TimeSpan.FromTicks(-1), EmptySequencer.Instance)));
        Assert.Throws<ArgumentOutOfRangeException>(() => CreateAndDispose(() => new(0, TimeSpan.FromTicks(-1))));
        Assert.Throws<ArgumentOutOfRangeException>(() => CreateAndDispose(() => new(0, TimeSpan.FromTicks(-1), EmptySequencer.Instance)));
        Assert.Throws<ArgumentNullException>(() => CreateAndDispose(() => new(null!)));
        Assert.Throws<ArgumentNullException>(() => CreateAndDispose(() => new(0, null!)));
        Assert.Throws<ArgumentNullException>(() => CreateAndDispose(() => new(TimeSpan.Zero, null!)));
        Assert.Throws<ArgumentNullException>(() => CreateAndDispose(() => new(0, TimeSpan.Zero, null!)));

        // zero allowed
        CreateAndDispose(() => new(0));
        CreateAndDispose(() => new(TimeSpan.Zero));
        CreateAndDispose(() => new(0, TimeSpan.Zero));
        CreateAndDispose(() => new(0, EmptySequencer.Instance));
        CreateAndDispose(() => new(TimeSpan.Zero, EmptySequencer.Instance));
        CreateAndDispose(() => new(0, TimeSpan.Zero, EmptySequencer.Instance));
        CreateAndDispose(() => new HistorySignal<int>());
        CreateAndDispose(() => new HistorySignal<int>(EmptySequencer.Instance));
        CreateAndDispose(() => new HistorySignal<int>(0));
        CreateAndDispose(() => new HistorySignal<int>(0, EmptySequencer.Instance));
        CreateAndDispose(() => new HistorySignal<int>(TimeSpan.Zero));
        CreateAndDispose(() => new HistorySignal<int>(TimeSpan.Zero, EmptySequencer.Instance));
        CreateAndDispose(() => new HistorySignal<int>(0, TimeSpan.Zero));
        CreateAndDispose(() => new HistorySignal<int>(0, TimeSpan.Zero, EmptySequencer.Instance));
    }

    /// <summary>Determines whether this instance has observers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers()
    {
        await HasObserversImpl(new());
        await HasObserversImpl(new(1));
        await HasObserversImpl(new(Three));
        await HasObserversImpl(new(TimeSpan.FromSeconds(1)));
    }

    /// <summary>Determines whether [has observers dispose1].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers_Dispose1()
    {
        await HasObservers_Dispose1Impl(new());
        await HasObservers_Dispose1Impl(new(1));
        await HasObservers_Dispose1Impl(new(Three));
        await HasObservers_Dispose1Impl(new(TimeSpan.FromSeconds(1)));
    }

    /// <summary>Determines whether [has observers dispose2].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers_Dispose2()
    {
        await HasObservers_Dispose2Impl(new());
        await HasObservers_Dispose2Impl(new(1));
        await HasObservers_Dispose2Impl(new(Three));
        await HasObservers_Dispose2Impl(new(TimeSpan.FromSeconds(1)));
    }

    /// <summary>Determines whether [has observers dispose3].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers_Dispose3()
    {
        await HasObservers_Dispose3Impl(new());
        await HasObservers_Dispose3Impl(new(1));
        await HasObservers_Dispose3Impl(new(Three));
        await HasObservers_Dispose3Impl(new(TimeSpan.FromSeconds(1)));
    }

    /// <summary>Determines whether [has observers on completed].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers_OnCompleted()
    {
        await HasObservers_OnCompletedImpl(new());
        await HasObservers_OnCompletedImpl(new(1));
        await HasObservers_OnCompletedImpl(new(Three));
        await HasObservers_OnCompletedImpl(new(TimeSpan.FromSeconds(1)));
    }

    /// <summary>Determines whether [has observers on error].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers_OnError()
    {
        await HasObservers_OnErrorImpl(new());
        await HasObservers_OnErrorImpl(new(1));
        await HasObservers_OnErrorImpl(new(Three));
        await HasObservers_OnErrorImpl(new(TimeSpan.FromSeconds(1)));
    }

    /// <summary>Called when [error argument checking].</summary>
    [Test]
    public void OnError_ArgumentChecking()
    {
        Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>().OnError(null!));
        Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(1).OnError(null!));
        Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(Two).OnError(null!));
        Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(EmptySequencer.Instance).OnError(null!));
    }

    /// <summary>Subscribes the argument checking.</summary>
    [Test]
    public void Subscribe_ArgumentChecking()
    {
        Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>().Subscribe(null!));
        Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(1).Subscribe(null!));
        Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(Two).Subscribe(null!));
        Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(EmptySequencer.Instance).Subscribe(null!));
    }

    /// <summary>Creates a replay signal and disposes it immediately.</summary>
    /// <param name = "factory">Factory used to create the signal.</param>
    private static void CreateAndDispose(Func<ReplaySignal<int>> factory)
    {
        using var signal = factory();
    }

    /// <summary>Verifies observer state when the source is disposed before subscription disposal.</summary>
    /// <param name = "s">Signal to test.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task HasObservers_Dispose1Impl(ReplaySignal<int> s)
    {
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

    /// <summary>Verifies observer state when the subscription is disposed before the source.</summary>
    /// <param name = "s">Signal to test.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task HasObservers_Dispose2Impl(ReplaySignal<int> s)
    {
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

    /// <summary>Verifies observer state when the source is disposed without subscribers.</summary>
    /// <param name = "s">Signal to test.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task HasObservers_Dispose3Impl(ReplaySignal<int> s)
    {
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsFalse();
        s.Dispose();
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsTrue();
    }

    /// <summary>Verifies observer state after completion.</summary>
    /// <param name = "s">Signal to test.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task HasObservers_OnCompletedImpl(ReplaySignal<int> s)
    {
        await Assert.That(s.HasObservers).IsFalse();
        using var subscription = s.Subscribe(_ =>
        {
        });
        await Assert.That(s.HasObservers).IsTrue();
        s.OnNext(ReplayValue);
        await Assert.That(s.HasObservers).IsTrue();
        s.OnCompleted();
        await Assert.That(s.HasObservers).IsFalse();
    }

    /// <summary>Verifies observer state after error.</summary>
    /// <param name = "s">Signal to test.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task HasObservers_OnErrorImpl(ReplaySignal<int> s)
    {
        await Assert.That(s.HasObservers).IsFalse();
        using var subscription = s.Subscribe(
            _ =>
        {
        },
            _ =>
        {
        });
        await Assert.That(s.HasObservers).IsTrue();
        s.OnNext(ReplayValue);
        await Assert.That(s.HasObservers).IsTrue();
        s.OnError(new InvalidOperationException());
        await Assert.That(s.HasObservers).IsFalse();
    }

    /// <summary>Verifies observer state as subscriptions are added and removed.</summary>
    /// <param name = "s">Signal to test.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task HasObserversImpl(ReplaySignal<int> s)
    {
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
}
