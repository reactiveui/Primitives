// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using ReactiveUI.Primitives.Signals;
using TUnit.Core;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// BehaviourSignalTests.
/// </summary>
public class BehaviourSignalTests
{
    /// <summary>
    /// Subscribes the argument checking.
    /// </summary>
    [Test]
    public void Subscribe_ArgumentChecking() =>
        Assert.Throws<ArgumentNullException>(() => new BehaviourSignal<int>(1).Subscribe(null!));

    /// <summary>
    /// Called when [error argument checking].
    /// </summary>
    [Test]
    public void OnError_ArgumentChecking() =>
        Assert.Throws<ArgumentNullException>(() => new BehaviourSignal<int>(1).OnError(null!));

    /// <summary>
    /// Determines whether this instance has observers.
    /// </summary>
    [Test]
    public void HasObservers()
    {
        var s = new BehaviourSignal<int>(42);
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
        var s = new BehaviourSignal<int>(42);
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
        var s = new BehaviourSignal<int>(42);
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
        var s = new BehaviourSignal<int>(42);
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
        var s = new BehaviourSignal<int>(42);
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
        var s = new BehaviourSignal<int>(42);
        Assert.False(s.HasObservers);

        var d = s.Subscribe(_ => { }, _ => { });
        Assert.True(s.HasObservers);

        s.OnNext(42);
        Assert.True(s.HasObservers);

        s.OnError(new Exception());
        Assert.False(s.HasObservers);
    }

    /// <summary>
    /// Values the initial.
    /// </summary>
    [Test]
    public void Value_Initial()
    {
        var s = new BehaviourSignal<int>(42);
        Assert.Equal(42, s.Value);

        Assert.True(s.TryGetValue(out var x));
        Assert.Equal(42, x);
    }

    /// <summary>
    /// Values the first.
    /// </summary>
    [Test]
    public void Value_First()
    {
        var s = new BehaviourSignal<int>(42);
        Assert.Equal(42, s.Value);

        Assert.True(s.TryGetValue(out var x));
        Assert.Equal(42, x);

        s.OnNext(43);
        Assert.Equal(43, s.Value);

        Assert.True(s.TryGetValue(out x));
        Assert.Equal(43, x);
    }

    /// <summary>
    /// Values the second.
    /// </summary>
    [Test]
    public void Value_Second()
    {
        var s = new BehaviourSignal<int>(42);
        Assert.Equal(42, s.Value);

        Assert.True(s.TryGetValue(out var x));
        Assert.Equal(42, x);

        s.OnNext(43);
        Assert.Equal(43, s.Value);

        Assert.True(s.TryGetValue(out x));
        Assert.Equal(43, x);

        s.OnNext(44);
        Assert.Equal(44, s.Value);

        Assert.True(s.TryGetValue(out x));
        Assert.Equal(44, x);
    }

    /// <summary>
    /// Values the frozen after on completed.
    /// </summary>
    [Test]
    public void Value_FrozenAfterOnCompleted()
    {
        var s = new BehaviourSignal<int>(42);
        Assert.Equal(42, s.Value);

        Assert.True(s.TryGetValue(out var x));
        Assert.Equal(42, x);

        s.OnNext(43);
        Assert.Equal(43, s.Value);

        Assert.True(s.TryGetValue(out x));
        Assert.Equal(43, x);

        s.OnNext(44);
        Assert.Equal(44, s.Value);

        Assert.True(s.TryGetValue(out x));
        Assert.Equal(44, x);

        s.OnCompleted();
        Assert.Equal(44, s.Value);

        Assert.True(s.TryGetValue(out x));
        Assert.Equal(44, x);

        s.OnNext(1234);
        Assert.Equal(44, s.Value);

        Assert.True(s.TryGetValue(out x));
        Assert.Equal(44, x);
    }

    /// <summary>
    /// Values the throws after on error.
    /// </summary>
    [Test]
    public void Value_ThrowsAfterOnError()
    {
        var s = new BehaviourSignal<int>(42);
        Assert.Equal(42, s.Value);

        s.OnError(new InvalidOperationException());

        Assert.Throws<InvalidOperationException>(() =>
        {
            var ignored = s.Value;
        });

        Assert.Throws<InvalidOperationException>(() => s.TryGetValue(out var x));
    }

    /// <summary>
    /// Values the throws on dispose.
    /// </summary>
    [Test]
    public void Value_ThrowsOnDispose()
    {
        var s = new BehaviourSignal<int>(42);
        Assert.Equal(42, s.Value);

        s.Dispose();

        Assert.Throws<ObjectDisposedException>(() =>
        {
            var ignored = s.Value;
        });

        Assert.False(s.TryGetValue(out var x));
    }
}
