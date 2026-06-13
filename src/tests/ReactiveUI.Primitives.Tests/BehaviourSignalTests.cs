// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>BehaviourSignalTests.</summary>
public class BehaviourSignalTests
{
    /// <summary>Initial value used by behavior signal value tests.</summary>
    private const int InitialValue = 42;

    /// <summary>First updated value used by behavior signal value tests.</summary>
    private const int FirstUpdatedValue = 43;

    /// <summary>Second updated value used by behavior signal value tests.</summary>
    private const int SecondUpdatedValue = 44;

    /// <summary>Value that should be ignored after completion.</summary>
    private const int IgnoredAfterCompletionValue = 1234;

    /// <summary>Subscribes the argument checking.</summary>
    [Test]
    public void Subscribe_ArgumentChecking() => Assert.Throws<ArgumentNullException>(() => new BehaviorSignal<int>(1).Subscribe(null!));

    /// <summary>Called when [error argument checking].</summary>
    [Test]
    public void OnError_ArgumentChecking() => Assert.Throws<ArgumentNullException>(() => new BehaviorSignal<int>(1).OnError(null!));

    /// <summary>Determines whether this instance has observers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers()
    {
        var s = new BehaviorSignal<int>(42);
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
        var s = new BehaviorSignal<int>(42);
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
        var s = new BehaviorSignal<int>(42);
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
        var s = new BehaviorSignal<int>(42);
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
        var s = new BehaviorSignal<int>(42);
        await Assert.That(s.HasObservers).IsFalse();
        using var subscription = s.Subscribe(_ =>
        {
        });
        await Assert.That(s.HasObservers).IsTrue();
        s.OnNext(InitialValue);
        await Assert.That(s.HasObservers).IsTrue();
        s.OnCompleted();
        await Assert.That(s.HasObservers).IsFalse();
    }

    /// <summary>Determines whether [has observers on error].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers_OnError()
    {
        var s = new BehaviorSignal<int>(42);
        await Assert.That(s.HasObservers).IsFalse();
        using var subscription = s.Subscribe(
            _ =>
        {
        },
            _ =>
        {
        });
        await Assert.That(s.HasObservers).IsTrue();
        s.OnNext(InitialValue);
        await Assert.That(s.HasObservers).IsTrue();
        s.OnError(new InvalidOperationException());
        await Assert.That(s.HasObservers).IsFalse();
    }

    /// <summary>Values the initial.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Value_Initial()
    {
        var s = new BehaviorSignal<int>(InitialValue);
        await Assert.That(s.Value).IsEqualTo(InitialValue);
        await Assert.That(s.TryGetValue(out var x)).IsTrue();
        await Assert.That(x).IsEqualTo(InitialValue);
    }

    /// <summary>Values the first.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Value_First()
    {
        var s = new BehaviorSignal<int>(InitialValue);
        await Assert.That(s.Value).IsEqualTo(InitialValue);
        await Assert.That(s.TryGetValue(out var x)).IsTrue();
        await Assert.That(x).IsEqualTo(InitialValue);
        s.OnNext(FirstUpdatedValue);
        await Assert.That(s.Value).IsEqualTo(FirstUpdatedValue);
        await Assert.That(s.TryGetValue(out x)).IsTrue();
        await Assert.That(x).IsEqualTo(FirstUpdatedValue);
    }

    /// <summary>Values the second.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Value_Second()
    {
        var s = new BehaviorSignal<int>(InitialValue);
        await Assert.That(s.Value).IsEqualTo(InitialValue);
        await Assert.That(s.TryGetValue(out var x)).IsTrue();
        await Assert.That(x).IsEqualTo(InitialValue);
        s.OnNext(FirstUpdatedValue);
        await Assert.That(s.Value).IsEqualTo(FirstUpdatedValue);
        await Assert.That(s.TryGetValue(out x)).IsTrue();
        await Assert.That(x).IsEqualTo(FirstUpdatedValue);
        s.OnNext(SecondUpdatedValue);
        await Assert.That(s.Value).IsEqualTo(SecondUpdatedValue);
        await Assert.That(s.TryGetValue(out x)).IsTrue();
        await Assert.That(x).IsEqualTo(SecondUpdatedValue);
    }

    /// <summary>Values the frozen after on completed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Value_FrozenAfterOnCompleted()
    {
        var s = new BehaviorSignal<int>(InitialValue);
        await Assert.That(s.Value).IsEqualTo(InitialValue);
        await Assert.That(s.TryGetValue(out var x)).IsTrue();
        await Assert.That(x).IsEqualTo(InitialValue);
        s.OnNext(FirstUpdatedValue);
        await Assert.That(s.Value).IsEqualTo(FirstUpdatedValue);
        await Assert.That(s.TryGetValue(out x)).IsTrue();
        await Assert.That(x).IsEqualTo(FirstUpdatedValue);
        s.OnNext(SecondUpdatedValue);
        await Assert.That(s.Value).IsEqualTo(SecondUpdatedValue);
        await Assert.That(s.TryGetValue(out x)).IsTrue();
        await Assert.That(x).IsEqualTo(SecondUpdatedValue);
        s.OnCompleted();
        await Assert.That(s.Value).IsEqualTo(SecondUpdatedValue);
        await Assert.That(s.TryGetValue(out x)).IsTrue();
        await Assert.That(x).IsEqualTo(SecondUpdatedValue);
        s.OnNext(IgnoredAfterCompletionValue);
        await Assert.That(s.Value).IsEqualTo(SecondUpdatedValue);
        await Assert.That(s.TryGetValue(out x)).IsTrue();
        await Assert.That(x).IsEqualTo(SecondUpdatedValue);
    }

    /// <summary>Values the throws after on error.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Value_ThrowsAfterOnError()
    {
        var s = new BehaviorSignal<int>(InitialValue);
        await Assert.That(s.Value).IsEqualTo(InitialValue);
        s.OnError(new InvalidOperationException());
        Assert.Throws<InvalidOperationException>(() => _ = s.Value);
        Assert.Throws<InvalidOperationException>(() => s.TryGetValue(out _));
    }

    /// <summary>Values the throws on dispose.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Value_ThrowsOnDispose()
    {
        var s = new BehaviorSignal<int>(InitialValue);
        await Assert.That(s.Value).IsEqualTo(InitialValue);
        s.Dispose();
        Assert.Throws<ObjectDisposedException>(() => _ = s.Value);
        await Assert.That(s.TryGetValue(out _)).IsFalse();
    }
}
