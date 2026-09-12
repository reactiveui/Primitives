// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for the behavior signal type.</summary>
public class BehaviorSignalTests
{
    /// <summary>Initial value used by behavior signal value tests.</summary>
    private const int InitialValue = 42;

    /// <summary>First updated value used by behavior signal value tests.</summary>
    private const int FirstUpdatedValue = 43;

    /// <summary>Second updated value used by behavior signal value tests.</summary>
    private const int SecondUpdatedValue = 44;

    /// <summary>Value that should be ignored after completion.</summary>
    private const int IgnoredAfterCompletionValue = 1234;

    /// <summary>The debugger display leaves the latest value and observer set intact.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DebuggerDisplay_PreservesSignalState()
    {
        using BehaviorSignal<int> signal = new(InitialValue);
        await Assert.That(GetDebuggerDisplay(signal)).IsEqualTo(signal.ToString());
        await Assert.That(signal.Value).IsEqualTo(InitialValue);
        await Assert.That(signal.HasObservers).IsFalse();
    }

    /// <summary>Verifies a behavior signal rejects a null observer.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void Subscribe_ArgumentChecking() =>
        Assert.Throws<ArgumentNullException>(static () => new BehaviorSignal<int>(1).Subscribe(null!));

    /// <summary>Verifies a behavior signal rejects a null error.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void OnError_ArgumentChecking() =>
        Assert.Throws<ArgumentNullException>(static () => new BehaviorSignal<int>(1).OnError(null!));

    /// <summary>Verifies a behavior signal tracks observers as subscriptions are added and removed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers()
    {
        BehaviorSignal<int> s = new(InitialValue);
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

    /// <summary>Verifies a behavior signal drops its observers when the signal is disposed first.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers_Dispose1()
    {
        BehaviorSignal<int> s = new(InitialValue);
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

    /// <summary>Verifies a behavior signal drops its observers when the subscription is disposed first.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers_Dispose2()
    {
        BehaviorSignal<int> s = new(InitialValue);
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

    /// <summary>Verifies a behavior signal with no subscribers reports itself as disposed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers_Dispose3()
    {
        BehaviorSignal<int> s = new(InitialValue);
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsFalse();
        s.Dispose();
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsTrue();
    }

    /// <summary>Verifies completion drops a behavior signal's observers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers_OnCompleted()
    {
        BehaviorSignal<int> s = new(InitialValue);
        await Assert.That(s.HasObservers).IsFalse();
        using var subscription = s.Subscribe(static _ => { });
        await Assert.That(s.HasObservers).IsTrue();
        s.OnNext(InitialValue);
        await Assert.That(s.HasObservers).IsTrue();
        s.OnCompleted();
        await Assert.That(s.HasObservers).IsFalse();
    }

    /// <summary>Verifies an error drops a behavior signal's observers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers_OnError()
    {
        BehaviorSignal<int> s = new(InitialValue);
        await Assert.That(s.HasObservers).IsFalse();
        using var subscription = s.Subscribe(
            static _ => { },
            static _ => { });
        await Assert.That(s.HasObservers).IsTrue();
        s.OnNext(InitialValue);
        await Assert.That(s.HasObservers).IsTrue();
        s.OnError(new InvalidOperationException());
        await Assert.That(s.HasObservers).IsFalse();
    }

    /// <summary>Verifies a new behavior signal exposes the initial value it was constructed with.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Value_Initial()
    {
        BehaviorSignal<int> s = new(InitialValue);
        await Assert.That(s.Value).IsEqualTo(InitialValue);
        await Assert.That(s.TryGetValue(out var x)).IsTrue();
        await Assert.That(x).IsEqualTo(InitialValue);
    }

    /// <summary>Verifies a behavior signal's value follows the first emitted value.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Value_First()
    {
        BehaviorSignal<int> s = new(InitialValue);
        await Assert.That(s.Value).IsEqualTo(InitialValue);
        await Assert.That(s.TryGetValue(out var x)).IsTrue();
        await Assert.That(x).IsEqualTo(InitialValue);
        s.OnNext(FirstUpdatedValue);
        await Assert.That(s.Value).IsEqualTo(FirstUpdatedValue);
        await Assert.That(s.TryGetValue(out x)).IsTrue();
        await Assert.That(x).IsEqualTo(FirstUpdatedValue);
    }

    /// <summary>Verifies a behavior signal's value follows each subsequent emitted value.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Value_Second()
    {
        BehaviorSignal<int> s = new(InitialValue);
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

    /// <summary>Verifies a completed behavior signal keeps its last value and ignores later values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Value_FrozenAfterOnCompleted()
    {
        BehaviorSignal<int> s = new(InitialValue);
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

    /// <summary>Verifies reading the value of a faulted behavior signal rethrows its error.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Value_ThrowsAfterOnError()
    {
        BehaviorSignal<int> s = new(InitialValue);
        await Assert.That(s.Value).IsEqualTo(InitialValue);
        s.OnError(new InvalidOperationException());
        _ = Assert.Throws<InvalidOperationException>(() => _ = s.Value);
        _ = Assert.Throws<InvalidOperationException>(() => s.TryGetValue(out _));
    }

    /// <summary>Verifies reading the value of a disposed behavior signal throws.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Value_ThrowsOnDispose()
    {
        BehaviorSignal<int> s = new(InitialValue);
        await Assert.That(s.Value).IsEqualTo(InitialValue);
        s.Dispose();
        _ = Assert.Throws<ObjectDisposedException>(() => _ = s.Value);
        await Assert.That(s.TryGetValue(out _)).IsFalse();
    }

    /// <summary>Reentrant emission follows the initial value promised to a new subscriber.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Subscribe_ReentrantOnNext_FollowsTheInitialValue()
    {
        using BehaviorSignal<int> signal = new(0);
        List<int> values = [];
        using var subscription = signal.Subscribe(value =>
        {
            values.Add(value);
            if (value != 0)
            {
                return;
            }

            signal.OnNext(1);
        });
        await Assert.That(values.SequenceEqual([0, 1])).IsTrue();
    }

    /// <summary>Invokes the getter used by the debugger without reflection.</summary>
    /// <param name="signal">The instance to display.</param>
    /// <returns>The debugger display text.</returns>
#if NET9_0_OR_GREATER
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string? GetDebuggerDisplay(BehaviorSignal<int> signal) => DebuggerAccessor<int>.Read(signal);

    /// <summary>Matches the target type's generic context required by .NET 9 and later.</summary>
    /// <typeparam name="T">The signal's value type.</typeparam>
    private static class DebuggerAccessor<T>
    {
        /// <summary>Invokes the getter evaluated by the debugger.</summary>
        /// <param name="signal">The signal to display.</param>
        /// <returns>The debugger text.</returns>
        [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "get_DebuggerDisplay")]
        internal static extern string? Read(BehaviorSignal<T> signal);
    }
#else
    [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "get_DebuggerDisplay")]
    private static extern string? GetDebuggerDisplay(BehaviorSignal<int> signal);
#endif
}
