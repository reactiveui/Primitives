// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Emits the same value endlessly until the subscription is disposed.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("LoopSignal: Value = {Value}")]
public sealed class LoopSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>Whether subscription must run on the current thread.</summary>
    private readonly bool _currentThreadRequired;

    /// <summary>Initializes a new instance of the <see cref="LoopSignal{T}"/> class.</summary>
    /// <param name="value">The value repeated by the signal.</param>
    public LoopSignal(T value)
    {
        _currentThreadRequired = true;
        Value = value;
    }

    /// <summary>Gets the repeated value.</summary>
    internal T Value { get; }

    /// <summary>Reports that subscription runs on the current-thread sequencer.</summary>
    /// <returns>Always <see langword="true"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => _currentThreadRequired;

    /// <summary>Subscribes the observer and starts the endless emission loop.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The disposable that stops the loop.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<T> observer) =>
        SignalSubscription.Subscribe(observer, _currentThreadRequired, SubscribeCore);

    /// <summary>Checks whether a cancellation handle has been disposed.</summary>
    /// <param name="disposable">The cancellation handle.</param>
    /// <returns><see langword="true"/> when the handle is disposed.</returns>
    private static bool IsDisposed(IDisposable disposable) =>
        disposable is IsDisposed state && state.IsDisposed;

    /// <summary>Schedules the recursive emission loop, which stops as soon as the handle is disposed.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cancel">The outer subscription handle.</param>
    /// <returns>The disposable that cancels the scheduled loop.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Allocations",
        "PSH1011",
        Justification = "No state-taking recursive Schedule overload exists.")]
    private IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel) =>
        Sequencer.CurrentThread.Schedule(self =>
        {
            if (IsDisposed(cancel))
            {
                return;
            }

            observer.OnNext(Value);
            if (IsDisposed(cancel))
            {
                return;
            }

            self();
        });
}
