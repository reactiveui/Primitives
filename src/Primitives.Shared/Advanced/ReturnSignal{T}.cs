// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Emits a single value and then completes, on the supplied sequencer.</summary>
/// <typeparam name="T">The emitted value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ReturnSignal: Value = {_value}, Scheduler = {_scheduler}")]
public sealed class ReturnSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>The value to emit.</summary>
    private readonly T _value;

    /// <summary>The sequencer that emits the value and completion.</summary>
    private readonly ISequencer _scheduler;

    /// <summary>Whether subscription must be dispatched through the current-thread sequencer.</summary>
    private readonly bool _currentThreadRequired;

    /// <summary>Initializes a new instance of the <see cref="ReturnSignal{T}"/> class.</summary>
    /// <param name="value">The value to emit.</param>
    /// <param name="scheduler">The sequencer that emits the value and completion.</param>
    public ReturnSignal(T value, ISequencer scheduler)
    {
        _value = value;
        _scheduler = scheduler;
        _currentThreadRequired = scheduler == Sequencer.CurrentThread;
    }

    /// <summary>Gets whether subscription has to be dispatched through the current-thread sequencer.</summary>
    /// <returns><see langword="true"/> when the supplied sequencer is the current-thread sequencer.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => _currentThreadRequired;

    /// <summary>Subscribes an observer that receives the value followed by completion.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>A disposable that cancels the emission when it has not run yet.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<T> observer) =>
        SignalSubscription.Subscribe(observer, _currentThreadRequired, SubscribeCore);

    /// <summary>Emits the value and completion inline for the immediate sequencer, otherwise on the sequencer.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cancel">The subscription handle the guard checks before forwarding.</param>
    /// <returns>The disposable that cancels the scheduled emission.</returns>
    private IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        observer = new GuardedWitness<T>(observer, cancel);

        if (_scheduler == Sequencer.Immediate)
        {
            observer.OnNext(_value);
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        return _scheduler.Schedule(
            (Self: this, observer),
            static (_, s) =>
            {
                s.observer.OnNext(s.Self._value);
                s.observer.OnCompleted();
                return EmptyDisposable.Instance;
            });
    }
}
