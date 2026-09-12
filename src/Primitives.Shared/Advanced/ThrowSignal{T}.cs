// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Emits a single error on the supplied sequencer without producing any value.</summary>
/// <typeparam name="T">The value type the sequence would have carried.</typeparam>
[System.Diagnostics.DebuggerDisplay("ThrowSignal: Error = {_error}, Scheduler = {_scheduler}")]
public sealed class ThrowSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>The error to emit.</summary>
    private readonly Exception _error;

    /// <summary>The sequencer that emits the error.</summary>
    private readonly ISequencer _scheduler;

    /// <summary>Whether subscription must be dispatched through the current-thread sequencer.</summary>
    private readonly bool _currentThreadRequired;

    /// <summary>Initializes a new instance of the <see cref="ThrowSignal{T}"/> class.</summary>
    /// <param name="error">The error to emit.</param>
    /// <param name="scheduler">The sequencer that emits the error.</param>
    public ThrowSignal(Exception error, ISequencer scheduler)
    {
        _error = error;
        _scheduler = scheduler;
        _currentThreadRequired = scheduler == Sequencer.CurrentThread;
    }

    /// <summary>Gets whether subscription has to be dispatched through the current-thread sequencer.</summary>
    /// <returns><see langword="true"/> when the supplied sequencer is the current-thread sequencer.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => _currentThreadRequired;

    /// <summary>Subscribes an observer that receives the error.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>A disposable that cancels the emission when it has not run yet.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<T> observer) =>
        SignalSubscription.Subscribe(observer, _currentThreadRequired, SubscribeCore);

    /// <summary>Emits the scheduled error notification.</summary>
    /// <param name="state">The observer and error state.</param>
    /// <returns>An empty disposable.</returns>
    private static EmptyDisposable SignalError((IObserver<T> Observer, Exception Error) state)
    {
        state.Observer.OnError(state.Error);
        state.Observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>Emits the error inline for the immediate sequencer, otherwise on the sequencer.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cancel">The subscription handle the guard checks before forwarding.</param>
    /// <returns>The disposable that cancels the scheduled emission.</returns>
    private IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        observer = new GuardedWitness<T>(observer, cancel);

        if (_scheduler == Sequencer.Immediate)
        {
            observer.OnError(_error);
            return EmptyDisposable.Instance;
        }

        return _scheduler.Schedule((observer, _error), static (_, state) => SignalError(state));
    }
}
