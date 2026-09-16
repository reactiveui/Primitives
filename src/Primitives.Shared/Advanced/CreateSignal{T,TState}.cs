// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Creates a signal from a subscribe delegate that receives a caller-supplied state value.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <typeparam name="TState">The state type handed to the subscribe delegate.</typeparam>
internal sealed class CreateSignal<T, TState> : IRequireCurrentThread<T>
{
    /// <summary>The state handed to the subscribe delegate.</summary>
    private readonly TState _state;

    /// <summary>The delegate invoked for each subscription.</summary>
    private readonly Func<TState, IObserver<T>, IDisposable> _subscribe;

    /// <summary>Whether subscription must be dispatched through the current-thread sequencer.</summary>
    private readonly bool _currentThreadRequired;

    /// <summary>Initializes a new instance of the <see cref="CreateSignal{T,TState}"/> class.</summary>
    /// <param name="state">The state handed to the subscribe delegate.</param>
    /// <param name="subscribe">The delegate invoked for each subscription.</param>
    public CreateSignal(TState state, Func<TState, IObserver<T>, IDisposable> subscribe)
    {
        _state = state;
        _subscribe = subscribe;
    }

    /// <summary>Initializes a new instance of the <see cref="CreateSignal{T,TState}"/> class.</summary>
    /// <param name="state">The state handed to the subscribe delegate.</param>
    /// <param name="subscribe">The delegate invoked for each subscription.</param>
    /// <param name="isRequiredSubscribeOnCurrentThread">Whether subscription must be dispatched through the current-thread sequencer.</param>
    public CreateSignal(
        TState state,
        Func<TState, IObserver<T>, IDisposable> subscribe,
        bool isRequiredSubscribeOnCurrentThread)
    {
        _state = state;
        _subscribe = subscribe;
        _currentThreadRequired = isRequiredSubscribeOnCurrentThread;
    }

    /// <summary>Reports whether subscription must be dispatched through the current-thread sequencer.</summary>
    /// <returns><see langword="true"/> when current-thread dispatch is required.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => _currentThreadRequired;

    /// <summary>Invokes the subscribe delegate with the state and a wrapper around the observer.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The disposable that releases the subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<T> observer) =>
        SignalSubscription.Subscribe(observer, _currentThreadRequired, SubscribeCore);

    /// <summary>Invokes the subscribe delegate with a wrapper that owns <paramref name="cancel"/>.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cancel">The outer subscription handle.</param>
    /// <returns>The disposable returned by the subscribe delegate.</returns>
    private IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        observer = new Create(observer, cancel);
        return _subscribe(_state, observer) ?? EmptyDisposable.Instance;
    }

    /// <summary>Forwards notifications downstream and releases the subscription on termination.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cancel">The outer subscription handle.</param>
    private sealed class Create(IObserver<T> observer, IDisposable cancel) : IObserver<T>, IDisposable
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer = observer;

        /// <summary>The outer subscription handle released on teardown.</summary>
        private IDisposable? _cancel = cancel;

        /// <summary>Disposed latch; 0 when alive, 1 once disposed.</summary>
        private int _disposed;

        /// <summary>Forwards a value downstream.</summary>
        /// <param name="value">The value to forward.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => _observer.OnNext(value);

        /// <summary>Forwards the error downstream and releases the subscription.</summary>
        /// <param name="error">The error to forward.</param>
        public void OnError(Exception error)
        {
            try
            {
                _observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <summary>Completes downstream and releases the subscription.</summary>
        public void OnCompleted()
        {
            try
            {
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }

        /// <summary>Releases the outer subscription handle once.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => WitnessTeardown.Dispose(ref _disposed, ref _cancel);
    }
}
