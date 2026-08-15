// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Represents the CreateSignal class.</summary>
/// <typeparam name="T">The T type.</typeparam>
/// <typeparam name="TState">The TState type.</typeparam>
internal sealed class CreateSignal<T, TState> : IRequireCurrentThread<T>
{
    /// <summary>Stores state for the signal implementation.</summary>
    private readonly TState _state;

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly Func<TState, IObserver<T>, IDisposable> _subscribe;

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly bool _currentThreadRequired;

    /// <summary>Initializes a new instance of the <see cref="CreateSignal{T,TState}"/> class.</summary>
    /// <param name="state">The state value.</param>
    /// <param name="subscribe">The subscribe value.</param>
    public CreateSignal(TState state, Func<TState, IObserver<T>, IDisposable> subscribe)
    {
        _state = state;
        _subscribe = subscribe;
    }

    /// <summary>Initializes a new instance of the <see cref="CreateSignal{T,TState}"/> class.</summary>
    /// <param name="state">The state value.</param>
    /// <param name="subscribe">The subscribe value.</param>
    /// <param name="isRequiredSubscribeOnCurrentThread">The isRequiredSubscribeOnCurrentThread value.</param>
    public CreateSignal(
        TState state,
        Func<TState, IObserver<T>, IDisposable> subscribe,
        bool isRequiredSubscribeOnCurrentThread)
    {
        _state = state;
        _subscribe = subscribe;
        _currentThreadRequired = isRequiredSubscribeOnCurrentThread;
    }

    /// <summary>Executes the IsRequiredSubscribeOnCurrentThread operation.</summary>
    /// <returns>The result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => _currentThreadRequired;

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<T> observer) =>
        SignalSubscription.Subscribe(observer, _currentThreadRequired, SubscribeCore);

    /// <summary>Executes the SubscribeCore operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="cancel">The cancel value.</param>
    /// <returns>The result.</returns>
    private IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        observer = new Create(observer, cancel);
        return _subscribe(_state, observer) ?? EmptyDisposable.Instance;
    }

    /// <summary>Represents the Create class.</summary>
    private sealed class Create : IObserver<T>, IDisposable
    {
        /// <summary>Stores the downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>Stores the upstream subscription.</summary>
        private IDisposable? _cancel;

        /// <summary>Disposed latch; 0 when alive, 1 once disposed.</summary>
        private int _disposed;

        /// <summary>Initializes a new instance of the <see cref="Create"/> class.</summary>
        /// <param name="observer">The observer value.</param>
        /// <param name="cancel">The cancel value.</param>
        /// <exception cref="ArgumentNullException"><paramref name="cancel"/> is <see langword="null"/>.</exception>
        public Create(IObserver<T> observer, IDisposable cancel)
        {
            _cancel = cancel ?? throw new ArgumentNullException(nameof(cancel));
            _observer = observer;
        }

        /// <summary>Executes the OnNext operation.</summary>
        /// <param name="value">The value.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => _observer.OnNext(value);

        /// <summary>Executes the OnError operation.</summary>
        /// <param name="error">The error value.</param>
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

        /// <summary>Executes the OnCompleted operation.</summary>
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

        /// <summary>Executes the Dispose operation.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => WitnessTeardown.Dispose(ref _disposed, ref _cancel);
    }
}
