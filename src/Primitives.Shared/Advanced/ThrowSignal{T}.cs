// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Represents the ThrowSignal class.</summary>
/// <typeparam name="T">The T type.</typeparam>
internal sealed class ThrowSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>Stores state for the signal implementation.</summary>
    private readonly Exception _error;

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly ISequencer _scheduler;

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly bool _currentThreadRequired;

    /// <summary>Initializes a new instance of the <see cref="ThrowSignal{T}"/> class.</summary>
    /// <param name="error">The error value.</param>
    /// <param name="scheduler">The scheduler value.</param>
    public ThrowSignal(Exception error, ISequencer scheduler)
    {
        _error = error;
        _scheduler = scheduler;
        _currentThreadRequired = scheduler == Sequencer.CurrentThread;
    }

    /// <summary>Executes the IsRequiredSubscribeOnCurrentThread operation.</summary>
    /// <returns>The result.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() => _currentThreadRequired;

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
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

    /// <summary>Executes the SubscribeCore operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="cancel">The cancel value.</param>
    /// <returns>The result.</returns>
    private IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        observer = new Throw(observer, cancel);

        if (_scheduler == Sequencer.Immediate)
        {
            observer.OnError(_error);
            return EmptyDisposable.Instance;
        }

        return _scheduler.Schedule((observer, _error), static (_, state) => SignalError(state));
    }

    /// <summary>Represents the Throw class.</summary>
    private sealed class Throw : IObserver<T>, IDisposable
    {
        /// <summary>Stores the downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>Stores the upstream subscription.</summary>
        private IDisposable? _cancel;

        /// <summary>Disposed latch; 0 when alive, 1 once disposed.</summary>
        private int _disposed;

        /// <summary>Initializes a new instance of the <see cref="Throw"/> class.</summary>
        /// <param name="observer">The observer value.</param>
        /// <param name="cancel">The cancel value.</param>
        public Throw(IObserver<T> observer, IDisposable cancel)
        {
            _cancel = cancel ?? throw new ArgumentNullException(nameof(cancel));
            _observer = observer;
        }

        /// <summary>Executes the OnNext operation.</summary>
        /// <param name="value">The value.</param>
        public void OnNext(T value)
        {
            try
            {
                _observer.OnNext(value);
            }
            catch
            {
                Dispose();
                throw;
            }
        }

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
        public void Dispose() => WitnessTeardown.Dispose(ref _disposed, ref _cancel);
    }
}
