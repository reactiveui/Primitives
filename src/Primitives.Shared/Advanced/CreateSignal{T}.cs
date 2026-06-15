// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Represents the CreateSignal class.</summary>
/// <typeparam name="T">The T type.</typeparam>
internal sealed class CreateSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>Stores state for the signal implementation.</summary>
    private readonly Func<IObserver<T>, IDisposable> _subscribe;

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly bool _currentThreadRequired;

    /// <summary>Initializes a new instance of the <see cref="CreateSignal{T}"/> class.</summary>
    /// <param name="subscribe">The subscribe value.</param>
    public CreateSignal(Func<IObserver<T>, IDisposable> subscribe) => _subscribe = subscribe;

    /// <summary>Initializes a new instance of the <see cref="CreateSignal{T}"/> class.</summary>
    /// <param name="subscribe">The subscribe value.</param>
    /// <param name="isRequiredSubscribeOnCurrentThread">The isRequiredSubscribeOnCurrentThread value.</param>
    public CreateSignal(Func<IObserver<T>, IDisposable> subscribe, bool isRequiredSubscribeOnCurrentThread)
    {
        _subscribe = subscribe;
        _currentThreadRequired = isRequiredSubscribeOnCurrentThread;
    }

    /// <summary>Executes the IsRequiredSubscribeOnCurrentThread operation.</summary>
    /// <returns>The result.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() => _currentThreadRequired;

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (_currentThreadRequired)
        {
            return SignalSubscription.Subscribe(observer, _currentThreadRequired, SubscribeCore);
        }

        Create sink = new(observer);
        sink.SetCancel(_subscribe(sink) ?? EmptyDisposable.Instance);
        return sink;
    }

    /// <summary>Executes the SubscribeCore operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="cancel">The cancel value.</param>
    /// <returns>The result.</returns>
    private IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        Create sink = new(observer, cancel);
        return _subscribe(sink) ?? EmptyDisposable.Instance;
    }

    /// <summary>Represents the Create class.</summary>
    private sealed class Create : IDisposable, IObserver<T>
    {
        /// <summary>Wrapped observer.</summary>
        private IObserver<T> _observer;

        /// <summary>Cancellation resource.</summary>
        private IDisposable? _cancel;

        /// <summary>Non-zero after disposal or termination.</summary>
        private int _stopped;

        /// <summary>Initializes a new instance of the <see cref="Create"/> class.</summary>
        /// <param name="observer">The observer value.</param>
        public Create(IObserver<T> observer) => _observer = observer;

        /// <summary>Initializes a new instance of the <see cref="Create"/> class.</summary>
        /// <param name="observer">The observer value.</param>
        /// <param name="cancel">The cancel value.</param>
        public Create(IObserver<T> observer, IDisposable cancel)
        {
            _observer = observer;
            _cancel = cancel;
        }

        /// <summary>Assigns the cancellation resource.</summary>
        /// <param name="cancel">Cancellation resource.</param>
        public void SetCancel(IDisposable cancel)
        {
            ArgumentExceptionHelper.ThrowIfNull(cancel);

            if (Interlocked.CompareExchange(ref _cancel, cancel, null) is not null)
            {
                cancel.Dispose();
                return;
            }

            if (Volatile.Read(ref _stopped) == 0)
            {
                return;
            }

            Interlocked.Exchange(ref _cancel, null)?.Dispose();
        }

        /// <summary>Executes the OnNext operation.</summary>
        /// <param name="value">The value.</param>
        public void OnNext(T value)
        {
            if (Volatile.Read(ref _stopped) != 0)
            {
                return;
            }

            _observer.OnNext(value);
        }

        /// <summary>Executes the OnError operation.</summary>
        /// <param name="error">The error value.</param>
        public void OnError(Exception error)
        {
            if (Interlocked.Exchange(ref _stopped, 1) != 0)
            {
                return;
            }

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
            if (Interlocked.Exchange(ref _stopped, 1) != 0)
            {
                return;
            }

            try
            {
                _observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            _observer = EmptyWitness<T>.Instance;
            Interlocked.Exchange(ref _cancel, null)?.Dispose();
            Volatile.Write(ref _stopped, 1);
        }
    }
}
