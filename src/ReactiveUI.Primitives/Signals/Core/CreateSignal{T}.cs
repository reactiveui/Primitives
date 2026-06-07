// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Represents the CreateSignal class.
/// </summary>
/// <typeparam name="T">The T type.</typeparam>
internal sealed class CreateSignal<T> : SignalsBase<T>
{
    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly Func<IObserver<T>, IDisposable> _subscribe;

    /// <summary>
    /// Initializes a new instance of the <see cref="CreateSignal{T}"/> class.
    /// </summary>
    /// <param name="subscribe">The subscribe value.</param>
    public CreateSignal(Func<IObserver<T>, IDisposable> subscribe)
        : base(false) => _subscribe = subscribe;

    /// <summary>
    /// Initializes a new instance of the <see cref="CreateSignal{T}"/> class.
    /// </summary>
    /// <param name="subscribe">The subscribe value.</param>
    /// <param name="isRequiredSubscribeOnCurrentThread">The isRequiredSubscribeOnCurrentThread value.</param>
    public CreateSignal(Func<IObserver<T>, IDisposable> subscribe, bool isRequiredSubscribeOnCurrentThread)
        : base(isRequiredSubscribeOnCurrentThread) => _subscribe = subscribe;

    /// <inheritdoc/>
    public override IDisposable Subscribe(IObserver<T> observer)
    {
        if (observer == null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        if (IsCurrentThreadSubscriptionRequired)
        {
            return base.Subscribe(observer);
        }

        var sink = new Create(observer);
        sink.SetCancel(_subscribe(sink) ?? EmptyDisposable.Instance);
        return sink;
    }

    /// <summary>
    /// Executes the SubscribeCore operation.
    /// </summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="cancel">The cancel value.</param>
    /// <returns>The result.</returns>
    protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        var sink = new Create(observer, cancel);
        return _subscribe(sink) ?? EmptyDisposable.Instance;
    }

    /// <summary>
    /// Represents the Create class.
    /// </summary>
    private sealed class Create : IDisposable, IObserver<T>
    {
        /// <summary>
        /// Wrapped observer.
        /// </summary>
        private IObserver<T> _observer;

        /// <summary>
        /// Cancellation resource.
        /// </summary>
        private IDisposable? _cancel;

        /// <summary>
        /// Non-zero after disposal or termination.
        /// </summary>
        private int _stopped;

        /// <summary>
        /// Initializes a new instance of the <see cref="Create"/> class.
        /// </summary>
        /// <param name="observer">The observer value.</param>
        public Create(IObserver<T> observer) => _observer = observer;

        /// <summary>
        /// Initializes a new instance of the <see cref="Create"/> class.
        /// </summary>
        /// <param name="observer">The observer value.</param>
        /// <param name="cancel">The cancel value.</param>
        public Create(IObserver<T> observer, IDisposable cancel)
        {
            _observer = observer;
            _cancel = cancel;
        }

        /// <summary>
        /// Assigns the cancellation resource.
        /// </summary>
        /// <param name="cancel">Cancellation resource.</param>
        public void SetCancel(IDisposable cancel)
        {
            if (cancel == null)
            {
                throw new ArgumentNullException(nameof(cancel));
            }

            if (Interlocked.CompareExchange(ref _cancel, cancel, null) != null)
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

        /// <summary>
        /// Executes the OnNext operation.
        /// </summary>
        /// <param name="value">The value.</param>
        public void OnNext(T value)
        {
            if (Volatile.Read(ref _stopped) != 0)
            {
                return;
            }

            _observer.OnNext(value);
        }

        /// <summary>
        /// Executes the OnError operation.
        /// </summary>
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

        /// <summary>
        /// Executes the OnCompleted operation.
        /// </summary>
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
