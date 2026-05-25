// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Represents the CatchSignal class.
/// </summary>
/// <typeparam name="T">The T type.</typeparam>
/// <typeparam name="TException">The TException type.</typeparam>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal sealed class CatchSignal<T, TException> : SignalsBase<T>
        where TException : Exception
{
    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly IObservable<T> _source;

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly Func<TException, IObservable<T>> _errorHandler;

    /// <summary>
    /// Initializes a new instance of the <see cref="CatchSignal{T,TException}"/> class.
    /// </summary>
    /// <param name="source">The source value.</param>
    /// <param name="errorHandler">The errorHandler value.</param>
    public CatchSignal(IObservable<T> source, Func<TException, IObservable<T>> errorHandler)
        : base(true)
    {
        _source = source;
        _errorHandler = errorHandler;
    }

    /// <summary>
    /// Executes the SubscribeCore operation.
    /// </summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="cancel">The cancel value.</param>
    /// <returns>The result.</returns>
    protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel) =>
        new Catch(this, observer, cancel).Run();

    /// <summary>
    /// Represents the Catch class.
    /// </summary>
    private sealed class Catch : WitnessBase<T, T>
    {
        /// <summary>
        /// Stores state for the signal implementation.
        /// </summary>
        private readonly CatchSignal<T, TException> _parent;

        /// <summary>
        /// Stores state for the signal implementation.
        /// </summary>
        private SingleDisposable? _exceptionSubscription;

        /// <summary>
        /// Initializes a new instance of the <see cref="Catch"/> class.
        /// </summary>
        /// <param name="parent">The parent value.</param>
        /// <param name="observer">The observer value.</param>
        /// <param name="cancel">The cancel value.</param>
        public Catch(CatchSignal<T, TException> parent, IObserver<T> observer, IDisposable cancel)
            : base(observer, cancel) => _parent = parent;

        /// <summary>
        /// Executes the Run operation.
        /// </summary>
        /// <returns>The result.</returns>
        public MultipleDisposable Run()
        {
            _exceptionSubscription = new SingleDisposable();
            var sourceSubscription = new SingleDisposable(_parent._source.Subscribe(this));

            return new MultipleDisposable(sourceSubscription, _exceptionSubscription);
        }

        /// <summary>
        /// Executes the OnNext operation.
        /// </summary>
        /// <param name="value">The value.</param>
        public override void OnNext(T value) => Observer.OnNext(value);

        /// <summary>
        /// Executes the OnError operation.
        /// </summary>
        /// <param name="error">The error value.</param>
        public override void OnError(Exception error)
        {
            if (error is TException e)
            {
                IObservable<T> next;
                try
                {
                    next = _parent._errorHandler == Handle.CatchIgnore<T> ? Signal.Empty<T>() : _parent._errorHandler(e);
                }
                catch (Exception ex)
                {
                    try
                    {
                        Observer.OnError(ex);
                    }
                    finally
                    {
                        Dispose();
                    }

                    return;
                }

                _exceptionSubscription?.Create(next.Subscribe(Observer));
            }
            else
            {
                try
                {
                    Observer.OnError(error);
                }
                finally
                {
                    Dispose();
                }
            }
        }

        /// <summary>
        /// Executes the OnCompleted operation.
        /// </summary>
        public override void OnCompleted()
        {
            try
            {
                Observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }

        /// <summary>
        /// Executes the Dispose operation.
        /// </summary>
        /// <param name="disposing">The disposing value.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _exceptionSubscription?.Dispose();
                _exceptionSubscription = null;
            }

            base.Dispose(disposing);
        }
    }
}
