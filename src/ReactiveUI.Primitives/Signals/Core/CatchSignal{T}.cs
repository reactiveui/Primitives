// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Represents the CatchSignal class.
/// </summary>
/// <typeparam name="T">The T type.</typeparam>
internal sealed class CatchSignal<T> : SignalsBase<T>
{
    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly IEnumerable<IObservable<T>> _sources;

    /// <summary>
    /// Initializes a new instance of the <see cref="CatchSignal{T}"/> class.
    /// </summary>
    /// <param name="sources">The sources value.</param>
    public CatchSignal(IEnumerable<IObservable<T>> sources)
        : base(true) => _sources = sources;

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
        private readonly CatchSignal<T> _parent;

        /// <summary>
        /// Executes the new operation.
        /// </summary>
        /// <returns>The result.</returns>
        private readonly object _gate = new();

        /// <summary>
        /// Stores state for the signal implementation.
        /// </summary>
        private bool _isDisposed;

        /// <summary>
        /// Stores state for the signal implementation.
        /// </summary>
        private IEnumerator<IObservable<T>>? _e;

        /// <summary>
        /// Stores state for the signal implementation.
        /// </summary>
        private SingleReplaceableDisposable? _subscription;

        /// <summary>
        /// Stores state for the signal implementation.
        /// </summary>
        private Exception? _lastException;

        /// <summary>
        /// Stores state for the signal implementation.
        /// </summary>
        private Action? _nextSelf;

        /// <summary>
        /// Initializes a new instance of the <see cref="Catch"/> class.
        /// </summary>
        /// <param name="parent">The parent value.</param>
        /// <param name="observer">The observer value.</param>
        /// <param name="cancel">The cancel value.</param>
        public Catch(CatchSignal<T> parent, IObserver<T> observer, IDisposable cancel)
            : base(observer, cancel) => _parent = parent;

        /// <summary>
        /// Executes the Run operation.
        /// </summary>
        /// <returns>The result.</returns>
        public MultipleDisposable Run()
        {
            _isDisposed = false;
            _e = _parent._sources.GetEnumerator();
            _subscription = new SingleReplaceableDisposable();

            var schedule = Sequencer.Immediate.Schedule(RecursiveRun);

            return new MultipleDisposable(schedule, _subscription, Disposable.Create(() =>
            {
                lock (_gate)
                {
                    _isDisposed = true;
                    _e?.Dispose();
                    _e = null;
                }
            }));
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
            _lastException = error;
            _nextSelf!();
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
                _e?.Dispose();
                _e = null;
                _subscription?.Dispose();
                _subscription = null;
            }

            base.Dispose(disposing);
        }

        /// <summary>
        /// Executes the RecursiveRun operation.
        /// </summary>
        /// <param name="self">The self value.</param>
        private void RecursiveRun(Action self)
        {
            lock (_gate)
            {
                _nextSelf = self;
                if (_isDisposed)
                {
                    return;
                }

                var current = default(IObservable<T>);
                var hasNext = false;
                var ex = default(Exception);

                try
                {
                    hasNext = _e!.MoveNext();
                    if (hasNext)
                    {
                        current = _e.Current ?? throw new InvalidOperationException("sequence is null.");
                    }
                    else
                    {
                        _e.Dispose();
                    }
                }
                catch (Exception exception)
                {
                    ex = exception;
                    _e?.Dispose();
                }

                if (ex != null)
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

                if (!hasNext)
                {
                    if (_lastException != null)
                    {
                        try
                        {
                            Observer.OnError(_lastException);
                        }
                        finally
                        {
                            Dispose();
                        }
                    }
                    else
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

                    return;
                }

                var source = current;
                _subscription?.Create(new SingleDisposable(source!.Subscribe(this)));
            }
        }
    }
}
