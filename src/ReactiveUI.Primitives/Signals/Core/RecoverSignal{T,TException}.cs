// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Dedicated cold signal for <c>Recover</c>/<c>Resume</c> (catch a typed error and switch to a
/// handler-selected sequence). Replaces the witness-framework subject with a lightweight sink that
/// holds its source and fallback subscriptions in a single pocket.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
/// <typeparam name="TException">The handled exception type.</typeparam>
internal sealed class RecoverSignal<T, TException> : IRequireCurrentThread<T>
    where TException : Exception
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The handler that selects the fallback sequence for a caught error.</summary>
    private readonly Func<TException, IObservable<T>> _handler;

    /// <summary>Initializes a new instance of the <see cref="RecoverSignal{T, TException}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="handler">The handler that selects the fallback sequence for a caught error.</param>
    internal RecoverSignal(IObservable<T> source, Func<TException, IObservable<T>> handler)
    {
        _source = source;
        _handler = handler;
    }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() => true;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        if (observer == null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        if (!Sequencer.CurrentThread.IsScheduleRequired)
        {
            return Run(observer);
        }

        var subscription = new SingleDisposable();
        Sequencer.CurrentThread.Schedule(() => subscription.Create(Run(observer)));
        return subscription;
    }

    /// <summary>Builds the sink and subscribes it to the source.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription pocket.</returns>
    private MultipleDisposable Run(IObserver<T> observer) => new RecoverObserver(observer, _handler).Run(_source);

    /// <summary>Forwards source values and, on a caught error, switches to the fallback sequence.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "CA1001:Types that own disposable fields should be disposable",
        Justification = "The pocket is the subscription returned from Run; its lifetime is owned by the subscriber.")]
    private sealed class RecoverObserver : IObserver<T>
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>The handler that selects the fallback sequence.</summary>
        private readonly Func<TException, IObservable<T>> _handler;

        /// <summary>Holds the source subscription and, after a caught error, the fallback subscription.</summary>
        private MultipleDisposable? _pocket;

        /// <summary>Initializes a new instance of the <see cref="RecoverObserver"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="handler">The handler that selects the fallback sequence.</param>
        internal RecoverObserver(IObserver<T> observer, Func<TException, IObservable<T>> handler)
        {
            _observer = observer;
            _handler = handler;
        }

        /// <inheritdoc/>
        public void OnNext(T value) => _observer.OnNext(value);

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (error is TException typed)
            {
                IObservable<T> next;
                try
                {
                    next = _handler == Handle.CatchIgnore<T> ? Signal.None<T>() : _handler(typed);
                }
                catch (Exception handlerError)
                {
                    _observer.OnError(handlerError);
                    _pocket?.Dispose();
                    return;
                }

                _pocket?.Add(next.Subscribe(_observer));
            }
            else
            {
                _observer.OnError(error);
                _pocket?.Dispose();
            }
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            _observer.OnCompleted();
            _pocket?.Dispose();
        }

        /// <summary>Subscribes to the source and returns the subscription pocket.</summary>
        /// <param name="source">The source observable.</param>
        /// <returns>The disposable owning the source and fallback subscriptions.</returns>
        internal MultipleDisposable Run(IObservable<T> source)
        {
            var pocket = new MultipleDisposable();
            _pocket = pocket;
            pocket.Add(source.Subscribe(this));
            return pocket;
        }
    }
}
