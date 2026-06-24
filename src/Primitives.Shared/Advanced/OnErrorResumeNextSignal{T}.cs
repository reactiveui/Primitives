// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Continues through observable sources after either completion or error.</summary>
/// <typeparam name="T">The value type.</typeparam>
internal sealed class OnErrorResumeNextSignal<T> : IObservable<T>
{
    /// <summary>The sources to subscribe in order.</summary>
    private readonly IEnumerable<IObservable<T>> _sources;

    /// <summary>Initializes a new instance of the <see cref="OnErrorResumeNextSignal{T}"/> class.</summary>
    /// <param name="sources">The sources to subscribe in order.</param>
    internal OnErrorResumeNextSignal(IEnumerable<IObservable<T>> sources) => _sources = sources;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return new Coordinator(observer).Run(_sources);
    }

    /// <summary>Coordinates ordered subscriptions while swallowing source errors.</summary>
    private sealed class Coordinator : IDisposable
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>Subscriptions created while walking the source list.</summary>
        private readonly MultipleDisposable _subscriptions = [];

        /// <summary>The active source enumerator.</summary>
        private IEnumerator<IObservable<T>>? _enumerator;

        /// <summary>Initializes a new instance of the <see cref="Coordinator"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        internal Coordinator(IObserver<T> observer) => _observer = observer;

        /// <inheritdoc/>
        public void Dispose()
        {
            _enumerator?.Dispose();
            _subscriptions.Dispose();
        }

        /// <summary>Starts iterating and subscribing to sources.</summary>
        /// <param name="sources">The sources to subscribe in order.</param>
        /// <returns>The coordinator that owns the subscriptions.</returns>
        internal Coordinator Run(IEnumerable<IObservable<T>> sources)
        {
            try
            {
                _enumerator = sources.GetEnumerator();
            }
            catch (Exception error)
            {
                _observer.OnError(error);
                return this;
            }

            SubscribeNext();
            return this;
        }

        /// <summary>Subscribes to the next source or completes when the sequence is exhausted.</summary>
        private void SubscribeNext()
        {
            IObservable<T> source;
            try
            {
                var enumerator = _enumerator;
                if (enumerator?.MoveNext() != true)
                {
                    _observer.OnCompleted();
                    Dispose();
                    return;
                }

                source = enumerator.Current ?? throw new InvalidOperationException("OnErrorResumeNext source contained null.");
            }
            catch (Exception error)
            {
                _observer.OnError(error);
                Dispose();
                return;
            }

            _subscriptions.Add(source.Subscribe(_observer.OnNext, _ => SubscribeNext(), SubscribeNext));
        }
    }
}
