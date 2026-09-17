// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Signal that projects each source value through a selector before forwarding it.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TResult">The projected value type.</typeparam>
/// <param name="source">The source sequence.</param>
/// <param name="selector">The projection applied to each source value.</param>
[System.Diagnostics.DebuggerDisplay("MapSignal: Source = {_source}, Selector = {_selector}")]
public sealed class MapSignal<TSource, TResult>(IObservable<TSource> source, Func<TSource, TResult> selector) : IRequireCurrentThread<TResult>
{
    /// <summary>The source sequence.</summary>
    private readonly IObservable<TSource> _source = source;

    /// <summary>The projection applied to each source value.</summary>
    private readonly Func<TSource, TResult> _selector = selector;

    /// <summary>Preserves the source's current-thread subscription requirement.</summary>
    /// <returns><see langword="true"/> when the source requires current-thread subscription.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        _source is IRequireCurrentThread<TSource> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <summary>Subscribes an observer to the projected source values.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription handle.</returns>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return _source.Subscribe(new MapWitness(observer, _selector));
    }

    /// <summary>Applies the selector to each source value and forwards the projection.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="selector">The projection applied to each source value.</param>
    private sealed class MapWitness(IObserver<TResult> observer, Func<TSource, TResult> selector) : IObserver<TSource>
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<TResult> _observer = observer;

        /// <summary>The projection applied to each source value.</summary>
        private readonly Func<TSource, TResult> _selector = selector;

        /// <summary>Non-zero once a terminal notification has been forwarded.</summary>
        private int _stopped;

        /// <summary>Forwards completion only while the sink is active.</summary>
        public void OnCompleted()
        {
            if (Interlocked.Exchange(ref _stopped, 1) != 0)
            {
                return;
            }

            _observer.OnCompleted();
        }

        /// <summary>Stops the sink and forwards its first error.</summary>
        /// <param name="error">The terminal error.</param>
        public void OnError(Exception error)
        {
            if (Interlocked.Exchange(ref _stopped, 1) != 0)
            {
                return;
            }

            _observer.OnError(error);
        }

        /// <summary>Projects active values and turns selector failures into terminal errors.</summary>
        /// <param name="value">The source value.</param>
        public void OnNext(TSource value)
        {
            if (Volatile.Read(ref _stopped) != 0)
            {
                return;
            }

            TResult result;
            try
            {
                result = _selector(value);
            }
            catch (Exception error)
            {
                OnError(error);
                return;
            }

            _observer.OnNext(result);
        }
    }
}
