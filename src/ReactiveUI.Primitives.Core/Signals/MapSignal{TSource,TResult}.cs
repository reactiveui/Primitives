// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Represents the MapSignal class.</summary>
/// <typeparam name="TSource">The TSource type.</typeparam>
/// <typeparam name="TResult">The TResult type.</typeparam>
/// <param name="source">The source value.</param>
/// <param name="selector">The selector value.</param>
[System.Diagnostics.DebuggerDisplay("MapSignal: Source = {_source}, Selector = {_selector}")]
public sealed class MapSignal<TSource, TResult>(IObservable<TSource> source, Func<TSource, TResult> selector) : IRequireCurrentThread<TResult>
{
    /// <summary>Stores state for the signal implementation.</summary>
    private readonly IObservable<TSource> _source = source;

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly Func<TSource, TResult> _selector = selector;

    /// <summary>Executes the IsRequiredSubscribeOnCurrentThread operation.</summary>
    /// <returns>The result.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        _source is IRequireCurrentThread<TSource> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return _source.Subscribe(new MapWitness(observer, _selector));
    }

    /// <summary>Represents the MapWitness class.</summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="selector">The selector value.</param>
    private sealed class MapWitness(IObserver<TResult> observer, Func<TSource, TResult> selector) : IObserver<TSource>
    {
        /// <summary>Stores state for the signal implementation.</summary>
        private readonly IObserver<TResult> _observer = observer;

        /// <summary>Stores state for the signal implementation.</summary>
        private readonly Func<TSource, TResult> _selector = selector;

        /// <summary>Stores state for the signal implementation; non-zero once the sink has terminated.</summary>
        private int _stopped;

        /// <summary>Executes the OnCompleted operation.</summary>
        public void OnCompleted()
        {
            if (Interlocked.Exchange(ref _stopped, 1) != 0)
            {
                return;
            }

            _observer.OnCompleted();
        }

        /// <summary>Executes the OnError operation.</summary>
        /// <param name="error">The error value.</param>
        public void OnError(Exception error)
        {
            if (Interlocked.Exchange(ref _stopped, 1) != 0)
            {
                return;
            }

            _observer.OnError(error);
        }

        /// <summary>Executes the OnNext operation.</summary>
        /// <param name="value">The value.</param>
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
