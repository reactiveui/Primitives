// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Projects each source value into a new form using a caller-supplied state value, without allocating a per-value
/// closure: the state is stored on the sink and passed to the selector for each element.
/// </summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TState">The state type passed to the selector.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
internal sealed class MapWithSignal<TSource, TState, TResult> : IRequireCurrentThread<TResult>
{
    /// <summary>The source sequence.</summary>
    private readonly IObservable<TSource> _source;

    /// <summary>The state passed to the selector.</summary>
    private readonly TState _state;

    /// <summary>The transform applied to each source value and the state.</summary>
    private readonly Func<TState, TSource, TResult> _selector;

    /// <summary>Initializes a new instance of the <see cref="MapWithSignal{TSource, TState, TResult}"/> class.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="state">The state passed to the selector.</param>
    /// <param name="selector">The transform applied to each source value and the state.</param>
    public MapWithSignal(IObservable<TSource> source, TState state, Func<TState, TSource, TResult> selector)
    {
        _source = source;
        _state = state;
        _selector = selector;
    }

    /// <summary>Determines whether the sink must subscribe on the current thread.</summary>
    /// <returns><see langword="true"/> when the source requires current-thread subscription.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        _source is IRequireCurrentThread<TSource> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <summary>Subscribes the observer to the projected sequence.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription handle.</returns>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        if (observer is null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        return _source.Subscribe(new MapWithObserver(observer, _state, _selector));
    }

    /// <summary>Applies the stateful selector to each source value.</summary>
    private sealed class MapWithObserver : IObserver<TSource>
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<TResult> _observer;

        /// <summary>The state passed to the selector.</summary>
        private readonly TState _state;

        /// <summary>The transform applied to each source value and the state.</summary>
        private readonly Func<TState, TSource, TResult> _selector;

        /// <summary>Whether a terminal notification has been forwarded.</summary>
        private bool _stopped;

        /// <summary>Initializes a new instance of the <see cref="MapWithObserver"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="state">The state passed to the selector.</param>
        /// <param name="selector">The transform applied to each source value and the state.</param>
        public MapWithObserver(IObserver<TResult> observer, TState state, Func<TState, TSource, TResult> selector)
        {
            _observer = observer;
            _state = state;
            _selector = selector;
        }

        /// <summary>Forwards completion downstream.</summary>
        public void OnCompleted()
        {
            if (_stopped)
            {
                return;
            }

            _stopped = true;
            _observer.OnCompleted();
        }

        /// <summary>Forwards an error downstream.</summary>
        /// <param name="error">The error value.</param>
        public void OnError(Exception error)
        {
            if (_stopped)
            {
                return;
            }

            _stopped = true;
            _observer.OnError(error);
        }

        /// <summary>Projects and forwards a source value.</summary>
        /// <param name="value">The source value.</param>
        public void OnNext(TSource value)
        {
            if (_stopped)
            {
                return;
            }

            TResult result;
            try
            {
                result = _selector(_state, value);
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
