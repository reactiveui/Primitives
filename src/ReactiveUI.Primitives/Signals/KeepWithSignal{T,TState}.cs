// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Filters source values using a caller-supplied state value, without allocating a per-value closure: the state is
/// stored on the sink and passed to the predicate for each element.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
/// <typeparam name="TState">The state type passed to the predicate.</typeparam>
internal sealed class KeepWithSignal<T, TState> : IRequireCurrentThread<T>
{
    /// <summary>The source sequence.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The state passed to the predicate.</summary>
    private readonly TState _state;

    /// <summary>The predicate applied to each source value and the state.</summary>
    private readonly Func<TState, T, bool> _predicate;

    /// <summary>Initializes a new instance of the <see cref="KeepWithSignal{T, TState}"/> class.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="state">The state passed to the predicate.</param>
    /// <param name="predicate">The predicate applied to each source value and the state.</param>
    public KeepWithSignal(IObservable<T> source, TState state, Func<TState, T, bool> predicate)
    {
        _source = source;
        _state = state;
        _predicate = predicate;
    }

    /// <summary>Determines whether the sink must subscribe on the current thread.</summary>
    /// <returns><see langword="true"/> when the source requires current-thread subscription.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <summary>Subscribes the observer to the filtered sequence.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription handle.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        if (observer is null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        return _source.Subscribe(new KeepWithObserver(observer, _state, _predicate));
    }

    /// <summary>Applies the stateful predicate to each source value.</summary>
    private sealed class KeepWithObserver : IObserver<T>
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>The state passed to the predicate.</summary>
        private readonly TState _state;

        /// <summary>The predicate applied to each source value and the state.</summary>
        private readonly Func<TState, T, bool> _predicate;

        /// <summary>Whether a terminal notification has been forwarded.</summary>
        private bool _stopped;

        /// <summary>Initializes a new instance of the <see cref="KeepWithObserver"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="state">The state passed to the predicate.</param>
        /// <param name="predicate">The predicate applied to each source value and the state.</param>
        public KeepWithObserver(IObserver<T> observer, TState state, Func<TState, T, bool> predicate)
        {
            _observer = observer;
            _state = state;
            _predicate = predicate;
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

        /// <summary>Filters and forwards a source value.</summary>
        /// <param name="value">The source value.</param>
        public void OnNext(T value)
        {
            if (_stopped)
            {
                return;
            }

            bool keep;
            try
            {
                keep = _predicate(_state, value);
            }
            catch (Exception error)
            {
                OnError(error);
                return;
            }

            if (!keep)
            {
                return;
            }

            _observer.OnNext(value);
        }
    }
}
