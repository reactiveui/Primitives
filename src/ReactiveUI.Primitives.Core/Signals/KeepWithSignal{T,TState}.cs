// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Filters source values using a caller-supplied state value, without allocating a per-value closure: the state is
/// stored on the sink and passed to the predicate for each element.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
/// <typeparam name="TState">The state type passed to the predicate.</typeparam>
/// <param name="source">The source sequence.</param>
/// <param name="state">The state passed to the predicate.</param>
/// <param name="predicate">The predicate applied to each source value and the state.</param>
[System.Diagnostics.DebuggerDisplay("KeepWithSignal: Source = {_source}, State = {_state}")]
public sealed class KeepWithSignal<T, TState>(IObservable<T> source, TState state, Func<TState, T, bool> predicate) : IRequireCurrentThread<T>
{
    /// <summary>The source sequence.</summary>
    private readonly IObservable<T> _source = source;

    /// <summary>The state passed to the predicate.</summary>
    private readonly TState _state = state;

    /// <summary>The predicate applied to each source value and the state.</summary>
    private readonly Func<TState, T, bool> _predicate = predicate;

    /// <summary>Determines whether the sink must subscribe on the current thread.</summary>
    /// <returns><see langword="true"/> when the source requires current-thread subscription.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <summary>Subscribes the observer to the filtered sequence.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription handle.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return _source.Subscribe(new KeepWithWitness(observer, _state, _predicate));
    }

    /// <summary>Applies the stateful predicate to each source value.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="state">The state passed to the predicate.</param>
    /// <param name="predicate">The predicate applied to each source value and the state.</param>
    private sealed class KeepWithWitness(IObserver<T> observer, TState state, Func<TState, T, bool> predicate) : IObserver<T>
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer = observer;

        /// <summary>The state passed to the predicate.</summary>
        private readonly TState _state = state;

        /// <summary>The predicate applied to each source value and the state.</summary>
        private readonly Func<TState, T, bool> _predicate = predicate;

        /// <summary>Non-zero once a terminal notification has been forwarded.</summary>
        private int _stopped;

        /// <summary>Forwards completion downstream.</summary>
        public void OnCompleted()
        {
            if (Interlocked.Exchange(ref _stopped, 1) != 0)
            {
                return;
            }

            _observer.OnCompleted();
        }

        /// <summary>Forwards an error downstream.</summary>
        /// <param name="error">The error value.</param>
        public void OnError(Exception error)
        {
            if (Interlocked.Exchange(ref _stopped, 1) != 0)
            {
                return;
            }

            _observer.OnError(error);
        }

        /// <summary>Filters and forwards a source value.</summary>
        /// <param name="value">The source value.</param>
        public void OnNext(T value)
        {
            if (Volatile.Read(ref _stopped) != 0)
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
