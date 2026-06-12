// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Invokes a stateful action for each source value while forwarding the value unchanged, without allocating a
/// per-value closure: the state is stored on the sink and passed to the action for each element.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
/// <typeparam name="TState">The state type passed to the action.</typeparam>
internal sealed class TapWithSignal<T, TState> : IRequireCurrentThread<T>
{
    /// <summary>The source sequence.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The state passed to the action.</summary>
    private readonly TState _state;

    /// <summary>The action invoked for each value and the state.</summary>
    private readonly Action<TState, T> _onNext;

    /// <summary>Initializes a new instance of the <see cref="TapWithSignal{T, TState}"/> class.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="state">The state passed to the action.</param>
    /// <param name="onNext">The action invoked for each value and the state.</param>
    public TapWithSignal(IObservable<T> source, TState state, Action<TState, T> onNext)
    {
        _source = source;
        _state = state;
        _onNext = onNext;
    }

    /// <summary>Determines whether the sink must subscribe on the current thread.</summary>
    /// <returns><see langword="true"/> when the source requires current-thread subscription.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <summary>Subscribes the observer to the tapped sequence.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription handle.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return _source.Subscribe(new TapWithWitness(observer, _state, _onNext));
    }

    /// <summary>Invokes the stateful side-effect action and forwards each value.</summary>
    private sealed class TapWithWitness : IObserver<T>
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>The state passed to the action.</summary>
        private readonly TState _state;

        /// <summary>The action invoked for each value and the state.</summary>
        private readonly Action<TState, T> _onNext;

        /// <summary>Whether a terminal notification has been forwarded.</summary>
        private bool _stopped;

        /// <summary>Initializes a new instance of the <see cref="TapWithWitness"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="state">The state passed to the action.</param>
        /// <param name="onNext">The action invoked for each value and the state.</param>
        public TapWithWitness(IObserver<T> observer, TState state, Action<TState, T> onNext)
        {
            _observer = observer;
            _state = state;
            _onNext = onNext;
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

        /// <summary>Runs the stateful side effect and forwards a source value.</summary>
        /// <param name="value">The source value.</param>
        public void OnNext(T value)
        {
            if (_stopped)
            {
                return;
            }

            _onNext(_state, value);
            _observer.OnNext(value);
        }
    }
}
