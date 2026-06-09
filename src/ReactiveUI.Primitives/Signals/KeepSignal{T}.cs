// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Represents the KeepSignal class.</summary>
/// <typeparam name="T">The T type.</typeparam>
public sealed class KeepSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>Stores state for the signal implementation.</summary>
    private readonly IObservable<T> _source;

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly Func<T, bool> _predicate;

    /// <summary>Initializes a new instance of the <see cref="KeepSignal{T}"/> class.</summary>
    /// <param name="source">The source value.</param>
    /// <param name="predicate">The predicate value.</param>
    public KeepSignal(IObservable<T> source, Func<T, bool> predicate)
    {
        _source = source;
        _predicate = predicate;
    }

    /// <summary>Executes the IsRequiredSubscribeOnCurrentThread operation.</summary>
    /// <returns>The result.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        if (observer is null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        return _source.Subscribe(new KeepWitness(observer, _predicate));
    }

    /// <summary>Represents the KeepWitness class.</summary>
    private sealed class KeepWitness : IObserver<T>
    {
        /// <summary>Stores state for the signal implementation.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>Stores state for the signal implementation.</summary>
        private readonly Func<T, bool> _predicate;

        /// <summary>Stores state for the signal implementation.</summary>
        private bool _stopped;

        /// <summary>Initializes a new instance of the <see cref="KeepWitness"/> class.</summary>
        /// <param name="observer">The observer value.</param>
        /// <param name="predicate">The predicate value.</param>
        public KeepWitness(IObserver<T> observer, Func<T, bool> predicate)
        {
            _observer = observer;
            _predicate = predicate;
        }

        /// <summary>Executes the OnCompleted operation.</summary>
        public void OnCompleted()
        {
            if (_stopped)
            {
                return;
            }

            _stopped = true;
            _observer.OnCompleted();
        }

        /// <summary>Executes the OnError operation.</summary>
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

        /// <summary>Executes the OnNext operation.</summary>
        /// <param name="value">The value.</param>
        public void OnNext(T value)
        {
            if (_stopped)
            {
                return;
            }

            bool keep;
            try
            {
                keep = _predicate(value);
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
