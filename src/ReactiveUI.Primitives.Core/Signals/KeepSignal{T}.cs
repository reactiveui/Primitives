// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Represents the KeepSignal class.</summary>
/// <typeparam name="T">The T type.</typeparam>
/// <param name="source">The source value.</param>
/// <param name="predicate">The predicate value.</param>
[System.Diagnostics.DebuggerDisplay("KeepSignal: Source = {_source}, Predicate = {_predicate}")]
public sealed class KeepSignal<T>(IObservable<T> source, Func<T, bool> predicate) : IRequireCurrentThread<T>
{
    /// <summary>Stores state for the signal implementation.</summary>
    private readonly IObservable<T> _source = source;

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly Func<T, bool> _predicate = predicate;

    /// <summary>Executes the IsRequiredSubscribeOnCurrentThread operation.</summary>
    /// <returns>The result.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return _source.Subscribe(new KeepWitness(observer, _predicate));
    }

    /// <summary>Represents the KeepWitness class.</summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="predicate">The predicate value.</param>
    private sealed class KeepWitness(IObserver<T> observer, Func<T, bool> predicate) : IObserver<T>
    {
        /// <summary>Stores state for the signal implementation.</summary>
        private readonly IObserver<T> _observer = observer;

        /// <summary>Stores state for the signal implementation.</summary>
        private readonly Func<T, bool> _predicate = predicate;

        /// <summary>Stores state for the signal implementation.</summary>
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
        public void OnNext(T value)
        {
            if (Volatile.Read(ref _stopped) != 0)
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
