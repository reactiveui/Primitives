// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Signal that forwards only the source values the predicate accepts.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="source">The source sequence.</param>
/// <param name="predicate">The predicate applied to each source value.</param>
[System.Diagnostics.DebuggerDisplay("KeepSignal: Source = {_source}, Predicate = {_predicate}")]
public sealed class KeepSignal<T>(IObservable<T> source, Func<T, bool> predicate) : IRequireCurrentThread<T>
{
    /// <summary>The source sequence.</summary>
    private readonly IObservable<T> _source = source;

    /// <summary>The predicate applied to each source value.</summary>
    private readonly Func<T, bool> _predicate = predicate;

    /// <summary>Preserves the source's current-thread subscription requirement.</summary>
    /// <returns><see langword="true"/> when the source requires current-thread subscription.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <summary>Subscribes an observer to source values accepted by the predicate.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription handle.</returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return _source.Subscribe(new KeepWitness(observer, _predicate));
    }

    /// <summary>Applies the predicate to each source value and forwards the ones it accepts.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="predicate">The predicate applied to each source value.</param>
    private sealed class KeepWitness(IObserver<T> observer, Func<T, bool> predicate) : IObserver<T>
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer = observer;

        /// <summary>The predicate applied to each source value.</summary>
        private readonly Func<T, bool> _predicate = predicate;

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

        /// <summary>Filters active values and turns predicate failures into terminal errors.</summary>
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
