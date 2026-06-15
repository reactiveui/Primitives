// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Takes elements from the source sequence until a predicate returns true for an element.
/// The element that satisfies the predicate is included in the sequence.
/// </summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="predicate">The predicate to determine when to stop taking elements.</param>
public sealed class TakeUntilInclusiveObservable<T>(
    IObservable<T> source,
    Func<T, bool> predicate) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(predicate);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return source.Subscribe(new TakeUntilInclusiveWitness(observer, predicate));
    }

    /// <summary>The observer for the <see cref="TakeUntilInclusiveObservable{T}"/>.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="predicate">The predicate to determine when to stop taking elements.</param>
    private sealed class TakeUntilInclusiveWitness(
        IObserver<T> downstream,
        Func<T, bool> predicate) : IObserver<T>
    {
        /// <summary>The gate for state access.</summary>
        private readonly Lock _gate = new();

        /// <summary>Whether the observer is done.</summary>
        private bool _done;

        /// <inheritdoc/>
        /// <param name="value">The value.</param>
        public void OnNext(T value)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                bool isMatch;
                try
                {
                    isMatch = predicate(value);
                }
                catch (Exception ex)
                {
                    _done = true;
                    downstream.OnError(ex);
                    return;
                }

                downstream.OnNext(value);

                if (!isMatch)
                {
                    return;
                }

                _done = true;
                downstream.OnCompleted();
            }
        }

        /// <inheritdoc/>
        /// <param name="error">The error.</param>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
                downstream.OnError(error);
            }
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
                downstream.OnCompleted();
            }
        }
    }
}
