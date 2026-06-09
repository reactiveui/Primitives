// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Fused <c>Where(predicate).Take(1)</c>. Subscribes a single observer to the source,
/// emits the first value satisfying <paramref name="predicate"/> followed by
/// <see cref="IObserver{T}.OnCompleted"/>, then disposes the source subscription —
/// avoiding the two intermediate observer wrappers that the equivalent Rx chain would
/// allocate per subscription.
/// </summary>
/// <typeparam name="T">The element type of the source observable.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="predicate">The match predicate; the first value returning <see langword="true"/> is emitted.</param>
internal sealed class WaitUntilObservable<T>(
    IObservable<T> source,
    Func<T, bool> predicate) : IObservable<T>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(predicate);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var subscription = new OnceDisposable();
        var sink = new WaitUntilWitness(observer, predicate, subscription);
        subscription.Disposable = source.Subscribe(sink);
        return subscription;
    }

    /// <summary>Forwarding observer that filters by <paramref name="predicate"/> and completes after the first match, disposing the upstream subscription handle.</summary>
    /// <param name="downstream">The downstream observer.</param>
    /// <param name="predicate">The match predicate.</param>
    /// <param name="subscription">The handle controlling the upstream subscription.</param>
    private sealed class WaitUntilWitness(
        IObserver<T> downstream,
        Func<T, bool> predicate,
        IDisposable subscription) : IObserver<T>
    {
        /// <summary>The gate for state access.</summary>
        private readonly Lock _gate = new();

        /// <summary>Whether the observer is done.</summary>
        private bool _done;

        /// <inheritdoc/>
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
                    subscription.Dispose();
                    return;
                }

                if (!isMatch)
                {
                    return;
                }

                _done = true;
                downstream.OnNext(value);
                downstream.OnCompleted();
            }

            subscription.Dispose();
        }

        /// <inheritdoc/>
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
