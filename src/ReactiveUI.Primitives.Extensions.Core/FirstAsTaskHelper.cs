// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions;

/// <summary>Bridges the first value of an observable to a <see cref="Task{T}"/>, faulting on source error and on completion without a value.</summary>
public static class FirstAsTaskHelper
{
    /// <summary>Subscribes to <paramref name="source"/> and settles the returned task from its first notification, disposing the subscription at that point.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <returns>A task that completes with the first value, faults on error, or faults on empty completion.</returns>
    public static Task<T> FirstAsTask<T>(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        FirstWitness<T> observer = new();
        observer.Subscription = source.Subscribe(observer);
        return observer.Task;
    }

    /// <summary>Observer that settles its own task from the first notification it receives.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    private sealed class FirstWitness<T>() : TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously), IObserver<T>
    {
        /// <summary>Latches to <c>1</c> when the task is settled so later callbacks are no-ops.</summary>
        private int _settled;

        /// <summary>Gets or sets the source subscription so the first-value path can dispose it on completion.</summary>
        public IDisposable? Subscription { get; set; }

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            if (Interlocked.Exchange(ref _settled, 1) != 0)
            {
                return;
            }

            _ = TrySetResult(value);
            Subscription?.Dispose();
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (Interlocked.Exchange(ref _settled, 1) != 0)
            {
                return;
            }

            _ = TrySetException(error);
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            if (Interlocked.Exchange(ref _settled, 1) != 0)
            {
                return;
            }

            _ = TrySetException(new InvalidOperationException("Sequence contains no elements."));
        }
    }
}
