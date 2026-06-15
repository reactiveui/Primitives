// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions;

/// <summary>
/// Subscribes once and completes the returned <see cref="Task{T}"/> with the first emitted value;
/// faults the task on source error or on empty completion. Combines the <see cref="TaskCompletionSource{T}"/>
/// and the IObserver into a single allocation per call.
/// </summary>
public static class FirstAsTaskHelper
{
    /// <summary>Subscribes and resolves a task with the first value.</summary>
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

    /// <summary>Combined TaskCompletionSource + IObserver — one heap allocation per call instead of two.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    private sealed class FirstWitness<T>() : TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously), IObserver<T>
    {
        /// <summary>Latches to <c>1</c> once the task has been settled so subsequent callbacks are no-ops.</summary>
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

            TrySetResult(value);
            Subscription?.Dispose();
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (Interlocked.Exchange(ref _settled, 1) != 0)
            {
                return;
            }

            TrySetException(error);
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            if (Interlocked.Exchange(ref _settled, 1) != 0)
            {
                return;
            }

            TrySetException(new InvalidOperationException("Sequence contains no elements."));
        }
    }
}
