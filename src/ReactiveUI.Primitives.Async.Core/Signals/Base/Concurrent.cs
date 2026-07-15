// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>Provides helper methods for forwarding asynchronous observer notifications concurrently to multiple observers.</summary>
/// <remarks>The methods in this class are intended for scenarios where multiple asynchronous observers need to be
/// notified in parallel. All observer notifications are dispatched concurrently, and the returned ValueTask completes
/// when all observer operations have finished. If the observers collection is empty, the methods complete immediately.
/// Exceptions thrown by individual observers are aggregated into a single exception, consistent with Task.WhenAll
/// behavior.</remarks>
public static class Concurrent
{
    /// <summary>Forwards the specified value to all observers concurrently by invoking their OnNextAsync methods.</summary>
    /// <remarks>If the observers list is empty, the returned ValueTask is already completed. All OnNextAsync
    /// calls are started concurrently; exceptions from observers are aggregated in the returned task.</remarks>
    /// <typeparam name="T">The type of the value to forward to the observers.</typeparam>
    /// <param name="observers">A read-only list of observers that will receive the value. Cannot be null.</param>
    /// <param name="value">The value to forward to each observer.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the forwarding operation.</param>
    /// <returns>A ValueTask that represents the asynchronous operation of forwarding the value to all observers. The task
    /// completes when all observers have processed the value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask ForwardOnNextConcurrently<T>(
        ImmutableArray<IObserverAsync<T>> observers,
        T value,
        CancellationToken cancellationToken)
    {
        var count = observers.Length;
        if (count == 0)
        {
            return default;
        }

        if (count == 1)
        {
            return observers[0].OnNextAsync(value, cancellationToken);
        }

        for (var i = 0; i < count; i++)
        {
            var vt = observers[i].OnNextAsync(value, cancellationToken);
            if (vt.IsCompletedSuccessfully)
            {
                continue;
            }

            var tasks = new Task[count - i];
            tasks[0] = vt.AsTask();
            for (var j = i + 1; j < count; j++)
            {
                tasks[j - i] = observers[j].OnNextAsync(value, cancellationToken).AsTask();
            }

            return new(Task.WhenAll(tasks));
        }

        return default;
    }

    /// <summary>
    /// Forwards an error notification to all specified asynchronous observers concurrently, allowing each observer to
    /// handle the error and resume as appropriate.
    /// </summary>
    /// <remarks>If the list of observers is empty, the method returns a default ValueTask and no
    /// notifications are sent. Each observer receives the error notification concurrently. If cancellation is requested
    /// via the cancellation token, the operation may be canceled before completion.</remarks>
    /// <typeparam name="T">The type of the elements observed by the observers.</typeparam>
    /// <param name="observers">A read-only list of asynchronous observers to which the error notification will be forwarded. Cannot be null.</param>
    /// <param name="error">The exception representing the error to forward to each observer. Cannot be null.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the forwarding operation.</param>
    /// <returns>A ValueTask that represents the asynchronous operation of forwarding the error to all observers. The task
    /// completes when all observers have processed the error notification.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask ForwardOnErrorResumeConcurrently<T>(
        ImmutableArray<IObserverAsync<T>> observers,
        Exception error,
        CancellationToken cancellationToken)
    {
        var count = observers.Length;
        if (count == 0)
        {
            return default;
        }

        if (count == 1)
        {
            return observers[0].OnErrorResumeAsync(error, cancellationToken);
        }

        for (var i = 0; i < count; i++)
        {
            var vt = observers[i].OnErrorResumeAsync(error, cancellationToken);
            if (vt.IsCompletedSuccessfully)
            {
                continue;
            }

            var tasks = new Task[count - i];
            tasks[0] = vt.AsTask();
            for (var j = i + 1; j < count; j++)
            {
                tasks[j - i] = observers[j].OnErrorResumeAsync(error, cancellationToken).AsTask();
            }

            return new(Task.WhenAll(tasks));
        }

        return default;
    }

    /// <summary>
    /// Invokes the OnCompletedAsync method on each observer in the collection concurrently, forwarding the specified
    /// result to all observers.
    /// </summary>
    /// <remarks>All observers are notified concurrently. The returned ValueTask completes when all
    /// OnCompletedAsync operations have finished. If any observer throws an exception, the returned task will complete
    /// with an AggregateException containing all exceptions thrown.</remarks>
    /// <typeparam name="T">The type of the elements observed by the observers.</typeparam>
    /// <param name="observers">A read-only list of observers to which the completion notification will be forwarded. Cannot be null.</param>
    /// <param name="result">The result to pass to each observer's OnCompletedAsync method.</param>
    /// <returns>A ValueTask that represents the asynchronous operation of notifying all observers. The task completes when all
    /// observers have finished processing the completion notification. If the observers list is empty, a default
    /// ValueTask is returned.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask ForwardOnCompletedConcurrently<T>(
        ImmutableArray<IObserverAsync<T>> observers,
        Result result)
    {
        var count = observers.Length;
        if (count == 0)
        {
            return default;
        }

        if (count == 1)
        {
            return observers[0].OnCompletedAsync(result);
        }

        for (var i = 0; i < count; i++)
        {
            var vt = observers[i].OnCompletedAsync(result);
            if (vt.IsCompletedSuccessfully)
            {
                continue;
            }

            var tasks = new Task[count - i];
            tasks[0] = vt.AsTask();
            for (var j = i + 1; j < count; j++)
            {
                tasks[j - i] = observers[j].OnCompletedAsync(result).AsTask();
            }

            return new(Task.WhenAll(tasks));
        }

        return default;
    }
}
