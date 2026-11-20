// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Advanced;

namespace ReactiveUI.Primitives.Async.Internal;

/// <summary>Subscribes a terminal task witness to an <see cref="IObservableAsync{T}"/> source and awaits its single result while ensuring subscription disposal.</summary>
internal static class AsyncTaskTerminalRunner
{
    /// <summary>Subscribes <paramref name="witness"/> to <paramref name="source"/>, awaits <paramref name="completion"/>, and disposes the upstream subscription.</summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TWitness">The terminal witness type.</typeparam>
    /// <typeparam name="TResult">The single task result type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="witness">The terminal witness instance.</param>
    /// <param name="completion">The completion source owned by <paramref name="witness"/>.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the terminal result.</returns>
    internal static async ValueTask<TResult> RunToResultAsync<TSource, TWitness, TResult>(
        IObservableAsync<TSource> source,
        TWitness witness,
        TaskResultCompletionSource<TResult> completion,
        CancellationToken cancellationToken)
        where TWitness : class, IWitnessAsync<TSource>
    {
        await using var subscription =
            await source.SubscribeAsync(witness, cancellationToken).ConfigureAwait(false);
        return await completion.AwaitResultAsync(witness).ConfigureAwait(false);
    }

    /// <summary>Subscribes <paramref name="witness"/> to <paramref name="source"/>, awaits <paramref name="completion"/> without returning a payload, and disposes the upstream subscription.</summary>
    /// <typeparam name="TSource">The source element type.</typeparam>
    /// <typeparam name="TWitness">The terminal witness type.</typeparam>
    /// <typeparam name="TResult">The completion-source payload type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="witness">The terminal witness instance.</param>
    /// <param name="completion">The completion source owned by <paramref name="witness"/>.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous terminal operation.</returns>
    internal static async ValueTask RunToCompletionAsync<TSource, TWitness, TResult>(
        IObservableAsync<TSource> source,
        TWitness witness,
        TaskResultCompletionSource<TResult> completion,
        CancellationToken cancellationToken)
        where TWitness : class, IWitnessAsync<TSource>
    {
        await using var subscription =
            await source.SubscribeAsync(witness, cancellationToken).ConfigureAwait(false);
        _ = await completion.AwaitResultAsync(witness).ConfigureAwait(false);
    }

    /// <summary>Creates a <see cref="CountTaskWitness{T}"/> and awaits its 32-bit element count.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="predicate">Optional element filter.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the 32-bit element count.</returns>
    internal static ValueTask<int> RunCountInt32Async<T>(
        IObservableAsync<T> source,
        Func<T, bool>? predicate,
        CancellationToken cancellationToken)
    {
        CountTaskWitness<T> observer = new(predicate, cancellationToken);
        return RunToResultAsync(source, observer, observer.Completion, cancellationToken);
    }

    /// <summary>Creates a <see cref="LongCountTaskWitness{T}"/> and awaits its 64-bit element count.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="predicate">Optional element filter.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the 64-bit element count.</returns>
    internal static ValueTask<long> RunCountInt64Async<T>(
        IObservableAsync<T> source,
        Func<T, bool>? predicate,
        CancellationToken cancellationToken)
    {
        LongCountTaskWitness<T> observer = new(predicate, cancellationToken);
        return RunToResultAsync(source, observer, observer.Completion, cancellationToken);
    }

    /// <summary>Increments <paramref name="count"/> with overflow checking when <paramref name="predicate"/> is <see langword="null"/> or accepts <paramref name="value"/>.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="value">The incoming element.</param>
    /// <param name="predicate">Optional element filter.</param>
    /// <param name="count">The running 32-bit count.</param>
    /// <returns>A completed <see cref="ValueTask"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ValueTask CountMatchingInt32<T>(T value, Func<T, bool>? predicate, ref int count)
    {
        if (predicate is null || predicate(value))
        {
            count = checked(count + 1);
        }

        return default;
    }

    /// <summary>Increments <paramref name="count"/> with overflow checking when <paramref name="predicate"/> is <see langword="null"/> or accepts <paramref name="value"/>.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="value">The incoming element.</param>
    /// <param name="predicate">Optional element filter.</param>
    /// <param name="count">The running 64-bit count.</param>
    /// <returns>A completed <see cref="ValueTask"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ValueTask CountMatchingInt64<T>(T value, Func<T, bool>? predicate, ref long count)
    {
        if (predicate is null || predicate(value))
        {
            count = checked(count + 1);
        }

        return default;
    }

    /// <summary>
    /// Completes <paramref name="completion"/> with the upstream failure when <paramref name="result"/> faulted,
    /// or with a standard empty-sequence <see cref="InvalidOperationException"/> when empty.
    /// </summary>
    /// <typeparam name="TWitness">The owning witness type.</typeparam>
    /// <typeparam name="TResult">The completion-source payload type.</typeparam>
    /// <param name="completion">The completion source to fault.</param>
    /// <param name="result">The terminal completion result reported by upstream.</param>
    /// <param name="hasPredicate">Whether a filter predicate was supplied.</param>
    /// <param name="owner">The witness to dispose before releasing waiters.</param>
    /// <returns>A task representing the asynchronous disposal and completion.</returns>
    internal static ValueTask CompleteEmptySequence<TWitness, TResult>(
        TaskResultCompletionSource<TResult> completion,
        Result result,
        bool hasPredicate,
        TWitness owner)
        where TWitness : class, IWitnessState
    {
        var exception = result.IsSuccess
            ? new InvalidOperationException(
                hasPredicate ? "Sequence contains no matching elements." : "Sequence contains no elements.")
            : result.Exception;
        return completion.SetExceptionAndDisposeAsync(exception, owner);
    }
}
