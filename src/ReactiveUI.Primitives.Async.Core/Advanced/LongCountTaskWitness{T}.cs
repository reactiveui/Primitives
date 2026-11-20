// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Internal;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Witness that counts elements in a sequence as a 64-bit integer, optionally filtered by a predicate.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="predicate">An optional predicate to filter elements. If null, all elements are counted.</param>
/// <param name="cancellationToken">A cancellation token for the operation.</param>
[System.Diagnostics.DebuggerDisplay("LongCountTaskWitness: {_witness}")]
public sealed class LongCountTaskWitness<T>(Func<T, bool>? predicate, CancellationToken cancellationToken) : IWitnessAsync<T>
{
    /// <summary>The running count of elements that satisfy the predicate.</summary>
    private long _count;

    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <summary>Gets the completion source that publishes the single 64-bit count result.</summary>
    internal TaskResultCompletionSource<long> Completion { get; } = new(cancellationToken);

    /// <summary>Asynchronously counts all elements in <paramref name="source"/> as a 64-bit integer.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <returns>A task containing the element count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<long> RunAsync(IObservableAsync<T> source) =>
        RunAsync(source, null, CancellationToken.None);

    /// <summary>Asynchronously counts all elements in <paramref name="source"/> as a 64-bit integer.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the element count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<long> RunAsync(IObservableAsync<T> source, CancellationToken cancellationToken) =>
        RunAsync(source, null, cancellationToken);

    /// <summary>Asynchronously counts elements in <paramref name="source"/> matching <paramref name="predicate"/> as a 64-bit integer.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">Optional predicate to filter counted elements.</param>
    /// <returns>A task containing the matching element count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<long> RunAsync(IObservableAsync<T> source, Func<T, bool>? predicate) =>
        RunAsync(source, predicate, CancellationToken.None);

    /// <summary>Asynchronously counts elements in <paramref name="source"/> matching <paramref name="predicate"/> as a 64-bit integer.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">Optional predicate to filter counted elements.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the matching element count.</returns>
    public static ValueTask<long> RunAsync(
        IObservableAsync<T> source,
        Func<T, bool>? predicate,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return AsyncTaskTerminalRunner.RunCountInt64Async(source, predicate, cancellationToken);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
        WitnessAsync.OnNextAsync(this, value, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
        WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

    /// <inheritdoc/>
    ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        return AsyncTaskTerminalRunner.CountMatchingInt64(value, predicate, ref _count);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        Completion.SetExceptionAndDisposeAsync(error, this);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
        Completion.CompleteAndDisposeAsync(result, _count, this);
}
