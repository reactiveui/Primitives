// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Observer that accumulates values using a synchronous accumulator function and produces the final result.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <typeparam name="TAcc">The type of the accumulated value.</typeparam>
/// <param name="seed">The initial accumulator value.</param>
/// <param name="accumulator">The synchronous accumulator function.</param>
/// <param name="cancellationToken">A cancellation token for the operation.</param>
[System.Diagnostics.DebuggerDisplay("ReduceSyncTaskWitness: {_witness}")]
public sealed class ReduceSyncTaskWitness<T, TAcc>(
    TAcc seed,
    Func<TAcc, T, TAcc> accumulator,
    CancellationToken cancellationToken) : IWitnessAsync<T>
{
    /// <summary>Produces and cancels the witness's single result value.</summary>
    private readonly TaskResultCompletionSource<TAcc> _completion = new(cancellationToken);

    /// <summary>The current accumulated value.</summary>
    private TAcc _acc = seed;

    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <summary>Accumulates elements of <paramref name="source"/> synchronously from <paramref name="seed"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="seed">The initial accumulator value.</param>
    /// <param name="accumulator">The synchronous accumulator function.</param>
    /// <returns>A task containing the final accumulated value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<TAcc> RunAsync(
        IObservableAsync<T> source,
        TAcc seed,
        Func<TAcc, T, TAcc> accumulator) =>
        RunAsync(source, seed, accumulator, CancellationToken.None);

    /// <summary>Accumulates elements of <paramref name="source"/> synchronously from <paramref name="seed"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="seed">The initial accumulator value.</param>
    /// <param name="accumulator">The synchronous accumulator function.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the final accumulated value.</returns>
    public static ValueTask<TAcc> RunAsync(
        IObservableAsync<T> source,
        TAcc seed,
        Func<TAcc, T, TAcc> accumulator,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(accumulator);
        cancellationToken.ThrowIfCancellationRequested();

        ReduceSyncTaskWitness<T, TAcc> observer = new(seed, accumulator, cancellationToken);
        return ReactiveUI.Primitives.Async.Internal.AsyncTaskTerminalRunner.RunToResultAsync(
            source,
            observer,
            observer._completion,
            cancellationToken);
    }

    /// <summary>Accumulates elements of <paramref name="source"/> synchronously from <paramref name="seed"/> and projects the final accumulator.</summary>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="seed">The initial accumulator value.</param>
    /// <param name="accumulator">The synchronous accumulator function.</param>
    /// <param name="resultSelector">Projects the final accumulated value.</param>
    /// <returns>A task containing the projected result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<TResult> RunAsync<TResult>(
        IObservableAsync<T> source,
        TAcc seed,
        Func<TAcc, T, TAcc> accumulator,
        Func<TAcc, TResult> resultSelector) =>
        RunAsync(source, seed, accumulator, resultSelector, CancellationToken.None);

    /// <summary>Accumulates elements of <paramref name="source"/> synchronously from <paramref name="seed"/> and projects the final accumulator.</summary>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="seed">The initial accumulator value.</param>
    /// <param name="accumulator">The synchronous accumulator function.</param>
    /// <param name="resultSelector">Projects the final accumulated value.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the projected result.</returns>
    public static async ValueTask<TResult> RunAsync<TResult>(
        IObservableAsync<T> source,
        TAcc seed,
        Func<TAcc, T, TAcc> accumulator,
        Func<TAcc, TResult> resultSelector,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(resultSelector);

        var acc = await RunAsync(source, seed, accumulator, cancellationToken).ConfigureAwait(false);
        return resultSelector(acc);
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
        _acc = accumulator(_acc, value);
        return default;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        _completion.SetExceptionAndDisposeAsync(error, this);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
        _completion.CompleteAndDisposeAsync(result, _acc, this);
}
