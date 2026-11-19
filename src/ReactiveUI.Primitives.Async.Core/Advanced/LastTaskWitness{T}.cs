// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Observer that captures the last element matching an optional predicate.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="predicate">An optional predicate to filter elements.</param>
/// <param name="cancellationToken">A cancellation token for the operation.</param>
[System.Diagnostics.DebuggerDisplay("LastTaskWitness: {_witness}")]
public sealed class LastTaskWitness<T>(Func<T, bool>? predicate, CancellationToken cancellationToken) : IWitnessAsync<T>
{
    /// <summary>Produces and cancels the witness's single result value.</summary>
    private readonly TaskResultCompletionSource<T> _completion = new(cancellationToken);

    /// <summary>A value indicating whether any matching element has been observed.</summary>
    private bool _hasValue;

    /// <summary>The most recently observed matching element.</summary>
    private T? _last;

    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <summary>Asynchronously returns the last element of <paramref name="source"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <returns>A task containing the last element.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T> RunAsync(IObservableAsync<T> source) =>
        RunAsync(source, CancellationToken.None);

    /// <summary>Asynchronously returns the last element of <paramref name="source"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the last element.</returns>
    public static ValueTask<T> RunAsync(
        IObservableAsync<T> source,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        LastTaskWitness<T> observer = new(null, cancellationToken);
        return ReactiveUI.Primitives.Async.Internal.AsyncTaskTerminalRunner.RunToResultAsync(
            source,
            observer,
            observer._completion,
            cancellationToken);
    }

    /// <summary>Asynchronously returns the last element of <paramref name="source"/> matching <paramref name="predicate"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">Predicate used to test each element.</param>
    /// <returns>A task containing the last matching element.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T> RunAsync(IObservableAsync<T> source, Func<T, bool> predicate) =>
        RunAsync(source, predicate, CancellationToken.None);

    /// <summary>Asynchronously returns the last element of <paramref name="source"/> matching <paramref name="predicate"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">Predicate used to test each element.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the last matching element.</returns>
    public static ValueTask<T> RunAsync(
        IObservableAsync<T> source,
        Func<T, bool> predicate,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        LastTaskWitness<T> observer = new(predicate, cancellationToken);
        return ReactiveUI.Primitives.Async.Internal.AsyncTaskTerminalRunner.RunToResultAsync(
            source,
            observer,
            observer._completion,
            cancellationToken);
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
        if (predicate is not null && !predicate(value))
        {
            return default;
        }

        _hasValue = true;
        _last = value;

        return default;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        _completion.SetExceptionAndDisposeAsync(error, this);

    /// <inheritdoc/>
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
        result.IsSuccess && _hasValue
            ? _completion.SetResultAndDisposeAsync(_last!, this)
            : ReactiveUI.Primitives.Async.Internal.AsyncTaskTerminalRunner.CompleteEmptySequence(
                _completion,
                result,
                predicate is not null,
                this);
}
