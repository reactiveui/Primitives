// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Witness that collects all elements from a sequence into a list.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="cancellationToken">A cancellation token for the operation.</param>
[System.Diagnostics.DebuggerDisplay("ToListTaskWitness: {_witness}")]
public sealed class ToListTaskWitness<T>(CancellationToken cancellationToken) : IWitnessAsync<T>
{
    /// <summary>Produces and cancels the witness's single result value.</summary>
    private readonly TaskResultCompletionSource<List<T>> _completion = new(cancellationToken);

    /// <summary>The list that accumulates all elements received from the source sequence.</summary>
    private readonly List<T> _items = [];

    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <summary>Asynchronously collects all elements from <paramref name="source"/> into a list.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <returns>A task containing the collected list.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<List<T>> RunListAsync(IObservableAsync<T> source) =>
        RunListAsync(source, CancellationToken.None);

    /// <summary>Asynchronously collects all elements from <paramref name="source"/> into a list.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the collected list.</returns>
    public static ValueTask<List<T>> RunListAsync(
        IObservableAsync<T> source,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ToListTaskWitness<T> observer = new(cancellationToken);
        return ReactiveUI.Primitives.Async.Internal.AsyncTaskTerminalRunner.RunToResultAsync(
            source,
            observer,
            observer._completion,
            cancellationToken);
    }

    /// <summary>Asynchronously collects all elements from <paramref name="source"/> into an array.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <returns>A task containing the collected array.</returns>
    public static async ValueTask<T[]> RunArrayAsync(IObservableAsync<T> source)
    {
        var values = await RunListAsync(source, CancellationToken.None).ConfigureAwait(false);
        return [.. values];
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
        _items.Add(value);
        return default;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        _completion.SetExceptionAndDisposeAsync(error, this);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
        _completion.CompleteAndDisposeAsync(result, _items, this);
}
