// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Witness that builds a dictionary from the elements of a sequence using key and element selectors.</summary>
/// <typeparam name="TSource">The type of elements in the source sequence.</typeparam>
/// <typeparam name="TKey">The type of the dictionary keys.</typeparam>
/// <typeparam name="TValue">The type of the dictionary values.</typeparam>
/// <param name="keySelector">A function to extract a key from each element.</param>
/// <param name="elementSelector">A function to extract a value from each element.</param>
/// <param name="comparer">An optional equality comparer for keys.</param>
/// <param name="cancellationToken">A cancellation token for the operation.</param>
[System.Diagnostics.DebuggerDisplay("ToDictionaryTaskWitness: {_witness}")]
public sealed class ToDictionaryTaskWitness<TSource, TKey, TValue>(
    Func<TSource, TKey> keySelector,
    Func<TSource, TValue> elementSelector,
    IEqualityComparer<TKey>? comparer,
    CancellationToken cancellationToken) : IWitnessAsync<TSource>
    where TKey : notnull
{
    /// <summary>Produces and cancels the witness's single result value.</summary>
    private readonly TaskResultCompletionSource<Dictionary<TKey, TValue>> _completion = new(cancellationToken);

    /// <summary>The dictionary that accumulates key-value pairs from the source sequence.</summary>
    private readonly Dictionary<TKey, TValue> _map = comparer is null ? [] : [with(comparer)];

    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <summary>Materializes <paramref name="source"/> into a dictionary using <paramref name="keySelector"/> and identity values.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="keySelector">Extracts a key from each element.</param>
    /// <returns>A task containing the resulting dictionary.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<Dictionary<TKey, TSource>> RunIdentityAsync(
        IObservableAsync<TSource> source,
        Func<TSource, TKey> keySelector) =>
        ToDictionaryTaskWitness<TSource, TKey, TSource>.RunAsync(
            source,
            keySelector,
            DictionaryIdentity<TSource>.Instance,
            null,
            CancellationToken.None);

    /// <summary>Materializes <paramref name="source"/> into a dictionary using <paramref name="keySelector"/>, <paramref name="comparer"/>, and identity values.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="keySelector">Extracts a key from each element.</param>
    /// <param name="comparer">Optional key equality comparer.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the resulting dictionary.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<Dictionary<TKey, TSource>> RunIdentityAsync(
        IObservableAsync<TSource> source,
        Func<TSource, TKey> keySelector,
        IEqualityComparer<TKey>? comparer,
        CancellationToken cancellationToken) =>
        ToDictionaryTaskWitness<TSource, TKey, TSource>.RunAsync(
            source,
            keySelector,
            DictionaryIdentity<TSource>.Instance,
            comparer,
            cancellationToken);

    /// <summary>Materializes <paramref name="source"/> into a dictionary using <paramref name="keySelector"/> and <paramref name="elementSelector"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="keySelector">Extracts a key from each element.</param>
    /// <param name="elementSelector">Extracts a value from each element.</param>
    /// <returns>A task containing the resulting dictionary.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<Dictionary<TKey, TValue>> RunAsync(
        IObservableAsync<TSource> source,
        Func<TSource, TKey> keySelector,
        Func<TSource, TValue> elementSelector) =>
        RunAsync(source, keySelector, elementSelector, null, CancellationToken.None);

    /// <summary>Materializes <paramref name="source"/> into a dictionary using <paramref name="keySelector"/>, <paramref name="elementSelector"/>, and <paramref name="comparer"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="keySelector">Extracts a key from each element.</param>
    /// <param name="elementSelector">Extracts a value from each element.</param>
    /// <param name="comparer">Optional key equality comparer.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the resulting dictionary.</returns>
    public static ValueTask<Dictionary<TKey, TValue>> RunAsync(
        IObservableAsync<TSource> source,
        Func<TSource, TKey> keySelector,
        Func<TSource, TValue> elementSelector,
        IEqualityComparer<TKey>? comparer,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(keySelector);
        ArgumentExceptionHelper.ThrowIfNull(elementSelector);
        cancellationToken.ThrowIfCancellationRequested();

        ToDictionaryTaskWitness<TSource, TKey, TValue> sink = new(
            keySelector,
            elementSelector,
            comparer,
            cancellationToken);
        return ReactiveUI.Primitives.Async.Internal.AsyncTaskTerminalRunner.RunToResultAsync(
            source,
            sink,
            sink._completion,
            cancellationToken);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnNextAsync(TSource value, CancellationToken cancellationToken) =>
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
    ValueTask IWitnessAsync<TSource>.OnNextAsyncCore(TSource value, CancellationToken cancellationToken)
    {
        var key = keySelector(value);
        _map.Add(key, elementSelector(value));
        return default;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<TSource>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        _completion.SetExceptionAndDisposeAsync(error, this);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<TSource>.OnCompletedAsyncCore(Result result) =>
        _completion.CompleteAndDisposeAsync(result, _map, this);
}
