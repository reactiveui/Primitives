// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for asynchronously converting an observable sequence to a dictionary.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Asynchronous dictionary-materialization operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Asynchronously creates a dictionary from the elements of the sequence, using the specified key selector function.</summary>
        /// <typeparam name="TKey">The type of the keys in the resulting dictionary. Must be non-nullable.</typeparam>
        /// <param name="keySelector">A function to extract a key from each element in the sequence. Cannot be null.</param>
        /// <param name="comparer">An optional equality comparer to compare keys. If null, the default equality comparer for the key type is
        /// used.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a dictionary mapping keys to
        /// elements from the sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="keySelector"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException">Two elements of the source sequence produce the same key.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(
            Func<T, TKey> keySelector,
            IEqualityComparer<TKey>? comparer,
            CancellationToken cancellationToken)
            where TKey : notnull =>
            ToDictionaryCore(source, keySelector, DictionaryIdentity<T>.Instance, comparer, cancellationToken);

        /// <summary>Asynchronously creates a dictionary from the elements of the sequence, using the specified key selector function and the default equality comparer for the key type.</summary>
        /// <typeparam name="TKey">The type of the keys in the resulting dictionary. Must be non-nullable.</typeparam>
        /// <param name="keySelector">A function to extract a key from each element in the sequence. Cannot be null.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a dictionary mapping keys to
        /// elements from the sequence.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="keySelector"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException">Two elements of the source sequence produce the same key.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<Dictionary<TKey, T>> ToDictionaryAsync<TKey>(Func<T, TKey> keySelector)
            where TKey : notnull =>
            source.ToDictionaryAsync(keySelector, null, CancellationToken.None);

        /// <summary>Asynchronously creates a dictionary from the elements of the sequence using the specified key and element selector functions.</summary>
        /// <typeparam name="TKey">The type of the keys in the resulting dictionary. Must be non-nullable.</typeparam>
        /// <typeparam name="TValue">The type of the values in the resulting dictionary.</typeparam>
        /// <param name="keySelector">A function to extract a key from each element in the sequence.</param>
        /// <param name="elementSelector">A function to map each element in the sequence to a value in the resulting dictionary.</param>
        /// <param name="comparer">An optional equality comparer to compare keys. If null, the default equality comparer for the key type is
        /// used.</param>
        /// <param name="cancellationToken">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a dictionary mapping keys to
        /// values as defined by the selector functions.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="keySelector"/> or <paramref name="elementSelector"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException">Two elements of the source sequence produce the same key.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<Dictionary<TKey, TValue>> ToDictionaryAsync<TKey, TValue>(
            Func<T, TKey> keySelector,
            Func<T, TValue> elementSelector,
            IEqualityComparer<TKey>? comparer,
            CancellationToken cancellationToken)
            where TKey : notnull =>
            ToDictionaryCore(source, keySelector, elementSelector, comparer, cancellationToken);

        /// <summary>
        /// Asynchronously creates a dictionary from the elements of the sequence using the specified key and element
        /// selector functions, with the default equality comparer for the key type.
        /// </summary>
        /// <typeparam name="TKey">The type of the keys in the resulting dictionary. Must be non-nullable.</typeparam>
        /// <typeparam name="TValue">The type of the values in the resulting dictionary.</typeparam>
        /// <param name="keySelector">A function to extract a key from each element in the sequence.</param>
        /// <param name="elementSelector">A function to map each element in the sequence to a value in the resulting dictionary.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a dictionary mapping keys to
        /// values as defined by the selector functions.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="keySelector"/> or <paramref name="elementSelector"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException">Two elements of the source sequence produce the same key.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<Dictionary<TKey, TValue>> ToDictionaryAsync<TKey, TValue>(
            Func<T, TKey> keySelector,
            Func<T, TValue> elementSelector)
            where TKey : notnull =>
            source.ToDictionaryAsync(keySelector, elementSelector, null, CancellationToken.None);
    }

    /// <summary>Runs dictionary materialization through the shared subscription path.</summary>
    /// <typeparam name="TSource">The type of source values.</typeparam>
    /// <typeparam name="TKey">The type of dictionary keys.</typeparam>
    /// <typeparam name="TValue">The type of dictionary values.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="keySelector">The function that selects a key from each source value.</param>
    /// <param name="elementSelector">The function that selects a dictionary value from each source value.</param>
    /// <param name="comparer">The optional key comparer.</param>
    /// <param name="cancellationToken">The cancellation token for the operation.</param>
    /// <returns>A dictionary built from the source sequence.</returns>
    private static async ValueTask<Dictionary<TKey, TValue>> ToDictionaryCore<TSource, TKey, TValue>(
        IObservableAsync<TSource> source,
        Func<TSource, TKey> keySelector,
        Func<TSource, TValue> elementSelector,
        IEqualityComparer<TKey>? comparer,
        CancellationToken cancellationToken)
        where TKey : notnull
    {
        ArgumentExceptionHelper.ThrowIfNull(keySelector);
        ArgumentExceptionHelper.ThrowIfNull(elementSelector);
        cancellationToken.ThrowIfCancellationRequested();

        ToDictionaryTaskWitness<TSource, TKey, TValue> sink = new(
            keySelector,
            elementSelector,
            comparer,
            cancellationToken);
        await using var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
        return await sink.AwaitResultAsync().ConfigureAwait(false);
    }

    /// <summary>Caches identity selectors by source type for dictionary materialization.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private static class DictionaryIdentity<T>
    {
        /// <summary>The cached identity selector.</summary>
        internal static readonly Func<T, T> Instance = static value => value;
    }

    /// <summary>Witness that builds a dictionary from the elements of a sequence using key and element selectors.</summary>
    /// <typeparam name="TSource">The type of elements in the source sequence.</typeparam>
    /// <typeparam name="TKey">The type of the dictionary keys.</typeparam>
    /// <typeparam name="TValue">The type of the dictionary values.</typeparam>
    /// <param name="keySelector">A function to extract a key from each element.</param>
    /// <param name="elementSelector">A function to extract a value from each element.</param>
    /// <param name="comparer">An optional equality comparer for keys.</param>
    /// <param name="cancellationToken">A cancellation token for the operation.</param>
    [DebuggerDisplay("ToDictionaryTaskWitness: {_witness}")]
    private sealed class ToDictionaryTaskWitness<TSource, TKey, TValue>(
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

        /// <summary>Asynchronously waits for the witness to produce its result value.</summary>
        /// <returns>A task representing the asynchronous operation, containing the result value.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<Dictionary<TKey, TValue>> AwaitResultAsync() => _completion.AwaitResultAsync(this);

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
}
