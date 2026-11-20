// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>
/// Observer that resolves the one element of the source sequence matching a predicate, faulting the
/// result with an <see cref="InvalidOperationException"/> as soon as a second match arrives.
/// </summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="predicate">An optional predicate to filter elements; <c>null</c> matches all elements.</param>
/// <param name="requireExactlyOne">
/// When <c>true</c>, an empty sequence faults the result task with an
/// <see cref="InvalidOperationException"/>; when <c>false</c>, it resolves the result task with
/// <paramref name="defaultValue"/>.
/// </param>
/// <param name="defaultValue">The value to return on empty when <paramref name="requireExactlyOne"/> is <c>false</c>.</param>
/// <param name="cancellationToken">A cancellation token for the operation.</param>
[DebuggerDisplay("SingleElementWitness: HasValue = {_hasValue}, Value = {_value}")]
public sealed class SingleElementWitness<T>(
    Func<T, bool>? predicate,
    bool requireExactlyOne,
    T? defaultValue,
    CancellationToken cancellationToken) : IWitnessAsync<T>
{
    /// <summary>Produces and cancels the witness's single result value.</summary>
    private readonly TaskResultCompletionSource<T?> _completion = new(cancellationToken);

    /// <summary>A value indicating whether a matching element has been found.</summary>
    private bool _hasValue;

    /// <summary>The single matching element, or the default value if no match has been found.</summary>
    private T? _value = defaultValue;

    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <summary>Asynchronously returns the single element of <paramref name="source"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <returns>A task containing the single element.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T> SingleAsync(IObservableAsync<T> source) =>
        SingleCoreAsync(source, null, CancellationToken.None);

    /// <summary>Asynchronously returns the single element of <paramref name="source"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the single element.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T> SingleAsync(IObservableAsync<T> source, CancellationToken cancellationToken) =>
        SingleCoreAsync(source, null, cancellationToken);

    /// <summary>Asynchronously returns the single element of <paramref name="source"/> matching <paramref name="predicate"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">Predicate used to test each element.</param>
    /// <returns>A task containing the single matching element.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T> SingleAsync(IObservableAsync<T> source, Func<T, bool> predicate) =>
        SingleCoreAsync(source, predicate, CancellationToken.None);

    /// <summary>Asynchronously returns the single element of <paramref name="source"/> matching <paramref name="predicate"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">Predicate used to test each element.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the single matching element.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T> SingleAsync(
        IObservableAsync<T> source,
        Func<T, bool> predicate,
        CancellationToken cancellationToken) =>
        SingleCoreAsync(source, predicate, cancellationToken);

    /// <summary>Asynchronously returns the single element of <paramref name="source"/>, or <see langword="default"/> when empty.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <returns>A task containing the single element or <see langword="default"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T?> SingleOrDefaultAsync(IObservableAsync<T> source) =>
        SingleOrDefaultCoreAsync(source, null, default, CancellationToken.None);

    /// <summary>Asynchronously returns the single element of <paramref name="source"/>, or <see langword="default"/> when empty.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the single element or <see langword="default"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T?> SingleOrDefaultAsync(
        IObservableAsync<T> source,
        CancellationToken cancellationToken) =>
        SingleOrDefaultCoreAsync(source, null, default, cancellationToken);

    /// <summary>Asynchronously returns the single element of <paramref name="source"/>, or <paramref name="defaultValue"/> when empty.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="defaultValue">Fallback value returned when empty.</param>
    /// <returns>A task containing the single element or <paramref name="defaultValue"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T?> SingleOrDefaultAsync(IObservableAsync<T> source, T? defaultValue) =>
        SingleOrDefaultCoreAsync(source, null, defaultValue, CancellationToken.None);

    /// <summary>Asynchronously returns the single element of <paramref name="source"/>, or <paramref name="defaultValue"/> when empty.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="defaultValue">Fallback value returned when empty.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the single element or <paramref name="defaultValue"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T?> SingleOrDefaultAsync(
        IObservableAsync<T> source,
        T? defaultValue,
        CancellationToken cancellationToken) =>
        SingleOrDefaultCoreAsync(source, null, defaultValue, cancellationToken);

    /// <summary>Asynchronously returns the single element of <paramref name="source"/> matching <paramref name="predicate"/>, or <paramref name="defaultValue"/> when none match.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">Predicate used to test each element.</param>
    /// <param name="defaultValue">Fallback value returned when none match.</param>
    /// <returns>A task containing the single matching element or <paramref name="defaultValue"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T?> SingleOrDefaultAsync(
        IObservableAsync<T> source,
        Func<T, bool> predicate,
        T? defaultValue) =>
        SingleOrDefaultCoreAsync(source, predicate, defaultValue, CancellationToken.None);

    /// <summary>Asynchronously returns the single element of <paramref name="source"/> matching <paramref name="predicate"/>, or <paramref name="defaultValue"/> when none match.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">Predicate used to test each element.</param>
    /// <param name="defaultValue">Fallback value returned when none match.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the single matching element or <paramref name="defaultValue"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T?> SingleOrDefaultAsync(
        IObservableAsync<T> source,
        Func<T, bool> predicate,
        T? defaultValue,
        CancellationToken cancellationToken) =>
        SingleOrDefaultCoreAsync(source, predicate, defaultValue, cancellationToken);

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

    /// <summary>Asynchronously waits for the witness to produce its result value.</summary>
    /// <returns>A task representing the asynchronous operation, containing the result value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<T?> AwaitResultAsync() => _completion.AwaitResultAsync(this);

    /// <summary>Shared body for single-element resolution requiring at least one match.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">Optional predicate to filter elements.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The single matching element.</returns>
    private static async ValueTask<T> SingleCoreAsync(
        IObservableAsync<T> source,
        Func<T, bool>? predicate,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        SingleElementWitness<T> observer = new(predicate, true, default, cancellationToken);
        await using var subscription = await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
        var result = await observer.AwaitResultAsync().ConfigureAwait(false);
        return result!;
    }

    /// <summary>Shared body for single-or-default element resolution.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">Optional predicate to filter elements.</param>
    /// <param name="defaultValue">Fallback value returned on empty.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The single matching element or <paramref name="defaultValue"/>.</returns>
    private static async ValueTask<T?> SingleOrDefaultCoreAsync(
        IObservableAsync<T> source,
        Func<T, bool>? predicate,
        T? defaultValue,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        SingleElementWitness<T> observer = new(predicate, false, defaultValue, cancellationToken);
        await using var subscription = await source.SubscribeAsync(observer, cancellationToken).ConfigureAwait(false);
        return await observer.AwaitResultAsync().ConfigureAwait(false);
    }

    /// <inheritdoc/>
    async ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
    {
        if (predicate is not null && !predicate(value))
        {
            return;
        }

        if (_hasValue)
        {
            var message = predicate is null
                ? "Sequence contains more than one element."
                : "Sequence contains more than one matching element.";
            await _completion
                .SetExceptionAndDisposeAsync(new InvalidOperationException(message), this)
                .ConfigureAwait(false);
            return;
        }

        _hasValue = true;
        _value = value;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        _completion.SetExceptionAndDisposeAsync(error, this);

    /// <inheritdoc/>
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result)
    {
        if (!result.IsSuccess)
        {
            return _completion.SetExceptionAndDisposeAsync(result.Exception, this);
        }

        if (!_hasValue && requireExactlyOne)
        {
            var message = predicate is null
                ? "Sequence contains no elements."
                : "Sequence contains no matching elements.";
            return _completion.SetExceptionAndDisposeAsync(new InvalidOperationException(message), this);
        }

        return _completion.SetResultAndDisposeAsync(_value, this);
    }
}
