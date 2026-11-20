// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A witness that captures the first element matching an optional predicate.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="predicate">An optional predicate to filter elements.</param>
/// <param name="hasDefault">When <see langword="true"/>, an empty sequence yields <paramref name="defaultValue"/> instead of failing.</param>
/// <param name="defaultValue">The value an empty sequence yields when <paramref name="hasDefault"/> is set.</param>
/// <param name="cancellationToken">A cancellation token for the operation.</param>
[System.Diagnostics.DebuggerDisplay("FirstTaskWitness: {_witness}")]
public sealed class FirstTaskWitness<T>(
    Func<T, bool>? predicate,
    bool hasDefault,
    T? defaultValue,
    CancellationToken cancellationToken) : IWitnessAsync<T>
{
    /// <summary>Produces and cancels the witness's single result value.</summary>
    private readonly TaskResultCompletionSource<T> _completion = new(cancellationToken);

    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <summary>Asynchronously returns the first element of <paramref name="source"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <returns>A task containing the first element.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T> FirstAsync(IObservableAsync<T> source) =>
        FirstAsync(source, CancellationToken.None);

    /// <summary>Asynchronously returns the first element of <paramref name="source"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the first element.</returns>
    public static ValueTask<T> FirstAsync(
        IObservableAsync<T> source,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        FirstTaskWitness<T> observer = new(null, false, default, cancellationToken);
        return ReactiveUI.Primitives.Async.Internal.AsyncTaskTerminalRunner.RunToResultAsync(
            source,
            observer,
            observer._completion,
            cancellationToken);
    }

    /// <summary>Asynchronously returns the first element of <paramref name="source"/> matching <paramref name="predicate"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">Predicate used to test each element.</param>
    /// <returns>A task containing the first matching element.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T> FirstAsync(IObservableAsync<T> source, Func<T, bool> predicate) =>
        FirstAsync(source, predicate, CancellationToken.None);

    /// <summary>Asynchronously returns the first element of <paramref name="source"/> matching <paramref name="predicate"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">Predicate used to test each element.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the first matching element.</returns>
    public static ValueTask<T> FirstAsync(
        IObservableAsync<T> source,
        Func<T, bool> predicate,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        FirstTaskWitness<T> observer = new(predicate, false, default, cancellationToken);
        return ReactiveUI.Primitives.Async.Internal.AsyncTaskTerminalRunner.RunToResultAsync(
            source,
            observer,
            observer._completion,
            cancellationToken);
    }

    /// <summary>Asynchronously returns the first element of <paramref name="source"/>, or <see langword="default"/> when empty.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <returns>A task containing the first element or <see langword="default"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T?> FirstOrDefaultAsync(IObservableAsync<T> source) =>
        FirstOrDefaultAsync(source, default, CancellationToken.None);

    /// <summary>Asynchronously returns the first element of <paramref name="source"/>, or <see langword="default"/> when empty.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the first element or <see langword="default"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T?> FirstOrDefaultAsync(
        IObservableAsync<T> source,
        CancellationToken cancellationToken) =>
        FirstOrDefaultAsync(source, default, cancellationToken);

    /// <summary>Asynchronously returns the first element of <paramref name="source"/>, or <paramref name="defaultValue"/> when empty.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="defaultValue">Fallback value returned when empty.</param>
    /// <returns>A task containing the first element or <paramref name="defaultValue"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T?> FirstOrDefaultAsync(IObservableAsync<T> source, T? defaultValue) =>
        FirstOrDefaultAsync(source, defaultValue, CancellationToken.None);

    /// <summary>Asynchronously returns the first element of <paramref name="source"/>, or <paramref name="defaultValue"/> when empty.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="defaultValue">Fallback value returned when empty.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the first element or <paramref name="defaultValue"/>.</returns>
    public static ValueTask<T?> FirstOrDefaultAsync(
        IObservableAsync<T> source,
        T? defaultValue,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        FirstTaskWitness<T> observer = new(null, true, defaultValue, cancellationToken);
        return ReactiveUI.Primitives.Async.Internal.AsyncTaskTerminalRunner.RunToResultAsync(
            source,
            observer,
            observer._completion,
            cancellationToken)!;
    }

    /// <summary>Asynchronously returns the first element of <paramref name="source"/> matching <paramref name="predicate"/>, or <paramref name="defaultValue"/> when none match.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">Predicate used to test each element.</param>
    /// <param name="defaultValue">Fallback value returned when none match.</param>
    /// <returns>A task containing the first matching element or <paramref name="defaultValue"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T?> FirstOrDefaultAsync(
        IObservableAsync<T> source,
        Func<T, bool> predicate,
        T? defaultValue) =>
        FirstOrDefaultAsync(source, predicate, defaultValue, CancellationToken.None);

    /// <summary>Asynchronously returns the first element of <paramref name="source"/> matching <paramref name="predicate"/>, or <paramref name="defaultValue"/> when none match.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">Predicate used to test each element.</param>
    /// <param name="defaultValue">Fallback value returned when none match.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the first matching element or <paramref name="defaultValue"/>.</returns>
    public static ValueTask<T?> FirstOrDefaultAsync(
        IObservableAsync<T> source,
        Func<T, bool> predicate,
        T? defaultValue,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        FirstTaskWitness<T> observer = new(predicate, true, defaultValue, cancellationToken);
        return ReactiveUI.Primitives.Async.Internal.AsyncTaskTerminalRunner.RunToResultAsync(
            source,
            observer,
            observer._completion,
            cancellationToken)!;
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
        return predicate is not null && !predicate(value)
            ? default
            : _completion.SetResultAndDisposeAsync(value, this);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        _completion.SetExceptionAndDisposeAsync(error, this);

    /// <inheritdoc/>
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
        hasDefault
            ? _completion.CompleteAndDisposeAsync(result, defaultValue!, this)
            : ReactiveUI.Primitives.Async.Internal.AsyncTaskTerminalRunner.CompleteEmptySequence(
                _completion,
                result,
                predicate is not null,
                this);
}
