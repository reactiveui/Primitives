// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Witness that captures the last element matching an optional predicate, or returns a default value.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="predicate">An optional predicate to filter elements.</param>
/// <param name="defaultValue">The default value to return if no element matches.</param>
/// <param name="cancellationToken">A cancellation token for the operation.</param>
[System.Diagnostics.DebuggerDisplay("LastOrDefaultTaskWitness: {_witness}")]
public sealed class LastOrDefaultTaskWitness<T>(
    Func<T, bool>? predicate,
    T? defaultValue,
    CancellationToken cancellationToken) : IWitnessAsync<T>
{
    /// <summary>Produces and cancels the witness's single result value.</summary>
    private readonly TaskResultCompletionSource<T> _completion = new(cancellationToken);

    /// <summary>The most recently observed matching element, or the default value if no match has been found.</summary>
    private T? _last = defaultValue;

    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <summary>Asynchronously returns the last element of <paramref name="source"/>, or <see langword="default"/> when empty.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <returns>A task containing the last element or <see langword="default"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T?> RunAsync(IObservableAsync<T> source) =>
        RunAsync(source, default, CancellationToken.None);

    /// <summary>Asynchronously returns the last element of <paramref name="source"/>, or <see langword="default"/> when empty.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the last element or <see langword="default"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T?> RunAsync(
        IObservableAsync<T> source,
        CancellationToken cancellationToken) =>
        RunAsync(source, default, cancellationToken);

    /// <summary>Asynchronously returns the last element of <paramref name="source"/>, or <paramref name="defaultValue"/> when empty.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="defaultValue">Fallback value returned when empty.</param>
    /// <returns>A task containing the last element or <paramref name="defaultValue"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T?> RunAsync(IObservableAsync<T> source, T? defaultValue) =>
        RunAsync(source, defaultValue, CancellationToken.None);

    /// <summary>Asynchronously returns the last element of <paramref name="source"/>, or <paramref name="defaultValue"/> when empty.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="defaultValue">Fallback value returned when empty.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the last element or <paramref name="defaultValue"/>.</returns>
    public static ValueTask<T?> RunAsync(
        IObservableAsync<T> source,
        T? defaultValue,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        LastOrDefaultTaskWitness<T> observer = new(null, defaultValue, cancellationToken);
        return ReactiveUI.Primitives.Async.Internal.AsyncTaskTerminalRunner.RunToResultAsync(
            source,
            observer,
            observer._completion,
            cancellationToken)!;
    }

    /// <summary>Asynchronously returns the last element of <paramref name="source"/> matching <paramref name="predicate"/>, or <paramref name="defaultValue"/> when none match.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">Predicate used to test each element.</param>
    /// <param name="defaultValue">Fallback value returned when none match.</param>
    /// <returns>A task containing the last matching element or <paramref name="defaultValue"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<T?> RunAsync(
        IObservableAsync<T> source,
        Func<T, bool> predicate,
        T? defaultValue) =>
        RunAsync(source, predicate, defaultValue, CancellationToken.None);

    /// <summary>Asynchronously returns the last element of <paramref name="source"/> matching <paramref name="predicate"/>, or <paramref name="defaultValue"/> when none match.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">Predicate used to test each element.</param>
    /// <param name="defaultValue">Fallback value returned when none match.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task containing the last matching element or <paramref name="defaultValue"/>.</returns>
    public static ValueTask<T?> RunAsync(
        IObservableAsync<T> source,
        Func<T, bool> predicate,
        T? defaultValue,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        LastOrDefaultTaskWitness<T> observer = new(predicate, defaultValue, cancellationToken);
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
        if (predicate is not null && !predicate(value))
        {
            return default;
        }

        _last = value;

        return default;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        _completion.SetExceptionAndDisposeAsync(error, this);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
        _completion.CompleteAndDisposeAsync(result, _last!, this);
}
