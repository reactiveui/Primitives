// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A witness that determines whether a sequence contains a specified value.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="target">The value to search for.</param>
/// <param name="comparer">The equality comparer to use for comparison.</param>
/// <param name="cancellationToken">A cancellation token for the operation.</param>
[System.Diagnostics.DebuggerDisplay("ContainsTaskWitness: {_witness}")]
public sealed class ContainsTaskWitness<T>(
    T target,
    IEqualityComparer<T> comparer,
    CancellationToken cancellationToken) : IWitnessAsync<T>
{
    /// <summary>Produces and cancels the witness's single result value.</summary>
    private readonly TaskResultCompletionSource<bool> _completion = new(cancellationToken);

    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <summary>Asynchronously determines whether <paramref name="source"/> contains <paramref name="value"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="value">The value to locate.</param>
    /// <returns>A task whose result is <see langword="true"/> when <paramref name="value"/> is found.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<bool> RunAsync(IObservableAsync<T> source, T value) =>
        RunAsync(source, value, null, CancellationToken.None);

    /// <summary>Asynchronously determines whether <paramref name="source"/> contains <paramref name="value"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="value">The value to locate.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task whose result is <see langword="true"/> when <paramref name="value"/> is found.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<bool> RunAsync(
        IObservableAsync<T> source,
        T value,
        CancellationToken cancellationToken) =>
        RunAsync(source, value, null, cancellationToken);

    /// <summary>Asynchronously determines whether <paramref name="source"/> contains <paramref name="value"/> using <paramref name="comparer"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="value">The value to locate.</param>
    /// <param name="comparer">Optional equality comparer.</param>
    /// <returns>A task whose result is <see langword="true"/> when <paramref name="value"/> is found.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<bool> RunAsync(
        IObservableAsync<T> source,
        T value,
        IEqualityComparer<T>? comparer) =>
        RunAsync(source, value, comparer, CancellationToken.None);

    /// <summary>Asynchronously determines whether <paramref name="source"/> contains <paramref name="value"/> using <paramref name="comparer"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="value">The value to locate.</param>
    /// <param name="comparer">Optional equality comparer.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task whose result is <see langword="true"/> when <paramref name="value"/> is found.</returns>
    public static ValueTask<bool> RunAsync(
        IObservableAsync<T> source,
        T value,
        IEqualityComparer<T>? comparer,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var cmp = comparer ?? EqualityComparer<T>.Default;
        ContainsTaskWitness<T> observer = new(value, cmp, cancellationToken);
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
        _ = cancellationToken;
        return !comparer.Equals(target, value)
            ? default
            : _completion.SetResultAndDisposeAsync(true, this);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        _completion.SetExceptionAndDisposeAsync(error, this);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
        _completion.CompleteAndDisposeAsync(result, false, this);
}
