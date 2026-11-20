// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A witness that determines whether any element in the sequence satisfies a predicate.</summary>
/// <typeparam name="T">The type of elements in the sequence.</typeparam>
/// <param name="predicate">An optional predicate to test each element. If null, the sequence is checked for any elements.</param>
/// <param name="cancellationToken">A cancellation token for the operation.</param>
[System.Diagnostics.DebuggerDisplay("AnyTaskWitness: {_witness}")]
public sealed class AnyTaskWitness<T>(Func<T, bool>? predicate, CancellationToken cancellationToken) : IWitnessAsync<T>
{
    /// <summary>Produces and cancels the witness's single result value.</summary>
    private readonly TaskResultCompletionSource<bool> _completion = new(cancellationToken);

    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <summary>Asynchronously determines whether <paramref name="source"/> contains any elements.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <returns>A task whose result is <see langword="true"/> when <paramref name="source"/> emits any element.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<bool> RunAsync(IObservableAsync<T> source) =>
        RunAsync(source, null, CancellationToken.None);

    /// <summary>Asynchronously determines whether <paramref name="source"/> contains any elements.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task whose result is <see langword="true"/> when <paramref name="source"/> emits any element.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<bool> RunAsync(IObservableAsync<T> source, CancellationToken cancellationToken) =>
        RunAsync(source, null, cancellationToken);

    /// <summary>Asynchronously determines whether any element in <paramref name="source"/> satisfies <paramref name="predicate"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">A function to test each element for a condition.</param>
    /// <returns>A task whose result is <see langword="true"/> when any element satisfies <paramref name="predicate"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask<bool> RunAsync(IObservableAsync<T> source, Func<T, bool> predicate) =>
        RunAsync(source, predicate, CancellationToken.None);

    /// <summary>Asynchronously determines whether any element in <paramref name="source"/> satisfies <paramref name="predicate"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">An optional function to test each element; <see langword="null"/> checks for any element.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task whose result is <see langword="true"/> when any element matches.</returns>
    public static ValueTask<bool> RunAsync(
        IObservableAsync<T> source,
        Func<T, bool>? predicate,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        AnyTaskWitness<T> observer = new(predicate, cancellationToken);
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
        return predicate is not null && !predicate(value)
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
