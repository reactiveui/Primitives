// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Observer that waits for a sequence to complete, ignoring all emitted values.</summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="cancellationToken">A cancellation token for the operation.</param>
[System.Diagnostics.DebuggerDisplay("CompletionTaskWitness: {_witness}")]
public sealed class CompletionTaskWitness<T>(CancellationToken cancellationToken) : IWitnessAsync<T>
{
    /// <summary>Produces and cancels the witness's single result value.</summary>
    private readonly TaskResultCompletionSource<object?> _completion = new(cancellationToken);

    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <summary>Asynchronously waits for <paramref name="source"/> to complete.</summary>
    /// <param name="source">The observable sequence to wait on.</param>
    /// <returns>A task representing the asynchronous wait operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask RunAsync(IObservableAsync<T> source) =>
        RunAsync(source, CancellationToken.None);

    /// <summary>Asynchronously waits for <paramref name="source"/> to complete.</summary>
    /// <param name="source">The observable sequence to wait on.</param>
    /// <param name="cancellationToken">A token to cancel the wait operation.</param>
    /// <returns>A task representing the asynchronous wait operation.</returns>
    public static ValueTask RunAsync(IObservableAsync<T> source, CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        cancellationToken.ThrowIfCancellationRequested();

        CompletionTaskWitness<T> observer = new(cancellationToken);
        return ReactiveUI.Primitives.Async.Internal.AsyncTaskTerminalRunner.RunToCompletionAsync(
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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken) => default;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        _completion.SetExceptionAndDisposeAsync(error, this);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
        _completion.CompleteAndDisposeAsync(result, null, this);
}
