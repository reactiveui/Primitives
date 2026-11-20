// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A witness that invokes a synchronous callback for each element and signals completion via a task.</summary>
/// <typeparam name="T">The type of elements in the sequence.</typeparam>
/// <param name="onNext">The synchronous callback to invoke for each element.</param>
/// <param name="cancellationToken">A cancellation token for the operation.</param>
[System.Diagnostics.DebuggerDisplay("ForEachSyncTaskWitness: {_witness}")]
public sealed class ForEachSyncTaskWitness<T>(Action<T> onNext, CancellationToken cancellationToken) : IWitnessAsync<T>
{
    /// <summary>Produces and cancels the witness's single result value.</summary>
    private readonly TaskResultCompletionSource<bool> _completion = new(cancellationToken);

    /// <summary>The synchronous callback invoked for each element in the sequence.</summary>
    private readonly Action<T> _onNext = onNext;

    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <summary>Asynchronously invokes <paramref name="onNext"/> for each element of <paramref name="source"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="onNext">The synchronous callback invoked per element.</param>
    /// <returns>A task that completes when all elements have been processed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ValueTask RunAsync(IObservableAsync<T> source, Action<T> onNext) =>
        RunAsync(source, onNext, CancellationToken.None);

    /// <summary>Asynchronously invokes <paramref name="onNext"/> for each element of <paramref name="source"/>.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="onNext">The synchronous callback invoked per element.</param>
    /// <param name="cancellationToken">A token to cancel the iteration.</param>
    /// <returns>A task that completes when all elements have been processed.</returns>
    public static ValueTask RunAsync(
        IObservableAsync<T> source,
        Action<T> onNext,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(onNext);
        cancellationToken.ThrowIfCancellationRequested();

        ForEachSyncTaskWitness<T> observer = new(onNext, cancellationToken);
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
    ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
    {
        _onNext(value);
        return default;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        _completion.SetExceptionAndDisposeAsync(error, this);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
        _completion.CompleteAndDisposeAsync(result, true, this);
}
