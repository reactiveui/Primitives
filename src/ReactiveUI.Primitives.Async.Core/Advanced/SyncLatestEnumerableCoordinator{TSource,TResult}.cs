// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Coordinates subscriptions and projected snapshot emission for enumerable <c>SyncLatest</c> sources.</summary>
/// <typeparam name="TSource">The source element type.</typeparam>
/// <typeparam name="TResult">The projected result type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Sources = {Sources.Length}, Completed = {_completedCount}, Disposed = {_disposed}")]
public sealed class SyncLatestEnumerableCoordinator<TSource, TResult> : IAsyncDisposable
{
    /// <summary>Synchronization gate.</summary>
    private readonly AsyncSerialGate _gate = new();

    /// <summary>Cancellation source for disposal.</summary>
    private readonly CancellationTokenSource _disposeCts = new();

    /// <summary>The completion lock.</summary>
    private readonly Lock _completionLock = new();

    /// <summary>Number of completed sources.</summary>
    private int _completedCount;

    /// <summary>Disposed flag.</summary>
    private int _disposed;

    /// <summary>Number of sources that have produced a value.</summary>
    private int _hasValueCount;

    /// <summary>Initializes a new instance of the <see cref="SyncLatestEnumerableCoordinator{TSource, TResult}"/> class.</summary>
    /// <param name="sources">The source sequences.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="resultSelector">The result selector.</param>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> or <paramref name="observer"/> is <see langword="null"/>.</exception>
    public SyncLatestEnumerableCoordinator(
        IObservableAsync<TSource>[] sources,
        IObserverAsync<TResult> observer,
        Func<IReadOnlyList<TSource>, TResult> resultSelector)
    {
        Sources = sources ?? throw new ArgumentNullException(nameof(sources));
        Observer = observer ?? throw new ArgumentNullException(nameof(observer));
        ResultSelector = resultSelector;
        Values = new Optional<TSource>[sources.Length];
        Completed = new bool[sources.Length];
        Subscriptions = new IAsyncDisposable?[sources.Length];
        SnapshotBuffer = new TSource[sources.Length];
    }

    /// <summary>Gets the downstream observer.</summary>
    [SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "The observer is disposed by the caller or downstream.")]
    private IObserverAsync<TResult> Observer { get; }

    /// <summary>Gets the source list.</summary>
    private IObservableAsync<TSource>[] Sources { get; }

    /// <summary>Gets the latest values from each source.</summary>
    private Optional<TSource>[] Values { get; }

    /// <summary>Gets the completion status of each source.</summary>
    private bool[] Completed { get; }

    /// <summary>Gets the active subscriptions.</summary>
    private IAsyncDisposable?[] Subscriptions { get; }

    /// <summary>Gets the reusable selector snapshot buffer.</summary>
    private TSource[] SnapshotBuffer { get; }

    /// <summary>Gets the result selector.</summary>
    private Func<IReadOnlyList<TSource>, TResult> ResultSelector { get; }

    /// <summary>Subscribes to all source sequences.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    public async ValueTask SubscribeSourcesAsync(CancellationToken cancellationToken)
    {
        for (var index = 0; index < Sources.Length; index++)
        {
            if (_disposeCts.IsCancellationRequested)
            {
                return;
            }

            Subscriptions[index] = await Sources[index]
                .SubscribeAsync(new SyncLatestEnumerableWitness<TSource, TResult>(this, index), cancellationToken)
                .ConfigureAwait(false);
        }
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => FinishAsync(null);

    /// <summary>Handles OnNext from a source.</summary>
    /// <param name="index">The source index.</param>
    /// <param name="indexValue">The value.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A value task representing the operation.</returns>
    internal async ValueTask OnNextAsync(int index, TSource indexValue, CancellationToken cancellationToken)
    {
        using (await _gate.EnterAsync(cancellationToken).ConfigureAwait(false))
        {
            if (DisposalHelper.HasDisposed(_disposed))
            {
                return;
            }

            if (!Values[index].HasValue)
            {
                _hasValueCount++;
            }

            Values[index] = new(indexValue);

            if (_hasValueCount < Values.Length)
            {
                return;
            }

            for (var i = 0; i < Values.Length; i++)
            {
                SnapshotBuffer[i] = Values[i].Value!;
            }

            TResult projected;
            try
            {
                projected = ResultSelector(SnapshotBuffer);
            }
            catch (Exception ex)
            {
                await FinishAsync(Result.Failure(ex)).ConfigureAwait(false);
                return;
            }

            await Observer.OnNextAsync(projected, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Handles OnErrorResume from a source.</summary>
    /// <param name="error">The error.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A value task representing the operation.</returns>
    internal async ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken)
    {
        using (await _gate.EnterAsync(cancellationToken).ConfigureAwait(false))
        {
            if (DisposalHelper.HasDisposed(_disposed))
            {
                return;
            }

            await Observer.OnErrorResumeAsync(error, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Handles OnCompleted from a source.</summary>
    /// <param name="index">The source index.</param>
    /// <param name="result">The result.</param>
    /// <returns>A value task representing the operation.</returns>
    internal ValueTask OnCompletedAsync(int index, Result result)
    {
        if (result.IsFailure)
        {
            return FinishAsync(result);
        }

        bool shouldComplete;
        lock (_completionLock)
        {
            if (_disposed == 1 || Completed[index])
            {
                return default;
            }

            Completed[index] = true;
            _completedCount++;
            shouldComplete = !Values[index].HasValue || _completedCount == Sources.Length;
        }

        return shouldComplete ? FinishAsync(Result.Success) : default;
    }

    /// <summary>Completes the subscription. The gate and dispose CTS are always released in the finally block.</summary>
    /// <param name="result">The result.</param>
    /// <returns>A value task representing the operation.</returns>
    internal async ValueTask FinishAsync(Result? result)
    {
        if (DisposalHelper.TrySetDisposed(ref _disposed))
        {
            return;
        }

        try
        {
            await _disposeCts.CancelAsync().ConfigureAwait(false);

            for (var i = 0; i < Subscriptions.Length; i++)
            {
                var subscription = Subscriptions[i];
                if (subscription is not null)
                {
                    await subscription.DisposeAsync().ConfigureAwait(false);
                }
            }

            if (result is not null)
            {
                await Observer.OnCompletedAsync(result.Value).ConfigureAwait(false);
            }
        }
        finally
        {
            _disposeCts.Dispose();
            _gate.Dispose();
        }
    }
}
