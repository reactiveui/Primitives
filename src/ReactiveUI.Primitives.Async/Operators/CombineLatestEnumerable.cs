// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides CombineLatest overloads for enumerable collections of asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>CombineLatest operators for an enumerable collection of observable source sequences.</summary>
    /// <param name="sources">The source sequences to combine.</param>
    /// <typeparam name="TSource">The element type produced by the source sequences.</typeparam>
    extension<TSource>(IEnumerable<IObservableAsync<TSource>> sources)
    {
        /// <summary>Combines the latest value from each asynchronous observable sequence in the supplied collection.</summary>
        /// <remarks>
        /// <para>For perf reasons each emitted <see cref="IReadOnlyList{T}"/> is a reference to a single shared buffer
        /// owned by the subscription, not a fresh allocation. Downstream observers MUST consume the snapshot synchronously
        /// inside their <c>OnNextAsync</c> handler; retaining a reference past the handler will surface the next
        /// emission's values instead, because the buffer is overwritten under the operator's gate before each emit.
        /// If you need a stable copy, project to one via the projecting <c>CombineLatest</c> overload or
        /// <c>.Select(static s =&gt; s.ToArray())</c>.</para>
        /// </remarks>
        /// <returns>An observable sequence that emits a snapshot of the latest values whenever any source produces a new value,
        /// after all sources have produced at least one value.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="sources"/> is <see langword="null"/>.</exception>
        public IObservableAsync<IReadOnlyList<TSource>> CombineLatest()
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);

            // Delegate to the projecting variant with an identity selector so a single subscription
            // implementation backs both shapes. The static lambda avoids capturing and matches the
            // perf-critical zero-alloc rule for selectors that don't reference enclosing state.
            return new CombineLatestEnumerableSignal<TSource, IReadOnlyList<TSource>>(sources, static s => s);
        }

        /// <summary>
        /// Combines the latest value from each asynchronous observable sequence in the supplied collection and projects the
        /// resulting snapshot into a result value.
        /// </summary>
        /// <typeparam name="TResult">The projected result type.</typeparam>
        /// <param name="resultSelector">A selector that projects the current snapshot of latest values into a result value.</param>
        /// <returns>An observable sequence that emits projected results whenever any source produces a new value, after all
        /// sources have produced at least one value.</returns>
        /// <exception cref="ArgumentNullException">Thrown if <paramref name="sources"/> or <paramref name="resultSelector"/>
        /// is <see langword="null"/>.</exception>
        public IObservableAsync<TResult> CombineLatest<TResult>(
            Func<IReadOnlyList<TSource>, TResult> resultSelector)
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);
            ArgumentExceptionHelper.ThrowIfNull(resultSelector);

            return new CombineLatestEnumerableSignal<TSource, TResult>(sources, resultSelector);
        }
    }

    /// <summary>
    /// Async observable that combines latest values from an enumerable of sources and projects through a selector.
    /// The no-selector public overload delegates here with an identity selector so a single subscription
    /// implementation backs both shapes.
    /// </summary>
    /// <typeparam name="TSource">The element type.</typeparam>
    /// <typeparam name="TResult">The projected result type.</typeparam>
    /// <param name="sources">The source sequences to combine.</param>
    /// <param name="resultSelector">The result selector.</param>
    internal sealed class CombineLatestEnumerableSignal<TSource, TResult>(
        IEnumerable<IObservableAsync<TSource>> sources,
        Func<IReadOnlyList<TSource>, TResult> resultSelector)
        : SignalAsync<TResult>
    {
        /// <summary>The source sequences.</summary>
        private readonly IObservableAsync<TSource>[] _sources =
            (sources as IObservableAsync<TSource>[]) ?? [.. sources ?? throw new ArgumentNullException(nameof(sources))];

        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<TResult> observer,
            CancellationToken cancellationToken)
        {
            if (_sources.Length == 0)
            {
                await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
                return DisposableAsync.Empty;
            }

            var subscription = new EnumerableCombineLatestCoordinator(_sources, observer, resultSelector);
            return await SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
                subscription,
                () => subscription.SubscribeSourcesAsync(cancellationToken)).ConfigureAwait(false);
        }

        /// <summary>
        /// Per-source witness that forwards to the parent subscription with its own index. Replaces the three
        /// captured-index lambdas the previous shape allocated per source.
        /// </summary>
        /// <param name="parent">The parent coordinator.</param>
        /// <param name="index">The source index.</param>
        internal sealed class IndexedWitness(EnumerableCombineLatestCoordinator parent, int index) : IObserverAsync<TSource>
        {
            /// <inheritdoc/>
            public ValueTask OnNextAsync(TSource value, CancellationToken cancellationToken) =>
                parent.OnNextAsync(index, value, cancellationToken);

            /// <inheritdoc/>
            public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
                parent.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            public ValueTask OnCompletedAsync(Result result) =>
                parent.OnCompletedAsync(index, result);

            /// <inheritdoc/>
            public ValueTask DisposeAsync() => default;
        }

        /// <summary>Manages subscriptions to all source sequences and coordinates emission of projected snapshots.</summary>
        /// <param name="sources">The source sequences.</param>
        /// <param name="observer">The observer.</param>
        /// <param name="resultSelector">The result selector.</param>
        internal sealed class EnumerableCombineLatestCoordinator(
            IObservableAsync<TSource>[] sources,
            IObserverAsync<TResult> observer,
            Func<IReadOnlyList<TSource>, TResult> resultSelector) : IAsyncDisposable
        {
            /// <summary>Synchronization gate.</summary>
            private readonly AsyncSerialGate _gate = new();

            /// <summary>Cancellation source for disposal.</summary>
            private readonly CancellationTokenSource _disposeCts = new();

            /// <summary>The completion lock.</summary>
            private readonly Lock _completionLock = new();

            /// <summary>Downstream observer.</summary>
            [SuppressMessage(
                "Usage",
                "CA2213:Disposable fields should be disposed",
                Justification = "The observer is disposed by the caller or downstream.")]
            private readonly IObserverAsync<TResult> _observer = observer ?? throw new ArgumentNullException(nameof(observer));

            /// <summary>Source list.</summary>
            private readonly IObservableAsync<TSource>[] _sources = sources ?? throw new ArgumentNullException(nameof(sources));

            /// <summary>Latest values from each source.</summary>
            private readonly Optional<TSource>[] _values = new Optional<TSource>[sources.Length];

            /// <summary>Completion status of each source.</summary>
            private readonly bool[] _completed = new bool[sources.Length];

            /// <summary>Active subscriptions.</summary>
            private readonly IAsyncDisposable?[] _subscriptions = new IAsyncDisposable?[sources.Length];

            /// <summary>
            /// Reusable buffer fed to the projecting selector. Safe to reuse: the selector is invoked
            /// synchronously inside the gate, so the projected result is computed before the next emission
            /// can overwrite the buffer.
            /// </summary>
            private readonly TSource[] _snapshotBuffer = new TSource[sources.Length];

            /// <summary>Number of completed sources.</summary>
            private int _completedCount;

            /// <summary>Disposed flag.</summary>
            private int _disposed;

            /// <summary>Number of sources that have produced a value.</summary>
            private int _hasValueCount;

            /// <summary>Subscribes to all source sequences.</summary>
            /// <param name="cancellationToken">The cancellation token.</param>
            /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
            public async ValueTask SubscribeSourcesAsync(CancellationToken cancellationToken)
            {
                for (var index = 0; index < _sources.Length; index++)
                {
                    if (_disposeCts.IsCancellationRequested)
                    {
                        return;
                    }

                    _subscriptions[index] = await _sources[index]
                        .SubscribeAsync(new IndexedWitness(this, index), cancellationToken)
                        .ConfigureAwait(false);
                }
            }

            /// <inheritdoc/>
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

                    if (!_values[index].HasValue)
                    {
                        _hasValueCount++;
                    }

                    _values[index] = new(indexValue);

                    if (_hasValueCount < _values.Length)
                    {
                        return;
                    }

                    // Invariant: _hasValueCount transitioning to _values.Length above means every
                    // slot in _values has received at least one value, so the snapshot is fully
                    // populated by the time we reach this point.
                    for (var i = 0; i < _values.Length; i++)
                    {
                        _snapshotBuffer[i] = _values[i].Value!;
                    }

                    TResult projected;
                    try
                    {
                        projected = resultSelector(_snapshotBuffer);
                    }
                    catch (Exception ex)
                    {
                        await FinishAsync(Result.Failure(ex)).ConfigureAwait(false);
                        return;
                    }

                    await _observer.OnNextAsync(projected, cancellationToken).ConfigureAwait(false);
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

                    await _observer.OnErrorResumeAsync(error, cancellationToken).ConfigureAwait(false);
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
                    if (_disposed == 1 || _completed[index])
                    {
                        return default;
                    }

                    _completed[index] = true;
                    _completedCount++;
                    shouldComplete = !_values[index].HasValue || _completedCount == _sources.Length;
                }

                return shouldComplete ? FinishAsync(Result.Success) : default;
            }

            /// <summary>
            /// Completes the subscription. The gate / dispose CTS are always released in the finally
            /// block so a misbehaving downstream's OnCompletedAsync can't leak the SemaphoreSlim inside
            /// AsyncSerialGate or the dispose CTS's wait handles.
            /// </summary>
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

                    for (var i = 0; i < _subscriptions.Length; i++)
                    {
                        var subscription = _subscriptions[i];
                        if (subscription is not null)
                        {
                            await subscription.DisposeAsync().ConfigureAwait(false);
                        }
                    }

                    if (result is not null)
                    {
                        await _observer.OnCompletedAsync(result.Value).ConfigureAwait(false);
                    }
                }
                finally
                {
                    _disposeCts.Dispose();
                    _gate.Dispose();
                }
            }
        }
    }
}
