// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
namespace ReactiveUI.Primitives.Advanced;

/// <summary>Async-enumerable observable adapter.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Values = {Values}, CancellationRequested = {CancellationToken.IsCancellationRequested}")]
public sealed class AsyncEnumerableSignal<T> : IAsyncEnumerableBackedSignal<T>
{
    /// <summary>Initializes a new instance of the <see cref="AsyncEnumerableSignal{T}"/> class.</summary>
    /// <param name="values">The source async enumerable.</param>
    /// <param name="cancellationToken">The cancellation token used by the adapter.</param>
    public AsyncEnumerableSignal(IAsyncEnumerable<T> values, CancellationToken cancellationToken)
    {
        Values = values;
        CancellationToken = cancellationToken;
    }

    /// <inheritdoc/>
    public IAsyncEnumerable<T> Values { get; }

    /// <inheritdoc/>
    public CancellationToken CancellationToken { get; }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var subscription = new Subscription(observer, Values, CancellationToken);
        subscription.Start();
        return subscription;
    }

    /// <summary>Drives a single subscription's pump and coordinates single-owner enumerator disposal.</summary>
    /// <remarks>
    /// Enumerator disposal is single-owner via an interlocked claim: whichever of the pump's
    /// <c>finally</c> and <see cref="Dispose"/> wins disposes the enumerator exactly once. The disposer
    /// disposes immediately when it wins, so a non-cooperative enumerator (one that ignores cancellation
    /// in <see cref="IAsyncEnumerator{T}.MoveNextAsync"/>) is still torn down promptly without waiting on it.
    /// </remarks>
    private sealed class Subscription : IDisposable
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>The source async enumerable.</summary>
        private readonly IAsyncEnumerable<T> _values;

        /// <summary>The subscription cancellation source.</summary>
        private readonly CancellationTokenSource _cts;

        /// <summary>The active enumerator once created, published for the disposer to observe.</summary>
        private IAsyncEnumerator<T>? _enumerator;

        /// <summary>One once <see cref="Dispose"/> has run, making the disposer idempotent.</summary>
        private int _disposed;

        /// <summary>One once the enumerator has been disposed by whichever path won ownership.</summary>
        private int _enumeratorDisposed;

        /// <summary>Initializes a new instance of the <see cref="Subscription"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="values">The source async enumerable.</param>
        /// <param name="cancellationToken">The adapter cancellation token.</param>
        public Subscription(IObserver<T> observer, IAsyncEnumerable<T> values, CancellationToken cancellationToken)
        {
            _observer = observer;
            _values = values;
            _cts = cancellationToken.CanBeCanceled
                ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
                : new();
        }

        /// <summary>Starts the asynchronous pump.</summary>
        public void Start() => _ = PumpAsync();

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            try
            {
                _cts.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // The pump already completed and disposed the cancellation source.
            }

            // Dispose the enumerator immediately when this path wins ownership, rather than waiting
            // on MoveNextAsync, so a non-cooperative enumerator (one that ignores cancellation) is
            // still torn down promptly. The pump's finally disposes it only if it wins the race.
            if (TryClaimEnumerator(out var enumerator))
            {
                FireAndForgetDispose(enumerator);
            }

            _cts.Dispose();
        }

        /// <summary>Disposes an enumerator without surfacing the resulting task to the caller.</summary>
        /// <param name="enumerator">The enumerator to dispose.</param>
        /// <remarks>
        /// The disposer cannot await, so disposal runs detached; the local method observes the task to
        /// prevent unobserved-fault tear-downs, swallowing the <see cref="NotSupportedException"/> raised
        /// by enumerators that only permit disposal from the enumeration path.
        /// </remarks>
        private static void FireAndForgetDispose(IAsyncEnumerator<T> enumerator)
        {
            _ = ObserveAsync(enumerator);

            static async Task ObserveAsync(IAsyncEnumerator<T> enumerator)
            {
                try
                {
                    await enumerator.DisposeAsync().ConfigureAwait(false);
                }
                catch (NotSupportedException)
                {
                    // Some enumerators only support disposal from the enumeration path.
                }
            }
        }

        /// <summary>Claims sole ownership of enumerator disposal for the calling path.</summary>
        /// <param name="enumerator">The enumerator to dispose when the claim succeeds.</param>
        /// <returns><see langword="true"/> when the caller won the claim and must dispose the enumerator.</returns>
        private bool TryClaimEnumerator(out IAsyncEnumerator<T> enumerator)
        {
            var current = Volatile.Read(ref _enumerator);
            if (current is not null && Interlocked.Exchange(ref _enumeratorDisposed, 1) == 0)
            {
                enumerator = current;
                return true;
            }

            enumerator = null!;
            return false;
        }

        /// <summary>Pumps the async enumerable into the observer.</summary>
        /// <returns>The asynchronous pump task.</returns>
        private async Task PumpAsync()
        {
            try
            {
                var enumerator = _values.GetAsyncEnumerator(_cts.Token);
                Volatile.Write(ref _enumerator, enumerator);
                while (!_cts.IsCancellationRequested && await enumerator.MoveNextAsync().ConfigureAwait(false))
                {
                    // Re-check after the await: disposal may have torn down the observer while the
                    // element was in flight, so a buffered value must not reach a stopped observer.
                    if (_cts.IsCancellationRequested)
                    {
                        break;
                    }

                    _observer.OnNext(enumerator.Current);
                }

                if (!_cts.IsCancellationRequested)
                {
                    _observer.OnCompleted();
                }
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
                // Disposal requested cancellation; observers should not receive a terminal signal.
            }
            catch (Exception error) when (!_cts.IsCancellationRequested)
            {
                _observer.OnError(error);
            }
            finally
            {
                if (TryClaimEnumerator(out var enumerator))
                {
                    await enumerator.DisposeAsync().ConfigureAwait(false);
                }

                Dispose();
            }
        }
    }
}
#endif
