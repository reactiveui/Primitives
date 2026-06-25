// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Async-enumerable observable adapter.</summary>
/// <typeparam name="T">The value type.</typeparam>
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

        var cts = CancellationToken.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(CancellationToken)
            : new();
        var disposed = 0;
        _ = PumpAsync(observer, cts);

        return new ActionDisposable(() =>
        {
            if (Interlocked.Exchange(ref disposed, 1) != 0)
            {
                return;
            }

            try
            {
                cts.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // The pump already completed and disposed the cancellation source.
            }
        });
    }

    /// <summary>Pumps the async enumerable into an observer.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cts">The subscription cancellation source.</param>
    /// <returns>The asynchronous pump task.</returns>
    /// <remarks>
    /// Disposal is single-owner: this method's <c>finally</c> is the only path that disposes the
    /// enumerator. The subscription disposer merely cancels <paramref name="cts"/>, which unblocks
    /// <see cref="IAsyncEnumerator{T}.MoveNextAsync"/> so the pump terminates and cleans up exactly once.
    /// </remarks>
    private async Task PumpAsync(IObserver<T> observer, CancellationTokenSource cts)
    {
        IAsyncEnumerator<T>? enumerator = null;
        try
        {
            enumerator = Values.GetAsyncEnumerator(cts.Token);
            while (!cts.IsCancellationRequested && await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                // Re-check after the await: disposal may have torn down the observer while the
                // element was in flight, so a buffered value must not reach a stopped observer.
                if (cts.IsCancellationRequested)
                {
                    break;
                }

                observer.OnNext(enumerator.Current);
            }

            if (!cts.IsCancellationRequested)
            {
                observer.OnCompleted();
            }
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            // Disposal requested cancellation; observers should not receive a terminal signal.
        }
        catch (Exception error) when (!cts.IsCancellationRequested)
        {
            observer.OnError(error);
        }
        finally
        {
            if (enumerator is not null)
            {
                await enumerator.DisposeAsync().ConfigureAwait(false);
            }

            cts.Dispose();
        }
    }
}
#endif
