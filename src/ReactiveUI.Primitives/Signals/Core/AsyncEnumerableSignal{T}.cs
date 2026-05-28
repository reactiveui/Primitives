// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Async-enumerable observable adapter.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
internal sealed class AsyncEnumerableSignal<T> : IAsyncEnumerableBackedSignal<T>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="AsyncEnumerableSignal{T}"/> class.
    /// </summary>
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
        if (observer == null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        var cts = CancellationToken.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(CancellationToken)
            : new CancellationTokenSource();
        var disposed = 0;
        IAsyncEnumerator<T>? enumerator = null;
        _ = RunAsync();

        return Disposable.Create(() =>
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
                return;
            }

            var current = Volatile.Read(ref enumerator);
            if (current == null)
            {
                return;
            }

            try
            {
                _ = current.DisposeAsync().AsTask();
            }
            catch (NotSupportedException)
            {
                // Some enumerators only support disposal from the enumeration path.
            }
        });

        async Task RunAsync()
        {
            try
            {
                await PumpAsync(observer, cts, current => enumerator = current).ConfigureAwait(false);
            }
            finally
            {
                Volatile.Write(ref disposed, 1);
            }
        }
    }

    /// <summary>
    /// Pumps the async enumerable into an observer.
    /// </summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cts">The subscription cancellation source.</param>
    /// <param name="setEnumerator">Captures the active enumerator.</param>
    /// <returns>The asynchronous pump task.</returns>
    private async Task PumpAsync(IObserver<T> observer, CancellationTokenSource cts, Action<IAsyncEnumerator<T>> setEnumerator)
    {
        IAsyncEnumerator<T>? enumerator = null;
        try
        {
            enumerator = Values.GetAsyncEnumerator(cts.Token);
            setEnumerator(enumerator);
            while (!cts.IsCancellationRequested && await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
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
            if (enumerator != null)
            {
                await enumerator.DisposeAsync().ConfigureAwait(false);
            }

            cts.Dispose();
        }
    }
}
#endif
