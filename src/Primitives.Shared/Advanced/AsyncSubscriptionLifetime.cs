// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Owns cancellation and the eventual inner disposable for asynchronous signal subscriptions.</summary>
/// <remarks>
/// Call <see cref="Complete"/> exactly once after asynchronous setup has finished, even when setup faults or is canceled.
/// </remarks>
public sealed class AsyncSubscriptionLifetime : IDisposable
{
    /// <summary>The cancellation source passed to the asynchronous subscription.</summary>
    private readonly CancellationTokenSource _cts = new();

    /// <summary>The inner disposable returned by the asynchronous subscription.</summary>
    private readonly SingleDisposable _subscription = new();

    /// <summary>Non-zero once the outer subscription has been disposed.</summary>
    private int _disposed;

    /// <summary>Non-zero when disposal requested cancellation before the asynchronous subscription completed.</summary>
    private int _canceled;

    /// <summary>Non-zero once the asynchronous subscription task has completed.</summary>
    private int _completed;

    /// <summary>Gets the token supplied to the asynchronous subscription.</summary>
    public CancellationToken Token => _cts.Token;

    /// <summary>Gets a value indicating whether disposal requested cancellation before asynchronous setup completed.</summary>
    public bool IsCancellationRequested => Volatile.Read(ref _canceled) != 0;

    /// <summary>Assigns the disposable returned by asynchronous setup.</summary>
    /// <param name="disposable">The returned disposable, or <see langword="null"/> for an empty lifetime.</param>
    public void SetSubscription(IDisposable? disposable) =>
        _subscription.Create(disposable ?? EmptyDisposable.Instance);

    /// <summary>Marks asynchronous setup complete and releases the cancellation source when still owned here.</summary>
    public void Complete()
    {
        Volatile.Write(ref _completed, 1);
        if (Volatile.Read(ref _disposed) != 0)
        {
            return;
        }

        _cts.Dispose();
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        if (Volatile.Read(ref _completed) != 0)
        {
            _subscription.Dispose();
            _cts.Dispose();
            return;
        }

        Volatile.Write(ref _canceled, 1);
        try
        {
            _cts.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // Completion can release the CTS concurrently; disposal still continues with the inner subscription.
        }

        _subscription.Dispose();
        _cts.Dispose();
    }
}
