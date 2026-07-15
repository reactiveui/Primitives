// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Observer used by Take to dispose the upstream subscription as soon as the requested count is reached.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="observer">Downstream observer.</param>
/// <param name="count">Number of values to forward.</param>
public sealed class TakeWitness<T>(IObserver<T> observer, int count) : IObserver<T>, IDisposable
{
    /// <summary>Downstream observer.</summary>
    private readonly IObserver<T> _observer = observer;

    /// <summary>Upstream subscription.</summary>
    private readonly SingleReplaceableDisposable _subscription = new();

    /// <summary>Remaining values to forward.</summary>
    private int _remaining = count;

    /// <summary>Non-zero after completion, error, or disposal.</summary>
    private int _stopped;

    /// <summary>Sets the upstream subscription.</summary>
    /// <param name="subscription">Upstream subscription.</param>
    public void SetSubscription(IDisposable subscription) => _subscription.Create(subscription);

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (Volatile.Read(ref _stopped) != 0 || _remaining <= 0)
        {
            return;
        }

        try
        {
            _observer.OnNext(value);
        }
        catch
        {
            Dispose();
            throw;
        }

        _remaining--;
        if (_remaining != 0)
        {
            return;
        }

        Complete();
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        try
        {
            _observer.OnError(error);
        }
        finally
        {
            _subscription.Dispose();
        }
    }

    /// <inheritdoc/>
    public void OnCompleted() => Complete();

    /// <inheritdoc/>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        _subscription.Dispose();
    }

    /// <summary>Completes the downstream observer and releases the upstream subscription.</summary>
    private void Complete()
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        try
        {
            _observer.OnCompleted();
        }
        finally
        {
            _subscription.Dispose();
        }
    }
}
