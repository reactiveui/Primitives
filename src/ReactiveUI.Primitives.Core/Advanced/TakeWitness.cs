// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Observer used by Take to dispose the upstream subscription as soon as the requested count is reached.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="observer">Downstream observer.</param>
/// <param name="count">Number of values to forward.</param>
[System.Diagnostics.DebuggerDisplay("TakeWitness: Remaining = {_remaining}, Stopped = {_stopped}")]
public sealed class TakeWitness<T>(IObserver<T> observer, int count) : IObserver<T>, IDisposable
{
    /// <summary>Downstream observer.</summary>
    private readonly IObserver<T> _observer = observer;

    /// <summary>Upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <summary>Remaining values to forward.</summary>
    private int _remaining = count;

    /// <summary>Non-zero after completion, error, or disposal.</summary>
    private int _stopped;

    /// <summary>Assigns the upstream subscription so the sink can release it once the count is reached.</summary>
    /// <param name="subscription">Upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (Volatile.Read(ref _stopped) != 0 || _remaining <= 0)
        {
            return;
        }

        var remaining = --_remaining;
        SinkDelivery.Next(_observer, value, this);
        if (remaining != 0)
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
            SinkSubscription.Dispose(ref _subscription);
        }
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => Complete();

    /// <inheritdoc/>
    public void Dispose()
    {
        Volatile.Write(ref _stopped, 1);
        SinkSubscription.Dispose(ref _subscription);
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
            SinkSubscription.Dispose(ref _subscription);
        }
    }
}
