// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>
/// Base class for observer "sinks" that manage a single upstream subscription and forward values to a
/// downstream observer. Operator sinks derive from this type; it is public so consumers can build or
/// extend their own sink-based operators.
/// </summary>
/// <typeparam name="T">The type of elements observed.</typeparam>
public abstract class SingleSourceWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>Disposed marker.</summary>
    private static readonly IDisposable DisposedSentinel = new DisposedMarker();

    /// <summary>Upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <inheritdoc/>
    public abstract void OnNext(T value);

    /// <inheritdoc/>
    public abstract void OnError(Exception error);

    /// <inheritdoc/>
    public abstract void OnCompleted();

    /// <inheritdoc/>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Assigns the upstream subscription. Disposing the sink disposes the subscription; assigning a second
    /// subscription disposes that subscription immediately.
    /// </summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription)
    {
        if (Interlocked.CompareExchange(ref _subscription, subscription, null) is null)
        {
            return;
        }

        subscription.Dispose();
    }

    /// <summary>Releases the upstream subscription.</summary>
    /// <param name="disposing">A value indicating whether managed resources should be disposed.</param>
    protected virtual void Dispose(bool disposing)
    {
        var subscription = Interlocked.Exchange(ref _subscription, DisposedSentinel);
        if (subscription is null || ReferenceEquals(subscription, DisposedSentinel) || !disposing)
        {
            return;
        }

        subscription.Dispose();
    }
}
