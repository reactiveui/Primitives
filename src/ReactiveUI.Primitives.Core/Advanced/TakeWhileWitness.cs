// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Sink that forwards values while the predicate holds, then completes and unsubscribes.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="observer">The downstream observer.</param>
/// <param name="predicate">The predicate that determines whether to keep taking values.</param>
[System.Diagnostics.DebuggerDisplay("TakeWhileWitness: Completed = {_completed}, Subscription = {_subscription}")]
public sealed class TakeWhileWitness<T>(IObserver<T> observer, Func<T, bool> predicate) : IObserver<T>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer = observer;

    /// <summary>The predicate that determines whether to keep taking values.</summary>
    private readonly Func<T, bool> _predicate = predicate;

    /// <summary>Non-zero once a terminal notification has been emitted.</summary>
    private int _completed;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (Volatile.Read(ref _completed) != 0)
        {
            return;
        }

        if (!_predicate(value))
        {
            Complete();
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
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0)
        {
            return;
        }

        SinkTerminal.Fault(_observer, error, this);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => Complete();

    /// <summary>Assigns the upstream subscription, disposing it if one is already held.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);

    /// <summary>Completes the downstream observer once and releases the upstream subscription.</summary>
    private void Complete()
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0)
        {
            return;
        }

        SinkTerminal.Complete(_observer, this);
    }
}
