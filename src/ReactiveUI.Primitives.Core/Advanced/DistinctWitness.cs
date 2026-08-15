// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Sink that forwards the first occurrence of each value.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="observer">The downstream observer.</param>
/// <param name="seen">The set used to track already-observed values.</param>
[System.Diagnostics.DebuggerDisplay("Seen = {_seen.Count}, Subscription = {_subscription}")]
public sealed class DistinctWitness<T>(IObserver<T> observer, HashSet<T> seen) : IObserver<T>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer = observer;

    /// <summary>The set of values already observed.</summary>
    private readonly HashSet<T> _seen = seen;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (!_seen.Add(value))
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
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => SinkTerminal.Fault(_observer, error, this);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => SinkTerminal.Complete(_observer, this);

    /// <summary>Assigns the upstream subscription, disposing it if one is already held.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);
}
