// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Sink that emits the final accumulation once the source completes.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TAccumulate">The accumulated value type.</typeparam>
/// <param name="observer">The downstream observer.</param>
/// <param name="seed">The initial accumulated value.</param>
/// <param name="accumulator">The accumulator function.</param>
[System.Diagnostics.DebuggerDisplay("ReduceWitness: Current = {_current}, Subscription = {_subscription}")]
public sealed class ReduceWitness<TSource, TAccumulate>(
    IObserver<TAccumulate> observer,
    TAccumulate seed,
    Func<TAccumulate, TSource, TAccumulate> accumulator) : IObserver<TSource>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<TAccumulate> _observer = observer;

    /// <summary>The accumulator function.</summary>
    private readonly Func<TAccumulate, TSource, TAccumulate> _accumulator = accumulator;

    /// <summary>The current accumulated value.</summary>
    private TAccumulate _current = seed;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <inheritdoc/>
    public void OnNext(TSource value) => _current = _accumulator(_current, value);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => SinkTerminal.Fault(_observer, error, this);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => SinkTerminal.Complete(_observer, _current, this);

    /// <summary>Assigns the upstream subscription, disposing it if one is already held.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);
}
