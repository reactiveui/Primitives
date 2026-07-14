// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>
/// Single-source sink that folds every observed value through an immutable value-type <typeparamref name="TAggregator"/>
/// and emits the aggregate result once the source completes. The accumulator is advanced functionally through a
/// constrained (devirtualized, allocation-free) call, so each concrete aggregate operator shares this one
/// implementation without a base class or per-value indirection.
/// </summary>
/// <typeparam name="T">The observed value type.</typeparam>
/// <typeparam name="TResult">The terminal result type.</typeparam>
/// <typeparam name="TAggregator">The value-type accumulator that folds values and yields the result.</typeparam>
/// <param name="observer">The downstream observer.</param>
/// <param name="aggregator">The seed accumulator.</param>
public sealed class AggregateWitness<T, TResult, TAggregator>(IObserver<TResult> observer, TAggregator aggregator)
    : IObserver<T>, IDisposable
    where TAggregator : struct, IAggregator<T, TResult, TAggregator>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<TResult> _observer = observer;

    /// <summary>The running accumulator, reassigned with the next state on each value.</summary>
    private TAggregator _aggregator = aggregator;

    /// <summary>A value indicating whether the observer has terminated.</summary>
    private bool _done;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (_done)
        {
            return;
        }

        _aggregator = _aggregator.Add(value);
    }

    /// <inheritdoc/>
    public void OnError(Exception error) => SinkTerminal.Fault(_observer, error, this, ref _done);

    /// <inheritdoc/>
    public void OnCompleted() => SinkTerminal.Complete(_observer, _aggregator.Result, this, ref _done);

    /// <summary>Assigns the upstream subscription, disposing it if one is already held.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);
}
