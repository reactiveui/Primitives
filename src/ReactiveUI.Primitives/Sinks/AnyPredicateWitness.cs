// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Observer for detecting whether any value matches a predicate.</summary>
/// <typeparam name="T">The source value type.</typeparam>
public sealed class AnyPredicateWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<bool> _observer;

    /// <summary>The predicate.</summary>
    private readonly Func<T, bool> _predicate;

    /// <summary>A value indicating whether the observer has terminated.</summary>
    private bool _done;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <summary>Initializes a new instance of the <see cref="AnyPredicateWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="predicate">The predicate.</param>
    public AnyPredicateWitness(IObserver<bool> observer, Func<T, bool> predicate)
    {
        _observer = observer;
        _predicate = predicate;
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (_done || !_predicate(value))
        {
            return;
        }

        SinkTerminal.Complete(_observer, true, this, ref _done);
    }

    /// <inheritdoc/>
    public void OnError(Exception error) => SinkTerminal.Fault(_observer, error, this, ref _done);

    /// <inheritdoc/>
    public void OnCompleted() => SinkTerminal.Complete(_observer, false, this, ref _done);

    /// <summary>Assigns the upstream subscription, disposing it if one is already held.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);
}
