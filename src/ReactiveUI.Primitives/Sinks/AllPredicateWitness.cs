// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Observer for detecting whether all values match a predicate.</summary>
/// <typeparam name="T">The source value type.</typeparam>
public sealed class AllPredicateWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>The predicate.</summary>
    private readonly Func<T, bool> _predicate;

    /// <summary>The downstream observer.</summary>
    private readonly IObserver<bool> _observer;

    /// <summary>A value indicating whether the observer has terminated.</summary>
    private bool _done;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <summary>Initializes a new instance of the <see cref="AllPredicateWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="predicate">The predicate.</param>
    public AllPredicateWitness(IObserver<bool> observer, Func<T, bool> predicate)
    {
        _observer = observer;
        _predicate = predicate;
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (_done)
        {
            return;
        }

        bool matches;
        try
        {
            matches = _predicate(value);
        }
        catch (Exception error)
        {
            OnError(error);
            return;
        }

        if (matches)
        {
            return;
        }

        EmitCompleted(false);
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        if (_done)
        {
            return;
        }

        _done = true;
        SinkTerminal.Fault(_observer, error, this);
    }

    /// <inheritdoc/>
    public void OnCompleted() => EmitCompleted(true);

    /// <summary>Assigns the upstream subscription, disposing it if one is already held.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);

    /// <summary>Emits the terminal boolean value and completes the observer.</summary>
    /// <param name="value">The terminal result.</param>
    private void EmitCompleted(bool value)
    {
        if (_done)
        {
            return;
        }

        _done = true;
        SinkTerminal.Complete(_observer, value, this);
    }
}
