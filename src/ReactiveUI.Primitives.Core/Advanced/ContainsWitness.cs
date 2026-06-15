// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Observer for detecting whether a value is contained in a sequence.</summary>
/// <typeparam name="T">The source value type.</typeparam>
public sealed class ContainsWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>The value to locate.</summary>
    private readonly T _value;

    /// <summary>The comparer used for equality checks.</summary>
    private readonly IEqualityComparer<T> _comparer;

    /// <summary>The downstream observer.</summary>
    private readonly IObserver<bool> _observer;

    /// <summary>A value indicating whether the observer has terminated.</summary>
    private bool _done;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <summary>Initializes a new instance of the <see cref="ContainsWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="value">The value to locate.</param>
    /// <param name="comparer">The comparer used for equality checks.</param>
    public ContainsWitness(IObserver<bool> observer, T value, IEqualityComparer<T> comparer)
    {
        _observer = observer;
        _value = value;
        _comparer = comparer;
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
            matches = _comparer.Equals(value, _value);
        }
        catch (Exception error)
        {
            OnError(error);
            return;
        }

        if (!matches)
        {
            return;
        }

        EmitCompleted(true);
    }

    /// <inheritdoc/>
    public void OnError(Exception error) => SinkTerminal.Fault(_observer, error, this, ref _done);

    /// <inheritdoc/>
    public void OnCompleted() => EmitCompleted(false);

    /// <summary>Assigns the upstream subscription, disposing it if one is already held.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);

    /// <summary>Emits the terminal boolean value and completes the observer.</summary>
    /// <param name="value">The terminal result.</param>
    private void EmitCompleted(bool value) => SinkTerminal.Complete(_observer, value, this, ref _done);
}
