// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Observer for detecting whether a value is contained in a sequence.</summary>
/// <typeparam name="T">The source value type.</typeparam>
/// <param name="observer">The downstream observer.</param>
/// <param name="value">The value to locate.</param>
/// <param name="comparer">The comparer used for equality checks.</param>
[System.Diagnostics.DebuggerDisplay("SoughtValue = {_value}, Done = {_done}")]
public sealed class ContainsWitness<T>(IObserver<bool> observer, T value, IEqualityComparer<T> comparer) : IObserver<T>, IDisposable
{
    /// <summary>The value to locate.</summary>
    private readonly T _value = value;

    /// <summary>The comparer used for equality checks.</summary>
    private readonly IEqualityComparer<T> _comparer = comparer;

    /// <summary>The downstream observer.</summary>
    private readonly IObserver<bool> _observer = observer;

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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => SinkTerminal.Fault(_observer, error, this, ref _done);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => EmitCompleted(false);

    /// <summary>Assigns the upstream subscription, disposing it if one is already held.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);

    /// <summary>Emits the terminal boolean value and completes the observer.</summary>
    /// <param name="value">The terminal result.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EmitCompleted(bool value) => SinkTerminal.Complete(_observer, value, this, ref _done);
}
