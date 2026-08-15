// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Observer for append.</summary>
/// <typeparam name="T">The source value type.</typeparam>
/// <param name="observer">The downstream observer.</param>
/// <param name="value">The appended value.</param>
[System.Diagnostics.DebuggerDisplay("AppendedValue = {_value}, Observer = {_observer}")]
public sealed class AppendWitness<T>(IObserver<T> observer, T value) : IObserver<T>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer = observer;

    /// <summary>The appended value.</summary>
    private readonly T _value = value;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <inheritdoc/>
    public void OnNext(T value)
    {
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
    public void OnCompleted() => SinkTerminal.Complete(_observer, _value, this);

    /// <summary>Assigns the upstream subscription, disposing it if one is already held.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);
}
