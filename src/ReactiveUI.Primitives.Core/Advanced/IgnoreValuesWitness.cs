// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Sink that drops values and forwards only terminal notifications.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("IgnoreValuesWitness: Observer = {_observer}, Subscription = {_subscription}")]
public sealed class IgnoreValuesWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <summary>Initializes a new instance of the <see cref="IgnoreValuesWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public IgnoreValuesWitness(IObserver<T> observer) => _observer = observer;

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        // Values are intentionally ignored; only terminal notifications are forwarded.
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
