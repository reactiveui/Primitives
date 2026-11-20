// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Core.Internal;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Sink that runs side-effects before forwarding each notification.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="observer">The downstream observer.</param>
/// <param name="onNext">The value side-effect.</param>
/// <param name="onError">The error side-effect.</param>
/// <param name="onCompleted">The completion side-effect.</param>
[System.Diagnostics.DebuggerDisplay("TapWitness: Observer = {_observer}, Subscription = {_subscription}")]
public sealed class TapWitness<T>(
    IObserver<T> observer,
    Action<T> onNext,
    Action<Exception> onError,
    Action onCompleted) : IObserver<T>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer = observer;

    /// <summary>The value side-effect.</summary>
    private readonly Action<T> _onNext = onNext;

    /// <summary>The error side-effect.</summary>
    private readonly Action<Exception> _onError = onError;

    /// <summary>The completion side-effect.</summary>
    private readonly Action _onCompleted = onCompleted;

    /// <summary>Non-zero once stopped.</summary>
    private int _stopped;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(T value) =>
        UnaryWitnessDelivery.NextTapped<T, Action<T>>(
            ref _stopped,
            value,
            new(_onNext, static (callback, item) => callback(item)),
            _observer,
            this);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) =>
        UnaryWitnessDelivery.FaultTapped(ref _stopped, error, _onError, _observer, this);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() =>
        UnaryWitnessDelivery.CompleteTapped(ref _stopped, _onCompleted, _observer, this);

    /// <summary>Assigns the upstream subscription, disposing the incoming one when this sink holds a subscription or has been disposed.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => UnaryWitnessDelivery.Dispose(ref _stopped, ref _subscription);
}
