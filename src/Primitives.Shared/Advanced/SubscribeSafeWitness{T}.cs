// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Observer that turns downstream <c>OnNext</c> exceptions into a terminal error and upstream disposal.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Stopped = {_stopped}, Observer = {_observer}")]
public sealed class SubscribeSafeWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>The wrapped observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The upstream subscription.</summary>
    private readonly SingleReplaceableDisposable _subscription = new();

    /// <summary>Non-zero after terminal notification or disposal.</summary>
    private int _stopped;

    /// <summary>Initializes a new instance of the <see cref="SubscribeSafeWitness{T}"/> class.</summary>
    /// <param name="observer">The wrapped observer.</param>
    public SubscribeSafeWitness(IObserver<T> observer) => _observer = observer;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => WitnessLifetime.Dispose(ref _stopped, _subscription);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => WitnessLifetime.Complete(ref _stopped, _subscription, _observer);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => WitnessLifetime.Error(ref _stopped, _subscription, _observer, error);

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (WitnessLifetime.IsStopped(ref _stopped))
        {
            return;
        }

        try
        {
            _observer.OnNext(value);
        }
        catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
        {
            WitnessLifetime.Error(ref _stopped, _subscription, _observer, error);
        }
    }

    /// <summary>Assigns the upstream subscription.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) =>
        WitnessLifetime.SetSubscription(ref _stopped, _subscription, subscription);
}
