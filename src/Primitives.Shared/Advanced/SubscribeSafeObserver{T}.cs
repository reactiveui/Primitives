// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Observer that turns downstream <c>OnNext</c> exceptions into a terminal error and upstream disposal.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class SubscribeSafeObserver<T> : IObserver<T>, IDisposable
{
    /// <summary>The wrapped observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The upstream subscription.</summary>
    private readonly SingleReplaceableDisposable _subscription = new();

    /// <summary>Non-zero after terminal notification or disposal.</summary>
    private int _stopped;

    /// <summary>Initializes a new instance of the <see cref="SubscribeSafeObserver{T}"/> class.</summary>
    /// <param name="observer">The wrapped observer.</param>
    public SubscribeSafeObserver(IObserver<T> observer) => _observer = observer;

    /// <inheritdoc/>
    public void Dispose() => ObserverSinkLifetime.Dispose(ref _stopped, _subscription);

    /// <inheritdoc/>
    public void OnCompleted() => ObserverSinkLifetime.Complete(ref _stopped, _subscription, _observer);

    /// <inheritdoc/>
    public void OnError(Exception error) => ObserverSinkLifetime.Error(ref _stopped, _subscription, _observer, error);

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (ObserverSinkLifetime.IsStopped(ref _stopped))
        {
            return;
        }

        try
        {
            _observer.OnNext(value);
        }
        catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
        {
            ObserverSinkLifetime.Error(ref _stopped, _subscription, _observer, error);
        }
    }

    /// <summary>Assigns the upstream subscription.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription) =>
        ObserverSinkLifetime.SetSubscription(ref _stopped, _subscription, subscription);
}
