// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Observer wrapper for detecting whether a source completes without values.</summary>
/// <typeparam name="T">The source value type.</typeparam>
public sealed class IsEmptyWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>Stores the stopped flag for interlocked/ref helper calls.</summary>
    private int _stopped;

    /// <summary>Initializes a new instance of the <see cref="IsEmptyWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public IsEmptyWitness(IObserver<bool> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        Observer = observer;
    }

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<bool> Observer { get; }

    /// <summary>Gets the upstream subscription slot.</summary>
    private SingleReplaceableDisposable Subscription { get; } = new();

    /// <inheritdoc/>
    public void Dispose() => WitnessLifetime.Dispose(ref _stopped, Subscription);

    /// <inheritdoc/>
    public void OnCompleted()
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        using var _ = Subscription;
        Observer.OnNext(true);
        Observer.OnCompleted();
    }

    /// <inheritdoc/>
    public void OnError(Exception error) => WitnessLifetime.Error(ref _stopped, Subscription, Observer, error);

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        using var _ = Subscription;
        Observer.OnNext(false);
        Observer.OnCompleted();
    }

    /// <summary>Assigns the upstream subscription.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription) =>
        WitnessLifetime.SetSubscription(ref _stopped, Subscription, subscription);
}
