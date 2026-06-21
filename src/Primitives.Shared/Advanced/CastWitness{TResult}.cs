// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Observer wrapper for object casts.</summary>
/// <typeparam name="TResult">The result value type.</typeparam>
public sealed class CastWitness<TResult> : IObserver<object?>, IDisposable
{
    /// <summary>Non-zero after terminal notification or disposal.</summary>
    private int _stopped;

    /// <summary>Initializes a new instance of the <see cref="CastWitness{TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public CastWitness(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        Observer = observer;
    }

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<TResult> Observer { get; }

    /// <summary>Gets the upstream subscription slot.</summary>
    private SingleReplaceableDisposable Subscription { get; } = new();

    /// <inheritdoc/>
    public void OnNext(object? value)
    {
        if (WitnessLifetime.IsStopped(ref _stopped))
        {
            return;
        }

        TResult result;
        try
        {
            result = (TResult)value!;
        }
        catch (Exception error)
        {
            OnError(error);
            return;
        }

        Observer.OnNext(result);
    }

    /// <inheritdoc/>
    public void OnError(Exception error) =>
        WitnessLifetime.Error(ref _stopped, Subscription, Observer, error);

    /// <inheritdoc/>
    public void OnCompleted() =>
        WitnessLifetime.Complete(ref _stopped, Subscription, Observer);

    /// <inheritdoc/>
    public void Dispose() => WitnessLifetime.Dispose(ref _stopped, Subscription);

    /// <summary>Assigns the upstream subscription.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription) =>
        WitnessLifetime.SetSubscription(ref _stopped, Subscription, subscription);
}
