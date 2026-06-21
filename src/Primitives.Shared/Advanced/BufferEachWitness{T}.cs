// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Witness that emits each source value as a single-item buffer.</summary>
/// <typeparam name="T">The source value type.</typeparam>
public sealed class BufferEachWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>The upstream subscription slot.</summary>
    private readonly SingleReplaceableDisposable _subscription = new();

    /// <summary>Non-zero after termination or disposal.</summary>
    private int _stopped;

    /// <summary>Initializes a new instance of the <see cref="BufferEachWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public BufferEachWitness(IObserver<IList<T>> observer) => Observer = observer ?? throw new ArgumentNullException(nameof(observer));

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<IList<T>> Observer { get; }

    /// <inheritdoc/>
    public void Dispose() => WitnessLifetime.Dispose(ref _stopped, _subscription);

    /// <inheritdoc/>
    public void OnCompleted() => WitnessLifetime.Complete(ref _stopped, _subscription, Observer);

    /// <inheritdoc/>
    public void OnError(Exception error) => WitnessLifetime.Error(ref _stopped, _subscription, Observer, error);

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (WitnessLifetime.IsStopped(ref _stopped))
        {
            return;
        }

        Observer.OnNext([value]);
    }

    /// <summary>Assigns the upstream subscription.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription) =>
        WitnessLifetime.SetSubscription(ref _stopped, _subscription, subscription);
}
