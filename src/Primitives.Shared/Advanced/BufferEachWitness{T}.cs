// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Witness that emits each source value as a single-item buffer.</summary>
/// <typeparam name="T">The source value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Stopped = {_stopped}, Observer = {Observer}")]
public sealed class BufferEachWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>The upstream subscription slot.</summary>
    private readonly SingleReplaceableDisposable _subscription = new();

    /// <summary>Non-zero after termination or disposal.</summary>
    private int _stopped;

    /// <summary>Initializes a new instance of the <see cref="BufferEachWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <see langword="null"/>.</exception>
    public BufferEachWitness(IObserver<IList<T>> observer) =>
        Observer = observer ?? throw new ArgumentNullException(nameof(observer));

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<IList<T>> Observer { get; }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => WitnessLifetime.Dispose(ref _stopped, _subscription);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => WitnessLifetime.Complete(ref _stopped, _subscription, Observer);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) =>
        WitnessLifetime.SetSubscription(ref _stopped, _subscription, subscription);
}
