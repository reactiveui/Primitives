// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Observer wrapper used by create-style signals.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Stopped = {_stopped}, Observer = {Observer}")]
public sealed class CreateWitness<T> : IDisposable, IObserver<T>
{
    /// <summary>Cancellation resource assigned by the subscription factory.</summary>
    private IDisposable? _cancel;

    /// <summary>Non-zero after disposal or termination.</summary>
    private int _stopped;

    /// <summary>Initializes a new instance of the <see cref="CreateWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public CreateWitness(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        Observer = observer;
    }

    /// <summary>Gets or sets the wrapped observer.</summary>
    private IObserver<T> Observer { get; set; }

    /// <summary>Assigns the cancellation resource.</summary>
    /// <param name="cancel">Cancellation resource.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetCancel(IDisposable cancel) =>
        WitnessLifetime.SetCancel(ref _cancel, ref _stopped, cancel);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(T value) =>
        WitnessLifetime.OnNext(ref _stopped, this, value, static (owner, item) => owner.Observer.OnNext(item));

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) =>
        WitnessLifetime.OnError(
            ref _stopped,
            this,
            error,
            static (owner, failure) => owner.Observer.OnError(failure),
            static owner => owner.Dispose());

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() =>
        WitnessLifetime.OnCompleted(
            ref _stopped,
            this,
            static owner => owner.Observer.OnCompleted(),
            static owner => owner.Dispose());

    /// <inheritdoc/>
    public void Dispose()
    {
        Observer = EmptyWitness<T>.Instance;
        WitnessLifetime.Dispose(ref _cancel, ref _stopped);
    }
}
