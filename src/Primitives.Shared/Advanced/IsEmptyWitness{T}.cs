// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Observer wrapper for detecting whether a source completes without values.</summary>
/// <typeparam name="T">The source value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("IsEmptyWitness: Stopped = {_stopped}, Subscription = {Subscription}")]
public sealed class IsEmptyWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>The upstream subscription slot.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "Disposed atomically via WitnessLifetime.Dispose.")]
    private IDisposable? _subscription;

    /// <summary>Non-zero once the result has been emitted or the sink disposed.</summary>
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

    /// <summary>Gets the current upstream subscription slot value.</summary>
    private IDisposable? Subscription => Volatile.Read(ref _subscription);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => WitnessLifetime.Dispose(ref _stopped, ref _subscription);

    /// <inheritdoc/>
    public void OnCompleted()
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        try
        {
            Observer.OnNext(true);
            Observer.OnCompleted();
        }
        finally
        {
            SinkSubscription.Dispose(ref _subscription);
        }
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => WitnessLifetime.Error(ref _stopped, ref _subscription, Observer, error);

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        try
        {
            Observer.OnNext(false);
            Observer.OnCompleted();
        }
        finally
        {
            SinkSubscription.Dispose(ref _subscription);
        }
    }

    /// <summary>Assigns the upstream subscription.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription)
    {
        ArgumentExceptionHelper.ThrowIfNull(subscription);

        if (Subscription is not null)
        {
            subscription.Dispose();
            return;
        }

        WitnessLifetime.SetSubscription(ref _stopped, ref _subscription, subscription);
    }
}
