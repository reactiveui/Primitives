// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Observer wrapper for object casts.</summary>
/// <typeparam name="TResult">The result value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("CastWitness: Stopped = {_stopped}, Observer = {Observer}")]
public sealed class CastWitness<TResult> : IObserver<object?>, IDisposable
{
    /// <summary>Non-zero after terminal notification or disposal.</summary>
    private int _stopped;

    /// <summary>The upstream subscription slot.</summary>
    private IDisposable? _subscription;

    /// <summary>Initializes a new instance of the <see cref="CastWitness{TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public CastWitness(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        Observer = observer;
    }

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<TResult> Observer { get; }

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
        catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
        {
            OnError(error);
            return;
        }

        SinkDelivery.Next(Observer, result, this);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error)
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        SinkTerminal.Fault(Observer, error, this);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted()
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        SinkTerminal.Complete(Observer, this);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose()
    {
        Volatile.Write(ref _stopped, 1);
        SinkSubscription.Dispose(ref _subscription);
    }

    /// <summary>Assigns the upstream subscription.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) =>
        SinkSubscription.Set(ref _subscription, subscription);
}
