// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Sink that dematerializes <see cref="Spark{T}"/> values into notifications.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("UnsparkWitness: Observer = {_observer}, Subscription = {_subscription}")]
public sealed class UnsparkWitness<T> : IObserver<Spark<T>>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <summary>Non-zero after a terminal notification or disposal.</summary>
    private int _stopped;

    /// <summary>Initializes a new instance of the <see cref="UnsparkWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public UnsparkWitness(IObserver<T> observer) => _observer = observer;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(Spark<T> value)
    {
        if (Volatile.Read(ref _stopped) != 0)
        {
            return;
        }

        switch (value.Kind)
        {
            case SparkKind.OnNext:
            {
                SinkDelivery.Next(_observer, value.Value, this);
                break;
            }

            case SparkKind.OnError:
            {
                OnError(value.Exception);
                break;
            }

            default:
            {
                OnCompleted();
                break;
            }
        }
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error)
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        SinkTerminal.Fault(_observer, error, this);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted()
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

        SinkTerminal.Complete(_observer, this);
    }

    /// <summary>Assigns the upstream subscription, disposing the incoming one when the slot is occupied.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose()
    {
        Volatile.Write(ref _stopped, 1);
        SinkSubscription.Dispose(ref _subscription);
    }
}
