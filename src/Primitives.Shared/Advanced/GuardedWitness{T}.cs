// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Forwarding observer that releases its upstream cancel resource when the sequence terminates or a downstream <c>OnNext</c> throws, rethrowing the latter after release.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("GuardedWitness: Disposed = {_disposed}, Observer = {_observer}")]
public sealed class GuardedWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The upstream cancel resource released on termination.</summary>
    private IDisposable? _cancel;

    /// <summary>Disposed latch; 0 when alive, 1 once disposed.</summary>
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="GuardedWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cancel">The upstream cancel resource released on termination.</param>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> or <paramref name="cancel"/> is <see langword="null"/>.</exception>
    public GuardedWitness(IObserver<T> observer, IDisposable cancel)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        _cancel = cancel ?? throw new ArgumentNullException(nameof(cancel));
        _observer = observer;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(T value) => SinkDelivery.Next(_observer, value, this);

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        try
        {
            _observer.OnError(error);
        }
        finally
        {
            Dispose();
        }
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
        try
        {
            _observer.OnCompleted();
        }
        finally
        {
            Dispose();
        }
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => WitnessTeardown.Dispose(ref _disposed, ref _cancel);
}
