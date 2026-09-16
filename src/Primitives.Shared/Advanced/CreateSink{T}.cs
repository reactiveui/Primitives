// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Forwards notifications until termination and owns the resource returned by the subscription factory.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>When disposeOnNextThrow is enabled, a downstream OnNext failure disposes the resource before rethrowing.</remarks>
[System.Diagnostics.DebuggerDisplay("CreateSink: Stopped = {_stopped}, Observer = {_observer}")]
public sealed class CreateSink<T> : IDisposable, IObserver<T>
{
    /// <summary>A value indicating whether a throwing downstream <c>OnNext</c> releases the subscription.</summary>
    private readonly bool _disposeOnNextThrow;

    /// <summary>The downstream observer; swapped for the empty witness on disposal.</summary>
    private IObserver<T> _observer;

    /// <summary>Cancellation resource assigned by the subscription factory.</summary>
    private IDisposable? _cancel;

    /// <summary>Non-zero after disposal or termination.</summary>
    private int _stopped;

    /// <summary>Initializes a new instance of the <see cref="CreateSink{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="disposeOnNextThrow">Whether a throwing downstream <c>OnNext</c> releases the subscription.</param>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <see langword="null"/>.</exception>
    public CreateSink(IObserver<T> observer, bool disposeOnNextThrow)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        _observer = observer;
        _disposeOnNextThrow = disposeOnNextThrow;
    }

    /// <summary>Initializes a new instance of the <see cref="CreateSink{T}"/> class with an eager cancel resource.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cancel">The subscription's cancel resource.</param>
    /// <param name="disposeOnNextThrow">Whether a throwing downstream <c>OnNext</c> releases the subscription.</param>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> or <paramref name="cancel"/> is <see langword="null"/>.</exception>
    public CreateSink(IObserver<T> observer, IDisposable cancel, bool disposeOnNextThrow)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        ArgumentExceptionHelper.ThrowIfNull(cancel);

        _observer = observer;
        _cancel = cancel;
        _disposeOnNextThrow = disposeOnNextThrow;
    }

    /// <summary>Assigns the cancellation resource, releasing it immediately when the sink has stopped.</summary>
    /// <param name="cancel">Cancellation resource.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetCancel(IDisposable cancel) =>
        WitnessLifetime.SetCancel(ref _cancel, ref _stopped, cancel);

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (Volatile.Read(ref _stopped) != 0)
        {
            return;
        }

        if (!_disposeOnNextThrow)
        {
            _observer.OnNext(value);
            return;
        }

        SinkDelivery.Next(_observer, value, this);
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

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
        if (Interlocked.Exchange(ref _stopped, 1) != 0)
        {
            return;
        }

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
    public void Dispose()
    {
        _observer = EmptyWitness<T>.Instance;
        WitnessLifetime.Dispose(ref _cancel, ref _stopped);
    }
}
