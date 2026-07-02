// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>
/// The observer handed to create-style subscription factories: forwards until terminated, owns the factory's
/// cancel resource, and optionally releases it when a downstream <c>OnNext</c> throws (the safe-create contract).
/// The shared sink used by <see cref="CreateSignal{T}"/> and <see cref="CreateSafeSignal{T}"/>, which previously
/// each nested an identical copy.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
internal sealed class CreateSink<T> : IDisposable, IObserver<T>
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
    public CreateSink(IObserver<T> observer, bool disposeOnNextThrow)
    {
        _observer = observer;
        _disposeOnNextThrow = disposeOnNextThrow;
    }

    /// <summary>Initializes a new instance of the <see cref="CreateSink{T}"/> class with an eager cancel resource.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cancel">The subscription's cancel resource.</param>
    /// <param name="disposeOnNextThrow">Whether a throwing downstream <c>OnNext</c> releases the subscription.</param>
    public CreateSink(IObserver<T> observer, IDisposable cancel, bool disposeOnNextThrow)
    {
        _observer = observer;
        _cancel = cancel;
        _disposeOnNextThrow = disposeOnNextThrow;
    }

    /// <summary>Assigns the cancellation resource, releasing it immediately when already stopped.</summary>
    /// <param name="cancel">Cancellation resource.</param>
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

        try
        {
            _observer.OnNext(value);
        }
        catch
        {
            Dispose();
            throw;
        }
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
