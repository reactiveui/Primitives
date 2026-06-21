// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Observer wrapper used by create-style signals.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class CreateWitness<T> : IDisposable, IObserver<T>
{
    /// <summary>Cancellation resource assigned by the subscription factory.</summary>
    private IDisposable? _cancel;

    /// <summary>Non-zero after disposal or termination.</summary>
    private int _stopped;

    /// <summary>Initializes a new instance of the <see cref="CreateWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public CreateWitness(IObserver<T> observer) => Observer = observer ?? throw new ArgumentNullException(nameof(observer));

    /// <summary>Gets or sets the wrapped observer.</summary>
    private IObserver<T> Observer { get; set; }

    /// <summary>Assigns the cancellation resource.</summary>
    /// <param name="cancel">Cancellation resource.</param>
    public void SetCancel(IDisposable cancel)
    {
        ArgumentExceptionHelper.ThrowIfNull(cancel);

        if (Interlocked.CompareExchange(ref _cancel, cancel, null) is not null)
        {
            cancel.Dispose();
            return;
        }

        if (Volatile.Read(ref _stopped) == 0)
        {
            return;
        }

        Interlocked.Exchange(ref _cancel, null)?.Dispose();
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (Volatile.Read(ref _stopped) != 0)
        {
            return;
        }

        Observer.OnNext(value);
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
            Observer.OnError(error);
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
            Observer.OnCompleted();
        }
        finally
        {
            Dispose();
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        Observer = EmptyWitness<T>.Instance;
        Interlocked.Exchange(ref _cancel, null)?.Dispose();
        Volatile.Write(ref _stopped, 1);
    }
}
