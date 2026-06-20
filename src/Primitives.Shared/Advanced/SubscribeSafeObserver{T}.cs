// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Observer that turns downstream <c>OnNext</c> exceptions into a terminal error and upstream disposal.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class SubscribeSafeObserver<T> : IObserver<T>, IDisposable
{
    /// <summary>The wrapped observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The upstream subscription.</summary>
    private readonly SingleReplaceableDisposable _subscription = new();

    /// <summary>Non-zero after terminal notification or disposal.</summary>
    private int _stopped;

    /// <summary>Initializes a new instance of the <see cref="SubscribeSafeObserver{T}"/> class.</summary>
    /// <param name="observer">The wrapped observer.</param>
    public SubscribeSafeObserver(IObserver<T> observer) => _observer = observer;

    /// <inheritdoc/>
    public void Dispose()
    {
        Interlocked.Exchange(ref _stopped, 1);
        _subscription.Dispose();
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
            _subscription.Dispose();
        }
    }

    /// <inheritdoc/>
    public void OnError(Exception error) => StopWithError(error);

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (Volatile.Read(ref _stopped) != 0)
        {
            return;
        }

        try
        {
            _observer.OnNext(value);
        }
        catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
        {
            StopWithError(error);
        }
    }

    /// <summary>Assigns the upstream subscription.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    public void SetSubscription(IDisposable subscription)
    {
        _subscription.Create(subscription);
        if (Volatile.Read(ref _stopped) == 0)
        {
            return;
        }

        _subscription.Dispose();
    }

    /// <summary>Forwards an error exactly once and disposes the upstream subscription.</summary>
    /// <param name="error">The terminal error.</param>
    private void StopWithError(Exception error)
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
            _subscription.Dispose();
        }
    }
}
