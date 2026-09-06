// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Creates an observable from an asynchronous subscription function.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("AsyncCreateSignal: SubscribeAsync = {SubscribeAsync}")]
public sealed class AsyncCreateSignal<T> : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="AsyncCreateSignal{T}"/> class.</summary>
    /// <param name="subscribe">The asynchronous subscription function.</param>
    public AsyncCreateSignal(Func<IObserver<T>, Task<IDisposable>> subscribe)
    {
        ArgumentExceptionHelper.ThrowIfNull(subscribe);

        SubscribeAsync = (observer, _) => subscribe(observer);
    }

    /// <summary>Initializes a new instance of the <see cref="AsyncCreateSignal{T}"/> class.</summary>
    /// <param name="subscribe">The asynchronous subscription function.</param>
    /// <exception cref="ArgumentNullException"><paramref name="subscribe"/> is <see langword="null"/>.</exception>
    public AsyncCreateSignal(Func<IObserver<T>, CancellationToken, Task<IDisposable>> subscribe) =>
        SubscribeAsync = subscribe ?? throw new ArgumentNullException(nameof(subscribe));

    /// <summary>Gets the asynchronous subscription function.</summary>
    private Func<IObserver<T>, CancellationToken, Task<IDisposable>> SubscribeAsync { get; }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        AsyncSubscriptionLifetime subscription = new();
        CreateWitness<T> createObserver = new(observer);
        createObserver.SetCancel(subscription);
        _ = RunAsyncSubscription(SubscribeAsync, createObserver, subscription);
        return createObserver;
    }

    /// <summary>Completes an asynchronous subscription and assigns its disposable lifetime.</summary>
    /// <param name="subscribe">The asynchronous subscription function.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="subscription">The subscription slot.</param>
    /// <returns>A task that completes when the asynchronous subscription has assigned its disposable.</returns>
    private static async Task RunAsyncSubscription(
        Func<IObserver<T>, CancellationToken, Task<IDisposable>> subscribe,
        CreateWitness<T> observer,
        AsyncSubscriptionLifetime subscription)
    {
        try
        {
            var disposable = await subscribe(observer, subscription.Token).ConfigureAwait(false);
            subscription.SetSubscription(disposable);
        }
        catch (OperationCanceledException) when (subscription.IsCancellationRequested)
        {
            subscription.SetSubscription(EmptyDisposable.Instance);
        }
        catch (Exception error)
        {
            observer.OnError(error);
            subscription.SetSubscription(EmptyDisposable.Instance);
        }
        finally
        {
            subscription.Complete();
        }
    }
}
