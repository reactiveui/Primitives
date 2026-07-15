// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Creates a signal whose source is produced asynchronously for each subscription.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class AsyncDeferSignal<T> : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="AsyncDeferSignal{T}"/> class.</summary>
    /// <param name="observableFactory">The asynchronous factory that creates the source signal for a subscription.</param>
    public AsyncDeferSignal(Func<Task<IObservable<T>>> observableFactory)
    {
        ArgumentExceptionHelper.ThrowIfNull(observableFactory);

        ObservableFactory = _ => observableFactory();
    }

    /// <summary>Initializes a new instance of the <see cref="AsyncDeferSignal{T}"/> class.</summary>
    /// <param name="observableFactory">The asynchronous factory that creates the source signal for a subscription.</param>
    public AsyncDeferSignal(Func<CancellationToken, Task<IObservable<T>>> observableFactory) =>
        ObservableFactory = observableFactory ?? throw new ArgumentNullException(nameof(observableFactory));

    /// <summary>Gets the asynchronous source factory.</summary>
    private Func<CancellationToken, Task<IObservable<T>>> ObservableFactory { get; }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        AsyncSubscriptionLifetime subscription = new();
        CreateWitness<T> createObserver = new(observer);
        createObserver.SetCancel(subscription);
        _ = RunAsyncFactory(ObservableFactory, createObserver, subscription);
        return createObserver;
    }

    /// <summary>Runs the asynchronous factory and subscribes the observer to the produced source.</summary>
    /// <param name="observableFactory">The asynchronous source factory.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="subscription">The subscription slot.</param>
    /// <returns>A task that completes when the source subscription has been assigned.</returns>
    private static async Task RunAsyncFactory(
        Func<CancellationToken, Task<IObservable<T>>> observableFactory,
        CreateWitness<T> observer,
        AsyncSubscriptionLifetime subscription)
    {
        IObservable<T> source;
        try
        {
            source = await observableFactory(subscription.Token).ConfigureAwait(false);
        }
        catch (Exception error)
        {
            try
            {
                observer.OnError(error);
                subscription.SetSubscription(EmptyDisposable.Instance);
            }
            finally
            {
                subscription.Complete();
            }

            return;
        }

        try
        {
            subscription.SetSubscription(subscription.IsCancellationRequested
                ? EmptyDisposable.Instance
                : source.Subscribe(observer));
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
