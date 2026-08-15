// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observable that creates and disposes an asynchronous resource for each subscription.</summary>
/// <typeparam name="TResource">The resource type.</typeparam>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ResourceFactory = {ResourceFactory}, SignalFactory = {SignalFactory}")]
public sealed class UsingSignal<TResource, T> : IObservableAsync<T>
    where TResource : IAsyncDisposable
{
    /// <summary>Initializes a new instance of the <see cref="UsingSignal{TResource,T}"/> class.</summary>
    /// <param name="resourceFactory">The resource factory.</param>
    /// <param name="signalFactory">The signal factory.</param>
    public UsingSignal(
        Func<CancellationToken, ValueTask<TResource>> resourceFactory,
        Func<TResource, IObservableAsync<T>> signalFactory)
    {
        ResourceFactory = resourceFactory;
        SignalFactory = signalFactory;
    }

    /// <summary>Gets the resource factory.</summary>
    private Func<CancellationToken, ValueTask<TResource>> ResourceFactory { get; }

    /// <summary>Gets the signal factory.</summary>
    private Func<TResource, IObservableAsync<T>> SignalFactory { get; }

    /// <inheritdoc/>
    async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        var resource = await ResourceFactory(cancellationToken).ConfigureAwait(false);
        UsingWitness<TResource, T>? usingObserver = null;

        try
        {
            var signal = SignalFactory(resource);
            usingObserver = new(observer, resource);
            var subscription = await signal.SubscribeAsync(usingObserver, cancellationToken).ConfigureAwait(false);
            await usingObserver.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return usingObserver;
        }
        catch
        {
            if (usingObserver is not null)
            {
                await usingObserver.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
                await resource.DisposeAsync().ConfigureAwait(false);
            }

            throw;
        }
    }
}
