// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides factory methods for creating and composing asynchronous observable sequences.
/// </summary>
/// <remarks>The ObservableAsync class contains static methods for working with asynchronous observables, enabling
/// resource management and composition patterns similar to those found in reactive programming. All members are
/// thread-safe and intended for use in asynchronous and reactive scenarios.</remarks>
public static partial class ObservableAsync
{
    /// <summary>
    /// Creates an observable sequence that manages the lifetime of an asynchronous resource, ensuring the resource is
    /// disposed when the sequence terminates.
    /// </summary>
    /// <remarks>The resource is created for each subscription and is disposed asynchronously when the
    /// observable sequence terminates, either by completion or error. If the observable factory throws an exception,
    /// the resource is disposed before the exception is propagated. This method is useful for managing resources that
    /// must be disposed when no longer needed, such as streams or database connections, in conjunction with
    /// asynchronous observable sequences.</remarks>
    /// <typeparam name="T">The type of the elements produced by the observable sequence.</typeparam>
    /// <typeparam name="TResource">The type of the asynchronous resource that implements <see cref="IAsyncDisposable"/>.</typeparam>
    /// <param name="resourceFactory">A function that asynchronously creates the resource to be used by the observable sequence. The function receives
    /// a <see cref="CancellationToken"/> and returns a <see cref="ValueTask{TResource}"/> representing the asynchronous
    /// creation of the resource.</param>
    /// <param name="observableFactory">A function that, given the created resource, returns an <see cref="ObservableAsync{T}"/> representing the
    /// observable sequence that uses the resource.</param>
    /// <returns>An <see cref="ObservableAsync{T}"/> that uses the specified resource and ensures the resource is disposed
    /// asynchronously when the sequence completes or an error occurs.</returns>
    public static IObservableAsync<T> Using<T, TResource>(
        Func<CancellationToken, ValueTask<TResource>> resourceFactory,
        Func<TResource, IObservableAsync<T>> observableFactory)
        where TResource : IAsyncDisposable => Defer(async token =>
    {
        var resource = await resourceFactory(token).ConfigureAwait(false);

        try
        {
            var observable = observableFactory(resource);
            return observable.OnDispose(resource.DisposeAsync);
        }
        catch
        {
            await resource.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    });
}
