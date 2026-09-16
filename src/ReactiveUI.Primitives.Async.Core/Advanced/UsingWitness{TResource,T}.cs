// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Observer that disposes a resource with the source subscription.</summary>
/// <typeparam name="TResource">The resource type.</typeparam>
/// <typeparam name="T">The element type.</typeparam>
[DebuggerDisplay("UsingWitness: Resource = {Resource}")]
public sealed class UsingWitness<TResource, T> : IWitnessAsync<T>
    where TResource : IAsyncDisposable
{
    /// <summary>The notification gate, cancellation link and disposal state.</summary>
    private WitnessAsyncState _witness;

    /// <summary>Initializes a new instance of the <see cref="UsingWitness{TResource,T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="resource">The resource to dispose.</param>
    public UsingWitness(IObserverAsync<T> observer, TResource resource)
    {
        Downstream = observer;
        Resource = resource;
    }

    /// <summary>Gets the resource to dispose.</summary>
    private TResource Resource { get; }

    /// <summary>Gets the observer that receives forwarded notifications.</summary>
    private IObserverAsync<T> Downstream { get; }

    /// <inheritdoc/>
    ref WitnessAsyncState IWitnessState.Witness => ref _witness;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
        WitnessAsync.OnNextAsync(this, value, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
        WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
        Downstream.OnNextAsync(value, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        Downstream.OnErrorResumeAsync(error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) => Downstream.OnCompletedAsync(result);

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        ExceptionDispatchInfo? failure = null;
        try
        {
            await Resource.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            failure = ExceptionDispatchInfo.Capture(e);
        }

        await WitnessAsync.DisposeStateAsync(this).ConfigureAwait(false);
        failure?.Throw();
    }
}
