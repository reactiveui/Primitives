// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Observer that disposes a resource with the source subscription.</summary>
/// <typeparam name="TResource">The resource type.</typeparam>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Resource = {Resource}")]
public sealed class UsingWitness<TResource, T> : ForwardingWitnessAsync<T>
    where TResource : IAsyncDisposable
{
    /// <summary>Initializes a new instance of the <see cref="UsingWitness{TResource,T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="resource">The resource to dispose.</param>
    public UsingWitness(IObserverAsync<T> observer, TResource resource)
        : base(observer) =>
        Resource = resource;

    /// <summary>Gets the resource to dispose.</summary>
    private TResource Resource { get; }

    /// <inheritdoc/>
    protected override async ValueTask DisposeAsyncCore()
    {
        try
        {
            await Resource.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            await base.DisposeAsyncCore().ConfigureAwait(false);
        }
    }
}
