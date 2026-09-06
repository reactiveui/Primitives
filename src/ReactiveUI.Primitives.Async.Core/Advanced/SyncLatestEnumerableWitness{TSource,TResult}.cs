// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Per-source observer for enumerable <c>SyncLatest</c> sources.</summary>
/// <typeparam name="TSource">The source element type.</typeparam>
/// <typeparam name="TResult">The downstream element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("SyncLatestEnumerableWitness: Index = {Index}, Parent = {Parent}")]
public sealed class SyncLatestEnumerableWitness<TSource, TResult> : IObserverAsync<TSource>
{
    /// <summary>Initializes a new instance of the <see cref="SyncLatestEnumerableWitness{TSource, TResult}"/> class.</summary>
    /// <param name="parent">The parent coordinator.</param>
    /// <param name="index">The source index.</param>
    public SyncLatestEnumerableWitness(SyncLatestEnumerableCoordinator<TSource, TResult> parent, int index)
    {
        Parent = parent;
        Index = index;
    }

    /// <summary>Gets the parent coordinator.</summary>
    private SyncLatestEnumerableCoordinator<TSource, TResult> Parent { get; }

    /// <summary>Gets the source index.</summary>
    private int Index { get; }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnNextAsync(TSource value, CancellationToken cancellationToken) =>
        Parent.OnNextAsync(Index, value, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
        Parent.OnErrorResumeAsync(error, cancellationToken);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask OnCompletedAsync(Result result) => Parent.OnCompletedAsync(Index, result);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => default;
}
