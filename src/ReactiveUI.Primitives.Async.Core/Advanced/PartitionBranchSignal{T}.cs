// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Observable view of one branch.</summary>
/// <typeparam name="T">The element type partitioned across the two branches.</typeparam>
/// <param name="isTrueBranch"><see langword="true"/> for the truthy branch.</param>
[System.Diagnostics.DebuggerDisplay("PartitionBranchSignal<T>: Coordinator = {Coordinator}")]
public sealed class PartitionBranchSignal<T>(bool isTrueBranch) : IObservableAsync<T>
{
    /// <summary>Gets or sets the back-pointer to the owning coordinator.</summary>
    internal PartitionCoordinator<T> Coordinator { get; set; } = null!;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        Coordinator.SubscribeBranchAsync(isTrueBranch, observer, cancellationToken);
}
