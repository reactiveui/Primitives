// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Branch subscription handle returned to the subscriber.</summary>
/// <typeparam name="T">The element type partitioned across the two branches.</typeparam>
/// <param name="coordinator">The owning coordinator.</param>
/// <param name="isTrueBranch"><see langword="true"/> for the truthy branch.</param>
[System.Diagnostics.DebuggerDisplay("PartitionBranchSubscription: Disposed = {_disposed}")]
public sealed class PartitionBranchSubscription<T>(PartitionCoordinator<T> coordinator, bool isTrueBranch) : IAsyncDisposable
{
    /// <summary>Latches to <c>1</c> on the first <see cref="DisposeAsync"/> call so release is idempotent.</summary>
    private int _disposed;

    /// <inheritdoc/>
    public ValueTask DisposeAsync() =>
        Interlocked.Exchange(ref _disposed, 1) == 1
            ? default
            : coordinator.ReleaseBranchAsync(isTrueBranch);
}
