// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Owns the first execution for an admitted HTTP replay nonce.</summary>
internal sealed class HttpReplayOwner : IAsyncDisposable
{
    /// <summary>The owning coordinator.</summary>
    private readonly HttpReplayCoordinator _coordinator;

    /// <summary>Whether this owner has been closed.</summary>
    private int _closed;

    /// <summary>Initializes a new instance of the <see cref="HttpReplayOwner"/> class.</summary>
    /// <param name="coordinator">The owning coordinator.</param>
    /// <param name="entryId">The owned replay entry identifier.</param>
    internal HttpReplayOwner(HttpReplayCoordinator coordinator, long entryId)
    {
        _coordinator = coordinator;
        EntryId = entryId;
    }

    /// <summary>Gets the owned replay entry identifier.</summary>
    internal long EntryId { get; }

    /// <summary>Gets whether this owner was already completed or abandoned.</summary>
    internal bool IsClosed => Volatile.Read(ref _closed) != 0;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => AbandonAsync(CancellationToken.None);

    /// <summary>Marks this owner as closed exactly once.</summary>
    /// <returns>Whether the caller owns the close transition.</returns>
    internal bool TryClose() => Interlocked.Exchange(ref _closed, 1) == 0;

    /// <summary>Checks whether this handle belongs to the supplied coordinator.</summary>
    /// <param name="coordinator">The candidate owning coordinator.</param>
    /// <returns>Whether the coordinator owns this handle.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool IsOwnedBy(HttpReplayCoordinator coordinator) => ReferenceEquals(_coordinator, coordinator);

    /// <summary>Abandons the owner execution and waits for any required replay drain.</summary>
    /// <param name="cancellationToken">The cancellation token used only while waiting for drain.</param>
    /// <returns>The asynchronous abandonment operation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ValueTask AbandonAsync(CancellationToken cancellationToken) => _coordinator.AbandonAsync(this, cancellationToken);
}
