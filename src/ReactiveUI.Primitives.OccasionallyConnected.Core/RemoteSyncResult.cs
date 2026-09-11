// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes the remote result for a pushed synchronization batch.</summary>
[System.Diagnostics.DebuggerDisplay("{BatchId,nq}")]
public sealed record RemoteSyncResult
{
    /// <summary>Initializes a new instance of the <see cref="RemoteSyncResult"/> class.</summary>
    /// <param name="batchId">The batch identifier.</param>
    /// <param name="operations">The per-operation results.</param>
    /// <param name="serverCursor">The optional server cursor advanced by accepted operations.</param>
    /// <param name="retryAfter">The optional retry delay requested by the server.</param>
    public RemoteSyncResult(
        Guid batchId,
        IReadOnlyList<OperationSyncResult> operations,
        string? serverCursor,
        TimeSpan? retryAfter)
    {
        BatchId = batchId;
        Operations = CollectionCopy.List(operations);
        ServerCursor = serverCursor;
        RetryAfter = retryAfter;
    }

    /// <summary>Gets the batch identifier.</summary>
    public Guid BatchId { get; }

    /// <summary>Gets the per-operation results.</summary>
    public IReadOnlyList<OperationSyncResult> Operations { get; }

    /// <summary>Gets the optional server cursor advanced by accepted operations.</summary>
    public string? ServerCursor { get; }

    /// <summary>Gets the optional retry delay requested by the server.</summary>
    public TimeSpan? RetryAfter { get; }
}
