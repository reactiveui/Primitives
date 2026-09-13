// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures finite structural limits for snapshot recovery validation.</summary>
[System.Diagnostics.DebuggerDisplay("Pending={MaximumPendingOperations,nq}; Bytes={MaximumLogicalBytes,nq}")]
public sealed record SnapshotRecoveryLimits
{
    /// <summary>The default pending operation limit.</summary>
    private const int DefaultMaximumPendingOperations = 1024;

    /// <summary>The default single payload byte limit.</summary>
    private const int DefaultMaximumPayloadBytes = 1024 * 1024;

    /// <summary>The default operation metadata entry limit.</summary>
    private const int DefaultMaximumMetadataEntries = 64;

    /// <summary>The default operation metadata byte limit.</summary>
    private const int DefaultMaximumMetadataBytes = 64 * 1024;

    /// <summary>The default cursor UTF-8 byte limit.</summary>
    private const int DefaultMaximumCursorUtf8Bytes = 4096;

    /// <summary>The default stream identifier UTF-8 byte limit.</summary>
    private const int DefaultMaximumStreamIdUtf8Bytes = 4096;

    /// <summary>The default contract and version string UTF-8 byte limit.</summary>
    private const int DefaultMaximumContractUtf8Bytes = 256;

    /// <summary>The default stable reason code UTF-8 byte limit.</summary>
    private const int DefaultMaximumReasonCodeUtf8Bytes = 128;

    /// <summary>The default aggregate logical byte limit.</summary>
    private const long DefaultMaximumLogicalBytes = 16L * 1024 * 1024;

    /// <summary>Gets the maximum pending operations in one recovery request.</summary>
    public int MaximumPendingOperations { get; init; } = DefaultMaximumPendingOperations;

    /// <summary>Gets the maximum bytes in one payload envelope.</summary>
    public int MaximumPayloadBytes { get; init; } = DefaultMaximumPayloadBytes;

    /// <summary>Gets the maximum metadata entries on one operation.</summary>
    public int MaximumMetadataEntries { get; init; } = DefaultMaximumMetadataEntries;

    /// <summary>Gets the maximum UTF-8 metadata bytes on one operation.</summary>
    public int MaximumMetadataBytes { get; init; } = DefaultMaximumMetadataBytes;

    /// <summary>Gets the maximum UTF-8 bytes in a cursor.</summary>
    public int MaximumCursorUtf8Bytes { get; init; } = DefaultMaximumCursorUtf8Bytes;

    /// <summary>Gets the maximum UTF-8 bytes in a stream identity.</summary>
    public int MaximumStreamIdUtf8Bytes { get; init; } = DefaultMaximumStreamIdUtf8Bytes;

    /// <summary>Gets the maximum UTF-8 bytes in a contract identifier, content type, hash, or version string.</summary>
    public int MaximumContractUtf8Bytes { get; init; } = DefaultMaximumContractUtf8Bytes;

    /// <summary>Gets the maximum UTF-8 bytes in a stable reason code.</summary>
    public int MaximumReasonCodeUtf8Bytes { get; init; } = DefaultMaximumReasonCodeUtf8Bytes;

    /// <summary>Gets the maximum logical bytes counted across a request, result, or mutation.</summary>
    /// <remarks>
    /// Logical bytes include validated UTF-8 protocol strings, stream identities, payload byte counts, metadata
    /// key/value bytes, collection count headers, and fixed logical widths for scalar values: 4 bytes for Int32
    /// or enum values, 8 bytes for Int64 values, and 16 bytes for Guid or DateTimeOffset values. Transports must
    /// also enforce actual encoded bytes.
    /// </remarks>
    public long MaximumLogicalBytes { get; init; } = DefaultMaximumLogicalBytes;

    /// <summary>Validates all configured limits.</summary>
    /// <exception cref="ArgumentOutOfRangeException">A limit is not positive or exceeds the fixed ownership ceiling.</exception>
    public void Validate()
    {
        ValidatePendingOperationLimit();
        ThrowIfNotPositive(MaximumPayloadBytes, nameof(MaximumPayloadBytes));
        ThrowIfNotPositive(MaximumMetadataEntries, nameof(MaximumMetadataEntries));
        ThrowIfNotPositive(MaximumMetadataBytes, nameof(MaximumMetadataBytes));
        ThrowIfNotPositive(MaximumCursorUtf8Bytes, nameof(MaximumCursorUtf8Bytes));
        ThrowIfNotPositive(MaximumStreamIdUtf8Bytes, nameof(MaximumStreamIdUtf8Bytes));
        ThrowIfNotPositive(MaximumContractUtf8Bytes, nameof(MaximumContractUtf8Bytes));
        ThrowIfNotPositive(MaximumReasonCodeUtf8Bytes, nameof(MaximumReasonCodeUtf8Bytes));
        _ = MaximumLogicalBytes > 0
            ? true
            : throw new ArgumentOutOfRangeException(nameof(MaximumLogicalBytes), MaximumLogicalBytes, "MaximumLogicalBytes must be positive.");
    }

    /// <summary>Throws when a positive integer limit is invalid.</summary>
    /// <param name="value">The configured value.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="value"/> is not positive.</exception>
    private static void ThrowIfNotPositive(int value, string parameterName) =>
        _ = value > 0
            ? true
            : throw new ArgumentOutOfRangeException(parameterName, value, "The snapshot recovery limit must be positive.");

    /// <summary>Validates the pending operation limit against the fixed owned-copy ceiling.</summary>
    /// <exception cref="ArgumentOutOfRangeException"><see cref="MaximumPendingOperations"/> is invalid.</exception>
    private void ValidatePendingOperationLimit() =>
        _ = MaximumPendingOperations > 0 && MaximumPendingOperations <= SnapshotRecoveryCollectionCopy.MaximumOwnedItems
            ? true
            : throw new ArgumentOutOfRangeException(
                nameof(MaximumPendingOperations),
                MaximumPendingOperations,
                "MaximumPendingOperations must be positive and no larger than the fixed ownership ceiling.");
}
