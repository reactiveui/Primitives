// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="SnapshotRecoveryLimits"/>.</summary>
public sealed class SnapshotRecoveryLimitsTests
{
    /// <summary>The pending operation selector.</summary>
    private const int PendingOperationsSelector = 0;

    /// <summary>The payload bytes selector.</summary>
    private const int PayloadBytesSelector = 1;

    /// <summary>The metadata entries selector.</summary>
    private const int MetadataEntriesSelector = 2;

    /// <summary>The metadata bytes selector.</summary>
    private const int MetadataBytesSelector = 3;

    /// <summary>The cursor bytes selector.</summary>
    private const int CursorBytesSelector = 4;

    /// <summary>The contract bytes selector.</summary>
    private const int ContractBytesSelector = 5;

    /// <summary>The reason code bytes selector.</summary>
    private const int StreamIdBytesSelector = 6;

    /// <summary>The reason code bytes selector.</summary>
    private const int ReasonCodeBytesSelector = 7;

    /// <summary>The logical bytes selector.</summary>
    private const int LogicalBytesSelector = 8;

    /// <summary>Verifies default limits are valid.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValidateAcceptsDefaults()
    {
        var limits = new SnapshotRecoveryLimits();

        limits.Validate();

        await Assert.That(limits.MaximumPendingOperations).IsGreaterThan(0);
    }

    /// <summary>Verifies non-positive limits are rejected.</summary>
    /// <param name="selector">The invalid limit selector.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments(PendingOperationsSelector)]
    [Arguments(PayloadBytesSelector)]
    [Arguments(MetadataEntriesSelector)]
    [Arguments(MetadataBytesSelector)]
    [Arguments(CursorBytesSelector)]
    [Arguments(ContractBytesSelector)]
    [Arguments(StreamIdBytesSelector)]
    [Arguments(ReasonCodeBytesSelector)]
    [Arguments(LogicalBytesSelector)]
    public async Task ValidateRejectsNonPositiveLimits(int selector)
    {
        var limits = selector switch
        {
            PendingOperationsSelector => new SnapshotRecoveryLimits { MaximumPendingOperations = 0 },
            PayloadBytesSelector => new SnapshotRecoveryLimits { MaximumPayloadBytes = 0 },
            MetadataEntriesSelector => new SnapshotRecoveryLimits { MaximumMetadataEntries = 0 },
            MetadataBytesSelector => new SnapshotRecoveryLimits { MaximumMetadataBytes = 0 },
            CursorBytesSelector => new SnapshotRecoveryLimits { MaximumCursorUtf8Bytes = 0 },
            ContractBytesSelector => new SnapshotRecoveryLimits { MaximumContractUtf8Bytes = 0 },
            StreamIdBytesSelector => new SnapshotRecoveryLimits { MaximumStreamIdUtf8Bytes = 0 },
            ReasonCodeBytesSelector => new SnapshotRecoveryLimits { MaximumReasonCodeUtf8Bytes = 0 },
            _ => new SnapshotRecoveryLimits { MaximumLogicalBytes = 0 },
        };

        await Assert.That(() => limits.Validate()).ThrowsExactly<ArgumentOutOfRangeException>();
    }
}
