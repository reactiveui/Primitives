// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Replay row corruption cases.</summary>
internal enum ReplayCorruption
{
    /// <summary>An unsupported terminal result kind.</summary>
    InvalidResultKind = 0,

    /// <summary>A fractional real terminal result kind.</summary>
    FractionalResultKind = 1,

    /// <summary>A nonnumeric text terminal result kind.</summary>
    TextResultKind = 2,

    /// <summary>An integer terminal result kind outside the CLR int range.</summary>
    OutOfRangeResultKind = 3,

    /// <summary>A fingerprint blob with the wrong length.</summary>
    ShortFingerprint = 4,

    /// <summary>A malformed commit timestamp.</summary>
    InvalidCommitTimestamp = 5,

    /// <summary>An empty operation identifier.</summary>
    EmptyOperationId = 6,

    /// <summary>A blank client identifier.</summary>
    BlankClientId = 7,

    /// <summary>A blob stored in a client text column.</summary>
    BlobClientId = 8,

    /// <summary>A blank conflict resolution code.</summary>
    BlankConflictResolution = 9,

    /// <summary>A partially null conflict payload segment.</summary>
    PartialConflictPayload = 10,

    /// <summary>An empty event identifier.</summary>
    EmptyEventId = 11,

    /// <summary>A blank event cursor.</summary>
    BlankEventCursor = 12,

    /// <summary>A non-positive event payload schema.</summary>
    InvalidEventPayloadSchema = 13,

    /// <summary>A fractional real event payload schema.</summary>
    FractionalEventPayloadSchema = 14,

    /// <summary>A nonnumeric text event payload schema.</summary>
    TextEventPayloadSchema = 15,

    /// <summary>A non-blob event payload.</summary>
    NonBlobEventPayload = 16,

    /// <summary>A partially null event origin segment.</summary>
    PartialEventOrigin = 17,

    /// <summary>A partially null stream state segment.</summary>
    PartialStreamState = 18,

    /// <summary>A blank retained stream cursor.</summary>
    BlankStreamCursor = 19,

    /// <summary>A partially null write stamp segment.</summary>
    PartialWriteStamp = 20,
}
