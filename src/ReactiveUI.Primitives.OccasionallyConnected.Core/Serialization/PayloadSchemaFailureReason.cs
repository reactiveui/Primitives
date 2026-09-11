// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Identifies why a payload schema operation failed.</summary>
public enum PayloadSchemaFailureReason
{
    /// <summary>The contract identifier was not registered.</summary>
    UnknownContract = 0,

    /// <summary>The schema version was invalid or unavailable.</summary>
    InvalidSchemaVersion = 1,

    /// <summary>The requested type was not allowlisted for the contract version.</summary>
    TypeNotAllowed = 2,

    /// <summary>The content type does not match the serializer.</summary>
    ContentTypeMismatch = 3,

    /// <summary>The payload hash is missing or does not match the payload bytes.</summary>
    PayloadHashMismatch = 4,

    /// <summary>The payload exceeds the configured size limit.</summary>
    PayloadTooLarge = 5,

    /// <summary>A required upcaster is missing.</summary>
    MissingUpcaster = 6,

    /// <summary>More than one upcaster is registered for the same source version.</summary>
    AmbiguousUpcaster = 7,

    /// <summary>The requested conversion would require a downcast.</summary>
    DowncastNotSupported = 8,

    /// <summary>An upcaster changed immutable contract metadata.</summary>
    UpcasterContractMismatch = 9,

    /// <summary>The payload could not be deserialized as the requested schema.</summary>
    DeserializationFailed = 10,

    /// <summary>The payload could not be serialized as the registered schema.</summary>
    SerializationFailed = 11,

    /// <summary>The upcaster failed while converting the payload schema.</summary>
    UpcasterFailed = 12,
}
