// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Classifies HTTP transport failures without exposing remote payload text.</summary>
public enum HttpTransportFailureKind
{
    /// <summary>The remote response violated the negotiated protocol.</summary>
    ProtocolViolation = 0,

    /// <summary>The peer rejected authentication.</summary>
    Authentication = 1,

    /// <summary>The peer denied authorization.</summary>
    AuthorizationDenied = 2,

    /// <summary>The peer rejected request validation.</summary>
    ValidationRejected = 3,

    /// <summary>The peer rejected an incompatible schema or protocol version.</summary>
    SchemaIncompatible = 4,

    /// <summary>The peer or local codec rejected an oversized payload.</summary>
    PayloadTooLarge = 5,

    /// <summary>The remote service is temporarily unavailable or rate limited.</summary>
    Transient = 6,

    /// <summary>The transport outcome is ambiguous and must be resolved by the synchronization engine.</summary>
    AmbiguousTransportOutcome = 7,

    /// <summary>The configured endpoint is invalid for the requested operation.</summary>
    Configuration = 8,
}
