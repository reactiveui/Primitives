// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Identifies why a local payload was quarantined.</summary>
public enum LocalPayloadQuarantineReason
{
    /// <summary>The payload envelope failed schema validation.</summary>
    SchemaRejected = 0,

    /// <summary>The payload hash did not match its bytes.</summary>
    PayloadHashMismatch = 1,

    /// <summary>The payload could not be upcast to the configured schema.</summary>
    UpcastFailed = 2,

    /// <summary>The persisted row was structurally corrupt.</summary>
    PersistedRecordCorrupt = 3,
}
