// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Classifies operation failures before retry policy evaluation.</summary>
public enum RetryFailureKind
{
    /// <summary>A transport, remote availability, or other temporary failure.</summary>
    Transient = 0,

    /// <summary>An authentication failure that may receive one immediate retry after token renewal.</summary>
    Authentication = 1,

    /// <summary>An authorization denial that must not be retried as transient.</summary>
    AuthorizationDenied = 2,

    /// <summary>A validation rejection that must not be retried as transient.</summary>
    ValidationRejected = 3,

    /// <summary>A schema incompatibility that must not be retried as transient.</summary>
    SchemaIncompatible = 4,

    /// <summary>A payload size rejection that must not be retried as transient.</summary>
    PayloadTooLarge = 5,

    /// <summary>A deterministic conflict rejection that must not be retried as transient.</summary>
    DeterministicConflictRejected = 6,

    /// <summary>An ambiguous transport result whose retry behavior is selected by the delivery guarantee.</summary>
    AmbiguousTransportOutcome = 7,
}
