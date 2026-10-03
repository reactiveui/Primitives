// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Explains why a retry decision stopped an operation.</summary>
public enum RetryStopReason
{
    /// <summary>The decision did not stop retrying.</summary>
    None = 0,

    /// <summary>The configured retry attempts were exhausted.</summary>
    AttemptsExhausted = 1,

    /// <summary>The configured retry age was exhausted.</summary>
    RetryAgeExhausted = 2,

    /// <summary>The failure is permanent and must not be retried as transient.</summary>
    PermanentFailure = 3,

    /// <summary>Authentication remains permanent until credentials change.</summary>
    PermanentUntilCredentialsChange = 4,
}
