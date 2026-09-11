// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Tracks whether the single immediate authentication retry has been used.</summary>
public enum RetryAuthenticationState
{
    /// <summary>No authentication renewal retry has been used for the current credential version.</summary>
    None = 0,

    /// <summary>The immediate retry after authentication renewal has been used for the current credential version.</summary>
    RenewalRetryUsed = 1,
}
