// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes whether an operation should retry or stop.</summary>
public enum RetryDecisionKind
{
    /// <summary>The operation should retry when the computed due time is reached.</summary>
    Retry = 0,

    /// <summary>The operation should stop retrying and transition to a terminal state.</summary>
    Stop = 1,
}
