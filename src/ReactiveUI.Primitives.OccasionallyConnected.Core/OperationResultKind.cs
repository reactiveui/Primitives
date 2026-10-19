// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Specifies the remote result kind for an operation.</summary>
public enum OperationResultKind
{
    /// <summary>The operation was accepted.</summary>
    Accepted = 0,

    /// <summary>The operation produced or requires conflict handling.</summary>
    Conflict = 1,

    /// <summary>The operation was rejected permanently.</summary>
    Rejected = 2,

    /// <summary>The operation may be retried later.</summary>
    Retryable = 3,
}
