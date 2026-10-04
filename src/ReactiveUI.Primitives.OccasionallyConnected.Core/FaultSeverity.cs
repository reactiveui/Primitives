// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes the impact of a typed operational fault.</summary>
public enum FaultSeverity
{
    /// <summary>A report that does not prevent local or remote progress.</summary>
    Information = 0,

    /// <summary>A recoverable condition requires attention or retry.</summary>
    Warning = 1,

    /// <summary>An operation or stream cannot continue without recovery.</summary>
    Error = 2,

    /// <summary>Durable integrity or a context-wide invariant is at risk.</summary>
    Critical = 3,
}
