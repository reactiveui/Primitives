// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Specifies how write conflicts are resolved.</summary>
public enum ConflictPolicy
{
    /// <summary>Uses the server's canonical last-writer-wins conflict rule.</summary>
    LastWriterWins = 0,

    /// <summary>Uses the configured deterministic merge pipeline.</summary>
    Merge = 1,

    /// <summary>Uses an explicitly registered caller-supplied policy.</summary>
    Custom = 2,
}
