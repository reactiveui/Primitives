// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Validates built-in CRDT server adapter inputs.</summary>
internal static class MergeableServerGuards
{
    /// <summary>Validates a CRDT kind.</summary>
    /// <param name="kind">The kind.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentOutOfRangeException">The kind is invalid.</exception>
    internal static void ValidateKind(MergeableKind kind, string parameterName) =>
        _ = kind switch
        {
            MergeableKind.IncrementOnlyCounter or MergeableKind.Counter or MergeableKind.AddWinsSet or MergeableKind.LastWriterWinsValue => true,
            _ => throw new ArgumentOutOfRangeException(parameterName, kind, "The CRDT kind is not supported."),
        };
}
