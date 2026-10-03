// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Validates built-in CRDT server adapter inputs.</summary>
internal static class CrdtServerGuards
{
    /// <summary>Validates a CRDT kind.</summary>
    /// <param name="kind">The kind.</param>
    /// <param name="parameterName">The parameter name.</param>
    /// <exception cref="ArgumentOutOfRangeException">The kind is invalid.</exception>
    internal static void ValidateKind(CrdtKind kind, string parameterName) =>
        _ = kind switch
        {
            CrdtKind.GCounter or CrdtKind.PNCounter or CrdtKind.ORSet or CrdtKind.LwwRegister => true,
            _ => throw new ArgumentOutOfRangeException(parameterName, kind, "The CRDT kind is not supported."),
        };
}
