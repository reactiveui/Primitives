// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Describes one lab context over a SQLite store and the in-memory loopback hub.</summary>
/// <param name="DirectoryPath">The directory that holds the SQLite database.</param>
/// <param name="Hub">The in-memory server hub.</param>
/// <param name="ClientId">The trusted client identifier.</param>
/// <param name="OutboxOperations">The outbox operation limit.</param>
[System.Diagnostics.DebuggerDisplay("{ClientId,nq}; Outbox={OutboxOperations,nq}")]
internal sealed record ResilienceLabContextSettings(
    string DirectoryPath,
    IServerStreamHub Hub,
    string ClientId,
    int OutboxOperations);
