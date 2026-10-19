// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace OccasionallyConnected.DurableOutbox;

/// <summary>Runs one stage of the deterministic durable outbox demonstration.</summary>
/// <param name="databasePath">The owned SQLite database path.</param>
/// <param name="cancellationToken">The cancellation token.</param>
/// <returns>The stage command result.</returns>
internal delegate ValueTask<OutboxCommandResult> DemoStage(string databasePath, CancellationToken cancellationToken);
