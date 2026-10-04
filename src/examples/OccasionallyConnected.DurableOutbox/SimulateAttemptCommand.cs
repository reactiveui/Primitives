// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace OccasionallyConnected.DurableOutbox;

/// <summary>Runs an explicitly local upload-attempt simulation.</summary>
/// <param name="DatabasePath">The SQLite database path.</param>
/// <param name="OperationId">The operation to simulate, or empty to use the next pending operation.</param>
/// <param name="Outcome">The simulated outcome.</param>
internal sealed record SimulateAttemptCommand(
    string DatabasePath,
    OperationId OperationId,
    SimulatedAttemptOutcome Outcome) : IOutboxCommand
{
    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<OutboxCommandResult> ExecuteAsync(DurableOutboxApplication application, CancellationToken cancellationToken) =>
        application.SimulateAttemptAsync(DatabasePath, OperationId, Outcome, cancellationToken);
}
