// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace OccasionallyConnected.DurableOutbox;

/// <summary>Inspects durable local store state after reopening SQLite.</summary>
/// <param name="DatabasePath">The SQLite database path.</param>
/// <param name="View">The view to print.</param>
/// <param name="Take">The bounded number of subscription entries to print.</param>
/// <param name="OperationId">The optional operation id whose retained status should be printed.</param>
internal sealed record InspectCommand(
    string DatabasePath,
    InspectView View,
    int Take = 1,
    OperationId? OperationId = null) : IOutboxCommand
{
    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<OutboxCommandResult> ExecuteAsync(DurableOutboxApplication application, CancellationToken cancellationToken) =>
        application.InspectAsync(DatabasePath, View, Take, OperationId, cancellationToken);
}
