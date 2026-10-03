// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace OccasionallyConnected.DurableOutbox;

/// <summary>Appends one reading to the durable local outbox.</summary>
/// <param name="DatabasePath">The SQLite database path.</param>
/// <param name="DeviceId">The device identifier.</param>
/// <param name="Value">The reading value.</param>
/// <param name="Guarantee">The requested delivery guarantee.</param>
internal sealed record AppendReadingCommand(
    string DatabasePath,
    string DeviceId,
    double Value,
    DeliveryGuarantee Guarantee) : IOutboxCommand
{
    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<OutboxCommandResult> ExecuteAsync(DurableOutboxApplication application, CancellationToken cancellationToken) =>
        application.AppendReadingAsync(DatabasePath, DeviceId, Value, Guarantee, cancellationToken);
}
