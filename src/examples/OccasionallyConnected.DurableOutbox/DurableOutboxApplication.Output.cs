// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace OccasionallyConnected.DurableOutbox;

/// <summary>Output helpers for <see cref="DurableOutboxApplication"/>.</summary>
internal sealed partial class DurableOutboxApplication
{
    /// <summary>Adds recovered subscription details to output lines.</summary>
    /// <param name="lines">The output lines.</param>
    /// <param name="session">The recovered store session.</param>
    private static void AppendSubscription(List<string> lines, StoreSession session)
    {
        lines.Add(string.Create(CultureInfo.InvariantCulture, $"subscription: {session.SubscriptionId.Value}"));
        lines.Add(string.Create(CultureInfo.InvariantCulture, $"server-cursor: {session.Recovered.ServerCursor ?? "none"}"));
        lines.Add(string.Create(CultureInfo.InvariantCulture, $"pending: {session.Recovered.PendingOperations.Count}"));
        lines.Add(string.Create(CultureInfo.InvariantCulture, $"replay: {session.Recovered.ReplayOperations.Count}"));
        lines.Add(string.Create(CultureInfo.InvariantCulture, $"dead-lettered: {session.Recovered.DeadLetters.Count}"));
        lines.Add(string.Create(CultureInfo.InvariantCulture, $"next-client-sequence: {session.Recovered.NextClientSequence}"));
    }

    /// <summary>Adds configured capacity details to output lines.</summary>
    /// <param name="lines">The output lines.</param>
    private static void AppendCapacity(List<string> lines)
    {
        lines.Add(string.Create(CultureInfo.InvariantCulture, $"configured-pending-capacity: {MaximumPendingOperations}"));
        lines.Add(string.Create(CultureInfo.InvariantCulture, $"configured-worker-bytes: {WorkerCapacityBytes}"));
        lines.Add(string.Create(CultureInfo.InvariantCulture, $"configured-lease-batch: {MaximumLeaseOperations}"));
    }

    /// <summary>Adds pending operation details to output lines.</summary>
    /// <param name="lines">The output lines.</param>
    /// <param name="session">The recovered store session.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous append operation.</returns>
    private static async ValueTask AppendPendingAsync(
        List<string> lines,
        StoreSession session,
        CancellationToken cancellationToken)
    {
        for (var index = 0; index < session.Recovered.PendingOperations.Count; index++)
        {
            var operation = session.Recovered.PendingOperations[index];
            var status = await session.Store.GetOperationStatusAsync(operation.OperationId, cancellationToken).ConfigureAwait(false);
            var reason = string.IsNullOrEmpty(status?.ReasonCode) ? string.Empty : $" reason={status.ReasonCode}";
            var line = new StringBuilder("pending-operation: operation=")
                .Append(operation.OperationId.Value)
                .Append(" sequence=")
                .Append(operation.ClientSequence)
                .Append(" guarantee=")
                .Append(operation.Policy.DeliveryGuarantee)
                .Append(" state=")
                .Append(status?.State.ToString() ?? UnknownText)
                .Append(" attempt=")
                .Append(status?.Attempt ?? 0)
                .Append(reason);
            lines.Add(line.ToString());
        }
    }

    /// <summary>Adds retained operation status to output lines.</summary>
    /// <param name="lines">The output lines.</param>
    /// <param name="session">The recovered store session.</param>
    /// <param name="operationId">The operation id to inspect.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous append operation.</returns>
    private static async ValueTask AppendOperationStatusAsync(
        List<string> lines,
        StoreSession session,
        OperationId operationId,
        CancellationToken cancellationToken)
    {
        var status = await session.Store.GetOperationStatusAsync(operationId, cancellationToken).ConfigureAwait(false);
        var reason = string.IsNullOrEmpty(status?.ReasonCode) ? string.Empty : $" reason={status.ReasonCode}";
        lines.Add(
            string.Create(
                CultureInfo.InvariantCulture,
                $"operation-status: operation={operationId.Value} state={status?.State.ToString() ?? UnknownText} attempt={status?.Attempt ?? 0}{reason}"));
    }

    /// <summary>Adds the bounded subscription view to output lines.</summary>
    /// <param name="lines">The output lines.</param>
    /// <param name="session">The recovered store session.</param>
    /// <param name="take">The maximum entries to print.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous append operation.</returns>
    private async ValueTask AppendSubscriptionViewAsync(
        List<string> lines,
        StoreSession session,
        int take,
        CancellationToken cancellationToken)
    {
        var snapshot = await ReadSnapshotAsync(session.Recovered.Snapshot, cancellationToken).ConfigureAwait(false);
        var entries = Math.Min(take, Math.Max(1, snapshot.ReadingCount));
        for (var index = 0; index < entries; index++)
        {
            var line = new StringBuilder("subscription-entry: ")
                .Append(index + 1)
                .Append(" subscription=")
                .Append(session.SubscriptionId.Value)
                .Append(" local-reading-count=")
                .Append(snapshot.ReadingCount)
                .Append(" server-cursor=")
                .Append(session.Recovered.ServerCursor ?? "none");
            lines.Add(line.ToString());
        }
    }

    /// <summary>Adds snapshot details to output lines.</summary>
    /// <param name="lines">The output lines.</param>
    /// <param name="session">The recovered store session.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous append operation.</returns>
    private async ValueTask AppendSnapshotAsync(List<string> lines, StoreSession session, CancellationToken cancellationToken)
    {
        var snapshot = await ReadSnapshotAsync(session.Recovered.Snapshot, cancellationToken).ConfigureAwait(false);
        var lastDevice = snapshot.LastDeviceId.Length == 0 ? "none" : snapshot.LastDeviceId;
        lines.Add(string.Create(CultureInfo.InvariantCulture, $"snapshot-revision: {session.Recovered.Snapshot?.Revision ?? 0}"));
        lines.Add(string.Create(CultureInfo.InvariantCulture, $"reading-count: {snapshot.ReadingCount}"));
        lines.Add(string.Create(CultureInfo.InvariantCulture, $"last-device: {lastDevice}"));
        lines.Add(string.Create(CultureInfo.InvariantCulture, $"last-reading: {snapshot.LastReading:0.###}"));
        lines.Add(string.Create(CultureInfo.InvariantCulture, $"total-reading: {snapshot.TotalReading:0.###}"));
    }
}
