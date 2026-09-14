// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace OccasionallyConnected.DurableOutbox;

/// <summary>Static helpers for <see cref="DurableOutboxApplication"/>.</summary>
internal sealed partial class DurableOutboxApplication
{
    /// <summary>Runs a parsed command and captures process-style output.</summary>
    /// <param name="command">The command to run.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The captured result.</returns>
    internal static async Task<OutboxCommandResult> RunAsync(IOutboxCommand command, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(command);

        DurableOutboxApplication application = new();
        return await application.RunCommandAsync(command, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Runs command-line arguments and captures process-style output.</summary>
    /// <param name="args">The command-line arguments.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The captured result.</returns>
    internal static async Task<OutboxCommandResult> RunAsync(string[] args, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(args);

        await using StringWriter output = new(CultureInfo.InvariantCulture);
        await using StringWriter error = new(CultureInfo.InvariantCulture);
        var exitCode = await OutboxCommandLine.RunAsync(args, output, error, cancellationToken).ConfigureAwait(false);
        return new(exitCode, output.ToString(), error.ToString());
    }

    /// <summary>Prints a compact explanation of the three delivery guarantee modes.</summary>
    /// <returns>The command result.</returns>
    internal static OutboxCommandResult ExplainGuarantees()
    {
        var output = FormatLines(
            "AtMostOnce: one durable local attempt barrier; a lost response becomes ambiguous and is not retried automatically.",
            "AtLeastOnce: retained with the same OperationId and client sequence so an idempotent server can deduplicate retries.",
            "ExactlyOnce: rejected by this sample because exactly-once effect requires a server ledger, atomic apply-plus-ack, and retention negotiation.");
        return Ok(output, default);
    }

    /// <summary>Creates a successful command result.</summary>
    /// <param name="output">The standard output text.</param>
    /// <param name="operationId">The related operation id.</param>
    /// <returns>The command result.</returns>
    private static OutboxCommandResult Ok(string output, OperationId operationId) =>
        new(0, output, string.Empty) { OperationId = operationId };

    /// <summary>Creates a failed command result.</summary>
    /// <param name="error">The standard error text.</param>
    /// <param name="exitCode">The exit code.</param>
    /// <returns>The command result.</returns>
    private static OutboxCommandResult Error(string error, int exitCode) =>
        new(exitCode, string.Empty, $"{error}{Environment.NewLine}");

    /// <summary>Formats output lines with a terminal newline.</summary>
    /// <param name="lines">The output lines.</param>
    /// <returns>The formatted output text.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string FormatLines(params string[] lines) => FormatLines((IReadOnlyList<string>)lines);

    /// <summary>Formats output lines with a terminal newline.</summary>
    /// <param name="lines">The output lines.</param>
    /// <returns>The formatted output text.</returns>
    private static string FormatLines(IReadOnlyList<string> lines)
    {
        StringBuilder builder = new();
        for (var index = 0; index < lines.Count; index++)
        {
            _ = builder.AppendLine(lines[index]);
        }

        return builder.ToString();
    }

    /// <summary>Creates a sync operation for a local reading append.</summary>
    /// <param name="session">The recovered store session.</param>
    /// <param name="reading">The reading payload.</param>
    /// <param name="payload">The serialized payload.</param>
    /// <param name="policy">The operation policy.</param>
    /// <param name="guarantee">The requested delivery guarantee.</param>
    /// <returns>The sync operation.</returns>
    private static SyncOperation CreateOperation(
        StoreSession session,
        TemperatureReading reading,
        PayloadEnvelope payload,
        OperationPolicy policy,
        DeliveryGuarantee guarantee) =>
        new()
        {
            OperationId = OperationId.New(),
            StreamId = TemperatureStream,
            ClientSequence = session.Recovered.NextClientSequence,
            TimestampUtc = reading.ObservedAtUtc,
            Type = SyncOperationType.Append,
            Payload = payload,
            Policy = policy,
            Metadata = CreateOperationMetadata(guarantee),
        };

    /// <summary>Creates operation metadata for the chosen guarantee.</summary>
    /// <param name="guarantee">The requested guarantee.</param>
    /// <returns>The operation metadata.</returns>
    private static Dictionary<string, string> CreateOperationMetadata(DeliveryGuarantee guarantee) =>
        new() { [MetadataSourceKey] = MetadataSourceValue, [MetadataGuaranteeKey] = guarantee.ToString() };

    /// <summary>Creates a snapshot mutation with matching authoritative state.</summary>
    /// <param name="snapshotPayload">The optimistic snapshot payload.</param>
    /// <param name="authoritativePayload">The authoritative snapshot payload.</param>
    /// <param name="expectedRevision">The expected snapshot revision.</param>
    /// <returns>The snapshot mutation.</returns>
    private static SnapshotMutation CreateSnapshotMutation(
        PayloadEnvelope snapshotPayload,
        PayloadEnvelope authoritativePayload,
        long expectedRevision) =>
        new(TemperatureStream, snapshotPayload, SnapshotFormatVersion, expectedRevision) { AuthoritativeState = authoritativePayload };
}
