// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests snapshot recovery protocol encoding.</summary>
public sealed partial class HttpProtocolCodecTests
{
    /// <summary>Creates a snapshot recovery request for codec tests.</summary>
    /// <param name="pending">The pending operations.</param>
    /// <returns>The request.</returns>
    private static RemoteSnapshotRecoveryRequest CreateSnapshotRecoveryCodecRequest(IReadOnlyList<SyncOperation> pending) => new()
    {
        StreamId = new(StreamName),
        SubscriptionId = new(Guid.Parse(SnapshotSubscriptionIdText)),
        ExpiredCursor = SnapshotExpiredCursor,
        ClientStateContractId = "client-state",
        ClientStateSchemaVersion = 1,
        SnapshotFormatVersion = 1,
        PendingOperations = pending,
        MaximumResponseBytes = SnapshotRecoveryMaximumResponseBytes,
    };

    /// <summary>Creates a snapshot recovery operation for codec tests.</summary>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateSnapshotRecoveryOperation() => new()
    {
        OperationId = new(OperationGuid),
        StreamId = new(StreamName),
        ClientSequence = 1,
        TimestampUtc = DateTimeOffset.Parse("2026-09-18T00:00:00+00:00", CultureInfo.InvariantCulture),
        Type = SyncOperationType.Append,
        Payload = new(ContractName, 1, PayloadContentType, "{}"u8.ToArray(), PayloadHash),
        Metadata = new Dictionary<string, string> { [SnapshotMetadataTraceKey] = "snapshot" },
    };

    /// <summary>Creates a second snapshot recovery operation for codec tests.</summary>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateSecondSnapshotRecoveryOperation() => CreateSnapshotRecoveryOperation() with
    {
        OperationId = new(Guid.Parse(SnapshotSecondOperationIdText)),
        ClientSequence = SnapshotRecoverySecondSequence,
    };

    /// <summary>Creates a recovered snapshot result for codec tests.</summary>
    /// <param name="request">The recovery request.</param>
    /// <returns>The result.</returns>
    private static RemoteSnapshotRecoveryResult CreateSnapshotRecoveryCodecResult(RemoteSnapshotRecoveryRequest request) => new()
    {
        Status = RemoteSnapshotRecoveryStatus.Recovered,
        Checkpoint = new()
        {
            StreamId = request.StreamId,
            SubscriptionId = request.SubscriptionId,
            FrontierCursor = SnapshotFrontierCursor,
            ServerVersion = SnapshotServerVersion,
            SnapshotFormatVersion = request.SnapshotFormatVersion,
            ClientState = new("client-state", 1, PayloadContentType, "{}"u8.ToArray(), PayloadHash),
            ObservedAtUtc = DateTimeOffset.Parse("2026-09-18T00:00:01+00:00", CultureInfo.InvariantCulture),
        },
        OperationDispositions = request.PendingOperations.Concat(request.ReplayOperations).Select(static operation => new SnapshotOperationDisposition
        {
            OperationId = operation.OperationId,
            Kind = SnapshotOperationDispositionKind.IncludedAccepted,
            Result = new(operation.OperationId, OperationResultKind.Accepted, null, SnapshotServerVersion),
        }).ToArray(),
    };

    /// <summary>Creates a snapshot recovery request JSON document.</summary>
    /// <param name="operations">The operation JSON entries.</param>
    /// <returns>The JSON document.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string SnapshotRecoveryRequestJson(string operations) => SnapshotRecoveryRequestJson(operations, string.Empty, string.Empty);

    /// <summary>Creates a snapshot recovery request JSON document.</summary>
    /// <param name="operations">The operation JSON entries.</param>
    /// <param name="extraJson">The extra JSON properties.</param>
    /// <returns>The JSON document.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string SnapshotRecoveryRequestJson(string operations, string extraJson) => SnapshotRecoveryRequestJson(operations, replayOperations: string.Empty, extraJson);

    /// <summary>Creates a snapshot recovery request JSON document.</summary>
    /// <param name="operations">The pending operation JSON entries.</param>
    /// <param name="replayOperations">The replay operation JSON entries.</param>
    /// <param name="extraJson">The extra JSON properties.</param>
    /// <returns>The JSON document.</returns>
    private static string SnapshotRecoveryRequestJson(string operations, string replayOperations, string extraJson) =>
        $"{{\"streamId\":\"{StreamName}\",\"subscriptionId\":\"{SnapshotSubscriptionIdText}\""
        + $",\"expiredCursor\":\"{SnapshotExpiredCursor}\",\"clientStateContractId\":\"client-state\""
        + ",\"clientStateSchemaVersion\":1,\"snapshotFormatVersion\":1"
        + $",\"pendingOperations\":[{operations}]"
        + (string.IsNullOrEmpty(replayOperations) ? string.Empty : $",\"replayOperations\":[{replayOperations}]")
        + $",\"maximumResponseBytes\":4096{extraJson}}}";

    /// <summary>Creates a snapshot recovery response JSON document.</summary>
    /// <param name="payload">The checkpoint payload.</param>
    /// <returns>The JSON document.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string SnapshotRecoveryResponseJson(string payload) =>
        "{\"status\":0,\"checkpoint\":{\"streamId\":\"" + StreamName
        + "\",\"subscriptionId\":\"" + SnapshotSubscriptionIdText
        + "\",\"frontierCursor\":\"" + SnapshotFrontierCursor
        + "\",\"serverVersion\":\"" + SnapshotServerVersion
        + "\",\"snapshotFormatVersion\":1,\"clientState\":{\"contractId\":\"client-state\""
        + ",\"schemaVersion\":1,\"contentType\":\"application/json\",\"payload\":\"" + payload
        + "\",\"payloadHash\":\"sha256-test\"},\"observedAtUtc\":\"2026-09-18T00:00:01+00:00\"}"
        + ",\"operationDispositions\":[]}";

    /// <summary>Creates a non-recovered snapshot response JSON document.</summary>
    /// <param name="status">The status wire value.</param>
    /// <returns>The JSON document.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string SnapshotRecoveryNonRecoveredResponseJson(int status) =>
        $"{{\"status\":{status.ToString(CultureInfo.InvariantCulture)},\"operationDispositions\":[]}}";

    /// <summary>Creates one operation JSON object for snapshot recovery wire tests.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="sequence">The client sequence.</param>
    /// <returns>The operation JSON.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string SnapshotRecoveryOperationJson(string operationId, long sequence) =>
        SnapshotRecoveryOperationJson(operationId, sequence, "e30=");

    /// <summary>Creates one operation JSON object for snapshot recovery wire tests.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="sequence">The client sequence.</param>
    /// <param name="payload">The encoded payload text.</param>
    /// <returns>The operation JSON.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string SnapshotRecoveryOperationJson(string operationId, long sequence, string payload) =>
        SnapshotRecoveryOperationJson(operationId, sequence, payload, $"{{\"{SnapshotMetadataTraceKey}\":\"snapshot\"}}");

    /// <summary>Creates one operation JSON object for snapshot recovery wire tests.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="sequence">The client sequence.</param>
    /// <param name="payload">The encoded payload text.</param>
    /// <param name="metadata">The metadata object JSON.</param>
    /// <returns>The operation JSON.</returns>
    private static string SnapshotRecoveryOperationJson(string operationId, long sequence, string payload, string metadata) =>
        $"{{\"operationId\":\"{operationId}\",\"streamId\":\"{StreamName}\""
        + $",\"clientSequence\":{sequence.ToString(CultureInfo.InvariantCulture)}"
        + ",\"timestampUtc\":\"2026-09-18T00:00:00+00:00\",\"type\":0"
        + ",\"payload\":{\"contractId\":\"contract\",\"schemaVersion\":1"
        + $",\"contentType\":\"application/json\",\"payload\":\"{payload}\",\"payloadHash\":\"sha256-test\"}}"
        + ",\"policy\":{\"deliveryGuarantee\":0,\"durability\":0,\"priority\":0,\"conflictPolicy\":0}"
        + $",\"metadata\":{metadata}}}";
}
