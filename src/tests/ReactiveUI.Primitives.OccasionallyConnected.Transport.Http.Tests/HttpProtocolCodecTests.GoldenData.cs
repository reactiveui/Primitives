// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Holds the fixed protocol-v1 golden values for <see cref="HttpProtocolCodec"/>.</summary>
public sealed partial class HttpProtocolCodecTests
{
    /// <summary>The golden connect request fixture.</summary>
    private const string GoldenConnectRequestFile = "connect-request.json";

    /// <summary>The golden connect response fixture.</summary>
    private const string GoldenConnectResponseFile = "connect-response.json";

    /// <summary>The golden push request fixture.</summary>
    private const string GoldenPushRequestFile = "push-request.json";

    /// <summary>The golden push response fixture carrying every operation result kind.</summary>
    private const string GoldenPushResponseFile = "push-response.json";

    /// <summary>The golden subscribe response fixture.</summary>
    private const string GoldenSubscribeResponseFile = "subscribe-response.json";

    /// <summary>The golden acknowledgement request fixture.</summary>
    private const string GoldenAcknowledgeRequestFile = "acknowledge-request.json";

    /// <summary>The golden snapshot recovery request fixture.</summary>
    private const string GoldenSnapshotRecoveryRequestFile = "snapshot-recovery-request.json";

    /// <summary>The golden recovered snapshot response fixture.</summary>
    private const string GoldenSnapshotRecoveredResponseFile = "snapshot-recovery-response-recovered.json";

    /// <summary>The golden retention-gap snapshot response fixture.</summary>
    private const string GoldenSnapshotRetentionExpiredResponseFile = "snapshot-recovery-response-retention-expired.json";

    /// <summary>The golden subscribe query fixture holding the canonical cursor forms.</summary>
    private const string GoldenSubscribeQueriesFile = "subscribe-queries.txt";

    /// <summary>The golden client identifier.</summary>
    private const string GoldenClientId = "client-1";

    /// <summary>The golden tenant hint.</summary>
    private const string GoldenTenantHint = "tenant-1";

    /// <summary>The golden stream identifier. The slash proves cursor and stream escaping.</summary>
    private const string GoldenStreamId = "sensor/temperature";

    /// <summary>The golden payload contract identifier.</summary>
    private const string GoldenContractId = "temperature-reading";

    /// <summary>The golden client state contract identifier.</summary>
    private const string GoldenStateContractId = "temperature-state";

    /// <summary>The golden JSON content type.</summary>
    private const string GoldenContentType = "application/json";

    /// <summary>The SHA-256 payload hash of the first reading payload.</summary>
    private const string GoldenFirstReadingHash = "sha256-QMm8O6gF7xuFhW8fMH5aq829cNZL3UygF9155r+sJBw=";

    /// <summary>The SHA-256 payload hash of the second reading payload.</summary>
    private const string GoldenSecondReadingHash = "sha256-AHrmRLT/fVIWmAB/ynZthFtmjIjyxB262Fj0y6tmYUY=";

    /// <summary>The golden client state payload.</summary>
    private const string GoldenStateJson = """{"readings":3,"last":22.1}""";

    /// <summary>The SHA-256 payload hash of <see cref="GoldenStateJson"/>.</summary>
    private const string GoldenStateHash = "sha256-P5xyzqNgFYRDlrUn+Ckvesp71hydfu3VHQUAmFfkaco=";

    /// <summary>The golden timestamp for client operations.</summary>
    private const string GoldenOperationTimestampText = "2026-09-13T00:00:07+00:00";

    /// <summary>The golden timestamp for server commits.</summary>
    private const string GoldenCommitTimestampText = "2026-09-13T00:00:08+00:00";

    /// <summary>The golden batch identifier.</summary>
    private const string GoldenBatchIdText = "00000000-0000-0000-0000-000000000100";

    /// <summary>The golden second receive batch identifier.</summary>
    private const string GoldenSecondBatchIdText = "00000000-0000-0000-0000-000000000101";

    /// <summary>The golden subscription identifier.</summary>
    private const string GoldenSubscriptionIdText = "00000000-0000-0000-0000-000000000301";

    /// <summary>The golden first cursor.</summary>
    private const string GoldenFirstCursor = "cursor-1";

    /// <summary>The golden second cursor.</summary>
    private const string GoldenSecondCursor = "cursor-2";

    /// <summary>The golden third cursor.</summary>
    private const string GoldenThirdCursor = "cursor-3";

    /// <summary>The golden snapshot frontier cursor.</summary>
    private const string GoldenFrontierCursor = "cursor-9";

    /// <summary>The golden trace metadata key.</summary>
    private const string GoldenTraceKey = "trace";

    /// <summary>The golden maximum negotiated batch operation count.</summary>
    private const int GoldenMaximumBatchOperations = 100;

    /// <summary>The golden maximum negotiated batch byte count.</summary>
    private const long GoldenMaximumBatchBytes = 1_048_576;

    /// <summary>The golden snapshot recovery response byte budget.</summary>
    private const long GoldenSnapshotResponseBytes = 65_536;

    /// <summary>The golden server idempotency retention in hours.</summary>
    private const int GoldenIdempotencyRetentionHours = 24;

    /// <summary>The golden client inbox retention in hours.</summary>
    private const int GoldenInboxRetentionHours = 48;

    /// <summary>The golden positive priority.</summary>
    private const int GoldenPriority = 5;

    /// <summary>The golden negative priority.</summary>
    private const int GoldenNegativePriority = -3;

    /// <summary>The number of operations in the golden push batch.</summary>
    private const int GoldenOperationCount = 4;

    /// <summary>The index of the golden operation that uses the second policy.</summary>
    private const int GoldenSecondIndex = 1;

    /// <summary>The index of the golden operation that uses the third policy.</summary>
    private const int GoldenThirdIndex = 2;

    /// <summary>The index of the golden operation that uses the fourth policy.</summary>
    private const int GoldenFourthIndex = 3;

    /// <summary>The schema version of the second golden reading.</summary>
    private const int GoldenSecondSchemaVersion = 2;

    /// <summary>The golden initial subscribe sequence.</summary>
    private const long GoldenStartSequence = 42;

    /// <summary>Gets the golden operation identifiers in batch order.</summary>
    private static string[] GoldenOperationIdTexts =>
    [
        "00000000-0000-0000-0000-000000000001",
        "00000000-0000-0000-0000-000000000002",
        "00000000-0000-0000-0000-000000000003",
        "00000000-0000-0000-0000-000000000004",
    ];

    /// <summary>Gets the golden event identifiers in cursor order.</summary>
    private static string[] GoldenEventIdTexts =>
    [
        "00000000-0000-0000-0000-000000000201",
        "00000000-0000-0000-0000-000000000202",
        "00000000-0000-0000-0000-000000000203",
    ];

    /// <summary>Creates the golden codec with default protocol limits.</summary>
    /// <returns>The codec.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HttpProtocolCodec CreateGoldenCodec() => CreateCodec();

    /// <summary>Creates the golden connect request.</summary>
    /// <returns>The connect request.</returns>
    private static TransportConnectRequest CreateGoldenConnectRequest() =>
        new(new(new(1, 0), new(1, 1)), new(GoldenClientId, GoldenTenantHint), [DeliveryGuarantee.AtLeastOnce, DeliveryGuarantee.ExactlyOnce]);

    /// <summary>Creates the golden negotiated capabilities.</summary>
    /// <returns>The capabilities.</returns>
    private static NegotiatedCapabilities CreateGoldenCapabilities() =>
        new(
            new(1, 0),
            RemoteTransportCapabilities.BatchPush
                | RemoteTransportCapabilities.CursorResume
                | RemoteTransportCapabilities.ReceiveAcknowledgements
                | RemoteTransportCapabilities.ServerIdempotency
                | RemoteTransportCapabilities.AtomicApplyAndAcknowledge
                | RemoteTransportCapabilities.SnapshotRecovery,
            GoldenMaximumBatchOperations,
            GoldenMaximumBatchBytes,
            TimeSpan.FromHours(GoldenIdempotencyRetentionHours),
            TimeSpan.FromHours(GoldenInboxRetentionHours));

    /// <summary>Creates the golden push batch with one operation per result kind.</summary>
    /// <returns>The batch.</returns>
    private static SyncBatch CreateGoldenBatch()
    {
        var operations = new SyncOperation[GoldenOperationCount];
        for (var index = 0; index < operations.Length; index++)
        {
            operations[index] = CreateGoldenOperation(index);
        }

        return new(Guid.Parse(GoldenBatchIdText), operations);
    }

    /// <summary>Creates one golden operation.</summary>
    /// <param name="index">The zero-based operation index.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateGoldenOperation(int index) => new()
    {
        OperationId = CreateGoldenOperationId(index),
        StreamId = new(GoldenStreamId),
        ClientSequence = index + 1,
        TimestampUtc = ParseGoldenTimestamp(GoldenOperationTimestampText),
        BaseVersion = index is 0 ? null : $"v{index.ToString(CultureInfo.InvariantCulture)}",
        Type = (SyncOperationType)index,
        Payload = index % GoldenThirdIndex is 0 ? CreateGoldenFirstReading() : CreateGoldenSecondReading(),
        Policy = index switch
        {
            0 => OperationPolicy.Default,
            GoldenSecondIndex => new(DeliveryGuarantee.ExactlyOnce, OperationDurability.Durable, GoldenPriority, ConflictPolicy.LastWriterWins),
            GoldenThirdIndex => new(DeliveryGuarantee.AtMostOnce, OperationDurability.Volatile, GoldenNegativePriority, ConflictPolicy.Custom),
            _ => new(DeliveryGuarantee.AtLeastOnce, OperationDurability.Durable, 0, ConflictPolicy.Merge),
        },
        Metadata = new Dictionary<string, string> { [GoldenTraceKey] = $"op-{(index + 1).ToString(CultureInfo.InvariantCulture)}" },
    };

    /// <summary>Creates the golden push result with every operation result kind.</summary>
    /// <returns>The result.</returns>
    private static RemoteSyncResult CreateGoldenSyncResult() =>
        new(
            Guid.Parse(GoldenBatchIdText),
            [
                new(CreateGoldenOperationId(0), OperationResultKind.Accepted, null, "v1"),
                new(CreateGoldenOperationId(GoldenSecondIndex), OperationResultKind.Conflict, "version-mismatch", "v7"),
                new(CreateGoldenOperationId(GoldenThirdIndex), OperationResultKind.Rejected, "validation-failed", null),
                new(CreateGoldenOperationId(GoldenFourthIndex), OperationResultKind.Retryable, "server-busy", null),
            ],
            GoldenThirdCursor,
            null);

    /// <summary>Creates the golden receive batches.</summary>
    /// <returns>The batches.</returns>
    private static RemoteEventBatch[] CreateGoldenReceiveBatches()
    {
        var origin = new RemoteEventOrigin(GoldenClientId, CreateGoldenOperationId(0));
        var first = new RemoteEvent(
            Guid.Parse(GoldenEventIdTexts[0]),
            new(GoldenStreamId),
            GoldenFirstCursor,
            ParseGoldenTimestamp(GoldenCommitTimestampText),
            origin.OperationId,
            CreateGoldenFirstReading(),
            new Dictionary<string, string> { [GoldenTraceKey] = "evt-1" }) { Origin = origin };
        var second = new RemoteEvent(
            Guid.Parse(GoldenEventIdTexts[1]),
            new(GoldenStreamId),
            GoldenSecondCursor,
            ParseGoldenTimestamp(GoldenCommitTimestampText),
            null,
            CreateGoldenSecondReading(),
            new Dictionary<string, string>());
        var third = new RemoteEvent(
            Guid.Parse(GoldenEventIdTexts[2]),
            new(GoldenStreamId),
            GoldenThirdCursor,
            ParseGoldenTimestamp(GoldenCommitTimestampText),
            null,
            CreateGoldenFirstReading(),
            new Dictionary<string, string>());
        return
        [
            new(Guid.Parse(GoldenBatchIdText), new(GoldenStreamId), null, GoldenSecondCursor, [first, second]) { CompletedOperations = [new(origin, [first.EventId])] },
            new(Guid.Parse(GoldenSecondBatchIdText), new(GoldenStreamId), GoldenSecondCursor, GoldenThirdCursor, [third]),
        ];
    }

    /// <summary>Creates the golden acknowledgement.</summary>
    /// <returns>The acknowledgement.</returns>
    private static ReceiveAcknowledgement CreateGoldenAcknowledgement() =>
        new(new(Guid.Parse(GoldenSubscriptionIdText)), new(GoldenStreamId), GoldenThirdCursor);

    /// <summary>Creates the golden snapshot recovery request.</summary>
    /// <returns>The request.</returns>
    private static RemoteSnapshotRecoveryRequest CreateGoldenSnapshotRequest() => new()
    {
        StreamId = new(GoldenStreamId),
        SubscriptionId = new(Guid.Parse(GoldenSubscriptionIdText)),
        ExpiredCursor = GoldenFirstCursor,
        ClientStateContractId = GoldenStateContractId,
        ClientStateSchemaVersion = 1,
        SnapshotFormatVersion = 1,
        PendingOperations = [CreateGoldenOperation(0), CreateGoldenOperation(1)],
        ReplayOperations = [CreateGoldenOperation(GoldenOperationCount - 1)],
        MaximumResponseBytes = GoldenSnapshotResponseBytes,
    };

    /// <summary>Creates the golden recovered snapshot result.</summary>
    /// <param name="request">The recovery request.</param>
    /// <returns>The result.</returns>
    private static RemoteSnapshotRecoveryResult CreateGoldenRecoveredSnapshot(RemoteSnapshotRecoveryRequest request) => new()
    {
        Status = RemoteSnapshotRecoveryStatus.Recovered,
        Checkpoint = new()
        {
            StreamId = request.StreamId,
            SubscriptionId = request.SubscriptionId,
            FrontierCursor = GoldenFrontierCursor,
            ServerVersion = "v9",
            SnapshotFormatVersion = request.SnapshotFormatVersion,
            ClientState = new(GoldenStateContractId, 1, GoldenContentType, """{"readings":3,"last":22.1}"""u8.ToArray(), GoldenStateHash),
            ObservedAtUtc = ParseGoldenTimestamp(GoldenCommitTimestampText),
        },
        OperationDispositions =
        [
            new()
            {
                OperationId = CreateGoldenOperationId(0),
                Kind = SnapshotOperationDispositionKind.IncludedAccepted,
                Result = new(CreateGoldenOperationId(0), OperationResultKind.Accepted, null, "v9"),
            },
            new()
            {
                OperationId = CreateGoldenOperationId(1),
                Kind = SnapshotOperationDispositionKind.TerminalRejected,
                Result = new(CreateGoldenOperationId(1), OperationResultKind.Rejected, "validation-failed", null),
            },
            new()
            {
                OperationId = CreateGoldenOperationId(GoldenOperationCount - 1),
                Kind = SnapshotOperationDispositionKind.IncludedAccepted,
                Result = new(CreateGoldenOperationId(GoldenOperationCount - 1), OperationResultKind.Accepted, null, "v9"),
            },
        ],
    };

    /// <summary>Creates the golden retention-gap snapshot result.</summary>
    /// <returns>The result.</returns>
    private static RemoteSnapshotRecoveryResult CreateGoldenRetentionExpiredSnapshot() =>
        new() { Status = RemoteSnapshotRecoveryStatus.RetentionExpired, ReasonCode = "retention-expired" };

    /// <summary>Creates the golden subscribe requests covering every canonical start position form.</summary>
    /// <returns>The requests in fixture order.</returns>
    private static RemoteSubscribeRequest[] CreateGoldenSubscribeRequests()
    {
        StreamId stream = new(GoldenStreamId);
        SubscriptionId subscription = new(Guid.Parse(GoldenSubscriptionIdText));
        return
        [
            new(stream, subscription, null, StartPosition.Latest),
            new(stream, subscription, null, StartPosition.FromTimestamp(ParseGoldenTimestamp(GoldenOperationTimestampText))),
            new(stream, subscription, null, StartPosition.FromSequence(GoldenStartSequence)),
            new(stream, subscription, null, StartPosition.FromCursor("anchor/1 +x")),
            new(stream, subscription, "resume/2=?&", StartPosition.Latest),
        ];
    }

    /// <summary>Creates a golden operation identifier.</summary>
    /// <param name="index">The zero-based operation index.</param>
    /// <returns>The operation identifier.</returns>
    private static OperationId CreateGoldenOperationId(int index) => new(Guid.Parse(GoldenOperationIdTexts[index]));

    /// <summary>Creates the first golden reading payload.</summary>
    /// <returns>The payload.</returns>
    private static PayloadEnvelope CreateGoldenFirstReading() =>
        new(GoldenContractId, 1, GoldenContentType, """{"value":21.3,"unit":"C"}"""u8.ToArray(), GoldenFirstReadingHash);

    /// <summary>Creates the second golden reading payload.</summary>
    /// <returns>The payload.</returns>
    private static PayloadEnvelope CreateGoldenSecondReading() =>
        new(GoldenContractId, GoldenSecondSchemaVersion, GoldenContentType, """{"value":22.1,"unit":"C"}"""u8.ToArray(), GoldenSecondReadingHash);

    /// <summary>Parses a fixed golden timestamp.</summary>
    /// <param name="text">The timestamp text.</param>
    /// <returns>The timestamp.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static DateTimeOffset ParseGoldenTimestamp(string text) => DateTimeOffset.Parse(text, CultureInfo.InvariantCulture);
}
