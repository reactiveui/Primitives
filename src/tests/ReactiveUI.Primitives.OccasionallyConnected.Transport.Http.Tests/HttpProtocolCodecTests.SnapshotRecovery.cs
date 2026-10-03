// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests snapshot recovery protocol encoding.</summary>
public sealed partial class HttpProtocolCodecTests
{
    /// <summary>The snapshot recovery subscription identifier text.</summary>
    private const string SnapshotSubscriptionIdText = "00000000-0000-0000-0000-000000000501";

    /// <summary>The snapshot recovery expired cursor.</summary>
    private const string SnapshotExpiredCursor = "expired-cursor";

    /// <summary>The snapshot recovery frontier cursor.</summary>
    private const string SnapshotFrontierCursor = "frontier-cursor";

    /// <summary>The snapshot recovery server version.</summary>
    private const string SnapshotServerVersion = "server-snapshot-1";

    /// <summary>The empty operation dispositions JSON property fragment.</summary>
    private const string SnapshotEmptyOperationDispositionsJson = "\"operationDispositions\":[]";

    /// <summary>The operation dispositions JSON array property start.</summary>
    private const string SnapshotOperationDispositionsArrayStartJson = "\"operationDispositions\":[";

    /// <summary>The snapshot metadata trace key.</summary>
    private const string SnapshotMetadataTraceKey = "trace";

    /// <summary>The first snapshot operation identifier text.</summary>
    private const string SnapshotFirstOperationIdText = "00000000-0000-0000-0000-000000000001";

    /// <summary>The second snapshot operation identifier text.</summary>
    private const string SnapshotSecondOperationIdText = "00000000-0000-0000-0000-000000000002";

    /// <summary>The pending operation limit used by round-trip tests.</summary>
    private const int SnapshotRecoveryRoundTripPendingOperationLimit = 4;

    /// <summary>The pending operation limit used by rejected request tests.</summary>
    private const int SnapshotRecoveryRejectedPendingOperationLimit = 1;

    /// <summary>The second pending operation sequence fixture.</summary>
    private const int SnapshotRecoverySecondSequence = 2;

    /// <summary>The snapshot recovery response byte limit fixture.</summary>
    private const int SnapshotRecoveryMaximumResponseBytes = 4096;

    /// <summary>The snapshot recovery response byte limit JSON fragment.</summary>
    private const string SnapshotRecoveryMaximumResponseBytesJson = "\"maximumResponseBytes\":4096";

    /// <summary>A metadata byte limit below the logical entry-count header.</summary>
    private const int SnapshotRecoveryBelowMetadataHeaderBytes = 1;

    /// <summary>A JSON Unicode escape for an unpaired high surrogate.</summary>
    private const string SnapshotMalformedUnicodeEscape = "\\uD800";

    /// <summary>The unsupported projection snapshot recovery status wire value.</summary>
    private const int SnapshotUnsupportedProjectionStatus = 1;

    /// <summary>The retention expired snapshot recovery status wire value.</summary>
    private const int SnapshotRetentionExpiredStatus = 2;

    /// <summary>The ambiguous pending operation snapshot recovery status wire value.</summary>
    private const int SnapshotAmbiguousPendingOperationStatus = 3;

    /// <summary>The validation rejected snapshot recovery status wire value.</summary>
    private const int SnapshotValidationRejectedStatus = 4;

    /// <summary>The capacity exceeded snapshot recovery status wire value.</summary>
    private const int SnapshotCapacityExceededStatus = 5;

    /// <summary>The retryable concurrent change snapshot recovery status wire value.</summary>
    private const int SnapshotRetryableConcurrentChangeStatus = 6;

    /// <summary>An unknown snapshot recovery status wire value.</summary>
    private const int SnapshotUnknownStatus = 99;

    /// <summary>The terminal rejected snapshot disposition wire value.</summary>
    private const int SnapshotTerminalRejectedDisposition = 1;

    /// <summary>The unknown snapshot disposition wire value.</summary>
    private const int SnapshotUnknownDisposition = 2;

    /// <summary>An invalid snapshot disposition wire value.</summary>
    private const int SnapshotInvalidDisposition = 99;

    /// <summary>The rejected operation result wire value.</summary>
    private const int SnapshotRejectedResult = 2;

    /// <summary>The expected disposition count for two-operation response fixtures.</summary>
    private const int SnapshotExpectedTwoDispositionCount = 2;

    /// <summary>A maximum response byte value above the configured recovery limit.</summary>
    private const int SnapshotOversizedMaximumResponseBytes = 4097;

    /// <summary>The Core validator budget for the request header, two collection counts, and one fixture operation.</summary>
    private const int SnapshotRecoveryPendingOnlyLogicalBytes = 208;

    /// <summary>Verifies snapshot recovery requests preserve bounded pending operations.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotRecoveryRequestRoundTripsPendingOperationsAndLimits()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumPendingOperations = SnapshotRecoveryRoundTripPendingOperationLimit },
        });
        var request = CreateSnapshotRecoveryCodecRequest([CreateSnapshotRecoveryOperation()]);

        var bytes = codec.SerializeSnapshotRecoveryRequest(request);
        var decoded = codec.DeserializeSnapshotRecoveryRequest(bytes);

        await Assert.That(decoded.StreamId).IsEqualTo(request.StreamId);
        await Assert.That(decoded.SubscriptionId).IsEqualTo(request.SubscriptionId);
        await Assert.That(decoded.ExpiredCursor).IsEqualTo(request.ExpiredCursor);
        await Assert.That(decoded.PendingOperations).Count().IsEqualTo(1);
        await Assert.That(decoded.MaximumResponseBytes).IsEqualTo(request.MaximumResponseBytes);
        await Assert.That(decoded.PendingOperations[0].ClientSequence).IsEqualTo(request.PendingOperations[0].ClientSequence);
        await Assert.That(decoded.PendingOperations[0].Metadata[SnapshotMetadataTraceKey]).IsEqualTo("snapshot");
        await Assert.That(decoded.PendingOperations[0].Payload.Payload.ToArray().SequenceEqual(
            request.PendingOperations[0].Payload.Payload.ToArray())).IsTrue();
    }

    /// <summary>Verifies snapshot recovery requests preserve bounded replay operations separately from pending operations.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SnapshotRecoveryRequestRoundTripsPendingAndReplayOperations()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumPendingOperations = SnapshotRecoveryRoundTripPendingOperationLimit },
        });
        var replay = CreateSecondSnapshotRecoveryOperation();
        var request = CreateSnapshotRecoveryCodecRequest([CreateSnapshotRecoveryOperation()]) with
        {
            ReplayOperations = [replay],
        };

        var bytes = codec.SerializeSnapshotRecoveryRequest(request);
        var decoded = codec.DeserializeSnapshotRecoveryRequest(bytes);

        await Assert.That(decoded.PendingOperations).Count().IsEqualTo(1);
        await Assert.That(decoded.ReplayOperations).Count().IsEqualTo(1);
        await Assert.That(decoded.ReplayOperations[0].OperationId).IsEqualTo(replay.OperationId);
        await Assert.That(decoded.ReplayOperations[0].ClientSequence).IsEqualTo(replay.ClientSequence);
        await Assert.That(decoded.ReplayOperations[0].Payload.Payload.ToArray().SequenceEqual(
            replay.Payload.Payload.ToArray())).IsTrue();
    }

    /// <summary>Verifies outgoing snapshot recovery requests enforce the combined pending and replay operation limit.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializeSnapshotRecoveryRequestRejectsCombinedPendingAndReplayOperationCount()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumPendingOperations = SnapshotRecoveryRejectedPendingOperationLimit },
        });
        var request = CreateSnapshotRecoveryCodecRequest([CreateSnapshotRecoveryOperation()]) with
        {
            ReplayOperations = [CreateSecondSnapshotRecoveryOperation()],
        };

        var exception = CaptureHttpException(() => codec.SerializeSnapshotRecoveryRequest(request));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies older snapshot recovery request bodies without replay operations decode as empty replay state.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestDefaultsOmittedReplayOperationsToEmpty()
    {
        var codec = CreateCodec();
        var json = SnapshotRecoveryRequestJson(SnapshotRecoveryOperationJson(SnapshotFirstOperationIdText, sequence: 1));

        var decoded = codec.DeserializeSnapshotRecoveryRequest(Encode(json));

        await Assert.That(decoded.PendingOperations).Count().IsEqualTo(1);
        await Assert.That(decoded.ReplayOperations).IsEmpty();
    }

    /// <summary>Verifies present null replay operations are rejected before DTO materialization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestRejectsNullReplayOperationsBeforeMaterialization()
    {
        var codec = CreateCodec();
        var json = ReplaceRequiredSnapshotJsonFragment(
            SnapshotRecoveryRequestJson(SnapshotRecoveryOperationJson(SnapshotFirstOperationIdText, sequence: 1)),
            SnapshotRecoveryMaximumResponseBytesJson,
            $"\"replayOperations\":null,{SnapshotRecoveryMaximumResponseBytesJson}");

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies present non-array replay operations are rejected before DTO materialization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestRejectsNonArrayReplayOperationsBeforeMaterialization()
    {
        var codec = CreateCodec();
        var json = ReplaceRequiredSnapshotJsonFragment(
            SnapshotRecoveryRequestJson(SnapshotRecoveryOperationJson(SnapshotFirstOperationIdText, sequence: 1)),
            SnapshotRecoveryMaximumResponseBytesJson,
            $"\"replayOperations\":{{}},{SnapshotRecoveryMaximumResponseBytesJson}");

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies pending and replay operation counts share one pre-materialization limit.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestRejectsCombinedPendingAndReplayOperationCountBeforeMaterialization()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumPendingOperations = SnapshotRecoveryRejectedPendingOperationLimit },
        });
        var json = SnapshotRecoveryRequestJson(
            SnapshotRecoveryOperationJson(SnapshotFirstOperationIdText, sequence: 1),
            replayOperations: SnapshotRecoveryOperationJson(SnapshotSecondOperationIdText, SnapshotRecoverySecondSequence, "****"),
            extraJson: string.Empty);

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies combined pending and replay operation bytes are bounded before malformed replay payload materialization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestRejectsCombinedPendingAndReplayLogicalBytesBeforeMalformedPayload()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumLogicalBytes = SnapshotRecoveryPendingOnlyLogicalBytes },
        });
        var pendingOnlyJson = ReplaceRequiredSnapshotJsonFragment(
            SnapshotRecoveryRequestJson(SnapshotRecoveryOperationJson(SnapshotFirstOperationIdText, sequence: 1)),
            SnapshotRecoveryMaximumResponseBytesJson,
            $"\"maximumResponseBytes\":{SnapshotRecoveryPendingOnlyLogicalBytes.ToString(CultureInfo.InvariantCulture)}");
        var replayJson = ReplaceRequiredSnapshotJsonFragment(
            SnapshotRecoveryRequestJson(
                SnapshotRecoveryOperationJson(SnapshotFirstOperationIdText, sequence: 1),
                replayOperations: SnapshotRecoveryOperationJson(SnapshotSecondOperationIdText, SnapshotRecoverySecondSequence, "****"),
                extraJson: string.Empty),
            SnapshotRecoveryMaximumResponseBytesJson,
            $"\"maximumResponseBytes\":{SnapshotRecoveryPendingOnlyLogicalBytes.ToString(CultureInfo.InvariantCulture)}");

        var decodedPendingOnly = codec.DeserializeSnapshotRecoveryRequest(Encode(pendingOnlyJson));
        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(replayJson)));

        await Assert.That(decodedPendingOnly.PendingOperations).Count().IsEqualTo(1);
        await Assert.That(decodedPendingOnly.ReplayOperations).IsEmpty();
        await Assert.That(decodedPendingOnly.MaximumResponseBytes).IsEqualTo(SnapshotRecoveryPendingOnlyLogicalBytes);
        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies recovered responses bind dispositions to pending and replay operations.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected checkpoint is absent.</exception>
    [Test]
    public async Task SnapshotRecoveryResponseBindsPendingAndReplayDispositions()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumPendingOperations = SnapshotRecoveryRoundTripPendingOperationLimit },
        });
        var request = CreateSnapshotRecoveryCodecRequest([CreateSnapshotRecoveryOperation()]) with
        {
            ReplayOperations = [CreateSecondSnapshotRecoveryOperation()],
        };
        var response = CreateSnapshotRecoveryCodecResult(request);

        var bytes = codec.SerializeSnapshotRecoveryResponse(request, response);
        var decoded = codec.DeserializeSnapshotRecoveryResponse(request, bytes);

        await Assert.That(decoded.OperationDispositions).Count().IsEqualTo(SnapshotExpectedTwoDispositionCount);
        await Assert.That(decoded.OperationDispositions[0].OperationId).IsEqualTo(request.PendingOperations[0].OperationId);
        await Assert.That(decoded.OperationDispositions[1].OperationId).IsEqualTo(request.ReplayOperations[0].OperationId);
        await Assert.That(decoded.OperationDispositions[1].Result?.Kind).IsEqualTo(OperationResultKind.Accepted);
    }

    /// <summary>Verifies recovered checkpoint responses preserve dispositions and checkpoint binding.</summary>
    /// <returns>The asynchronous test operation.</returns>
    /// <exception cref="InvalidOperationException">The expected checkpoint is absent.</exception>
    [Test]
    public async Task SnapshotRecoveryResponseRoundTripsRecoveredCheckpointAndDispositions()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumPendingOperations = SnapshotRecoveryRoundTripPendingOperationLimit },
        });
        var request = CreateSnapshotRecoveryCodecRequest([CreateSnapshotRecoveryOperation()]);
        var response = CreateSnapshotRecoveryCodecResult(request);

        var bytes = codec.SerializeSnapshotRecoveryResponse(request, response);
        var decoded = codec.DeserializeSnapshotRecoveryResponse(request, bytes);

        await Assert.That(decoded.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.Recovered);
        await Assert.That(decoded.Checkpoint).IsNotNull();
        await Assert.That(response.Checkpoint).IsNotNull();
        var checkpoint = decoded.Checkpoint ?? throw new InvalidOperationException("Expected decoded snapshot checkpoint.");
        var expectedCheckpoint = response.Checkpoint ?? throw new InvalidOperationException("Expected expected snapshot checkpoint.");
        await Assert.That(checkpoint.SubscriptionId).IsEqualTo(request.SubscriptionId);
        await Assert.That(checkpoint.ClientState.Payload.ToArray().SequenceEqual(
            expectedCheckpoint.ClientState.Payload.ToArray())).IsTrue();
        await Assert.That(decoded.OperationDispositions).Count().IsEqualTo(1);
        await Assert.That(decoded.OperationDispositions[0].OperationId).IsEqualTo(request.PendingOperations[0].OperationId);
        await Assert.That(decoded.OperationDispositions[0].Result?.Kind).IsEqualTo(OperationResultKind.Accepted);
    }

    /// <summary>Verifies non-recovered snapshot response statuses round-trip without checkpoint materialization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryResponseReadsNonRecoveredStatuses()
    {
        var codec = CreateCodec();
        var request = CreateSnapshotRecoveryCodecRequest([]);
        (int Wire, RemoteSnapshotRecoveryStatus Expected)[] cases =
        [
            (SnapshotUnsupportedProjectionStatus, RemoteSnapshotRecoveryStatus.UnsupportedProjection),
            (SnapshotRetentionExpiredStatus, RemoteSnapshotRecoveryStatus.RetentionExpired),
            (SnapshotAmbiguousPendingOperationStatus, RemoteSnapshotRecoveryStatus.AmbiguousPendingOperation),
            (SnapshotValidationRejectedStatus, RemoteSnapshotRecoveryStatus.ValidationRejected),
            (SnapshotCapacityExceededStatus, RemoteSnapshotRecoveryStatus.CapacityExceeded),
            (SnapshotRetryableConcurrentChangeStatus, RemoteSnapshotRecoveryStatus.RetryableConcurrentChange),
        ];

        foreach (var item in cases)
        {
            var json = SnapshotRecoveryNonRecoveredResponseJson(item.Wire);

            var decoded = codec.DeserializeSnapshotRecoveryResponse(request, Encode(json));

            await Assert.That(decoded.Status).IsEqualTo(item.Expected);
            await Assert.That(decoded.Checkpoint).IsNull();
            await Assert.That(decoded.OperationDispositions).IsEmpty();
        }
    }

    /// <summary>Verifies terminal and unknown snapshot disposition kinds round-trip from recovered responses.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryResponseReadsTerminalAndUnknownDispositionKinds()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumPendingOperations = SnapshotRecoveryRoundTripPendingOperationLimit },
        });
        var request = CreateSnapshotRecoveryCodecRequest([CreateSnapshotRecoveryOperation(), CreateSecondSnapshotRecoveryOperation()]);
        var json = SnapshotRecoveryResponseJson("e30=")
            .Replace(
                SnapshotEmptyOperationDispositionsJson,
                SnapshotOperationDispositionsArrayStartJson
                + $"{{\"operationId\":\"{SnapshotFirstOperationIdText}\",\"kind\":{SnapshotTerminalRejectedDisposition.ToString(CultureInfo.InvariantCulture)},"
                + "\"result\":{"
                + $"\"operationId\":\"{SnapshotFirstOperationIdText}\","
                + $"\"kind\":{SnapshotRejectedResult.ToString(CultureInfo.InvariantCulture)},"
                + "\"reasonCode\":\"terminal\","
                + $"\"serverVersion\":\"{SnapshotServerVersion}\"}}}},"
                + $"{{\"operationId\":\"{SnapshotSecondOperationIdText}\",\"kind\":{SnapshotUnknownDisposition.ToString(CultureInfo.InvariantCulture)}}}]");

        var decoded = codec.DeserializeSnapshotRecoveryResponse(request, Encode(json));

        await Assert.That(decoded.OperationDispositions).Count().IsEqualTo(SnapshotExpectedTwoDispositionCount);
        await Assert.That(decoded.OperationDispositions[0].Kind).IsEqualTo(SnapshotOperationDispositionKind.TerminalRejected);
        await Assert.That(decoded.OperationDispositions[0].Result).IsNotNull();
        await Assert.That(decoded.OperationDispositions[0].Result?.Kind).IsEqualTo(OperationResultKind.Rejected);
        await Assert.That(decoded.OperationDispositions[0].Result?.ReasonCode).IsEqualTo("terminal");
        await Assert.That(decoded.OperationDispositions[0].Result?.ServerVersion).IsEqualTo(SnapshotServerVersion);
        await Assert.That(decoded.OperationDispositions[1].Kind).IsEqualTo(SnapshotOperationDispositionKind.Unknown);
        await Assert.That(decoded.OperationDispositions[1].Result).IsNull();
    }

    /// <summary>Verifies operation base versions are counted and decoded during request preflight.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestReadsOperationBaseVersionBeforeMaterialization()
    {
        var codec = CreateCodec();
        var operation = ReplaceRequiredSnapshotJsonFragment(
            SnapshotRecoveryOperationJson(SnapshotFirstOperationIdText, 1),
            ",\"type\":0,\"payload\":",
            ",\"type\":0,\"baseVersion\":\"base-v1\",\"payload\":");
        var json = SnapshotRecoveryRequestJson(operation);

        var decoded = codec.DeserializeSnapshotRecoveryRequest(Encode(json));

        await Assert.That(decoded.PendingOperations[0].BaseVersion).IsEqualTo("base-v1");
    }

    /// <summary>Verifies pending operation counts are rejected before unbounded request materialization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestRejectsOversizedPendingOperationsBeforeMaterialization()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumPendingOperations = SnapshotRecoveryRejectedPendingOperationLimit },
        });
        var first = SnapshotRecoveryOperationJson(SnapshotFirstOperationIdText, 1);
        var second = SnapshotRecoveryOperationJson(SnapshotSecondOperationIdText, SnapshotRecoverySecondSequence, "not-base64");
        var json = SnapshotRecoveryRequestJson($"{first},{second}");

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies excessive response budgets are rejected before request materialization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestRejectsMaximumResponseAboveRecoveryLimitBeforeMaterialization()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumLogicalBytes = SnapshotRecoveryMaximumResponseBytes },
        });
        var json = SnapshotRecoveryRequestJson(string.Empty)
            .Replace(
                $"\"maximumResponseBytes\":{SnapshotRecoveryMaximumResponseBytes.ToString(CultureInfo.InvariantCulture)}",
                $"\"maximumResponseBytes\":{SnapshotOversizedMaximumResponseBytes.ToString(CultureInfo.InvariantCulture)}");

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies malformed payload base64 length is rejected before pending operation materialization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestRejectsMalformedPayloadLengthBeforeMaterialization()
    {
        var codec = CreateCodec();
        var operation = SnapshotRecoveryOperationJson(SnapshotFirstOperationIdText, 1, "A");
        var json = SnapshotRecoveryRequestJson(operation);

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies malformed payload base64 characters are rejected before pending operation materialization.</summary>
    /// <param name="payload">The malformed payload text.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments("AA=A")]
    [Arguments("A===")]
    [Arguments("****")]
    [Arguments("AA-A")]
    [Arguments("AA@A")]
    [Arguments("AA[A")]
    [Arguments("AA`A")]
    [Arguments("AA:A")]
    [Arguments("AA{A")]
    public async Task DeserializeSnapshotRecoveryRequestRejectsMalformedPayloadCharactersBeforeMaterialization(string payload)
    {
        var codec = CreateCodec();
        var operation = SnapshotRecoveryOperationJson(SnapshotFirstOperationIdText, 1, payload);
        var json = SnapshotRecoveryRequestJson(operation);

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies valid base64 alphabet groups decode before pending operation materialization.</summary>
    /// <param name="payload">The valid payload text.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments("QUJD")]
    [Arguments("YWJj")]
    [Arguments("MDEy")]
    [Arguments("+///")]
    public async Task DeserializeSnapshotRecoveryRequestAcceptsValidPayloadAlphabetBeforeMaterialization(string payload)
    {
        var codec = CreateCodec();
        var operation = SnapshotRecoveryOperationJson(SnapshotFirstOperationIdText, 1, payload);
        var json = SnapshotRecoveryRequestJson(operation);

        var decoded = codec.DeserializeSnapshotRecoveryRequest(Encode(json));
        var expectedPayload = Convert.FromBase64String(payload);

        await Assert.That(decoded.PendingOperations.Count).IsEqualTo(1);

        var actualPayload = decoded.PendingOperations[0].Payload.Payload.ToArray();

        await Assert.That(actualPayload.SequenceEqual(expectedPayload)).IsTrue();
    }

    /// <summary>Verifies non-object operation metadata is rejected before dictionary materialization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestRejectsNonObjectMetadataBeforeMaterialization()
    {
        var codec = CreateCodec();
        var operation = SnapshotRecoveryOperationJson(SnapshotFirstOperationIdText, 1, "e30=", "[]");
        var json = SnapshotRecoveryRequestJson(operation);

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies duplicate operation metadata properties are rejected before dictionary materialization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestRejectsDuplicateMetadataBeforeMaterialization()
    {
        var codec = CreateCodec();
        var operation = SnapshotRecoveryOperationJson(
            SnapshotFirstOperationIdText,
            1,
            "e30=",
            $"{{\"{SnapshotMetadataTraceKey}\":\"first\",\"{SnapshotMetadataTraceKey}\":\"second\"}}");
        var json = SnapshotRecoveryRequestJson(operation);

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies operation metadata entry count is bounded before dictionary materialization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestRejectsTooManyMetadataEntriesBeforeMaterialization()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumMetadataEntries = 1 },
        });
        var operation = SnapshotRecoveryOperationJson(
            SnapshotFirstOperationIdText,
            1,
            "e30=",
            "{\"trace\":\"snapshot\",\"second\":\"entry\"}");
        var json = SnapshotRecoveryRequestJson(operation);

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies response payload bytes are bounded before returning a recovered result.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryResponseRejectsPayloadAboveRecoveryLimit()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumPayloadBytes = 1 },
        });
        var request = CreateSnapshotRecoveryCodecRequest([]);
        var json = SnapshotRecoveryResponseJson(payload: "AAA=");

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryResponse(request, Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies response disposition counts are bounded before disposition materialization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryResponseRejectsTooManyDispositionsBeforeMaterialization()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumPendingOperations = SnapshotRecoveryRejectedPendingOperationLimit },
        });
        var request = CreateSnapshotRecoveryCodecRequest([CreateSnapshotRecoveryOperation()]);
        var json = ReplaceRequiredSnapshotJsonFragment(
            SnapshotRecoveryResponseJson("e30="),
            SnapshotEmptyOperationDispositionsJson,
                SnapshotOperationDispositionsArrayStartJson
            + $"{{\"operationId\":\"{SnapshotFirstOperationIdText}\",\"kind\":{SnapshotTerminalRejectedDisposition.ToString(CultureInfo.InvariantCulture)}}},"
            + $"{{\"operationId\":\"{SnapshotSecondOperationIdText}\",\"kind\":{SnapshotInvalidDisposition.ToString(CultureInfo.InvariantCulture)}}}]");

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryResponse(request, Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies request payload bytes are bounded before pending operations are materialized.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestRejectsPayloadAboveRecoveryLimitBeforeMaterialization()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumPayloadBytes = 1 },
        });
        var operation = SnapshotRecoveryOperationJson(SnapshotFirstOperationIdText, 1, "AAA=");
        var json = SnapshotRecoveryRequestJson(operation);

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies empty request metadata still pays the dictionary count header before materialization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestRejectsEmptyMetadataWhenHeaderExceedsLimitBeforeMaterialization()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumMetadataBytes = SnapshotRecoveryBelowMetadataHeaderBytes },
        });
        var operation = SnapshotRecoveryOperationJson(
            SnapshotFirstOperationIdText,
            1,
            "e30=",
            "{}");
        var json = SnapshotRecoveryRequestJson(operation);

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies empty domain metadata still pays the dictionary count header before serialization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializeSnapshotRecoveryRequestRejectsEmptyMetadataWhenHeaderExceedsLimit()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumMetadataBytes = SnapshotRecoveryBelowMetadataHeaderBytes },
        });
        var operation = CreateSnapshotRecoveryOperation() with
        {
            Metadata = new Dictionary<string, string>(),
        };
        var request = CreateSnapshotRecoveryCodecRequest([operation]);

        var exception = CaptureHttpException(() => codec.SerializeSnapshotRecoveryRequest(request));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies malformed UTF-16 is reported as a sanitized protocol violation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializeSnapshotRecoveryRequestRejectsMalformedUtf16AsProtocolViolation()
    {
        var codec = CreateCodec();
        var operation = CreateSnapshotRecoveryOperation() with
        {
            Metadata = new Dictionary<string, string> { [SnapshotMetadataTraceKey] = "\ud800" },
        };
        var request = CreateSnapshotRecoveryCodecRequest([operation]);

        var exception = CaptureHttpException(() => codec.SerializeSnapshotRecoveryRequest(request));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies malformed domain request members are mapped to sanitized protocol failures.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializeSnapshotRecoveryRequestRejectsMalformedDomainMemberAsSanitizedProtocolViolation()
    {
        var codec = CreateCodec();
        IReadOnlyList<SyncOperation> pending = new SyncOperation[1];
        var request = CreateSnapshotRecoveryCodecRequest(pending);

        var exception = CaptureHttpException(() => codec.SerializeSnapshotRecoveryRequest(request));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(exception.InnerException).IsNull();
    }

    /// <summary>Verifies malformed domain response members are mapped to sanitized response-binding failures.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializeSnapshotRecoveryResponseRejectsMalformedDomainMemberAsSanitizedValidationRejected()
    {
        var codec = CreateCodec();
        var request = CreateSnapshotRecoveryCodecRequest([CreateSnapshotRecoveryOperation()]);
        IReadOnlyList<SnapshotOperationDisposition> dispositions = new SnapshotOperationDisposition[1];
        var result = CreateSnapshotRecoveryCodecResult(request) with
        {
            OperationDispositions = dispositions,
        };

        var exception = CaptureHttpException(() => codec.SerializeSnapshotRecoveryResponse(request, result));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
        await Assert.That(exception.InnerException).IsNull();
    }

    /// <summary>Verifies response logical bytes are bounded by the original request before response materialization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryResponseRejectsLogicalBytesAboveRequestLimitBeforeMaterialization()
    {
        var codec = CreateCodec();
        var request = CreateSnapshotRecoveryCodecRequest([]) with { MaximumResponseBytes = 1 };
        var json = SnapshotRecoveryResponseJson(payload: "e30=");

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryResponse(request, Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies duplicate JSON names are rejected for snapshot recovery bodies.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRejectsDuplicateJsonProperties()
    {
        var codec = CreateCodec();
        var json = SnapshotRecoveryRequestJson(string.Empty, extraJson: ",\"streamId\":\"stream-2\"");

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies malformed escaped UTF-16 in a required JSON string is sanitized.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestRejectsRequiredLoneSurrogateAsSanitizedProtocolViolation()
    {
        var codec = CreateCodec();
        var json = SnapshotRecoveryRequestJson(string.Empty)
            .Replace($"\"streamId\":\"{StreamName}\"", $"\"streamId\":\"{SnapshotMalformedUnicodeEscape}\"");

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(exception.InnerException).IsNull();
    }

    /// <summary>Verifies malformed escaped UTF-16 in JSON metadata is sanitized.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestRejectsMetadataLoneSurrogateAsSanitizedProtocolViolation()
    {
        var codec = CreateCodec();
        var operation = SnapshotRecoveryOperationJson(
            SnapshotFirstOperationIdText,
            1,
            "e30=",
            $"{{\"{SnapshotMetadataTraceKey}\":\"{SnapshotMalformedUnicodeEscape}\"}}");
        var json = SnapshotRecoveryRequestJson(operation);

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(exception.InnerException).IsNull();
    }

    /// <summary>Verifies malformed escaped UTF-16 in an optional request JSON string is sanitized.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestRejectsOptionalLoneSurrogateAsSanitizedProtocolViolation()
    {
        var codec = CreateCodec();
        var json = SnapshotRecoveryRequestJson(string.Empty)
            .Replace(
                $"\"expiredCursor\":\"{SnapshotExpiredCursor}\"",
                $"\"expiredCursor\":\"{SnapshotMalformedUnicodeEscape}\"");

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(exception.InnerException).IsNull();
    }

    /// <summary>Verifies malformed escaped UTF-16 in a metadata property name is sanitized.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestRejectsMetadataNameLoneSurrogateAsSanitizedProtocolViolation()
    {
        var codec = CreateCodec();
        var operation = SnapshotRecoveryOperationJson(
            SnapshotFirstOperationIdText,
            1,
            "e30=",
            $"{{\"{SnapshotMalformedUnicodeEscape}\":\"snapshot\"}}");
        var json = SnapshotRecoveryRequestJson(operation);

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(exception.InnerException).IsNull();
    }

    /// <summary>Verifies malformed escaped UTF-16 in an optional response JSON string is sanitized.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryResponseRejectsOptionalLoneSurrogateAsSanitizedProtocolViolation()
    {
        var codec = CreateCodec();
        var request = CreateSnapshotRecoveryCodecRequest([]);
        const string json = $"{{\"status\":1,\"operationDispositions\":[],\"reasonCode\":\"{SnapshotMalformedUnicodeEscape}\"}}";

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryResponse(request, Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(exception.InnerException).IsNull();
    }

    /// <summary>Verifies malformed escaped UTF-16 in a required response JSON string is sanitized.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryResponseRejectsRequiredLoneSurrogateAsSanitizedProtocolViolation()
    {
        var codec = CreateCodec();
        var request = CreateSnapshotRecoveryCodecRequest([]);
        var json = SnapshotRecoveryResponseJson("e30=")
            .Replace(
                $"\"frontierCursor\":\"{SnapshotFrontierCursor}\"",
                $"\"frontierCursor\":\"{SnapshotMalformedUnicodeEscape}\"");

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryResponse(request, Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(exception.InnerException).IsNull();
    }

    /// <summary>Verifies malformed escaped UTF-16 in a response property name is sanitized.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryResponseRejectsPropertyNameLoneSurrogateAsSanitizedProtocolViolation()
    {
        var codec = CreateCodec();
        var request = CreateSnapshotRecoveryCodecRequest([]);
        var json = SnapshotRecoveryResponseJson("e30=")
            .Replace(
                "{\"status\":0",
                $"{{\"{SnapshotMalformedUnicodeEscape}\":0,\"status\":0");

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryResponse(request, Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(exception.InnerException).IsNull();
    }

    /// <summary>Verifies optional response reason codes can be JSON null.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryResponseAcceptsNullOptionalReasonCode()
    {
        var codec = CreateCodec();
        var request = CreateSnapshotRecoveryCodecRequest([]);
        var json = $"{{\"status\":{SnapshotRetentionExpiredStatus.ToString(CultureInfo.InvariantCulture)},\"operationDispositions\":[],\"reasonCode\":null}}";

        var decoded = codec.DeserializeSnapshotRecoveryResponse(request, Encode(json));

        await Assert.That(decoded.Status).IsEqualTo(RemoteSnapshotRecoveryStatus.RetentionExpired);
        await Assert.That(decoded.ReasonCode).IsNull();
    }

    /// <summary>Verifies required checkpoint strings reject JSON null before response binding validation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryResponseRejectsNullCheckpointStreamIdAsProtocolViolation()
    {
        var codec = CreateCodec();
        var request = CreateSnapshotRecoveryCodecRequest([]);
        var json = ReplaceRequiredSnapshotJsonFragment(
            SnapshotRecoveryResponseJson("e30="),
            $"\"streamId\":\"{StreamName}\"",
            "\"streamId\":null");

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryResponse(request, Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies unknown snapshot statuses are rejected as sanitized protocol violations.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryResponseRejectsUnknownStatusAsProtocolViolation()
    {
        var codec = CreateCodec();
        var request = CreateSnapshotRecoveryCodecRequest([]);
        var json = SnapshotRecoveryNonRecoveredResponseJson(SnapshotUnknownStatus);

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryResponse(request, Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies unknown snapshot disposition kinds are rejected as sanitized protocol violations.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryResponseRejectsUnknownDispositionKindAsProtocolViolation()
    {
        var codec = CreateCodec();
        var request = CreateSnapshotRecoveryCodecRequest([CreateSnapshotRecoveryOperation()]);
        var json = SnapshotRecoveryResponseJson("e30=")
            .Replace(
                SnapshotEmptyOperationDispositionsJson,
                $"\"operationDispositions\":[{{\"operationId\":\"{SnapshotFirstOperationIdText}\",\"kind\":{SnapshotInvalidDisposition.ToString(CultureInfo.InvariantCulture)}}}]");

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryResponse(request, Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies recovered snapshot responses must bind to the requested stream.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryResponseRejectsCheckpointStreamMismatchAsValidationRejected()
    {
        var codec = CreateCodec();
        var request = CreateSnapshotRecoveryCodecRequest([]);
        var json = ReplaceRequiredSnapshotJsonFragment(
            SnapshotRecoveryResponseJson("e30="),
            $"\"streamId\":\"{StreamName}\"",
            "\"streamId\":\"other-stream\"");

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryResponse(request, Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
    }

    /// <summary>Verifies recovered snapshot responses must bind to the requested snapshot format.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryResponseRejectsCheckpointFormatMismatchAsValidationRejected()
    {
        var codec = CreateCodec();
        var request = CreateSnapshotRecoveryCodecRequest([]);
        var json = ReplaceRequiredSnapshotJsonFragment(
            SnapshotRecoveryResponseJson("e30="),
            "\"snapshotFormatVersion\":1",
            "\"snapshotFormatVersion\":2");

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryResponse(request, Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
    }

    /// <summary>Verifies non-recovered snapshot responses cannot carry recovered checkpoint data.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryResponseRejectsNonRecoveredCheckpointAsValidationRejected()
    {
        var codec = CreateCodec();
        var request = CreateSnapshotRecoveryCodecRequest([]);
        var json = SnapshotRecoveryResponseJson("e30=")
            .Replace("\"status\":0", $"\"status\":{SnapshotValidationRejectedStatus.ToString(CultureInfo.InvariantCulture)}");

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryResponse(request, Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
    }

    /// <summary>Verifies recovered snapshot responses must carry a checkpoint.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryResponseRejectsRecoveredWithoutCheckpointAsValidationRejected()
    {
        var codec = CreateCodec();
        var request = CreateSnapshotRecoveryCodecRequest([]);
        const string json = "{\"status\":0,\"operationDispositions\":[]}";

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryResponse(request, Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
    }

    /// <summary>Verifies response disposition count mismatches are rejected as response binding failures.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryResponseRejectsDispositionCountMismatchAsValidationRejected()
    {
        var codec = CreateCodec();
        var request = CreateSnapshotRecoveryCodecRequest([CreateSnapshotRecoveryOperation()]);
        var json = SnapshotRecoveryResponseJson("e30=")
            .Replace(
                SnapshotEmptyOperationDispositionsJson,
                SnapshotOperationDispositionsArrayStartJson
                + $"{{\"operationId\":\"{SnapshotFirstOperationIdText}\",\"kind\":{SnapshotTerminalRejectedDisposition.ToString(CultureInfo.InvariantCulture)},"
                + "\"result\":{"
                + $"\"operationId\":\"{SnapshotFirstOperationIdText}\","
                + $"\"kind\":{SnapshotRejectedResult.ToString(CultureInfo.InvariantCulture)},"
                + "\"reasonCode\":\"terminal\","
                + $"\"serverVersion\":\"{SnapshotServerVersion}\"}}}},"
                + $"{{\"operationId\":\"{SnapshotSecondOperationIdText}\",\"kind\":{SnapshotUnknownDisposition.ToString(CultureInfo.InvariantCulture)}}}]");

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryResponse(request, Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ValidationRejected);
    }
}
