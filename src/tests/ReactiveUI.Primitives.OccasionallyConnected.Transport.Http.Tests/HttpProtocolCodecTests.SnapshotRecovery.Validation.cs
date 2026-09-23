// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Snapshot recovery validation tests for <see cref="HttpProtocolCodec"/>.</summary>
public sealed partial class HttpProtocolCodecTests
{
    /// <summary>An invalid snapshot GUID wire value.</summary>
    private const string SnapshotInvalidGuidText = "not-a-guid";

    /// <summary>An invalid snapshot timestamp wire value.</summary>
    private const string SnapshotInvalidTimestampText = "not-a-timestamp";

    /// <summary>An Int32 overflow snapshot wire value.</summary>
    private const string SnapshotInt32OverflowText = "2147483648";

    /// <summary>An Int64 overflow snapshot wire value.</summary>
    private const string SnapshotInt64OverflowText = "9223372036854775808";

    /// <summary>The first snapshot sequence property fragment.</summary>
    private const string SnapshotFirstSequenceJson = "\"clientSequence\":1";

    /// <summary>The snapshot operation type property fragment.</summary>
    private const string SnapshotAppendTypeJson = "\"type\":0";

    /// <summary>The valid snapshot operation timestamp property fragment.</summary>
    private const string SnapshotOperationTimestampJson = "\"timestampUtc\":\"2026-09-18T00:00:00+00:00\"";

    /// <summary>The null operation dispositions JSON property fragment.</summary>
    private const string SnapshotObjectOperationDispositionsJson = "\"operationDispositions\":{}";

    /// <summary>The object pending operations JSON property fragment.</summary>
    private const string SnapshotObjectPendingOperationsJson = "\"pendingOperations\":{}";

    /// <summary>A stable terminal rejection reason fixture.</summary>
    private const string SnapshotTerminalReasonCode = "terminal";

    /// <summary>A stable non-recovered reason fixture.</summary>
    private const string SnapshotRetentionReasonCode = "retention-expired";

    /// <summary>The small snapshot limit used by outgoing validation tests.</summary>
    private const int SnapshotTinyValidationLimit = 1;

    /// <summary>Verifies malformed operation identifiers are rejected during bounded preflight conversion.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestRejectsInvalidOperationIdBeforeMaterialization()
    {
        var codec = CreateCodec();
        var operation = SnapshotRecoveryOperationJson(SnapshotInvalidGuidText, 1);
        var json = SnapshotRecoveryRequestJson(operation);

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies malformed operation timestamps are rejected during bounded preflight conversion.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestRejectsInvalidTimestampBeforeMaterialization()
    {
        var codec = CreateCodec();
        var operation = ReplaceRequiredSnapshotJsonFragment(
            SnapshotRecoveryOperationJson(SnapshotFirstOperationIdText, 1),
            SnapshotOperationTimestampJson,
            $"\"timestampUtc\":\"{SnapshotInvalidTimestampText}\"");
        var json = SnapshotRecoveryRequestJson(operation);

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies operation enum numeric overflow is rejected during bounded preflight conversion.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestRejectsOperationTypeOverflowBeforeMaterialization()
    {
        var codec = CreateCodec();
        var operation = ReplaceRequiredSnapshotJsonFragment(
            SnapshotRecoveryOperationJson(SnapshotFirstOperationIdText, 1),
            SnapshotAppendTypeJson,
            $"\"type\":{SnapshotInt32OverflowText}");
        var json = SnapshotRecoveryRequestJson(operation);

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies client sequence numeric overflow is rejected during bounded preflight conversion.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestRejectsClientSequenceOverflowBeforeMaterialization()
    {
        var codec = CreateCodec();
        var operation = ReplaceRequiredSnapshotJsonFragment(
            SnapshotRecoveryOperationJson(SnapshotFirstOperationIdText, 1),
            SnapshotFirstSequenceJson,
            $"\"clientSequence\":{SnapshotInt64OverflowText}");
        var json = SnapshotRecoveryRequestJson(operation);

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies request pending operations must be an array before any operation allocation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestRejectsNonArrayPendingOperationsBeforeMaterialization()
    {
        var codec = CreateCodec();
        var json = ReplaceRequiredSnapshotJsonFragment(
            SnapshotRecoveryRequestJson(string.Empty),
            "\"pendingOperations\":[]",
            SnapshotObjectPendingOperationsJson);

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryRequest(Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies response operation dispositions must be an array before any disposition allocation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryResponseRejectsNonArrayDispositionsBeforeMaterialization()
    {
        var codec = CreateCodec();
        var request = CreateSnapshotRecoveryCodecRequest([]);
        var json = ReplaceRequiredSnapshotJsonFragment(
            SnapshotRecoveryResponseJson("e30="),
            SnapshotEmptyOperationDispositionsJson,
            SnapshotObjectOperationDispositionsJson);

        var exception = CaptureHttpException(() => codec.DeserializeSnapshotRecoveryResponse(request, Encode(json)));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies nullable request cursor fields keep the optional-string preflight branch valid.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestAllowsNullExpiredCursor()
    {
        var codec = CreateCodec();
        var json = ReplaceRequiredSnapshotJsonFragment(
            SnapshotRecoveryRequestJson(string.Empty),
            $"\"expiredCursor\":\"{SnapshotExpiredCursor}\"",
            "\"expiredCursor\":null");

        var decoded = codec.DeserializeSnapshotRecoveryRequest(Encode(json));

        await Assert.That(decoded.ExpiredCursor).IsNull();
    }

    /// <summary>Verifies nullable response reason fields keep the optional-string preflight branch valid.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryResponseAllowsNullReasonCode()
    {
        var codec = CreateCodec();
        var request = CreateSnapshotRecoveryCodecRequest([]);
        const string json = "{\"status\":1,\"operationDispositions\":[],\"reasonCode\":null}";

        var decoded = codec.DeserializeSnapshotRecoveryResponse(request, Encode(json));

        await Assert.That(decoded.ReasonCode).IsNull();
    }

    /// <summary>Verifies empty base64 payloads are counted without forcing operation materialization failure.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeserializeSnapshotRecoveryRequestAllowsEmptyPayloadAfterBoundedPreflight()
    {
        var codec = CreateCodec();
        var operation = SnapshotRecoveryOperationJson(SnapshotFirstOperationIdText, 1, string.Empty);
        var json = SnapshotRecoveryRequestJson(operation);

        var decoded = codec.DeserializeSnapshotRecoveryRequest(Encode(json));

        await Assert.That(decoded.PendingOperations[0].Payload.PayloadLength).IsEqualTo(0);
    }

    /// <summary>Verifies outgoing stream identifiers honor the snapshot recovery string byte limit.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializeSnapshotRecoveryRequestRejectsStreamIdAboveRecoveryLimit()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumStreamIdUtf8Bytes = SnapshotTinyValidationLimit },
        });
        var request = CreateSnapshotRecoveryCodecRequest([]) with { StreamId = new("ab") };

        var exception = CaptureHttpException(() => codec.SerializeSnapshotRecoveryRequest(request));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies outgoing response byte budgets honor the configured snapshot recovery logical limit.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializeSnapshotRecoveryRequestRejectsMaximumResponseAboveRecoveryLimit()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumLogicalBytes = SnapshotRecoveryMaximumResponseBytes },
        });
        var request = CreateSnapshotRecoveryCodecRequest([]) with { MaximumResponseBytes = SnapshotOversizedMaximumResponseBytes };

        var exception = CaptureHttpException(() => codec.SerializeSnapshotRecoveryRequest(request));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies outgoing pending operation counts honor the snapshot recovery operation limit.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializeSnapshotRecoveryRequestRejectsTooManyPendingOperations()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumPendingOperations = SnapshotRecoveryRejectedPendingOperationLimit },
        });
        var request = CreateSnapshotRecoveryCodecRequest(
            [CreateSnapshotRecoveryOperation(), CreateSecondSnapshotRecoveryOperation()]);

        var exception = CaptureHttpException(() => codec.SerializeSnapshotRecoveryRequest(request));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies outgoing pending operation payload bytes honor the snapshot recovery payload limit.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializeSnapshotRecoveryRequestRejectsPayloadAboveRecoveryLimit()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumPayloadBytes = SnapshotTinyValidationLimit },
        });
        var request = CreateSnapshotRecoveryCodecRequest([CreateSnapshotRecoveryOperation()]);

        var exception = CaptureHttpException(() => codec.SerializeSnapshotRecoveryRequest(request));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies outgoing metadata entry counts honor the snapshot recovery metadata limit.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializeSnapshotRecoveryRequestRejectsTooManyMetadataEntries()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumMetadataEntries = SnapshotTinyValidationLimit },
        });
        var operation = CreateSnapshotRecoveryOperation() with
        {
            Metadata = new Dictionary<string, string> { [SnapshotMetadataTraceKey] = "snapshot", ["extra"] = "entry" },
        };
        var request = CreateSnapshotRecoveryCodecRequest([operation]);

        var exception = CaptureHttpException(() => codec.SerializeSnapshotRecoveryRequest(request));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Verifies outgoing non-recovered responses can omit checkpoints and dispositions.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializeSnapshotRecoveryResponseWritesNonRecoveredResultWithoutCheckpointOrDispositions()
    {
        var codec = CreateCodec();
        var request = CreateSnapshotRecoveryCodecRequest([]);
        const RemoteSnapshotRecoveryStatus status = RemoteSnapshotRecoveryStatus.RetentionExpired;
        RemoteSnapshotRecoveryResult result = new() { Status = status, OperationDispositions = [], ReasonCode = SnapshotRetentionReasonCode };

        var bytes = codec.SerializeSnapshotRecoveryResponse(request, result);
        var decoded = codec.DeserializeSnapshotRecoveryResponse(request, bytes);

        await Assert.That(decoded.Status).IsEqualTo(result.Status);
        await Assert.That(decoded.Checkpoint).IsNull();
        await Assert.That(decoded.OperationDispositions).IsEmpty();
        await Assert.That(decoded.ReasonCode).IsEqualTo(SnapshotRetentionReasonCode);
    }

    /// <summary>Verifies outgoing recovered responses preserve terminal and unknown disposition shapes.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializeSnapshotRecoveryResponseWritesTerminalAndUnknownDispositionResults()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumPendingOperations = SnapshotRecoveryRoundTripPendingOperationLimit },
        });
        var first = CreateSnapshotRecoveryOperation();
        var second = CreateSecondSnapshotRecoveryOperation();
        var request = CreateSnapshotRecoveryCodecRequest([first, second]);
        var result = CreateSnapshotRecoveryCodecResult(request) with
        {
            OperationDispositions =
            [
                new()
                {
                    OperationId = first.OperationId,
                    Kind = SnapshotOperationDispositionKind.TerminalRejected,
                    Result = new(first.OperationId, OperationResultKind.Rejected, SnapshotTerminalReasonCode, SnapshotServerVersion),
                },
                new() { OperationId = second.OperationId, Kind = SnapshotOperationDispositionKind.Unknown },
            ],
        };

        var bytes = codec.SerializeSnapshotRecoveryResponse(request, result);
        var decoded = codec.DeserializeSnapshotRecoveryResponse(request, bytes);

        await Assert.That(decoded.OperationDispositions).Count().IsEqualTo(SnapshotExpectedTwoDispositionCount);
        await Assert.That(decoded.OperationDispositions[0].Result?.Kind).IsEqualTo(OperationResultKind.Rejected);
        await Assert.That(decoded.OperationDispositions[0].Result?.ReasonCode).IsEqualTo(SnapshotTerminalReasonCode);
        await Assert.That(decoded.OperationDispositions[1].Kind).IsEqualTo(SnapshotOperationDispositionKind.Unknown);
        await Assert.That(decoded.OperationDispositions[1].Result).IsNull();
    }

    /// <summary>Verifies outgoing response dispositions honor the snapshot recovery operation limit.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SerializeSnapshotRecoveryResponseRejectsTooManyDispositions()
    {
        var codec = CreateCodec(static options => options with
        {
            SnapshotRecoveryLimits = new SnapshotRecoveryLimits { MaximumPendingOperations = SnapshotRecoveryRejectedPendingOperationLimit },
        });
        var request = CreateSnapshotRecoveryCodecRequest([CreateSnapshotRecoveryOperation()]);
        var result = CreateSnapshotRecoveryCodecResult(request) with
        {
            OperationDispositions =
            [
                new()
                {
                    OperationId = request.PendingOperations[0].OperationId,
                    Kind = SnapshotOperationDispositionKind.IncludedAccepted,
                    Result = new(request.PendingOperations[0].OperationId, OperationResultKind.Accepted, null, SnapshotServerVersion),
                },
                new() { OperationId = new(Guid.Parse(SnapshotSecondOperationIdText)), Kind = SnapshotOperationDispositionKind.Unknown },
            ],
        };

        var exception = CaptureHttpException(() => codec.SerializeSnapshotRecoveryResponse(request, result));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.PayloadTooLarge);
    }

    /// <summary>Replaces a JSON fixture fragment and fails fast if the fixture no longer contains it.</summary>
    /// <param name="json">The source JSON.</param>
    /// <param name="oldValue">The expected source fragment.</param>
    /// <param name="newValue">The replacement fragment.</param>
    /// <returns>The updated JSON.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="oldValue"/> was not present.</exception>
    private static string ReplaceRequiredSnapshotJsonFragment(string json, string oldValue, string newValue)
    {
        var firstIndex = json.IndexOf(oldValue, StringComparison.Ordinal);
        if (firstIndex < 0)
        {
            throw new InvalidOperationException("The snapshot recovery JSON fixture did not contain the expected fragment.");
        }

        if (json.IndexOf(oldValue, firstIndex + oldValue.Length, StringComparison.Ordinal) >= 0)
        {
            throw new InvalidOperationException("The snapshot recovery JSON fixture contained the expected fragment more than once.");
        }

        return json.Replace(oldValue, newValue);
    }
}
