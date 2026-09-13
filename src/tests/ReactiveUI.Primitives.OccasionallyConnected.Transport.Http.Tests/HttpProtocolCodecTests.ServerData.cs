// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpProtocolCodec"/>.</summary>
public sealed partial class HttpProtocolCodecTests
{
    /// <summary>Provides malformed server codec cases that must be rejected as HTTP protocol failures.</summary>
    /// <returns>The malformed server codec cases.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IEnumerable<Func<ServerHttpExceptionCase>> ServerHttpExceptionCases() =>
        ConnectExceptionCases().Concat(PushShapeExceptionCases()).Concat(PushValueExceptionCases()).Concat(AcknowledgementExceptionCases());

    /// <summary>Provides malformed connect request cases.</summary>
    /// <returns>The malformed connect cases.</returns>
    public static IEnumerable<Func<ServerHttpExceptionCase>> ConnectExceptionCases()
    {
        yield return ProtocolCase(
            "connect-min-version",
            static codec => codec.DeserializeConnectRequest(Encode(ConnectRequestJson(minimumVersion: InvalidWireText))));
        yield return ProtocolCase(
            "connect-max-version",
            static codec => codec.DeserializeConnectRequest(Encode(ConnectRequestJson(maximumVersion: InvalidWireText))));
        yield return ProtocolCase(
            "connect-version-range",
            static codec => codec.DeserializeConnectRequest(Encode(ConnectRequestJson(minimumVersion: "1.2"))));
        yield return ProtocolCase(
            "connect-empty-client",
            static codec => codec.DeserializeConnectRequest(Encode(ConnectRequestJson(clientId: string.Empty))));
        yield return ProtocolCase(
            "connect-invalid-guarantee",
            static codec => codec.DeserializeConnectRequest(Encode(ConnectRequestJson(requiredGuarantees: "99"))));
        yield return ProtocolCase(
            "connect-guarantee-string",
            static codec => codec.DeserializeConnectRequest(Encode(ConnectRequestJson(requiredGuarantees: "\"0\""))));
        yield return ProtocolCase(
            "connect-tenant-type",
            static codec => codec.DeserializeConnectRequest(Encode(ConnectRequestJson(tenantHintJson: "1"))));
        yield return ProtocolCase(
            "connect-duplicate-client",
            static codec => codec.DeserializeConnectRequest(Encode(ConnectRequestJson(extraJson: ",\"clientId\":\"again\""))));
        yield return ProtocolCase(
            "connect-extra",
            static codec => codec.DeserializeConnectRequest(Encode(ConnectRequestJson(extraJson: ",\"extra\":0"))));
        yield return ProtocolCase(
            "connect-too-many-guarantees",
            static codec => codec.DeserializeConnectRequest(Encode(ConnectRequestJson(requiredGuarantees: "0,1"))),
            HttpTransportFailureKind.PayloadTooLarge,
            static () => new HttpProtocolCodec(CreateServerLimits() with { MaximumBatchOperations = SingleOperation }));
    }

    /// <summary>Provides malformed push request shape cases.</summary>
    /// <returns>The malformed push shape cases.</returns>
    public static IEnumerable<Func<ServerHttpExceptionCase>> PushShapeExceptionCases()
    {
        yield return ProtocolCase(
            "push-request-over-body-limit",
            static codec => codec.DeserializePushRequest(Encode(EmptyJsonPayload)),
            HttpTransportFailureKind.PayloadTooLarge,
            static () => new HttpProtocolCodec(CreateServerLimits() with { MaximumRequestBytes = SingleOperation }));
        yield return ProtocolCase(
            "push-batchid-format",
            static codec => codec.DeserializePushRequest(Encode(PushRequestJson(InvalidWireText, OperationJson()))));
        yield return ProtocolCase(
            "push-batchid-empty",
            static codec => codec.DeserializePushRequest(Encode(PushRequestJson(EmptyGuidText, OperationJson()))));
        yield return ProtocolCase(
            "push-empty-operations",
            static codec => codec.DeserializePushRequest(Encode(PushRequestJson(ServerBatchIdText, string.Empty))));
        yield return ProtocolCase(
            "push-too-many-operations",
            static codec => codec.DeserializePushRequest(Encode(PushRequestJson(
                ServerBatchIdText,
                OperationJson(),
                OperationJson(new() { OperationId = SecondOperationIdText, Sequence = SecondSequence })))),
            HttpTransportFailureKind.PayloadTooLarge,
            static () => new HttpProtocolCodec(CreateServerLimits() with { MaximumBatchOperations = SingleOperation }));
        yield return ProtocolCase(
            "push-duplicate-operation",
            static codec => codec.DeserializePushRequest(Encode(PushRequestJson(
                ServerBatchIdText,
                OperationJson(),
                OperationJson(new() { Sequence = SecondSequence })))));
        yield return ProtocolCase(
            "push-duplicate-sequence",
            static codec => codec.DeserializePushRequest(Encode(PushRequestJson(
                ServerBatchIdText,
                OperationJson(),
                OperationJson(new() { OperationId = SecondOperationIdText })))));
        yield return ProtocolCase(
            "push-backward-sequence",
            static codec => codec.DeserializePushRequest(Encode(PushRequestJson(
                ServerBatchIdText,
                OperationJson(new() { Sequence = SecondSequence }),
                OperationJson(new() { OperationId = SecondOperationIdText })))));
        yield return ProtocolCase(
            "push-mixed-stream",
            static codec => codec.DeserializePushRequest(Encode(PushRequestJson(
                ServerBatchIdText,
                OperationJson(),
                OperationJson(new() { OperationId = SecondOperationIdText, Sequence = SecondSequence, StreamId = AlternateServerStreamName })))));
        yield return ProtocolCase(
            "push-invalid-stream-grammar",
            static codec => codec.DeserializePushRequest(Encode(PushRequestJson(
                ServerBatchIdText,
                OperationJson(new() { StreamId = InvalidStreamName })))));
    }

    /// <summary>Provides malformed push request value cases.</summary>
    /// <returns>The malformed push value cases.</returns>
    public static IEnumerable<Func<ServerHttpExceptionCase>> PushValueExceptionCases()
    {
        yield return ProtocolCase(
            "push-invalid-type",
            static codec => codec.DeserializePushRequest(Encode(PushRequestJson(
                ServerBatchIdText,
                OperationJson(new() { Type = InvalidEnumValue })))));
        yield return ProtocolCase(
            "push-invalid-guarantee",
            static codec => codec.DeserializePushRequest(Encode(PushRequestJson(
                ServerBatchIdText,
                OperationJson(new() { DeliveryGuaranteeValue = InvalidEnumValue })))));
        yield return ProtocolCase(
            "push-invalid-durability",
            static codec => codec.DeserializePushRequest(Encode(PushRequestJson(
                ServerBatchIdText,
                OperationJson(new() { Durability = InvalidEnumValue })))));
        yield return ProtocolCase(
            "push-invalid-conflict",
            static codec => codec.DeserializePushRequest(Encode(PushRequestJson(
                ServerBatchIdText,
                OperationJson(new() { ConflictPolicyValue = InvalidEnumValue })))));
        yield return ProtocolCase(
            "push-empty-contract",
            static codec => codec.DeserializePushRequest(Encode(PushRequestJson(
                ServerBatchIdText,
                OperationJson(new() { ContractId = string.Empty })))));
        yield return ProtocolCase(
            "push-zero-schema",
            static codec => codec.DeserializePushRequest(Encode(PushRequestJson(
                ServerBatchIdText,
                OperationJson(new() { SchemaVersion = 0 })))));
        yield return ProtocolCase(
            "push-bad-payload",
            static codec => codec.DeserializePushRequest(Encode(PushRequestJson(
                ServerBatchIdText,
                OperationJson(new() { Payload = "not-base64" })))));
        yield return ProtocolCase(
            "push-metadata-array",
            static codec => codec.DeserializePushRequest(Encode(PushRequestJson(
                ServerBatchIdText,
                OperationJson(new() { Metadata = "[]" })))));
        yield return ProtocolCase(
            "push-metadata-null-value",
            static codec => codec.DeserializePushRequest(Encode(PushRequestJson(
                ServerBatchIdText,
                OperationJson(new() { Metadata = "{\"trace\":null}" })))));
        yield return ProtocolCase(
            "push-metadata-duplicate",
            static codec => codec.DeserializePushRequest(Encode(PushRequestJson(
                ServerBatchIdText,
                OperationJson(new() { Metadata = "{\"trace\":\"1\",\"trace\":\"2\"}" })))));
    }

    /// <summary>Provides malformed acknowledgement cases.</summary>
    /// <returns>The malformed acknowledgement cases.</returns>
    public static IEnumerable<Func<ServerHttpExceptionCase>> AcknowledgementExceptionCases()
    {
        yield return ProtocolCase(
            "ack-body-over-limit",
            static codec => codec.DeserializeAcknowledgement(Encode(AcknowledgementJson())),
            HttpTransportFailureKind.PayloadTooLarge,
            static () => new HttpProtocolCodec(CreateServerLimits() with { MaximumRequestBytes = SingleOperation }));
        yield return ProtocolCase(
            "ack-empty-stream",
            static codec => codec.DeserializeAcknowledgement(Encode(AcknowledgementJson(streamId: string.Empty))));
        yield return ProtocolCase(
            "ack-invalid-stream-grammar",
            static codec => codec.DeserializeAcknowledgement(Encode(AcknowledgementJson(streamId: InvalidStreamName))));
        yield return ProtocolCase(
            "ack-empty-cursor",
            static codec => codec.DeserializeAcknowledgement(Encode(AcknowledgementJson(cursor: string.Empty))));
        yield return ProtocolCase(
            "ack-bad-subscription",
            static codec => codec.DeserializeAcknowledgement(Encode(AcknowledgementJson(subscriptionId: InvalidWireText))));
        yield return ProtocolCase(
            "ack-empty-subscription",
            static codec => codec.DeserializeAcknowledgement(Encode(AcknowledgementJson(subscriptionId: EmptyGuidText))));
    }
}
