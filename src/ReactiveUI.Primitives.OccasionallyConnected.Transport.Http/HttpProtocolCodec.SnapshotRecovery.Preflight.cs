// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text.Json;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Preflights snapshot recovery JSON before DTO materialization.</summary>
internal sealed partial class HttpProtocolCodec
{
    /// <summary>The sanitized malformed base64 payload message.</summary>
    private const string SnapshotMalformedBase64PayloadMessage = "Malformed base64 payload.";

    /// <summary>The snapshot server version property name.</summary>
    private const string ServerVersionPropertyName = "serverVersion";

    /// <summary>The snapshot format version property name.</summary>
    private const string SnapshotFormatVersionPropertyName = "snapshotFormatVersion";

    /// <summary>Reads a validated snapshot JSON string with sanitized malformed escape handling.</summary>
    /// <param name="element">The JSON string element.</param>
    /// <returns>The decoded JSON string.</returns>
    /// <exception cref="JsonException"><paramref name="element"/> contains malformed escaped string text.</exception>
    private static string ReadSnapshotStringElement(JsonElement element)
    {
        string? value;
        try
        {
            value = element.GetString();
        }
        catch (InvalidOperationException exception)
        {
            _ = exception;
            throw new JsonException(ExpectedJsonStringMessage);
        }

        return value ?? throw new JsonException(ExpectedJsonStringMessage);
    }

    /// <summary>Counts a required JSON string and enforces its UTF-8 byte limit.</summary>
    /// <param name="element">The JSON string element.</param>
    /// <param name="maximumUtf8Bytes">The byte limit.</param>
    /// <returns>The UTF-8 byte count.</returns>
    private static long CountSnapshotRequiredStringElement(JsonElement element, int maximumUtf8Bytes)
    {
        ValidateStringElement(element);
        return CountSnapshotRequiredString(ReadSnapshotStringElement(element), maximumUtf8Bytes);
    }

    /// <summary>Counts an optional JSON string and enforces its UTF-8 byte limit.</summary>
    /// <param name="element">The JSON string or null element.</param>
    /// <param name="maximumUtf8Bytes">The byte limit.</param>
    /// <returns>The UTF-8 byte count.</returns>
    private static long CountSnapshotOptionalStringElement(JsonElement element, int maximumUtf8Bytes)
    {
        ValidateOptionalStringElement(element);
        return element.ValueKind == JsonValueKind.Null
            ? 0
            : CountSnapshotRequiredString(ReadSnapshotStringElement(element), maximumUtf8Bytes);
    }

    /// <summary>Validates a GUID string element without allocating a domain identifier.</summary>
    /// <param name="element">The JSON string element.</param>
    /// <exception cref="JsonException"><paramref name="element"/> is not a valid GUID string.</exception>
    private static void ValidateSnapshotGuidElement(JsonElement element)
    {
        ValidateStringElement(element);
        if (Guid.TryParse(ReadSnapshotStringElement(element), out _))
        {
            return;
        }

        throw new JsonException(ExpectedJsonStringMessage);
    }

    /// <summary>Validates a DateTimeOffset string element without allocating a domain event.</summary>
    /// <param name="element">The JSON string element.</param>
    /// <exception cref="JsonException"><paramref name="element"/> is not a valid timestamp string.</exception>
    private static void ValidateSnapshotDateTimeOffsetElement(JsonElement element)
    {
        ValidateStringElement(element);
        if (DateTimeOffset.TryParse(
            ReadSnapshotStringElement(element),
            System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.None,
            out _))
        {
            return;
        }

        throw new JsonException(ExpectedJsonStringMessage);
    }

    /// <summary>Reads a required Int32 JSON element.</summary>
    /// <param name="element">The JSON number element.</param>
    /// <returns>The Int32 value.</returns>
    /// <exception cref="JsonException"><paramref name="element"/> is not a valid Int32 number.</exception>
    private static int ReadSnapshotInt32Element(JsonElement element)
    {
        ValidateNumberElement(element);
        return element.TryGetInt32(out var value) ? value : throw new JsonException(ExpectedJsonNumberMessage);
    }

    /// <summary>Reads a required Int64 JSON element.</summary>
    /// <param name="element">The JSON number element.</param>
    /// <returns>The Int64 value.</returns>
    /// <exception cref="JsonException"><paramref name="element"/> is not a valid Int64 number.</exception>
    private static long ReadSnapshotInt64Element(JsonElement element)
    {
        ValidateNumberElement(element);
        return element.TryGetInt64(out var value) ? value : throw new JsonException(ExpectedJsonNumberMessage);
    }

    /// <summary>Counts decoded payload bytes from a base64 JSON string without allocating payload bytes.</summary>
    /// <param name="element">The payload string element.</param>
    /// <returns>The decoded payload byte count.</returns>
    /// <exception cref="JsonException"><paramref name="element"/> is not a valid base64 string.</exception>
    private static int CountSnapshotPayloadBytesElement(JsonElement element)
    {
        ValidateStringElement(element);
        var value = ReadSnapshotStringElement(element);
        if (value.Length == 0)
        {
            return 0;
        }

        if (value.Length % Base64BlockOutputCharacters != 0)
        {
            throw new JsonException(SnapshotMalformedBase64PayloadMessage);
        }

        var padding = 0;
        for (var index = 0; index < value.Length; index++)
        {
            var character = value[index];
            if (character == '=')
            {
                padding++;
                if (index < value.Length - Base64RoundingBytes || padding > Base64RoundingBytes)
                {
                    throw new JsonException(SnapshotMalformedBase64PayloadMessage);
                }

                continue;
            }

            if (padding != 0 || !IsSnapshotBase64Character(character))
            {
                throw new JsonException(SnapshotMalformedBase64PayloadMessage);
            }
        }

        return checked((value.Length / Base64BlockOutputCharacters * Base64BlockInputBytes) - padding);
    }

    /// <summary>Checks whether a character belongs to the base64 alphabet used by JSON payloads.</summary>
    /// <param name="character">The candidate character.</param>
    /// <returns>Whether the character is base64 data.</returns>
    private static bool IsSnapshotBase64Character(char character) =>
        character is >= 'A' and <= 'Z'
        or >= 'a' and <= 'z'
        or >= '0' and <= '9'
        or '+'
        or '/';

    /// <summary>Gets one operation array count before item materialization.</summary>
    /// <param name="element">The operations array.</param>
    /// <returns>The operation count.</returns>
    /// <exception cref="JsonException"><paramref name="element"/> is not an array.</exception>
    private static int GetSnapshotOperationArrayCount(JsonElement element)
    {
        if (element.ValueKind != JsonValueKind.Array)
        {
            throw new JsonException("Expected a JSON array.");
        }

        return element.GetArrayLength();
    }

    /// <summary>Counts a snapshot payload JSON object before payload bytes are materialized.</summary>
    /// <param name="element">The payload object.</param>
    /// <returns>The logical byte count.</returns>
    private long CountSnapshotPayloadElement(JsonElement element)
    {
        var logicalBytes = SnapshotInt32LogicalBytes + SnapshotInt64LogicalBytes;
        ValidateObject(
            element,
            ["contractId", "schemaVersion", "contentType", PayloadPropertyName, "payloadHash"],
            [],
            property =>
            {
                switch (ReadJsonPropertyName(property))
                {
                    case "contractId" or "contentType" or "payloadHash":
                    {
                        logicalBytes = checked(
                            logicalBytes + CountSnapshotRequiredStringElement(
                                property.Value,
                                _snapshotRecoveryLimits.MaximumContractUtf8Bytes));
                        break;
                    }

                    case "schemaVersion":
                    {
                        _ = ReadSnapshotInt32Element(property.Value);
                        break;
                    }

                    case PayloadPropertyName:
                    {
                        var payloadBytes = CountSnapshotPayloadBytesElement(property.Value);
                        if (payloadBytes > _snapshotRecoveryLimits.MaximumPayloadBytes)
                        {
                            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
                        }

                        logicalBytes = checked(logicalBytes + payloadBytes);
                        break;
                    }
                }
            });
        return logicalBytes;
    }

    /// <summary>Counts snapshot metadata JSON before dictionary materialization.</summary>
    /// <param name="element">The metadata object.</param>
    /// <returns>The logical byte count.</returns>
    /// <exception cref="HttpRemoteTransportException"><paramref name="element"/> exceeds configured limits.</exception>
    /// <exception cref="JsonException"><paramref name="element"/> is not a valid metadata object.</exception>
    private long CountSnapshotMetadataElement(JsonElement element)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException("Expected a JSON object.");
        }

        var count = 0;
        var logicalBytes = SnapshotInt32LogicalBytes;
        ThrowIfSnapshotLogicalBytesExceeded(logicalBytes, _snapshotRecoveryLimits.MaximumMetadataBytes);
        HashSet<string> names = [with(comparer: StringComparer.Ordinal)];
        foreach (var property in element.EnumerateObject())
        {
            count++;
            if (count > _snapshotRecoveryLimits.MaximumMetadataEntries)
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
            }

            var propertyName = ReadJsonPropertyName(property);
            if (!names.Add(propertyName))
            {
                throw new JsonException("Duplicate JSON property name.");
            }

            logicalBytes = checked(logicalBytes + CountSnapshotRequiredString(propertyName, _snapshotRecoveryLimits.MaximumMetadataBytes));
            logicalBytes = checked(logicalBytes + CountSnapshotRequiredStringElement(property.Value, _snapshotRecoveryLimits.MaximumMetadataBytes));
            ThrowIfSnapshotLogicalBytesExceeded(logicalBytes, _snapshotRecoveryLimits.MaximumMetadataBytes);
        }

        return logicalBytes;
    }

    /// <summary>Counts one pending operation JSON object before DTO materialization.</summary>
    /// <param name="element">The operation object.</param>
    /// <param name="currentLogicalBytes">The logical byte count already charged to the request.</param>
    /// <returns>The logical byte count.</returns>
    private long CountSnapshotOperationElement(JsonElement element, long currentLogicalBytes)
    {
        var logicalBytes = SnapshotGuidLogicalBytes
            + SnapshotInt64LogicalBytes
            + SnapshotDateTimeOffsetLogicalBytes
            + SnapshotInt32LogicalBytes
            + SnapshotOperationPolicyLogicalBytes;
        ThrowIfSnapshotLogicalBytesExceeded(checked(currentLogicalBytes + logicalBytes), _snapshotRecoveryLimits.MaximumLogicalBytes);
        ValidateObject(
            element,
            [OperationIdPropertyName, StreamIdPropertyName, "clientSequence", "timestampUtc", "type", PayloadPropertyName, "policy", "metadata"],
            ["baseVersion"],
            property => logicalBytes = checked(logicalBytes + CountSnapshotOperationProperty(property)));
        return logicalBytes;
    }

    /// <summary>Counts one pending operation JSON property before DTO materialization.</summary>
    /// <param name="property">The operation property.</param>
    /// <returns>The logical bytes contributed by the property.</returns>
    private long CountSnapshotOperationProperty(JsonProperty property)
    {
        var propertyName = ReadJsonPropertyName(property);
        if (StringComparer.Ordinal.Equals(propertyName, OperationIdPropertyName))
        {
            ValidateSnapshotGuidElement(property.Value);
            return 0;
        }

        if (StringComparer.Ordinal.Equals(propertyName, StreamIdPropertyName))
        {
            return CountSnapshotRequiredStringElement(property.Value, _snapshotRecoveryLimits.MaximumStreamIdUtf8Bytes);
        }

        if (StringComparer.Ordinal.Equals(propertyName, "clientSequence"))
        {
            _ = ReadSnapshotInt64Element(property.Value);
            return 0;
        }

        if (StringComparer.Ordinal.Equals(propertyName, "timestampUtc"))
        {
            ValidateSnapshotDateTimeOffsetElement(property.Value);
            return 0;
        }

        if (StringComparer.Ordinal.Equals(propertyName, "type"))
        {
            _ = ReadSnapshotInt32Element(property.Value);
            return 0;
        }

        if (StringComparer.Ordinal.Equals(propertyName, PayloadPropertyName))
        {
            return CountSnapshotPayloadElement(property.Value);
        }

        if (StringComparer.Ordinal.Equals(propertyName, "policy"))
        {
            ValidatePolicyElement(property.Value);
            return 0;
        }

        return StringComparer.Ordinal.Equals(propertyName, "metadata")
            ? CountSnapshotMetadataElement(property.Value)
            : CountSnapshotOptionalStringElement(property.Value, _snapshotRecoveryLimits.MaximumContractUtf8Bytes);
    }

    /// <summary>Counts pending and replay operation JSON array bytes before DTO materialization.</summary>
    /// <param name="pendingElement">The pending operations array.</param>
    /// <param name="hasReplayElement">Whether the replay operations array was present.</param>
    /// <param name="replayElement">The replay operations array.</param>
    /// <param name="requestHeaderLogicalBytes">The request header logical bytes already charged.</param>
    /// <returns>The logical byte count.</returns>
    /// <exception cref="HttpRemoteTransportException">The combined operation set exceeds configured limits.</exception>
    /// <exception cref="JsonException">One operation collection is not a valid array.</exception>
    private long CountSnapshotOperationCollectionsElement(
        JsonElement pendingElement,
        bool hasReplayElement,
        JsonElement replayElement,
        long requestHeaderLogicalBytes)
    {
        var pendingCount = GetSnapshotOperationArrayCount(pendingElement);
        var replayCount = hasReplayElement ? GetSnapshotOperationArrayCount(replayElement) : 0;
        if ((long)pendingCount + replayCount > _snapshotRecoveryLimits.MaximumPendingOperations)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        var logicalBytes = SnapshotInt32LogicalBytes + SnapshotInt32LogicalBytes;
        ThrowIfSnapshotLogicalBytesExceeded(checked(requestHeaderLogicalBytes + logicalBytes), _snapshotRecoveryLimits.MaximumLogicalBytes);
        logicalBytes = checked(logicalBytes + CountSnapshotOperationsElement(pendingElement, checked(requestHeaderLogicalBytes + logicalBytes)));
        if (hasReplayElement)
        {
            logicalBytes = checked(logicalBytes + CountSnapshotOperationsElement(replayElement, checked(requestHeaderLogicalBytes + logicalBytes)));
        }

        return logicalBytes;
    }

    /// <summary>Counts one operation JSON array after combined collection headers have been bounded.</summary>
    /// <param name="element">The operations array.</param>
    /// <param name="currentLogicalBytes">The logical byte count already charged.</param>
    /// <returns>The operation item logical byte count.</returns>
    private long CountSnapshotOperationsElement(JsonElement element, long currentLogicalBytes)
    {
        var logicalBytes = 0L;
        foreach (var item in element.EnumerateArray())
        {
            logicalBytes = checked(logicalBytes + CountSnapshotOperationElement(item, checked(currentLogicalBytes + logicalBytes)));
            ThrowIfSnapshotLogicalBytesExceeded(checked(currentLogicalBytes + logicalBytes), _snapshotRecoveryLimits.MaximumLogicalBytes);
        }

        return logicalBytes;
    }

    /// <summary>Counts a response checkpoint JSON object before DTO materialization.</summary>
    /// <param name="request">The request binding.</param>
    /// <param name="element">The checkpoint object.</param>
    /// <returns>The logical byte count.</returns>
    /// <exception cref="HttpRemoteTransportException"><paramref name="element"/> is not bound to <paramref name="request"/>.</exception>
    private long CountSnapshotCheckpointElement(RemoteSnapshotRecoveryRequest request, JsonElement element)
    {
        var logicalBytes = SnapshotGuidLogicalBytes + SnapshotInt32LogicalBytes + SnapshotDateTimeOffsetLogicalBytes;
        var streamId = string.Empty;
        var subscriptionId = request.SubscriptionId;
        var snapshotFormatVersion = 0;
        ValidateObject(
            element,
            [
                StreamIdPropertyName,
                SubscriptionIdPropertyName,
                "frontierCursor",
                ServerVersionPropertyName,
                SnapshotFormatVersionPropertyName,
                "clientState",
                "observedAtUtc",
            ],
            [],
            property => logicalBytes = checked(logicalBytes + CountSnapshotCheckpointProperty(
                property,
                ref streamId,
                ref subscriptionId,
                ref snapshotFormatVersion)));

        if (!StringComparer.Ordinal.Equals(streamId, request.StreamId.Value)
            || subscriptionId != request.SubscriptionId
            || snapshotFormatVersion != request.SnapshotFormatVersion)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected);
        }

        return logicalBytes;
    }

    /// <summary>Counts one checkpoint JSON property before DTO materialization.</summary>
    /// <param name="property">The checkpoint property.</param>
    /// <param name="streamId">The decoded stream identifier binding.</param>
    /// <param name="subscriptionId">The decoded subscription identifier binding.</param>
    /// <param name="snapshotFormatVersion">The decoded snapshot format version binding.</param>
    /// <returns>The logical bytes contributed by the property.</returns>
    private long CountSnapshotCheckpointProperty(
        JsonProperty property,
        ref string streamId,
        ref SubscriptionId subscriptionId,
        ref int snapshotFormatVersion)
    {
        var propertyName = ReadJsonPropertyName(property);
        if (StringComparer.Ordinal.Equals(propertyName, StreamIdPropertyName))
        {
            streamId = ReadSnapshotStringElement(property.Value);
            return CountSnapshotRequiredStringElement(property.Value, _snapshotRecoveryLimits.MaximumStreamIdUtf8Bytes);
        }

        if (StringComparer.Ordinal.Equals(propertyName, SubscriptionIdPropertyName))
        {
            ValidateSnapshotGuidElement(property.Value);
            subscriptionId = new(Guid.Parse(ReadSnapshotStringElement(property.Value)));
            return 0;
        }

        if (StringComparer.Ordinal.Equals(propertyName, "frontierCursor"))
        {
            return CountSnapshotRequiredStringElement(property.Value, _snapshotRecoveryLimits.MaximumCursorUtf8Bytes);
        }

        if (StringComparer.Ordinal.Equals(propertyName, ServerVersionPropertyName))
        {
            return CountSnapshotRequiredStringElement(property.Value, _snapshotRecoveryLimits.MaximumContractUtf8Bytes);
        }

        if (StringComparer.Ordinal.Equals(propertyName, SnapshotFormatVersionPropertyName))
        {
            snapshotFormatVersion = ReadSnapshotInt32Element(property.Value);
            return 0;
        }

        if (StringComparer.Ordinal.Equals(propertyName, "clientState"))
        {
            return CountSnapshotPayloadElement(property.Value);
        }

        ValidateSnapshotDateTimeOffsetElement(property.Value);
        return 0;
    }

    /// <summary>Counts an operation result JSON object before DTO materialization.</summary>
    /// <param name="element">The operation result object.</param>
    /// <returns>The logical byte count.</returns>
    private long CountSnapshotOperationResultElement(JsonElement element)
    {
        var logicalBytes = SnapshotGuidLogicalBytes + SnapshotInt32LogicalBytes;
        ValidateObject(
            element,
            [OperationIdPropertyName, "kind"],
            ["reasonCode", ServerVersionPropertyName],
            property =>
            {
                switch (ReadJsonPropertyName(property))
                {
                    case OperationIdPropertyName:
                    {
                        ValidateSnapshotGuidElement(property.Value);
                        break;
                    }

                    case "kind":
                    {
                        _ = ReadSnapshotInt32Element(property.Value);
                        break;
                    }

                    case "reasonCode":
                    {
                        logicalBytes = checked(
                            logicalBytes + CountSnapshotOptionalStringElement(
                                property.Value,
                                _snapshotRecoveryLimits.MaximumReasonCodeUtf8Bytes));
                        break;
                    }

                    case ServerVersionPropertyName:
                    {
                        logicalBytes = checked(
                            logicalBytes + CountSnapshotOptionalStringElement(
                                property.Value,
                                _snapshotRecoveryLimits.MaximumContractUtf8Bytes));
                        break;
                    }
                }
            });
        return logicalBytes;
    }

    /// <summary>Counts one snapshot disposition JSON object before DTO materialization.</summary>
    /// <param name="element">The disposition object.</param>
    /// <returns>The logical byte count.</returns>
    private long CountSnapshotDispositionElement(JsonElement element)
    {
        var logicalBytes = SnapshotGuidLogicalBytes + SnapshotInt32LogicalBytes;
        ValidateObject(
            element,
            [OperationIdPropertyName, "kind"],
            ["result"],
            property =>
            {
                switch (ReadJsonPropertyName(property))
                {
                    case OperationIdPropertyName:
                    {
                        ValidateSnapshotGuidElement(property.Value);
                        break;
                    }

                    case "kind":
                    {
                        _ = ReadSnapshotInt32Element(property.Value);
                        break;
                    }

                    case "result":
                    {
                        if (property.Value.ValueKind != JsonValueKind.Null)
                        {
                            logicalBytes = checked(logicalBytes + CountSnapshotOperationResultElement(property.Value));
                        }

                        break;
                    }
                }
            });
        return logicalBytes;
    }

    /// <summary>Counts snapshot disposition JSON array bytes before DTO materialization.</summary>
    /// <param name="element">The dispositions array.</param>
    /// <param name="count">The item count.</param>
    /// <returns>The logical byte count.</returns>
    /// <exception cref="HttpRemoteTransportException"><paramref name="element"/> exceeds configured limits.</exception>
    /// <exception cref="JsonException"><paramref name="element"/> is not a valid dispositions array.</exception>
    private long CountSnapshotDispositionsElement(JsonElement element, out int count)
    {
        if (element.ValueKind != JsonValueKind.Array)
        {
            throw new JsonException("Expected a JSON array.");
        }

        count = element.GetArrayLength();
        if (count > _snapshotRecoveryLimits.MaximumPendingOperations)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        var logicalBytes = SnapshotInt32LogicalBytes;
        foreach (var item in element.EnumerateArray())
        {
            logicalBytes = checked(logicalBytes + CountSnapshotDispositionElement(item));
            ThrowIfSnapshotLogicalBytesExceeded(logicalBytes, _snapshotRecoveryLimits.MaximumLogicalBytes);
        }

        return logicalBytes;
    }

    /// <summary>Validates the closed JSON shape of a snapshot recovery request body.</summary>
    /// <param name="element">Snapshot recovery request JSON object.</param>
    private void ValidateSnapshotRecoveryRequestElement(JsonElement element)
    {
        var logicalBytes = 0L;
        var pendingOperationsElement = default(JsonElement);
        var replayOperationsElement = default(JsonElement);
        var hasReplayOperations = false;
        ValidateObject(
            element,
            [
                StreamIdPropertyName,
                SubscriptionIdPropertyName,
                "clientStateContractId",
                "clientStateSchemaVersion",
                SnapshotFormatVersionPropertyName,
                "pendingOperations",
                "maximumResponseBytes",
            ],
            ["expiredCursor", SnapshotReplayOperationsPropertyName],
            property => logicalBytes = checked(logicalBytes + CountSnapshotRecoveryRequestProperty(
                property,
                ref pendingOperationsElement,
                ref replayOperationsElement,
                ref hasReplayOperations)));

        logicalBytes = checked(logicalBytes + CountSnapshotOperationCollectionsElement(
            pendingOperationsElement,
            hasReplayOperations,
            replayOperationsElement,
            logicalBytes));
        ThrowIfSnapshotLogicalBytesExceeded(logicalBytes, _snapshotRecoveryLimits.MaximumLogicalBytes);
    }

    /// <summary>Counts one snapshot recovery request JSON property before DTO materialization.</summary>
    /// <param name="property">The request property.</param>
    /// <param name="pendingOperationsElement">The pending operations element.</param>
    /// <param name="replayOperationsElement">The replay operations element.</param>
    /// <param name="hasReplayOperations">Whether the replay operations element was present.</param>
    /// <returns>The logical bytes contributed by the property.</returns>
    private long CountSnapshotRecoveryRequestProperty(
        JsonProperty property,
        ref JsonElement pendingOperationsElement,
        ref JsonElement replayOperationsElement,
        ref bool hasReplayOperations)
    {
        var propertyName = ReadJsonPropertyName(property);
        if (StringComparer.Ordinal.Equals(propertyName, StreamIdPropertyName))
        {
            return CountSnapshotRequiredStringElement(property.Value, _snapshotRecoveryLimits.MaximumStreamIdUtf8Bytes);
        }

        if (StringComparer.Ordinal.Equals(propertyName, "clientStateContractId"))
        {
            return CountSnapshotRequiredStringElement(property.Value, _snapshotRecoveryLimits.MaximumContractUtf8Bytes);
        }

        if (StringComparer.Ordinal.Equals(propertyName, "expiredCursor"))
        {
            return CountSnapshotOptionalStringElement(property.Value, _snapshotRecoveryLimits.MaximumCursorUtf8Bytes);
        }

        if (StringComparer.Ordinal.Equals(propertyName, SubscriptionIdPropertyName))
        {
            ValidateSnapshotGuidElement(property.Value);
            return SnapshotGuidLogicalBytes;
        }

        if (StringComparer.Ordinal.Equals(propertyName, "clientStateSchemaVersion")
            || StringComparer.Ordinal.Equals(propertyName, SnapshotFormatVersionPropertyName))
        {
            _ = ReadSnapshotInt32Element(property.Value);
            return SnapshotInt32LogicalBytes;
        }

        if (StringComparer.Ordinal.Equals(propertyName, "maximumResponseBytes"))
        {
            return CountSnapshotMaximumResponseBytesElement(property.Value);
        }

        if (StringComparer.Ordinal.Equals(propertyName, "pendingOperations"))
        {
            pendingOperationsElement = property.Value;
            _ = GetSnapshotOperationArrayCount(property.Value);
            return 0;
        }

        replayOperationsElement = property.Value;
        hasReplayOperations = true;
        _ = GetSnapshotOperationArrayCount(property.Value);
        return 0;
    }

    /// <summary>Counts a request maximum response byte element.</summary>
    /// <param name="element">The maximum response byte element.</param>
    /// <returns>The logical byte count.</returns>
    /// <exception cref="HttpRemoteTransportException"><paramref name="element"/> exceeds configured limits.</exception>
    private long CountSnapshotMaximumResponseBytesElement(JsonElement element)
    {
        var maximumResponseBytes = ReadSnapshotInt64Element(element);
        if (maximumResponseBytes > _snapshotRecoveryLimits.MaximumLogicalBytes)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        return SnapshotInt64LogicalBytes;
    }

    /// <summary>Validates the closed JSON shape of a snapshot recovery response body.</summary>
    /// <param name="request">The request that bounds the response.</param>
    /// <param name="element">Snapshot recovery response JSON object.</param>
    /// <exception cref="HttpRemoteTransportException"><paramref name="element"/> is not bound to <paramref name="request"/>.</exception>
    private void ValidateSnapshotRecoveryResponseElement(RemoteSnapshotRecoveryRequest request, JsonElement element)
    {
        var status = -1;
        var logicalBytes = 0L;
        var checkpointLogicalBytes = 0L;
        var hasCheckpoint = false;
        var dispositionCount = 0;
        var dispositionsLogicalBytes = 0L;
        ValidateObject(
            element,
            ["status", "operationDispositions"],
            ["checkpoint", "reasonCode"],
            property => logicalBytes = checked(logicalBytes + CountSnapshotRecoveryResponseProperty(
                request,
                property,
                ref status,
                ref hasCheckpoint,
                ref checkpointLogicalBytes,
                ref dispositionCount,
                ref dispositionsLogicalBytes)));

        if (status != 0)
        {
            if (hasCheckpoint || dispositionCount != 0)
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected);
            }

            logicalBytes = checked(logicalBytes + SnapshotInt32LogicalBytes);
            ThrowIfSnapshotLogicalBytesExceeded(logicalBytes, request.MaximumResponseBytes);
            ThrowIfSnapshotLogicalBytesExceeded(logicalBytes, _snapshotRecoveryLimits.MaximumLogicalBytes);
            return;
        }

        if (!hasCheckpoint)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected);
        }

        logicalBytes = checked(logicalBytes + checkpointLogicalBytes + dispositionsLogicalBytes);
        ThrowIfSnapshotLogicalBytesExceeded(logicalBytes, request.MaximumResponseBytes);
        ThrowIfSnapshotLogicalBytesExceeded(logicalBytes, _snapshotRecoveryLimits.MaximumLogicalBytes);
    }

    /// <summary>Counts one snapshot recovery response JSON property before DTO materialization.</summary>
    /// <param name="request">The request binding.</param>
    /// <param name="property">The response property.</param>
    /// <param name="status">The decoded status.</param>
    /// <param name="hasCheckpoint">Whether a checkpoint was present.</param>
    /// <param name="checkpointLogicalBytes">The checkpoint logical bytes.</param>
    /// <param name="dispositionCount">The disposition count.</param>
    /// <param name="dispositionsLogicalBytes">The dispositions logical bytes.</param>
    /// <returns>The logical bytes contributed by the property.</returns>
    private long CountSnapshotRecoveryResponseProperty(
        RemoteSnapshotRecoveryRequest request,
        JsonProperty property,
        ref int status,
        ref bool hasCheckpoint,
        ref long checkpointLogicalBytes,
        ref int dispositionCount,
        ref long dispositionsLogicalBytes)
    {
        var propertyName = ReadJsonPropertyName(property);
        if (StringComparer.Ordinal.Equals(propertyName, "status"))
        {
            status = ReadSnapshotInt32Element(property.Value);
            return SnapshotInt32LogicalBytes;
        }

        if (StringComparer.Ordinal.Equals(propertyName, "checkpoint"))
        {
            if (property.Value.ValueKind != JsonValueKind.Null)
            {
                hasCheckpoint = true;
                checkpointLogicalBytes = CountSnapshotCheckpointElement(request, property.Value);
            }

            return 0;
        }

        if (StringComparer.Ordinal.Equals(propertyName, "operationDispositions"))
        {
            dispositionsLogicalBytes = CountSnapshotDispositionsElement(property.Value, out dispositionCount);
            return 0;
        }

        return CountSnapshotOptionalStringElement(property.Value, _snapshotRecoveryLimits.MaximumReasonCodeUtf8Bytes);
    }
}
