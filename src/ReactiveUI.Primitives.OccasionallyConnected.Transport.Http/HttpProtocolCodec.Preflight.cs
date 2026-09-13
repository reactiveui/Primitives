// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text.Json;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Encodes and decodes the bounded HTTP protocol DTOs.</summary>
internal sealed partial class HttpProtocolCodec
{
    /// <summary>Validates the closed JSON shape of a connect response body.</summary>
    /// <param name="element">Connect response JSON object to inspect before DTO conversion.</param>
    /// <exception cref="HttpRemoteTransportException">The connect response shape is malformed or exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The connect response body is malformed JSON.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateConnectResponseElement(JsonElement element) =>
        ValidateObject(
            element,
            ["protocolVersion", "features", "maximumBatchOperations", "maximumBatchBytes"],
            ["serverIdempotencyRetentionMilliseconds", "clientInboxRetentionRequiredMilliseconds"],
            static property =>
            {
                switch (property.Name)
                {
                    case "protocolVersion":
                    {
                        ValidateStringElement(property.Value);
                        break;
                    }

                    case "features" or "maximumBatchOperations" or "maximumBatchBytes":
                    {
                        ValidateNumberElement(property.Value);
                        break;
                    }

                    case "serverIdempotencyRetentionMilliseconds" or "clientInboxRetentionRequiredMilliseconds":
                    {
                        ValidateOptionalNumberElement(property.Value);
                        break;
                    }
                }
            });

    /// <summary>Validates the closed JSON shape of an acknowledgement request body.</summary>
    /// <param name="element">Acknowledgement request JSON object to inspect before DTO conversion.</param>
    /// <exception cref="HttpRemoteTransportException">The acknowledgement request shape is malformed or exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The acknowledgement request body is malformed JSON.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateAcknowledgementElement(JsonElement element) =>
        ValidateObject(
            element,
            [SubscriptionIdPropertyName, StreamIdPropertyName, CursorPropertyName],
            [],
            static property => ValidateStringElement(property.Value));

    /// <summary>Validates the closed JSON shape of an operation policy object.</summary>
    /// <param name="element">Operation policy JSON object to inspect before DTO conversion.</param>
    /// <exception cref="HttpRemoteTransportException">The operation policy shape is malformed or exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The operation policy body is malformed JSON.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidatePolicyElement(JsonElement element) =>
        ValidateObject(
            element,
            ["deliveryGuarantee", "durability", "priority", "conflictPolicy"],
            [],
            static property => ValidateNumberElement(property.Value));

    /// <summary>Validates the closed JSON shape of a payload envelope object.</summary>
    /// <param name="element">Payload envelope JSON object to inspect before DTO conversion.</param>
    /// <exception cref="HttpRemoteTransportException">The payload envelope shape is malformed or exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The payload envelope body is malformed JSON.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidatePayloadElement(JsonElement element) =>
        ValidateObject(
            element,
            ["contractId", "schemaVersion", "contentType", PayloadPropertyName, "payloadHash"],
            [],
            static property =>
            {
                if (property.Name == "schemaVersion")
                {
                    ValidateNumberElement(property.Value);
                    return;
                }

                ValidateStringElement(property.Value);
            });

    /// <summary>Validates the closed JSON shape of an operation result object.</summary>
    /// <param name="element">Operation result JSON object to inspect before DTO conversion.</param>
    /// <exception cref="HttpRemoteTransportException">The operation result shape is malformed or exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The operation result body is malformed JSON.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateOperationResultElement(JsonElement element) =>
        ValidateObject(
            element,
            [OperationIdPropertyName, "kind"],
            ["reasonCode", "serverVersion"],
            static property =>
            {
                switch (property.Name)
                {
                    case OperationIdPropertyName:
                    {
                        ValidateStringElement(property.Value);
                        break;
                    }

                    case "kind":
                    {
                        ValidateNumberElement(property.Value);
                        break;
                    }

                    case "reasonCode" or "serverVersion":
                    {
                        ValidateOptionalStringElement(property.Value);
                        break;
                    }
                }
            });

    /// <summary>Validates the closed JSON shape of a remote event origin object.</summary>
    /// <param name="element">Remote event origin JSON object to inspect before DTO conversion.</param>
    /// <exception cref="HttpRemoteTransportException">The remote event origin shape is malformed or exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The remote event origin body is malformed JSON.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateOriginElement(JsonElement element) =>
        ValidateObject(
            element,
            ["clientId", OperationIdPropertyName],
            [],
            static property => ValidateStringElement(property.Value));

    /// <summary>Checks object kind, required properties, unknown names, and duplicate names.</summary>
    /// <param name="element">JSON object whose property set must be closed.</param>
    /// <param name="required">Property names that must appear exactly once.</param>
    /// <param name="optional">Property names accepted when present.</param>
    /// <param name="validateProperty">Per-property value-kind validator.</param>
    /// <exception cref="HttpRemoteTransportException">The JSON object is missing required fields, repeats fields, or exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The inspected value is not a JSON object.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateObject(JsonElement element, string[] required, string[] optional, Action<JsonProperty> validateProperty)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException("Expected a JSON object.");
        }

        HashSet<string> seen = [with(comparer: StringComparer.Ordinal)];
        HashSet<string> remaining = [with(comparer: StringComparer.Ordinal)];
        for (var index = 0; index < required.Length; index++)
        {
            _ = remaining.Add(required[index]);
        }

        foreach (var property in element.EnumerateObject())
        {
            if (!seen.Add(property.Name))
            {
                throw new JsonException("Duplicate JSON property name.");
            }

            if (!Contains(required, property.Name) && !Contains(optional, property.Name))
            {
                throw new JsonException("Unknown JSON property name.");
            }

            _ = remaining.Remove(property.Name);
            validateProperty(property);
        }

        if (remaining.Count == 0)
        {
            return;
        }

        throw new JsonException("Missing required JSON property.");
    }

    /// <summary>Checks a fixed property-name set using ordinal comparison.</summary>
    /// <param name="values">Accepted property-name set.</param>
    /// <param name="value">Property name read from the JSON object.</param>
    /// <returns><see langword="true"/> when <paramref name="value"/> is accepted by the set.</returns>
    /// <exception cref="HttpRemoteTransportException">The property name is malformed or exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The property name cannot be read as JSON text.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool Contains(string[] values, string value)
    {
        for (var index = 0; index < values.Length; index++)
        {
            if (!StringComparer.Ordinal.Equals(values[index], value))
            {
                continue;
            }

            return true;
        }

        return false;
    }

    /// <summary>Requires a JSON string value.</summary>
    /// <param name="element">Required protocol string JSON value.</param>
    /// <exception cref="HttpRemoteTransportException">The required string is malformed or exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The JSON value is not a string.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateStringElement(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.String)
        {
            return;
        }

        throw new JsonException(ExpectedJsonStringMessage);
    }

    /// <summary>Requires a JSON string value or null.</summary>
    /// <param name="element">Optional protocol string JSON value.</param>
    /// <exception cref="HttpRemoteTransportException">The optional string is malformed or exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The JSON value is neither a string nor null.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateOptionalStringElement(JsonElement element)
    {
        if (element.ValueKind is JsonValueKind.String or JsonValueKind.Null)
        {
            return;
        }

        throw new JsonException(ExpectedJsonStringMessage);
    }

    /// <summary>Requires a JSON number value.</summary>
    /// <param name="element">Required protocol number JSON value.</param>
    /// <exception cref="HttpRemoteTransportException">The required number is malformed or exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The JSON value is not a number.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateNumberElement(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.Number)
        {
            return;
        }

        throw new JsonException(ExpectedJsonNumberMessage);
    }

    /// <summary>Requires a JSON number value or null.</summary>
    /// <param name="element">Optional protocol number JSON value.</param>
    /// <exception cref="HttpRemoteTransportException">The optional number is malformed or exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The JSON value is neither a number nor null.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateOptionalNumberElement(JsonElement element)
    {
        if (element.ValueKind is JsonValueKind.Number or JsonValueKind.Null)
        {
            return;
        }

        throw new JsonException(ExpectedJsonNumberMessage);
    }

    /// <summary>Validates a bounded JSON array whose entries must be numbers.</summary>
    /// <param name="element">JSON array containing protocol integer values.</param>
    /// <param name="maximumCount">Maximum accepted array item count.</param>
    /// <exception cref="HttpRemoteTransportException">The integer array exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The JSON value is not an array of numbers.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateInt32Array(JsonElement element, int maximumCount) =>
        ValidateArray(element, maximumCount, static item =>
        {
            if (item.ValueKind == JsonValueKind.Number)
            {
                return;
            }

            throw new JsonException(ExpectedJsonNumberMessage);
        });

    /// <summary>Validates a bounded JSON array whose entries must be GUID strings.</summary>
    /// <param name="element">JSON array containing event identifier strings.</param>
    /// <param name="maximumCount">Maximum accepted array item count.</param>
    /// <returns>The number of event identifiers in the array.</returns>
    /// <exception cref="HttpRemoteTransportException">The identifier array exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The JSON value is not an array of GUID strings.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int ValidateGuidArray(JsonElement element, int maximumCount) =>
        ValidateArray(element, maximumCount, static item =>
        {
            if (item.ValueKind == JsonValueKind.String)
            {
                return;
            }

            throw new JsonException(ExpectedJsonStringMessage);
        });

    /// <summary>Validates a bounded JSON array and applies an item preflight action.</summary>
    /// <param name="element">JSON array to inspect.</param>
    /// <param name="maximumCount">Maximum accepted array item count.</param>
    /// <param name="validateItem">Validator applied to each array item.</param>
    /// <returns>The number of JSON array items.</returns>
    /// <exception cref="HttpRemoteTransportException">The array exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The JSON value is not an array or an item has the wrong JSON kind.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int ValidateArray(JsonElement element, int maximumCount, Action<JsonElement> validateItem)
    {
        if (element.ValueKind != JsonValueKind.Array)
        {
            throw new JsonException("Expected a JSON array.");
        }

        var count = element.GetArrayLength();
        if (count > maximumCount)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
        }

        foreach (var item in element.EnumerateArray())
        {
            validateItem(item);
        }

        return count;
    }

    /// <summary>Validates the closed JSON shape of a connect request body.</summary>
    /// <param name="element">Connect request JSON object to inspect before DTO conversion.</param>
    /// <exception cref="HttpRemoteTransportException">The connect request shape is malformed or exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The connect request body is malformed JSON.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateConnectRequestElement(JsonElement element) =>
        ValidateObject(
            element,
            ["minimumProtocolVersion", "maximumProtocolVersion", "clientId", "requiredGuarantees"],
            ["tenantHint"],
            property =>
            {
                switch (property.Name)
                {
                    case "minimumProtocolVersion" or "maximumProtocolVersion" or "clientId":
                    {
                        ValidateStringElement(property.Value);
                        break;
                    }

                    case "tenantHint":
                    {
                        ValidateOptionalStringElement(property.Value);
                        break;
                    }

                    case "requiredGuarantees":
                    {
                        ValidateInt32Array(property.Value, _limits.MaximumBatchOperations);
                        break;
                    }
                }
            });

    /// <summary>Validates the closed JSON shape and operation count of a push request body.</summary>
    /// <param name="element">Push request JSON object to inspect before DTO conversion.</param>
    /// <exception cref="HttpRemoteTransportException">The push request shape is malformed or exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The push request body is malformed JSON.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidatePushRequestElement(JsonElement element) =>
        ValidateObject(
            element,
            [BatchIdPropertyName, "operations"],
            [],
            property =>
            {
                switch (property.Name)
                {
                    case BatchIdPropertyName:
                    {
                        ValidateStringElement(property.Value);
                        break;
                    }

                    case "operations":
                    {
                        _ = ValidateArray(property.Value, _limits.MaximumBatchOperations, ValidateOperationElement);
                        break;
                    }
                }
            });

    /// <summary>Validates the closed JSON shape and result count of a push response body.</summary>
    /// <param name="element">Push response JSON object to inspect before DTO conversion.</param>
    /// <exception cref="HttpRemoteTransportException">The push response shape is malformed or exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The push response body is malformed JSON.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidatePushResponseElement(JsonElement element) =>
        ValidateObject(
            element,
            [BatchIdPropertyName, "operations"],
            [ServerCursorPropertyName],
            property =>
            {
                switch (property.Name)
                {
                    case BatchIdPropertyName:
                    {
                        ValidateStringElement(property.Value);
                        break;
                    }

                    case "operations":
                    {
                        _ = ValidateArray(property.Value, _limits.MaximumBatchOperations, ValidateOperationResultElement);
                        break;
                    }

                    case ServerCursorPropertyName:
                    {
                        ValidateOptionalStringElement(property.Value);
                        break;
                    }
                }
            });

    /// <summary>Validates the closed JSON shape and batch count of a subscribe response body.</summary>
    /// <param name="element">Subscribe response JSON object to inspect before DTO conversion.</param>
    /// <exception cref="HttpRemoteTransportException">The subscribe response shape is malformed or exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The subscribe response body is malformed JSON.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateSubscribeResponseElement(JsonElement element) =>
        ValidateObject(
            element,
            ["batches"],
            [],
            property => ValidateArray(property.Value, _limits.MaximumBatchOperations, ValidateRemoteEventBatchElement));

    /// <summary>Validates the closed JSON shape of a pushed operation object.</summary>
    /// <param name="element">Pushed operation JSON object to inspect before DTO conversion.</param>
    /// <exception cref="HttpRemoteTransportException">The pushed operation shape is malformed or exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The pushed operation body is malformed JSON.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateOperationElement(JsonElement element) =>
        ValidateObject(
            element,
            [OperationIdPropertyName, StreamIdPropertyName, "clientSequence", "timestampUtc", "type", PayloadPropertyName, "policy", "metadata"],
            ["baseVersion"],
            ValidateOperationProperty);

    /// <summary>Validates the closed JSON shape and aggregate completion references of a receive batch.</summary>
    /// <param name="element">Remote event batch JSON object to inspect before DTO conversion.</param>
    /// <exception cref="HttpRemoteTransportException">The remote event batch shape is malformed or exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The remote event batch body is malformed JSON.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateRemoteEventBatchElement(JsonElement element)
    {
        var completionEventIds = 0;
        ValidateObject(
            element,
            [BatchIdPropertyName, StreamIdPropertyName, "nextCursor", "events", "completedOperations"],
            ["previousCursor"],
            property =>
            {
                switch (property.Name)
                {
                    case BatchIdPropertyName or StreamIdPropertyName or "nextCursor":
                    {
                        ValidateStringElement(property.Value);
                        break;
                    }

                    case "previousCursor":
                    {
                        ValidateOptionalStringElement(property.Value);
                        break;
                    }

                    case "events":
                    {
                        _ = ValidateArray(property.Value, _limits.MaximumEventsPerBatch, ValidateRemoteEventElement);
                        break;
                    }

                    case "completedOperations":
                    {
                        _ = ValidateArray(property.Value, _limits.MaximumCompletedOperationsPerBatch, item =>
                        {
                            completionEventIds = checked(completionEventIds + ValidateCompletionElement(item));
                            if (completionEventIds <= _limits.MaximumEventsPerBatch)
                            {
                                return;
                            }

                            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
                        });
                        break;
                    }
                }
            });
    }

    /// <summary>Validates the closed JSON shape of a received event object.</summary>
    /// <param name="element">Remote event JSON object to inspect before DTO conversion.</param>
    /// <exception cref="HttpRemoteTransportException">The remote event shape is malformed or exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The remote event body is malformed JSON.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateRemoteEventElement(JsonElement element) =>
        ValidateObject(
            element,
            ["eventId", StreamIdPropertyName, ServerCursorPropertyName, "committedAtUtc", PayloadPropertyName, "metadata"],
            ["causedByOperationId", OriginPropertyName],
            ValidateRemoteEventProperty);

    /// <summary>Validates one closed-shape operation property using its protocol value kind.</summary>
    /// <param name="property">Pushed operation property to validate by protocol name.</param>
    /// <exception cref="HttpRemoteTransportException">The pushed operation property exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The pushed operation property has the wrong JSON value kind.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateOperationProperty(JsonProperty property)
    {
        if (Contains([OperationIdPropertyName, StreamIdPropertyName, "timestampUtc"], property.Name))
        {
            ValidateStringElement(property.Value);
            return;
        }

        if (StringComparer.Ordinal.Equals("baseVersion", property.Name))
        {
            ValidateOptionalStringElement(property.Value);
            return;
        }

        if (Contains(["clientSequence", "type"], property.Name))
        {
            ValidateNumberElement(property.Value);
            return;
        }

        if (StringComparer.Ordinal.Equals(PayloadPropertyName, property.Name))
        {
            ValidatePayloadElement(property.Value);
            return;
        }

        if (StringComparer.Ordinal.Equals("policy", property.Name))
        {
            ValidatePolicyElement(property.Value);
            return;
        }

        ValidateMetadataElement(property.Value);
    }

    /// <summary>Validates one closed-shape remote event property using its protocol value kind.</summary>
    /// <param name="property">Remote event property to validate by protocol name.</param>
    /// <exception cref="HttpRemoteTransportException">The remote event property exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The remote event property has the wrong JSON value kind.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateRemoteEventProperty(JsonProperty property)
    {
        if (Contains(["eventId", StreamIdPropertyName, ServerCursorPropertyName, "committedAtUtc"], property.Name))
        {
            ValidateStringElement(property.Value);
            return;
        }

        if (StringComparer.Ordinal.Equals("causedByOperationId", property.Name))
        {
            ValidateOptionalStringElement(property.Value);
            return;
        }

        if (StringComparer.Ordinal.Equals(OriginPropertyName, property.Name))
        {
            if (property.Value.ValueKind != JsonValueKind.Null)
            {
                ValidateOriginElement(property.Value);
            }

            return;
        }

        if (StringComparer.Ordinal.Equals(PayloadPropertyName, property.Name))
        {
            ValidatePayloadElement(property.Value);
            return;
        }

        ValidateMetadataElement(property.Value);
    }

    /// <summary>Validates a completion object and returns its referenced event count.</summary>
    /// <param name="element">Completed operation JSON object to inspect before DTO conversion.</param>
    /// <returns>The number of event identifiers referenced by the completion.</returns>
    /// <exception cref="HttpRemoteTransportException">The completed operation shape is malformed or exceeds configured limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The completed operation body is malformed JSON.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int ValidateCompletionElement(JsonElement element)
    {
        var eventIds = 0;
        ValidateObject(
            element,
            [OriginPropertyName, "eventIds"],
            [],
            property =>
            {
                switch (property.Name)
                {
                    case OriginPropertyName:
                    {
                        ValidateOriginElement(property.Value);
                        break;
                    }

                    case "eventIds":
                    {
                        eventIds = ValidateGuidArray(property.Value, _limits.MaximumEventsPerBatch);
                        break;
                    }
                }
            });
        return eventIds;
    }

    /// <summary>Validates metadata object entries and their strict protocol string bounds.</summary>
    /// <param name="element">Metadata JSON object to inspect before DTO conversion.</param>
    /// <exception cref="HttpRemoteTransportException">The metadata object exceeds configured entry or text limits.</exception>
    /// <exception cref="System.Text.Json.JsonException">The metadata body is malformed JSON.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ValidateMetadataElement(JsonElement element)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException("Expected a JSON object.");
        }

        var count = 0;
        HashSet<string> names = [with(comparer: StringComparer.Ordinal)];
        foreach (var property in element.EnumerateObject())
        {
            count++;
            if (count > _limits.MaximumMetadataEntries)
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge);
            }

            if (!names.Add(property.Name))
            {
                throw new JsonException("Duplicate JSON property name.");
            }

            ValidateProtocolString(property.Name, _limits.MaximumMetadataKeyBytes);
            ValidateStringElement(property.Value);
            ValidateProtocolString(string.Concat(property.Value.GetString()), _limits.MaximumMetadataValueBytes);
        }
    }
}
