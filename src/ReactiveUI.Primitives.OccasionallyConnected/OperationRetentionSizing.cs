// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Calculates the retained size of durable operations using the engine diagnostic formula.</summary>
internal static class OperationRetentionSizing
{
    /// <summary>The nominal object overhead charged for retained operation and metadata envelopes.</summary>
    private const long RetainedEnvelopeOverheadBytes = 32;

    /// <summary>The byte count retained by a GUID field.</summary>
    private const long RetainedGuidBytes = 16;

    /// <summary>The operation envelope GUID fields retained by one operation.</summary>
    private const long RetainedOperationGuidFieldCount = 2;

    /// <summary>The minimum retained byte count.</summary>
    private const long UnknownProducerRetainedBytes = 1;

    /// <summary>Calculates the retained bytes for one raw operation.</summary>
    /// <param name="operation">The operation.</param>
    /// <returns>The retained byte count.</returns>
    internal static long GetOperationRetainedBytes(SyncOperation operation)
    {
        var retainedBytes = RetainedEnvelopeOverheadBytes
            + (RetainedGuidBytes * RetainedOperationGuidFieldCount)
            + sizeof(long)
            + sizeof(int)
            + GetRetainedTextBytes(operation.StreamId.Value)
            + GetRetainedTextBytes(operation.BaseVersion)
            + GetRetainedTextBytes(operation.Type.ToString())
            + GetRetainedTextBytes(operation.Policy.DeliveryGuarantee.ToString())
            + GetRetainedTextBytes(operation.Policy.Durability.ToString())
            + GetRetainedTextBytes(operation.Policy.ConflictPolicy.ToString())
            + GetPayloadRetainedBytes(operation.Payload)
            + GetMetadataRetainedBytes(operation.Metadata);
        return Math.Max(UnknownProducerRetainedBytes, retainedBytes);
    }

    /// <summary>Gets the retained byte count for a text value.</summary>
    /// <param name="value">The text value.</param>
    /// <returns>The retained byte count.</returns>
    private static long GetRetainedTextBytes(string? value) =>
        value is null ? 0 : RetainedEnvelopeOverheadBytes + ((long)value.Length * sizeof(char));

    /// <summary>Gets the retained byte count for a payload envelope.</summary>
    /// <param name="payload">The payload envelope.</param>
    /// <returns>The retained byte count.</returns>
    private static long GetPayloadRetainedBytes(PayloadEnvelope payload) =>
        RetainedEnvelopeOverheadBytes
        + sizeof(int)
        + payload.PayloadLength
        + GetRetainedTextBytes(payload.ContractId)
        + GetRetainedTextBytes(payload.ContentType)
        + GetRetainedTextBytes(payload.PayloadHash);

    /// <summary>Gets the retained byte count for operation metadata.</summary>
    /// <param name="metadata">The operation metadata.</param>
    /// <returns>The retained byte count.</returns>
    private static long GetMetadataRetainedBytes(IReadOnlyDictionary<string, string> metadata)
    {
        var retainedBytes = RetainedEnvelopeOverheadBytes;
        foreach (var pair in metadata)
        {
            retainedBytes += RetainedEnvelopeOverheadBytes
                + GetRetainedTextBytes(pair.Key)
                + GetRetainedTextBytes(pair.Value);
        }

        return retainedBytes;
    }
}
