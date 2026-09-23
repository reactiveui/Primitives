// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Retained-size accounting helpers for <see cref="SyncEngine"/>.</summary>
internal sealed partial class SyncEngine
{
    /// <summary>The nominal object overhead charged for retained operation and metadata envelopes.</summary>
    private const long RetainedEnvelopeOverheadBytes = 32;

    /// <summary>The byte count retained by a GUID field.</summary>
    private const long RetainedGuidBytes = 16;

    /// <summary>The operation envelope GUID fields retained by one operation.</summary>
    private const long RetainedOperationGuidFieldCount = 2;

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
