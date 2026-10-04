// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace OccasionallyConnected.PackedSample;

/// <summary>
/// Captures a CRDT input synchronously so <c>stream.Input.OnNext</c> can accept it. The envelope matches what
/// <see cref="CrdtPayloadSerializer"/> produces for the same input.
/// </summary>
internal sealed class CrdtInputCapture : IOccasionallyConnectedInputCapture<CrdtInput>
{
    /// <summary>Gets the shared instance.</summary>
    internal static CrdtInputCapture Instance { get; } = new();

    /// <inheritdoc/>
    public PayloadEnvelope Capture(CrdtInput value)
    {
        var payload = CrdtCodec.EncodeInput(value);
        return new PayloadEnvelope(
            CrdtContracts.InputContractId,
            CrdtContracts.SchemaVersion,
            new CrdtPayloadSerializer().ContentType,
            payload,
            JsonPayloadSerializer.ComputePayloadHash(payload));
    }

    /// <summary>The bound for the envelope strings (contract, content type and hash) and object overhead.</summary>
    private const int EnvelopeOverheadBytes = 1024;

    /// <inheritdoc/>
    /// <remarks>The declaration covers the whole retained envelope, not only the encoded payload.</remarks>
    public long GetRetainedByteCount(CrdtInput value) => CrdtCodec.EncodeInput(value).Length + EnvelopeOverheadBytes;
}
