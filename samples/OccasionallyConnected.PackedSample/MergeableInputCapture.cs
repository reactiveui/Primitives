// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace OccasionallyConnected.PackedSample;

/// <summary>
/// Captures a mergeable input synchronously so <c>stream.Input.OnNext</c> can accept it. The envelope matches what
/// <see cref="MergeablePayloadSerializer"/> produces for the same input.
/// </summary>
internal sealed class MergeableInputCapture : IOccasionallyConnectedInputCapture<MergeableInput>
{
    /// <summary>Gets the shared instance.</summary>
    internal static MergeableInputCapture Instance { get; } = new();

    /// <inheritdoc/>
    public PayloadEnvelope Capture(MergeableInput value)
    {
        var payload = MergeableCodec.EncodeInput(value);
        return new PayloadEnvelope(
            MergeableContracts.InputContractId,
            MergeableContracts.SchemaVersion,
            new MergeablePayloadSerializer().ContentType,
            payload,
            JsonPayloadSerializer.ComputePayloadHash(payload));
    }

    /// <summary>The bound for the envelope strings (contract, content type and hash) and object overhead.</summary>
    private const int EnvelopeOverheadBytes = 1024;

    /// <inheritdoc/>
    /// <remarks>The declaration covers the whole retained envelope, not only the encoded payload.</remarks>
    public long GetRetainedByteCount(MergeableInput value) => MergeableCodec.EncodeInput(value).Length + EnvelopeOverheadBytes;
}
