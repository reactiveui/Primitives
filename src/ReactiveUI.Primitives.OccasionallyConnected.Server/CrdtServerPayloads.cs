// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Creates built-in CRDT server payload envelopes.</summary>
public static class CrdtServerPayloads
{
    /// <summary>The binary CRDT content type.</summary>
    private const string BinaryContentType = "application/vnd.reactiveui.oc.crdt+binary";

    /// <summary>The SHA-256 payload hash prefix.</summary>
    private const string Sha256Prefix = "sha256-";

    /// <summary>The encoded SHA-256 payload hash length.</summary>
    private const int Sha256HashLength = 51;

    /// <summary>Gets the binary CRDT content type.</summary>
    public static string ContentType => BinaryContentType;

    /// <summary>Creates a CRDT state payload envelope.</summary>
    /// <param name="state">The complete CRDT state.</param>
    /// <returns>The payload envelope.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static PayloadEnvelope CreateState(CrdtState state) =>
        CreateState(state, CrdtBounds.Default);

    /// <summary>Creates a CRDT state payload envelope.</summary>
    /// <param name="state">The complete CRDT state.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The payload envelope.</returns>
    public static PayloadEnvelope CreateState(CrdtState state, CrdtBounds bounds)
    {
        var payload = CrdtCodec.EncodeState(state, bounds);
        return new(
            CrdtContracts.StateContractId,
            CrdtContracts.SchemaVersion,
            ContentType,
            payload,
            ComputePayloadHash(payload));
    }

    /// <summary>Creates a CRDT input payload envelope.</summary>
    /// <param name="input">The CRDT input.</param>
    /// <returns>The payload envelope.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static PayloadEnvelope CreateInput(CrdtInput input) =>
        CreateInput(input, CrdtBounds.Default);

    /// <summary>Creates a CRDT input payload envelope.</summary>
    /// <param name="input">The CRDT input.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The payload envelope.</returns>
    public static PayloadEnvelope CreateInput(CrdtInput input, CrdtBounds bounds)
    {
        var payload = CrdtCodec.EncodeInput(input, bounds);
        return new(
            CrdtContracts.InputContractId,
            CrdtContracts.SchemaVersion,
            ContentType,
            payload,
            ComputePayloadHash(payload));
    }

    /// <summary>Computes the payload hash format used by occasionally connected envelopes.</summary>
    /// <param name="payload">The payload bytes.</param>
    /// <returns>The encoded payload hash.</returns>
    internal static string ComputePayloadHash(ReadOnlySpan<byte> payload)
    {
#if NET5_0_OR_GREATER
        var hash = SHA256.HashData(payload);
#else
        using var sha256 = SHA256.Create();
        var hash = sha256.ComputeHash(payload.ToArray());
#endif
        return Sha256Prefix + Convert.ToBase64String(hash);
    }

    /// <summary>Tries to decode a CRDT state payload envelope.</summary>
    /// <param name="envelope">The payload envelope.</param>
    /// <param name="kind">The registered CRDT kind.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <param name="state">The decoded state when validation succeeds.</param>
    /// <param name="reasonCode">The stable rejection reason when validation fails.</param>
    /// <returns>Whether the payload was decoded.</returns>
    internal static bool TryDecodeState(
        PayloadEnvelope envelope,
        CrdtKind kind,
        CrdtBounds bounds,
        out CrdtState state,
        out string reasonCode)
    {
        state = CrdtFunctions.Empty(kind);
        if (!ValidateEnvelope(envelope, CrdtContracts.StateContractId, bounds, out reasonCode))
        {
            return false;
        }

        try
        {
            state = CrdtCodec.DecodeState(envelope.Payload, bounds);
        }
        catch (Exception exception) when (exception is InvalidOperationException or ArgumentException or OverflowException)
        {
            reasonCode = "crdt-invalid-state";
            return false;
        }

        if (state.Kind == kind)
        {
            return true;
        }

        reasonCode = "crdt-state-kind-mismatch";
        return false;
    }

    /// <summary>Tries to decode a CRDT input payload envelope.</summary>
    /// <param name="envelope">The payload envelope.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <param name="input">The decoded input when validation succeeds.</param>
    /// <param name="reasonCode">The stable rejection reason when validation fails.</param>
    /// <returns>Whether the payload was decoded.</returns>
    internal static bool TryDecodeInput(
        PayloadEnvelope envelope,
        CrdtBounds bounds,
        out CrdtInput input,
        out string reasonCode)
    {
        input = CrdtInput.ForMutation(CrdtMutation.LwwRegisterSet(Array.Empty<byte>()));
        if (!ValidateEnvelope(envelope, CrdtContracts.InputContractId, bounds, out reasonCode))
        {
            return false;
        }

        try
        {
            input = CrdtCodec.DecodeInput(envelope.Payload, bounds);
            return true;
        }
        catch (Exception exception) when (exception is InvalidOperationException or ArgumentException or OverflowException)
        {
            reasonCode = "crdt-invalid-mutation";
            return false;
        }
    }

    /// <summary>Validates common CRDT payload envelope metadata.</summary>
    /// <param name="envelope">The envelope.</param>
    /// <param name="contractId">The expected contract.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <param name="reasonCode">The stable rejection reason.</param>
    /// <returns>Whether the envelope metadata is valid.</returns>
    private static bool ValidateEnvelope(
        PayloadEnvelope envelope,
        string contractId,
        CrdtBounds bounds,
        out string reasonCode)
    {
        ArgumentExceptionHelper.ThrowIfNull(envelope);
        if (!string.Equals(envelope.ContractId, contractId, StringComparison.Ordinal)
            || envelope.SchemaVersion != CrdtContracts.SchemaVersion
            || !string.Equals(envelope.ContentType, ContentType, StringComparison.Ordinal))
        {
            reasonCode = "crdt-contract-mismatch";
            return false;
        }

        if (envelope.PayloadLength > bounds.MaximumEncodedBytes)
        {
            reasonCode = "crdt-invalid-state";
            return false;
        }

        var computed = ComputePayloadHash(envelope.Payload.Span);
        if (envelope.PayloadHash.Length == Sha256HashLength && FixedTimeEquals(envelope.PayloadHash, computed))
        {
            reasonCode = string.Empty;
            return true;
        }

        reasonCode = "crdt-payload-hash-mismatch";
        return false;
    }

    /// <summary>Compares two hashes without early exit.</summary>
    /// <param name="left">The left hash.</param>
    /// <param name="right">The right hash.</param>
    /// <returns>Whether the values are equal.</returns>
    private static bool FixedTimeEquals(string left, string right)
    {
        var leftBytes = Encoding.UTF8.GetBytes(left);
        var rightBytes = Encoding.UTF8.GetBytes(right);
#if NETFRAMEWORK
        var difference = leftBytes.Length ^ rightBytes.Length;
        var count = Math.Min(leftBytes.Length, rightBytes.Length);
        for (var index = 0; index < count; index++)
        {
            difference |= leftBytes[index] ^ rightBytes[index];
        }

        return difference == 0;
#else
        return CryptographicOperations.FixedTimeEquals(leftBytes, rightBytes);
#endif
    }
}
