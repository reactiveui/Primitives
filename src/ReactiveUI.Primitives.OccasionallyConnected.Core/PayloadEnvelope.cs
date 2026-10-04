// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Stores durable payload bytes together with the schema metadata needed to read them.</summary>
[DebuggerDisplay("{ContractId,nq} v{SchemaVersion,nq}")]
public sealed record PayloadEnvelope
{
    /// <summary>The privately owned payload bytes.</summary>
    private readonly byte[] _payload = [];

    /// <summary>Initializes a new instance of the <see cref="PayloadEnvelope"/> class.</summary>
    /// <param name="contractId">The stable wire contract identifier.</param>
    /// <param name="schemaVersion">The positive contract schema version.</param>
    /// <param name="contentType">The serializer content type.</param>
    /// <param name="payload">The stored canonical payload bytes.</param>
    /// <param name="payloadHash">The cryptographic hash for <paramref name="payload"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="contractId"/>, <paramref name="contentType"/>, or <paramref name="payloadHash"/> is <see langword="null"/>.</exception>
    public PayloadEnvelope(string contractId, int schemaVersion, string contentType, ReadOnlyMemory<byte> payload, string payloadHash)
    {
        ArgumentExceptionHelper.ThrowIfNull(contractId);
        ArgumentExceptionHelper.ThrowIfNull(contentType);
        ArgumentExceptionHelper.ThrowIfNull(payloadHash);

        ContractId = contractId;
        SchemaVersion = schemaVersion;
        ContentType = contentType;
        Payload = payload;
        PayloadHash = payloadHash;
    }

    /// <summary>Gets the stable wire contract identifier.</summary>
    public string ContractId { get; init; }

    /// <summary>Gets the positive contract schema version.</summary>
    public int SchemaVersion { get; init; }

    /// <summary>Gets the serializer content type.</summary>
    public string ContentType { get; init; }

    /// <summary>Gets an owned copy of the stored canonical payload bytes.</summary>
    public ReadOnlyMemory<byte> Payload
    {
        get => _payload.AsSpan().ToArray();
        init => _payload = value.ToArray();
    }

    /// <summary>Gets the encoded byte count without allocating a payload copy.</summary>
    public int PayloadLength => _payload.Length;

    /// <summary>Gets the cryptographic hash for <see cref="Payload"/>.</summary>
    public string PayloadHash { get; init; }

    /// <summary>Copies a bounded prefix of the stored canonical payload bytes.</summary>
    /// <param name="maximumBytes">The maximum number of bytes to copy.</param>
    /// <returns>An owned payload prefix copy.</returns>
    internal ReadOnlyMemory<byte> CopyPayloadPrefix(int maximumBytes)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(maximumBytes);
        var prefixLength = Math.Min(_payload.Length, maximumBytes);
        return _payload.AsSpan(0, prefixLength).ToArray();
    }
}
