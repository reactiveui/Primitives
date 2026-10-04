// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Stores bounded evidence for a quarantined payload without exposing raw payloads through exceptions or logs.</summary>
[System.Diagnostics.DebuggerDisplay("{ContractId,nq} Length={PayloadLength,nq}")]
public sealed record LocalPayloadQuarantineEvidence
{
    /// <summary>The owned payload prefix bytes.</summary>
    private byte[] _payloadPrefix = [];

    /// <summary>Initializes a new instance of the <see cref="LocalPayloadQuarantineEvidence"/> class.</summary>
    /// <param name="contractId">The payload contract identifier, when it could be read.</param>
    /// <param name="schemaVersion">The payload schema version, when it could be read.</param>
    /// <param name="contentType">The payload content type, when it could be read.</param>
    /// <param name="payloadLength">The original payload length.</param>
    /// <param name="payloadHash">The declared payload hash, when it could be read.</param>
    /// <param name="payloadPrefix">The bounded payload evidence prefix.</param>
    public LocalPayloadQuarantineEvidence(
        string? contractId,
        int? schemaVersion,
        string? contentType,
        int payloadLength,
        string? payloadHash,
        ReadOnlyMemory<byte> payloadPrefix)
    {
        ContractId = contractId;
        SchemaVersion = schemaVersion;
        ContentType = contentType;
        PayloadLength = payloadLength;
        PayloadHash = payloadHash;
        PayloadPrefix = payloadPrefix;
    }

    /// <summary>Gets the payload contract identifier, when it could be read.</summary>
    public string? ContractId { get; init; }

    /// <summary>Gets the payload schema version, when it could be read.</summary>
    public int? SchemaVersion { get; init; }

    /// <summary>Gets the payload content type, when it could be read.</summary>
    public string? ContentType { get; init; }

    /// <summary>Gets the original payload length.</summary>
    public int PayloadLength { get; init; }

    /// <summary>Gets the declared payload hash, when it could be read.</summary>
    public string? PayloadHash { get; init; }

    /// <summary>Gets an owned copy of the bounded payload byte prefix.</summary>
    public ReadOnlyMemory<byte> PayloadPrefix
    {
        get => CopyPayloadPrefix(_payloadPrefix.Length);
        init => _payloadPrefix = CopyPayloadPrefix(value);
    }

    /// <summary>Gets the owned payload prefix length without copying it.</summary>
    internal int PayloadPrefixLength => _payloadPrefix.Length;

    /// <summary>Copies at most the requested number of owned payload prefix bytes.</summary>
    /// <param name="maximumBytes">The maximum number of bytes to copy.</param>
    /// <returns>The copied payload prefix.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maximumBytes"/> is negative.</exception>
    internal byte[] CopyPayloadPrefix(int maximumBytes)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(maximumBytes);
        var prefixLength = Math.Min(maximumBytes, _payloadPrefix.Length);
        if (prefixLength == 0)
        {
            return [];
        }

        var prefix = new byte[prefixLength];
        Array.Copy(_payloadPrefix, prefix, prefixLength);
        return prefix;
    }

    /// <summary>Copies memory into an owned array.</summary>
    /// <param name="payloadPrefix">The payload prefix to copy.</param>
    /// <returns>The owned payload prefix.</returns>
    private static byte[] CopyPayloadPrefix(ReadOnlyMemory<byte> payloadPrefix)
    {
        if (payloadPrefix.IsEmpty)
        {
            return [];
        }

        var prefix = new byte[payloadPrefix.Length];
        payloadPrefix.Span.CopyTo(prefix);
        return prefix;
    }
}
