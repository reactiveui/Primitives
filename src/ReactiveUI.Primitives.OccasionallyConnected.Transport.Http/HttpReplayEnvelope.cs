// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Contains owned replay hashes derived from one canonical request envelope.</summary>
internal sealed class HttpReplayEnvelope
{
    /// <summary>The owned canonical request hash.</summary>
    private readonly byte[] _requestHash;

    /// <summary>The owned replay MAC input bytes.</summary>
    private readonly byte[] _macInput;

    /// <summary>The owned envelope fingerprint.</summary>
    private readonly byte[] _envelopeFingerprint;

    /// <summary>Initializes a new instance of the <see cref="HttpReplayEnvelope"/> class.</summary>
    /// <param name="requestHash">The canonical request hash.</param>
    /// <param name="macInput">The replay MAC input bytes.</param>
    /// <param name="envelopeFingerprint">The replay envelope fingerprint.</param>
    /// <param name="canonicalByteCount">The canonical request byte count.</param>
    /// <param name="replaySessionId">The validated replay session identifier field.</param>
    /// <param name="replayMac">The validated replay MAC field.</param>
    internal HttpReplayEnvelope(
        ReadOnlyMemory<byte> requestHash,
        ReadOnlyMemory<byte> macInput,
        ReadOnlyMemory<byte> envelopeFingerprint,
        int canonicalByteCount,
        string replaySessionId,
        string replayMac)
    {
        _requestHash = Copy(requestHash);
        _macInput = Copy(macInput);
        _envelopeFingerprint = Copy(envelopeFingerprint);
        CanonicalByteCount = canonicalByteCount;
        ReplaySessionId = replaySessionId;
        ReplayMac = replayMac;
    }

    /// <summary>Gets the owned canonical request hash.</summary>
    internal ReadOnlyMemory<byte> RequestHash => Copy(_requestHash);

    /// <summary>Gets the owned replay MAC input bytes.</summary>
    internal ReadOnlyMemory<byte> MacInput => Copy(_macInput);

    /// <summary>Gets the owned replay envelope fingerprint.</summary>
    internal ReadOnlyMemory<byte> EnvelopeFingerprint => Copy(_envelopeFingerprint);

    /// <summary>Gets the canonical request byte count.</summary>
    internal int CanonicalByteCount { get; }

    /// <summary>Gets the validated replay session identifier field.</summary>
    internal string ReplaySessionId { get; }

    /// <summary>Gets the validated replay MAC field.</summary>
    internal string ReplayMac { get; }

    /// <summary>Copies memory without LINQ.</summary>
    /// <param name="source">The source bytes.</param>
    /// <returns>The copied bytes.</returns>
    private static byte[] Copy(ReadOnlyMemory<byte> source)
    {
        var copy = new byte[source.Length];
        source.CopyTo(copy);
        return copy;
    }
}
