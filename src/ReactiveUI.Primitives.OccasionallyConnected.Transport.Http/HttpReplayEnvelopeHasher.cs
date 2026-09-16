// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.IO;
using System.Net;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Hashes replay request envelopes and verifies replay MAC values.</summary>
internal sealed class HttpReplayEnvelopeHasher
{
    /// <summary>The bytes needed to encode one length prefix.</summary>
    private const int LengthPrefixBytes = 4;

    /// <summary>The bit count in one byte.</summary>
    private const int BitsPerByte = 8;

    /// <summary>The maximum canonical request byte count accepted by this hasher.</summary>
    private readonly int _maximumCanonicalRequestBytes;

    /// <summary>Initializes a new instance of the <see cref="HttpReplayEnvelopeHasher"/> class.</summary>
    internal HttpReplayEnvelopeHasher()
        : this(new HttpReplayProtectionOptions())
    {
    }

    /// <summary>Initializes a new instance of the <see cref="HttpReplayEnvelopeHasher"/> class.</summary>
    /// <param name="options">The replay protection options.</param>
    internal HttpReplayEnvelopeHasher(HttpReplayProtectionOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        options.Validate();
        _maximumCanonicalRequestBytes = options.MaximumCanonicalRequestBytes;
    }

    /// <summary>Creates an owned replay envelope fingerprint for a canonical request.</summary>
    /// <param name="request">The replay request.</param>
    /// <returns>The owned replay envelope.</returns>
    /// <exception cref="ArgumentNullException">The request or canonical request is null.</exception>
    /// <exception cref="HttpRemoteTransportException">Input is invalid or too large.</exception>
    internal HttpReplayEnvelope Create(HttpReplayRequest request)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        ArgumentExceptionHelper.ThrowIfNull(request.CanonicalRequest);
        ValidateHeader(request.MessageId);
        ValidateHeader(request.Nonce);
        var replayHeaders = ValidateReplayHeaders(request);
        var canonicalBytes = request.CanonicalRequest.Bytes;
        if (canonicalBytes.Length > _maximumCanonicalRequestBytes)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge, HttpStatusCode.RequestEntityTooLarge);
        }

        var requestHash = ComputeHash(canonicalBytes);
        var macInput = CreateMacInput(request, requestHash, replayHeaders.SessionId);
        var fingerprintInput = CreateFingerprintInput(macInput, replayHeaders.Mac);
        var envelopeFingerprint = ComputeHash(fingerprintInput);
        return new(requestHash, macInput, envelopeFingerprint, canonicalBytes.Length, replayHeaders.SessionId, replayHeaders.Mac);
    }

    /// <summary>Computes the replay MAC header value for a session secret and MAC input.</summary>
    /// <param name="sessionSecret">The session secret bytes.</param>
    /// <param name="macInput">The canonical replay MAC input.</param>
    /// <returns>The replay MAC header value.</returns>
    /// <exception cref="HttpRemoteTransportException">Input is invalid.</exception>
    internal string ComputeMac(ReadOnlyMemory<byte> sessionSecret, ReadOnlyMemory<byte> macInput)
    {
        if (sessionSecret.IsEmpty || macInput.IsEmpty || macInput.Length > _maximumCanonicalRequestBytes)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.Authentication, HttpStatusCode.Unauthorized);
        }

        var key = Copy(sessionSecret);
        var input = Copy(macInput);
        using var hmac = new HMACSHA256(key);
        var hash = hmac.ComputeHash(input);
        return HttpReplayBase64Url.Encode(hash);
    }

    /// <summary>Compares two fingerprints without data-dependent early exit.</summary>
    /// <param name="left">The first fingerprint.</param>
    /// <param name="right">The second fingerprint.</param>
    /// <returns>Whether both byte sequences match.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool FixedTimeEquals(ReadOnlyMemory<byte> left, ReadOnlyMemory<byte> right) =>
        left.Length <= _maximumCanonicalRequestBytes
        && left.Length == right.Length
        && HttpReplayCryptography.FixedTimeEquals(left.Span, right.Span);

    /// <summary>Creates the framed MAC input for a replay request.</summary>
    /// <param name="request">The replay request.</param>
    /// <param name="requestHash">The canonical request hash.</param>
    /// <param name="replaySessionId">The validated replay session identifier field.</param>
    /// <returns>The MAC input bytes.</returns>
    private static byte[] CreateMacInput(HttpReplayRequest request, ReadOnlyMemory<byte> requestHash, string replaySessionId)
    {
        using MemoryStream stream = new();
        WriteField(stream, request.Operation.ToString());
        WriteField(stream, request.Principal.TenantId);
        WriteField(stream, request.Principal.ClientId);
        WriteField(stream, request.MessageId);
        WriteField(stream, request.Nonce);
        WriteField(stream, request.SentAtUtc.UtcDateTime.Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture));
        WriteField(stream, replaySessionId);
        WriteField(stream, requestHash);
        return stream.ToArray();
    }

    /// <summary>Creates the framed envelope fingerprint input.</summary>
    /// <param name="macInput">The MAC input bytes.</param>
    /// <param name="replayMac">The replay MAC text.</param>
    /// <returns>The fingerprint input bytes.</returns>
    private static byte[] CreateFingerprintInput(ReadOnlyMemory<byte> macInput, string replayMac)
    {
        using MemoryStream stream = new();
        WriteField(stream, macInput);
        WriteField(stream, replayMac);
        return stream.ToArray();
    }

    /// <summary>Computes SHA-256 over memory.</summary>
    /// <param name="source">The source bytes.</param>
    /// <returns>The hash bytes.</returns>
    private static byte[] ComputeHash(ReadOnlyMemory<byte> source)
    {
#if NET6_0_OR_GREATER
        return SHA256.HashData(source.Span);
#else
        using var sha256 = SHA256.Create();
        return sha256.ComputeHash(Copy(source));
#endif
    }

    /// <summary>Copies memory into an owned byte array.</summary>
    /// <param name="source">The source bytes.</param>
    /// <returns>The copied bytes.</returns>
    private static byte[] Copy(ReadOnlyMemory<byte> source)
    {
        var copy = new byte[source.Length];
        source.CopyTo(copy);
        return copy;
    }

    /// <summary>Validates replay session and MAC header presence for operations that require them.</summary>
    /// <param name="request">The replay request.</param>
    /// <returns>The validated replay header fields.</returns>
    /// <exception cref="HttpRemoteTransportException">A required header is missing or invalid.</exception>
    private static ReplayHeaderFields ValidateReplayHeaders(HttpReplayRequest request)
    {
        if (request.Operation == HttpReplayOperationKind.Connect)
        {
            return new(string.Empty, string.Empty);
        }

        if (request.ReplaySessionId is not { } replaySessionId || request.ReplayMac is not { } replayMac)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected, HttpStatusCode.BadRequest);
        }

        ValidateHeader(replaySessionId);
        ValidateHeader(replayMac);
        return new(replaySessionId, replayMac);
    }

    /// <summary>Validates an opaque replay header value.</summary>
    /// <param name="value">The candidate header value.</param>
    /// <exception cref="HttpRemoteTransportException">The header value is empty or contains a control character.</exception>
    private static void ValidateHeader(string value)
    {
        if (!string.IsNullOrWhiteSpace(value) && !ContainsControl(value))
        {
            return;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected, HttpStatusCode.BadRequest);
    }

    /// <summary>Writes a framed UTF-8 field.</summary>
    /// <param name="stream">The destination stream.</param>
    /// <param name="value">The field value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteField(Stream stream, string value) => WriteField(stream, Encoding.UTF8.GetBytes(value));

    /// <summary>Writes a framed byte field.</summary>
    /// <param name="stream">The destination stream.</param>
    /// <param name="value">The field value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteField(Stream stream, ReadOnlyMemory<byte> value)
    {
        var length = new byte[LengthPrefixBytes];
        for (var index = 0; index < length.Length; index++)
        {
            var shift = (length.Length - index - 1) * BitsPerByte;
            length[index] = (byte)(value.Length >> shift);
        }

        var bytes = Copy(value);
        stream.Write(length, 0, length.Length);
        stream.Write(bytes, 0, bytes.Length);
    }

    /// <summary>Checks whether text contains a control character.</summary>
    /// <param name="value">The candidate text.</param>
    /// <returns>Whether a control character is present.</returns>
    private static bool ContainsControl(string value)
    {
        for (var index = 0; index < value.Length; index++)
        {
            if (char.IsControl(value[index]))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Validated replay header fields used in replay hash framing.</summary>
    /// <param name="sessionId">The replay session identifier field.</param>
    /// <param name="mac">The replay MAC field.</param>
    private readonly struct ReplayHeaderFields(string sessionId, string mac)
    {
        /// <summary>Gets the replay session identifier field.</summary>
        internal string SessionId { get; } = sessionId;

        /// <summary>Gets the replay MAC field.</summary>
        internal string Mac { get; } = mac;
    }
}
