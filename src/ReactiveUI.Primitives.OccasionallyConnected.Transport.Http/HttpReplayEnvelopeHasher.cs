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

    /// <summary>The unpadded Base64URL length of a SHA-256 HMAC.</summary>
    private const int ReplayMacLength = 43;

    /// <summary>The strict UTF-8 encoding used for replay identity material.</summary>
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(false, true);

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

    /// <summary>Validates inbound replay MAC syntax before session lookup.</summary>
    /// <param name="request">The replay request.</param>
    /// <exception cref="HttpRemoteTransportException">The replay MAC is missing or malformed.</exception>
    internal static void ValidateReplayMacSyntax(HttpReplayRequest request)
    {
        if (request.Operation == HttpReplayOperationKind.Connect)
        {
            return;
        }

        if (request.ReplayMac is not { } replayMac)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected, HttpStatusCode.BadRequest);
        }

        ValidateReplayMac(replayMac);
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
        var replayPlan = CreateReplayPlan(request, _maximumCanonicalRequestBytes);
        var replayHeaders = ValidateReplayHeaders(request);
        var replaySessionIdBytes = request.Operation == HttpReplayOperationKind.Connect ? 0 : GetBoundedHeaderByteCount(replayHeaders.SessionId, _maximumCanonicalRequestBytes);
        var replayMacBytes = request.Operation == HttpReplayOperationKind.Connect ? 0 : GetBoundedHeaderByteCount(replayHeaders.Mac, _maximumCanonicalRequestBytes);
        var canonicalBytes = request.CanonicalRequest.Bytes;
        if (canonicalBytes.Length > _maximumCanonicalRequestBytes)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge, HttpStatusCode.RequestEntityTooLarge);
        }

        var requestHash = ComputeHash(canonicalBytes);
        var macInputLength = GetMacInputLength(replayPlan, replaySessionIdBytes, requestHash.Length, _maximumCanonicalRequestBytes);
        var fingerprintInputLength = GetFingerprintInputLength(macInputLength, replayMacBytes, _maximumCanonicalRequestBytes);
        var macInput = CreateMacInput(request, requestHash, replayHeaders.SessionId, macInputLength);
        var fingerprintInput = CreateFingerprintInput(macInput, replayHeaders.Mac, fingerprintInputLength);
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
    /// <param name="capacity">The preflighted MAC input capacity.</param>
    /// <returns>The MAC input bytes.</returns>
    private static byte[] CreateMacInput(HttpReplayRequest request, ReadOnlyMemory<byte> requestHash, string replaySessionId, int capacity)
    {
        using MemoryStream stream = new(capacity);
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
    /// <param name="capacity">The preflighted fingerprint input capacity.</param>
    /// <returns>The fingerprint input bytes.</returns>
    private static byte[] CreateFingerprintInput(ReadOnlyMemory<byte> macInput, string replayMac, int capacity)
    {
        using MemoryStream stream = new(capacity);
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

        return new(replaySessionId, replayMac);
    }

    /// <summary>Validates replay MAC syntax before session lookup.</summary>
    /// <param name="replayMac">The replay MAC value.</param>
    /// <exception cref="HttpRemoteTransportException">The replay MAC is malformed.</exception>
    private static void ValidateReplayMac(string replayMac)
    {
        if (replayMac.Length != ReplayMacLength)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected, HttpStatusCode.BadRequest);
        }

        for (var index = 0; index < replayMac.Length; index++)
        {
            if (!IsBase64UrlCharacter(replayMac[index]))
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected, HttpStatusCode.BadRequest);
            }
        }
    }

    /// <summary>Creates the bounded replay MAC field plan before allocating framed inputs.</summary>
    /// <param name="request">The replay request.</param>
    /// <param name="maximumBytes">The maximum accepted byte count.</param>
    /// <returns>The replay field plan.</returns>
    /// <exception cref="HttpRemoteTransportException">A replay field is invalid or too large.</exception>
    private static ReplayFieldPlan CreateReplayPlan(HttpReplayRequest request, int maximumBytes)
    {
        var operationBytes = GetBoundedTextByteCount(request.Operation.ToString(), maximumBytes);
        var tenantBytes = GetBoundedHeaderByteCount(request.Principal.TenantId, maximumBytes);
        var clientBytes = GetBoundedHeaderByteCount(request.Principal.ClientId, maximumBytes);
        var messageBytes = GetBoundedHeaderByteCount(request.MessageId, maximumBytes);
        var nonceBytes = GetBoundedHeaderByteCount(request.Nonce, maximumBytes);
        var sentAtBytes = GetBoundedTextByteCount(
            request.SentAtUtc.UtcDateTime.Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture),
            maximumBytes);
        return new(operationBytes, tenantBytes, clientBytes, messageBytes, nonceBytes, sentAtBytes);
    }

    /// <summary>Gets the preflighted MAC input length.</summary>
    /// <param name="plan">The replay field plan.</param>
    /// <param name="replaySessionIdBytes">The replay session identifier byte count.</param>
    /// <param name="requestHashBytes">The request hash byte count.</param>
    /// <param name="maximumBytes">The maximum accepted frame byte count.</param>
    /// <returns>The MAC input length.</returns>
    /// <exception cref="HttpRemoteTransportException">The framed MAC input is too large.</exception>
    private static int GetMacInputLength(ReplayFieldPlan plan, int replaySessionIdBytes, int requestHashBytes, int maximumBytes)
    {
        long length = 0;
        length = AddFramedLength(length, plan.OperationBytes, maximumBytes);
        length = AddFramedLength(length, plan.TenantBytes, maximumBytes);
        length = AddFramedLength(length, plan.ClientBytes, maximumBytes);
        length = AddFramedLength(length, plan.MessageBytes, maximumBytes);
        length = AddFramedLength(length, plan.NonceBytes, maximumBytes);
        length = AddFramedLength(length, plan.SentAtBytes, maximumBytes);
        length = AddFramedLength(length, replaySessionIdBytes, maximumBytes);
        length = AddFramedLength(length, requestHashBytes, maximumBytes);
        return (int)length;
    }

    /// <summary>Gets the preflighted envelope fingerprint input length.</summary>
    /// <param name="macInputBytes">The MAC input byte count.</param>
    /// <param name="replayMacBytes">The replay MAC byte count.</param>
    /// <param name="maximumBytes">The maximum accepted frame byte count.</param>
    /// <returns>The fingerprint input length.</returns>
    /// <exception cref="HttpRemoteTransportException">The framed fingerprint input is too large.</exception>
    private static int GetFingerprintInputLength(int macInputBytes, int replayMacBytes, int maximumBytes)
    {
        long length = 0;
        length = AddFramedLength(length, macInputBytes, maximumBytes);
        length = AddFramedLength(length, replayMacBytes, maximumBytes);
        return (int)length;
    }

    /// <summary>Adds a framed field length to a bounded total.</summary>
    /// <param name="length">The current total.</param>
    /// <param name="fieldBytes">The field byte count.</param>
    /// <param name="maximumBytes">The maximum accepted total.</param>
    /// <returns>The updated total.</returns>
    /// <exception cref="HttpRemoteTransportException">The framed length exceeds the accepted total.</exception>
    private static long AddFramedLength(long length, int fieldBytes, int maximumBytes)
    {
        var updated = length + LengthPrefixBytes + fieldBytes;
        if (updated <= maximumBytes)
        {
            return updated;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge, HttpStatusCode.RequestEntityTooLarge);
    }

    /// <summary>Gets a bounded UTF-8 byte count for a required replay header.</summary>
    /// <param name="value">The candidate header value.</param>
    /// <param name="maximumBytes">The maximum accepted byte count.</param>
    /// <returns>The strict UTF-8 byte count.</returns>
    /// <exception cref="HttpRemoteTransportException">The header is invalid or too large.</exception>
    private static int GetBoundedHeaderByteCount(string value, int maximumBytes)
    {
        if (string.IsNullOrEmpty(value))
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected, HttpStatusCode.BadRequest);
        }

        if (value.Length > maximumBytes)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge, HttpStatusCode.RequestEntityTooLarge);
        }

        if (string.IsNullOrWhiteSpace(value) || ContainsControl(value))
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected, HttpStatusCode.BadRequest);
        }

        return GetBoundedTextByteCount(value, maximumBytes);
    }

    /// <summary>Checks whether a character belongs to the unpadded Base64URL alphabet.</summary>
    /// <param name="character">The candidate character.</param>
    /// <returns>Whether the character is valid Base64URL text.</returns>
    private static bool IsBase64UrlCharacter(char character) =>
        character is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or >= '0' and <= '9' or '-' or '_';

    /// <summary>Gets a bounded strict UTF-8 byte count.</summary>
    /// <param name="value">The candidate text.</param>
    /// <param name="maximumBytes">The maximum accepted byte count.</param>
    /// <returns>The strict UTF-8 byte count.</returns>
    /// <exception cref="HttpRemoteTransportException">The text is invalid or too large.</exception>
    private static int GetBoundedTextByteCount(string value, int maximumBytes)
    {
        if (value.Length > maximumBytes)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge, HttpStatusCode.RequestEntityTooLarge);
        }

        try
        {
            var byteCount = StrictUtf8.GetByteCount(value);
            if (byteCount <= maximumBytes)
            {
                return byteCount;
            }
        }
        catch (EncoderFallbackException exception)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected, HttpStatusCode.BadRequest, retryAfter: null, innerException: exception);
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge, HttpStatusCode.RequestEntityTooLarge);
    }

    /// <summary>Writes a framed UTF-8 field.</summary>
    /// <param name="stream">The destination stream.</param>
    /// <param name="value">The field value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteField(Stream stream, string value) => WriteField(stream, StrictUtf8.GetBytes(value));

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

    /// <summary>Preflighted byte counts for replay MAC fields.</summary>
    /// <param name="operationBytes">The operation field byte count.</param>
    /// <param name="tenantBytes">The tenant field byte count.</param>
    /// <param name="clientBytes">The client field byte count.</param>
    /// <param name="messageBytes">The message field byte count.</param>
    /// <param name="nonceBytes">The nonce field byte count.</param>
    /// <param name="sentAtBytes">The sent timestamp field byte count.</param>
    private readonly struct ReplayFieldPlan(
        int operationBytes,
        int tenantBytes,
        int clientBytes,
        int messageBytes,
        int nonceBytes,
        int sentAtBytes)
    {
        /// <summary>Gets the operation field byte count.</summary>
        internal int OperationBytes { get; } = operationBytes;

        /// <summary>Gets the tenant field byte count.</summary>
        internal int TenantBytes { get; } = tenantBytes;

        /// <summary>Gets the client field byte count.</summary>
        internal int ClientBytes { get; } = clientBytes;

        /// <summary>Gets the message field byte count.</summary>
        internal int MessageBytes { get; } = messageBytes;

        /// <summary>Gets the nonce field byte count.</summary>
        internal int NonceBytes { get; } = nonceBytes;

        /// <summary>Gets the sent timestamp field byte count.</summary>
        internal int SentAtBytes { get; } = sentAtBytes;
    }
}
