// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Security.Cryptography;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpReplayEnvelopeHasher"/>.</summary>
public sealed class HttpReplayEnvelopeHasherTests
{
    /// <summary>The stable replay message identifier.</summary>
    private const string MessageId = "11111111-1111-1111-1111-111111111111";

    /// <summary>The stable replay nonce.</summary>
    private const string Nonce = "nonce-1";

    /// <summary>The stable replay session identifier.</summary>
    private const string ReplaySessionId = "session-1";

    /// <summary>The stable replay MAC.</summary>
    private const string ReplayMac = "mac-1";

    /// <summary>The changed replay field marker.</summary>
    private const string Changed = "changed";

    /// <summary>The replay message field selector.</summary>
    private const string MessageField = "message";

    /// <summary>The replay nonce field selector.</summary>
    private const string NonceField = "nonce";

    /// <summary>The replay timestamp field selector.</summary>
    private const string TimestampField = "timestamp";

    /// <summary>The replay session field selector.</summary>
    private const string SessionField = "session";

    /// <summary>The replay MAC field selector.</summary>
    private const string MacField = "mac";

    /// <summary>The canonical method field selector.</summary>
    private const string MethodField = "method";

    /// <summary>The canonical path field selector.</summary>
    private const string PathField = "path";

    /// <summary>The canonical query field selector.</summary>
    private const string QueryField = "query";

    /// <summary>The push endpoint path.</summary>
    private const string PushPath = "push";

    /// <summary>The alternate ack endpoint path.</summary>
    private const string AckPath = "ack";

    /// <summary>The stable POST method.</summary>
    private const string PostMethod = "POST";

    /// <summary>The alternate PUT method.</summary>
    private const string PutMethod = "PUT";

    /// <summary>The changed canonical query.</summary>
    private const string ChangedQuery = "cursor=changed";

    /// <summary>A header value containing a newline.</summary>
    private const string NewlineHeader = "bad\nvalue";

    /// <summary>The first adjacent ambiguous text.</summary>
    private const string AdjacentMessageOne = "ab";

    /// <summary>The second adjacent ambiguous text.</summary>
    private const string AdjacentMessageTwo = "a";

    /// <summary>The first adjacent nonce text.</summary>
    private const string AdjacentNonceOne = "c";

    /// <summary>The second adjacent nonce text.</summary>
    private const string AdjacentNonceTwo = "bc";

    /// <summary>The first ambiguous method text.</summary>
    private const string AmbiguousMethodOne = "AB";

    /// <summary>The second ambiguous method text.</summary>
    private const string AmbiguousMethodTwo = "A";

    /// <summary>The first ambiguous path text.</summary>
    private const string AmbiguousPathOne = "C";

    /// <summary>The second ambiguous path text.</summary>
    private const string AmbiguousPathTwo = "BC";

    /// <summary>The first body byte.</summary>
    private const byte BodyByteOne = 1;

    /// <summary>The second body byte.</summary>
    private const byte BodyByteTwo = 2;

    /// <summary>The third body byte.</summary>
    private const byte BodyByteThree = 3;

    /// <summary>The changed third body byte.</summary>
    private const byte BodyByteFour = 4;

    /// <summary>The line feed byte.</summary>
    private const byte LineFeedByte = (byte)'\n';

    /// <summary>The single byte limit.</summary>
    private const int SingleByteLimit = 1;

    /// <summary>The number of canonical separators.</summary>
    private const int CanonicalSeparatorCount = 3;

    /// <summary>The authenticated replay principal.</summary>
    private static readonly HttpReplayPrincipal Principal = new("tenant", "client");

    /// <summary>The replay timestamp.</summary>
    private static readonly DateTimeOffset SentAtUtc = new(2026, 9, 13, 12, 10, 0, TimeSpan.Zero);

    /// <summary>The unchanged body.</summary>
    private static readonly byte[] StableBody = [BodyByteOne, BodyByteTwo, BodyByteThree];

    /// <summary>The changed body.</summary>
    private static readonly byte[] ChangedBody = [BodyByteOne, BodyByteTwo, BodyByteFour];

    /// <summary>Verifies byte-identical replay envelopes compare equal.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CreateReturnsEqualFingerprintForByteIdenticalEnvelope()
    {
        HttpReplayEnvelopeHasher hasher = new();
        var first = hasher.Create(CreateRequest(StableBody));
        var second = hasher.Create(CreateRequest(StableBody));

        await Assert.That(hasher.FixedTimeEquals(first.EnvelopeFingerprint, second.EnvelopeFingerprint)).IsTrue();
    }

    /// <summary>Verifies nonce reuse with changed body bytes changes the envelope fingerprint.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CreateIncludesBodyHashInEnvelopeFingerprint()
    {
        HttpReplayEnvelopeHasher hasher = new();
        var first = hasher.Create(CreateRequest(StableBody));
        var second = hasher.Create(CreateRequest(ChangedBody));

        await Assert.That(hasher.FixedTimeEquals(first.EnvelopeFingerprint, second.EnvelopeFingerprint)).IsFalse();
    }

    /// <summary>Verifies every replay header field changes the envelope fingerprint independently.</summary>
    /// <param name="field">The field to change.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(MessageField)]
    [Arguments(NonceField)]
    [Arguments(TimestampField)]
    [Arguments(SessionField)]
    [Arguments(MacField)]
    public async Task CreateChangesFingerprintWhenOneReplayHeaderFieldChanges(string field)
    {
        HttpReplayEnvelopeHasher hasher = new();
        var original = hasher.Create(CreateRequest(StableBody));
        var changed = hasher.Create(ChangeField(CreateRequest(StableBody), field));

        await Assert.That(hasher.FixedTimeEquals(original.EnvelopeFingerprint, changed.EnvelopeFingerprint)).IsFalse();
    }

    /// <summary>Verifies every canonical request boundary field changes the envelope fingerprint independently.</summary>
    /// <param name="field">The canonical field to change.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(MethodField)]
    [Arguments(PathField)]
    [Arguments(QueryField)]
    public async Task CreateChangesFingerprintWhenOneCanonicalFieldChanges(string field)
    {
        HttpReplayEnvelopeHasher hasher = new();
        var original = hasher.Create(CreateRequest(StableBody));
        var changed = hasher.Create(ChangeCanonicalField(CreateRequest(StableBody), field));

        await Assert.That(hasher.FixedTimeEquals(original.EnvelopeFingerprint, changed.EnvelopeFingerprint)).IsFalse();
    }

    /// <summary>Verifies replay header newlines are rejected before ambiguous MAC input can be built.</summary>
    /// <param name="field">The field to corrupt.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(MessageField)]
    [Arguments(NonceField)]
    public async Task CreateRejectsNewlineReplayHeadersBeforeMacInput(string field)
    {
        HttpReplayEnvelopeHasher hasher = new();
        var request = ChangeField(CreateRequest(StableBody), field, NewlineHeader);

        await Assert.That(() => hasher.Create(request)).ThrowsExactly<HttpRemoteTransportException>();
    }

    /// <summary>Verifies missing signed replay headers are rejected before MAC input is built.</summary>
    /// <param name="field">The signed header field to omit.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(SessionField)]
    [Arguments(MacField)]
    public async Task CreateRejectsMissingSignedReplayHeadersBeforeMacInput(string field)
    {
        HttpReplayEnvelopeHasher hasher = new();
        var request = field == SessionField
            ? CreateRequest(StableBody) with { ReplaySessionId = null }
            : CreateRequest(StableBody) with { ReplayMac = null };

        await Assert.That(() => hasher.Create(request)).ThrowsExactly<HttpRemoteTransportException>();
    }

    /// <summary>Verifies canonical request bytes are bounded before request hashing.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CreateRejectsCanonicalBytesAboveLimit()
    {
        HttpReplayEnvelopeHasher hasher = new(new() { MaximumCanonicalRequestBytes = SingleByteLimit });

        await Assert.That(() => hasher.Create(CreateRequest(StableBody))).ThrowsExactly<HttpRemoteTransportException>();
    }

    /// <summary>Verifies MAC computation rejects missing or oversized protected input.</summary>
    /// <param name="fault">The MAC input fault.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments("empty-secret")]
    [Arguments("empty-input")]
    [Arguments("oversized-input")]
    public async Task ComputeMacRejectsInvalidProtectedInput(string fault)
    {
        HttpReplayEnvelopeHasher hasher = new(new() { MaximumCanonicalRequestBytes = SingleByteLimit });
        var secret = fault == "empty-secret" ? ReadOnlyMemory<byte>.Empty : StableBody;
        var input = fault switch
        {
            "empty-input" => ReadOnlyMemory<byte>.Empty,
            "oversized-input" => new byte[BodyByteTwo],
            _ => new byte[SingleByteLimit],
        };

        await Assert.That(() => hasher.ComputeMac(secret, input)).ThrowsExactly<HttpRemoteTransportException>();
    }

    /// <summary>Verifies fingerprint comparison rejects oversized and differently sized inputs.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task FixedTimeEqualsRejectsInvalidInputLengths()
    {
        HttpReplayEnvelopeHasher hasher = new(new() { MaximumCanonicalRequestBytes = SingleByteLimit });

        await Assert.That(hasher.FixedTimeEquals(new byte[BodyByteTwo], new byte[BodyByteTwo])).IsFalse();
        await Assert.That(hasher.FixedTimeEquals(new byte[SingleByteLimit], ReadOnlyMemory<byte>.Empty)).IsFalse();
    }

    /// <summary>Verifies adjacent replay header fields cannot collapse into the same MAC input.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CreateSeparatesAdjacentReplayHeaderBoundaries()
    {
        HttpReplayEnvelopeHasher hasher = new();
        var first = CreateRequest(StableBody) with { MessageId = AdjacentMessageOne, Nonce = AdjacentNonceOne };
        var second = CreateRequest(StableBody) with { MessageId = AdjacentMessageTwo, Nonce = AdjacentNonceTwo };
        var firstEnvelope = hasher.Create(first);
        var secondEnvelope = hasher.Create(second);

        await Assert.That(hasher.FixedTimeEquals(firstEnvelope.MacInput, secondEnvelope.MacInput)).IsFalse();
        await Assert.That(hasher.FixedTimeEquals(firstEnvelope.EnvelopeFingerprint, secondEnvelope.EnvelopeFingerprint)).IsFalse();
    }

    /// <summary>Verifies ambiguous canonical request field concatenations cannot share MAC input.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CreateSeparatesCanonicalRequestBoundaries()
    {
        HttpReplayEnvelopeHasher hasher = new();
        var first = CreateRequest(StableBody) with { CanonicalRequest = CreateCanonical(AmbiguousMethodOne, AmbiguousPathOne, string.Empty, StableBody) };
        var second = CreateRequest(StableBody) with { CanonicalRequest = CreateCanonical(AmbiguousMethodTwo, AmbiguousPathTwo, string.Empty, StableBody) };
        var firstEnvelope = hasher.Create(first);
        var secondEnvelope = hasher.Create(second);

        await Assert.That(hasher.FixedTimeEquals(firstEnvelope.MacInput, secondEnvelope.MacInput)).IsFalse();
        await Assert.That(hasher.FixedTimeEquals(firstEnvelope.EnvelopeFingerprint, secondEnvelope.EnvelopeFingerprint)).IsFalse();
    }

    /// <summary>Verifies connect fingerprints omit replay session and MAC material.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CreateUsesEmptySessionAndMacForConnect()
    {
        HttpReplayEnvelopeHasher hasher = new();
        var request = CreateRequest(StableBody) with { Operation = HttpReplayOperationKind.Connect, ReplaySessionId = null, ReplayMac = null };
        var envelope = hasher.Create(request);

        await Assert.That(envelope.MacInput.Length).IsGreaterThan(0);
    }

    /// <summary>Changes one replay request field while holding every other field fixed.</summary>
    /// <param name="request">The original replay request.</param>
    /// <param name="field">The field to change.</param>
    /// <param name="value">The replacement value.</param>
    /// <returns>The changed replay request.</returns>
    private static HttpReplayRequest ChangeField(HttpReplayRequest request, string field, string value = Changed) =>
        field switch
        {
            MessageField => request with { MessageId = value },
            NonceField => request with { Nonce = value },
            TimestampField => request with { SentAtUtc = SentAtUtc.AddTicks(BodyByteOne) },
            SessionField => request with { ReplaySessionId = value },
            _ => request with { ReplayMac = value },
        };

    /// <summary>Changes one canonical request field while holding every replay header fixed.</summary>
    /// <param name="request">The original replay request.</param>
    /// <param name="field">The canonical field to change.</param>
    /// <returns>The changed replay request.</returns>
    private static HttpReplayRequest ChangeCanonicalField(HttpReplayRequest request, string field)
    {
        var original = request.CanonicalRequest;
        var canonical = field switch
        {
            MethodField => CreateCanonical(PutMethod, original.NormalizedRelativePath, original.CanonicalQuery, StableBody),
            PathField => CreateCanonical(original.Method, AckPath, original.CanonicalQuery, StableBody),
            _ => CreateCanonical(original.Method, original.NormalizedRelativePath, ChangedQuery, StableBody),
        };

        return request with { CanonicalRequest = canonical };
    }

    /// <summary>Creates a replay request for hasher tests.</summary>
    /// <param name="body">The canonical body bytes.</param>
    /// <returns>The replay request.</returns>
    private static HttpReplayRequest CreateRequest(ReadOnlyMemory<byte> body)
    {
        var canonical = CreateCanonical(PostMethod, PushPath, string.Empty, body);
        return new()
        {
            Operation = HttpReplayOperationKind.Push,
            Principal = Principal,
            MessageId = MessageId,
            Nonce = Nonce,
            SentAtUtc = SentAtUtc,
            ReplaySessionId = ReplaySessionId,
            ReplayMac = ReplayMac,
            CanonicalRequest = canonical,
        };
    }

    /// <summary>Creates a canonical request whose hash and bytes are derived from the supplied body.</summary>
    /// <param name="method">The HTTP method.</param>
    /// <param name="path">The normalized relative path.</param>
    /// <param name="query">The canonical query.</param>
    /// <param name="body">The request body.</param>
    /// <returns>The canonical request.</returns>
    private static HttpCanonicalRequest CreateCanonical(string method, string path, string query, ReadOnlyMemory<byte> body)
    {
        var bodyHash = SHA256.HashData(body.Span);
        var methodBytes = Encoding.UTF8.GetBytes(method);
        var pathBytes = Encoding.UTF8.GetBytes(path);
        var queryBytes = Encoding.UTF8.GetBytes(query);
        var canonicalBytes = new byte[methodBytes.Length + pathBytes.Length + queryBytes.Length + bodyHash.Length + CanonicalSeparatorCount];
        var offset = 0;
        methodBytes.CopyTo(canonicalBytes.AsSpan(offset));
        offset += methodBytes.Length;
        canonicalBytes[offset] = LineFeedByte;
        offset++;
        pathBytes.CopyTo(canonicalBytes.AsSpan(offset));
        offset += pathBytes.Length;
        canonicalBytes[offset] = LineFeedByte;
        offset++;
        queryBytes.CopyTo(canonicalBytes.AsSpan(offset));
        offset += queryBytes.Length;
        canonicalBytes[offset] = LineFeedByte;
        offset++;
        bodyHash.CopyTo(canonicalBytes.AsSpan(offset));

        return new(method, path, query, bodyHash, canonicalBytes);
    }
}
