// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.InteropServices;
using System.Security.Cryptography;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpCanonicalRequestBuilder"/>.</summary>
public sealed class HttpCanonicalRequestBuilderTests
{
    /// <summary>The stream identifier query key.</summary>
    private const string StreamIdKey = "streamId";

    /// <summary>The subscription identifier query key.</summary>
    private const string SubscriptionIdKey = "subscriptionId";

    /// <summary>The cursor query key.</summary>
    private const string CursorKey = "cursor";

    /// <summary>The stable stream identifier.</summary>
    private const string StreamId = "stream-1";

    /// <summary>The subscription identifier.</summary>
    private const string SubscriptionId = "sub-1";

    /// <summary>The cursor identifier.</summary>
    private const string Cursor = "cursor-1";

    /// <summary>The subscribe endpoint path.</summary>
    private const string SubscribePath = "subscribe";

    /// <summary>The push endpoint path.</summary>
    private const string PushPath = "push";

    /// <summary>The HTTP GET method.</summary>
    private const string GetMethod = "GET";

    /// <summary>The HTTP POST method.</summary>
    private const string PostMethod = "POST";

    /// <summary>The first body byte.</summary>
    private const byte BodyByteOne = 1;

    /// <summary>The second body byte.</summary>
    private const byte BodyByteTwo = 2;

    /// <summary>The third body byte.</summary>
    private const byte BodyByteThree = 3;

    /// <summary>The caller mutation byte.</summary>
    private const byte CallerMutationByte = 9;

    /// <summary>The canonical memory mutation byte.</summary>
    private const byte CanonicalMutationByte = 8;

    /// <summary>The body hash memory mutation byte.</summary>
    private const byte BodyHashMutationByte = 7;

    /// <summary>The undefined replay operation value.</summary>
    private const int UndefinedOperationValue = 999;

    /// <summary>A canonical byte limit smaller than the minimal hashed envelope.</summary>
    private const int SmallCanonicalByteLimit = 40;

    /// <summary>The encoded slash path injection.</summary>
    private const string EncodedSlashPath = "push%2Fack";

    /// <summary>The encoded backslash path injection.</summary>
    private const string EncodedBackslashPath = "push%5Cack";

    /// <summary>The encoded newline path injection.</summary>
    private const string EncodedNewlinePath = "push%0Aack";

    /// <summary>A query value containing a control character.</summary>
    private const string ControlQueryValue = "cursor\n1";

    /// <summary>The escaped query value.</summary>
    private const string EscapedCursor = "a&streamId=b";

    /// <summary>The canonical escaped query value.</summary>
    private const string EscapedCanonicalQuery = "cursor=a%26streamId%3Db&streamId=stream-1&subscriptionId=sub-1";

    /// <summary>The expected ordered subscribe query.</summary>
    private const string OrderedSubscribeQuery = "cursor=cursor-1&streamId=stream-1&subscriptionId=sub-1";

    /// <summary>The replay timestamp.</summary>
    private static readonly DateTimeOffset SentAtUtc = new(2026, 9, 13, 12, 5, 0, TimeSpan.Zero);

    /// <summary>Verifies subscribe query keys are rendered once in ordinal order.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task BuildOrdersSubscribeQueryKeysOrdinally()
    {
        var builder = new HttpCanonicalRequestBuilder(new());
        KeyValuePair<string, string>[] query =
        [
            new(SubscriptionIdKey, SubscriptionId),
            new(StreamIdKey, StreamId),
            new(CursorKey, Cursor),
        ];

        var canonical = builder.Build(HttpReplayOperationKind.Subscribe, GetMethod, SubscribePath, query, SentAtUtc, ReadOnlyMemory<byte>.Empty);

        await Assert.That(canonical.Method).IsEqualTo(GetMethod);
        await Assert.That(canonical.CanonicalQuery).IsEqualTo(OrderedSubscribeQuery);
    }

    /// <summary>Verifies duplicate subscribe query keys are rejected before fingerprinting.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task BuildRejectsDuplicateSubscribeQueryKeys()
    {
        var builder = new HttpCanonicalRequestBuilder(new());
        KeyValuePair<string, string>[] query = [new(StreamIdKey, StreamId), new(StreamIdKey, "stream-2")];

        await Assert.That(() => builder.Build(HttpReplayOperationKind.Subscribe, GetMethod, SubscribePath, query, SentAtUtc, ReadOnlyMemory<byte>.Empty)).ThrowsExactly<HttpRemoteTransportException>();
    }

    /// <summary>Verifies encoded path separators and controls cannot alter canonical path boundaries.</summary>
    /// <param name="path">The unsafe relative path.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(EncodedSlashPath)]
    [Arguments(EncodedBackslashPath)]
    [Arguments(EncodedNewlinePath)]
    public async Task BuildRejectsEncodedPathBoundaryInjection(string path)
    {
        var builder = new HttpCanonicalRequestBuilder(new());

        await Assert.That(() => builder.Build(HttpReplayOperationKind.Push, PostMethod, path, [], SentAtUtc, CreateBody())).ThrowsExactly<HttpRemoteTransportException>();
    }

    /// <summary>Verifies the body is bounded before canonical bytes are allocated.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task BuildRejectsBodyAboveCanonicalRequestLimit()
    {
        var builder = new HttpCanonicalRequestBuilder(new() { MaximumCanonicalRequestBytes = BodyByteTwo });

        await Assert
            .That(() => builder.Build(HttpReplayOperationKind.Push, PostMethod, PushPath, [], SentAtUtc, CreateBody()))
            .ThrowsExactly<HttpRemoteTransportException>();
    }

    /// <summary>Verifies canonical request bytes are bounded after hashing and field normalization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task BuildRejectsCanonicalBytesAboveLimit()
    {
        var builder = new HttpCanonicalRequestBuilder(new() { MaximumCanonicalRequestBytes = SmallCanonicalByteLimit });

        await Assert
            .That(() => builder.Build(HttpReplayOperationKind.Push, PostMethod, PushPath, [], SentAtUtc, ReadOnlyMemory<byte>.Empty))
            .ThrowsExactly<HttpRemoteTransportException>();
    }

    /// <summary>Verifies undefined operation values are rejected before canonicalization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task BuildRejectsUndefinedOperationKind()
    {
        var builder = new HttpCanonicalRequestBuilder(new());
        const HttpReplayOperationKind Operation = (HttpReplayOperationKind)UndefinedOperationValue;

        await Assert
            .That(() => builder.Build(Operation, PostMethod, PushPath, [], SentAtUtc, CreateBody()))
            .ThrowsExactly<HttpRemoteTransportException>();
    }

    /// <summary>Verifies replay timestamps must be UTC.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task BuildRejectsNonUtcTimestamp()
    {
        var builder = new HttpCanonicalRequestBuilder(new());
        var localTimestamp = SentAtUtc.ToOffset(TimeSpan.FromHours(BodyByteOne));

        await Assert
            .That(() => builder.Build(HttpReplayOperationKind.Push, PostMethod, PushPath, [], localTimestamp, CreateBody()))
            .ThrowsExactly<HttpRemoteTransportException>();
    }

    /// <summary>Verifies whitespace methods and control query values are rejected as canonical text.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task BuildRejectsAmbiguousCanonicalText()
    {
        var builder = new HttpCanonicalRequestBuilder(new());
        KeyValuePair<string, string>[] query = [new(CursorKey, ControlQueryValue)];

        await Assert
            .That(() => builder.Build(HttpReplayOperationKind.Push, " ", PushPath, [], SentAtUtc, CreateBody()))
            .ThrowsExactly<HttpRemoteTransportException>();
        await Assert
            .That(() => builder.Build(HttpReplayOperationKind.Subscribe, GetMethod, SubscribePath, query, SentAtUtc, ReadOnlyMemory<byte>.Empty))
            .ThrowsExactly<HttpRemoteTransportException>();
    }

    /// <summary>Verifies query values are escaped so separators cannot create alternate canonical fields.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task BuildEscapesQueryBoundaryValues()
    {
        var builder = new HttpCanonicalRequestBuilder(new());
        KeyValuePair<string, string>[] query =
        [
            new(SubscriptionIdKey, SubscriptionId),
            new(StreamIdKey, StreamId),
            new(CursorKey, EscapedCursor),
        ];

        var canonical = builder.Build(HttpReplayOperationKind.Subscribe, GetMethod, SubscribePath, query, SentAtUtc, ReadOnlyMemory<byte>.Empty);

        await Assert.That(canonical.CanonicalQuery).IsEqualTo(EscapedCanonicalQuery);
    }

    /// <summary>Verifies canonical request bytes are copied into owned immutable storage.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task BuildCopiesCanonicalBytesAndBodyHashBeforeCallerMutation()
    {
        var builder = new HttpCanonicalRequestBuilder(new());
        var body = CreateBody();
        var expectedBodyHash = SHA256.HashData(body);
        var canonical = builder.Build(HttpReplayOperationKind.Push, PostMethod, PushPath, [], SentAtUtc, body);
        var expectedCanonicalBytes = canonical.Bytes.ToArray();
        var originalBodyHash = canonical.BodyHash.ToArray();

        body[0] = CallerMutationByte;
        MutateReturnedMemory(canonical.Bytes, CanonicalMutationByte);
        MutateReturnedMemory(canonical.BodyHash, BodyHashMutationByte);

        await Assert.That(CryptographicOperations.FixedTimeEquals(originalBodyHash, expectedBodyHash)).IsTrue();
        await Assert.That(CryptographicOperations.FixedTimeEquals(canonical.BodyHash.ToArray(), expectedBodyHash)).IsTrue();
        await Assert.That(CryptographicOperations.FixedTimeEquals(canonical.Bytes.ToArray(), expectedCanonicalBytes)).IsTrue();
    }

    /// <summary>Creates a mutable test body.</summary>
    /// <returns>The test body.</returns>
    private static byte[] CreateBody()
    {
        var body = new byte[BodyByteThree];
        body[0] = BodyByteOne;
        body[1] = BodyByteTwo;
        body[2] = BodyByteThree;
        return body;
    }

    /// <summary>Attempts to mutate an exposed read-only memory backing array when one is available.</summary>
    /// <param name="memory">The exposed memory.</param>
    /// <param name="value">The mutation value.</param>
    private static void MutateReturnedMemory(ReadOnlyMemory<byte> memory, byte value)
    {
        if (!MemoryMarshal.TryGetArray(memory, out var segment) || segment.Count == 0 || segment.Array is not { } array)
        {
            return;
        }

        array[segment.Offset] = value;
    }
}
