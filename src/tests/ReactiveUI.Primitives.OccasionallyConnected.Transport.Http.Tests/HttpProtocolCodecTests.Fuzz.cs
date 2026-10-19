// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>
/// Deterministic, seeded and bounded fuzz tests for <see cref="HttpProtocolCodec"/>. Every case derives its own seed
/// from a fixed base seed, the target name and the case index, so a failure message is enough to replay it.
/// </summary>
public sealed partial class HttpProtocolCodecTests
{
    /// <summary>The fixed base seed for golden message fuzzing.</summary>
    private const ulong CodecFuzzSeed = 0x5EED_C0DE_C0DE_0001UL;

    /// <summary>The fixed base seed for subscribe query fuzzing.</summary>
    private const ulong QueryFuzzSeed = 0x5EED_C0DE_0000_0002UL;

    /// <summary>The fixed base seed for oversized body fuzzing.</summary>
    private const ulong OversizeFuzzSeed = 0x5EED_C0DE_0000_0003UL;

    /// <summary>The mutated cases per golden message fixture.</summary>
    private const int CodecFuzzCasesPerFixture = 1500;

    /// <summary>The mutated subscribe query cases.</summary>
    private const int QueryFuzzCases = 5000;

    /// <summary>The oversized body cases.</summary>
    private const int OversizeFuzzCases = 400;

    /// <summary>The maximum number of failures collected before a fuzz test stops.</summary>
    private const int FuzzFailureReportLimit = 8;

    /// <summary>The allocation budget, in bytes, for rejecting an oversized body before parsing.</summary>
    private const long OversizeRejectionAllocationBudget = 16 * TestKilobyte;

    /// <summary>The odds denominator for an even coin toss.</summary>
    private const int FuzzCoinOdds = 2;

    /// <summary>The number of subscribe query mutation strategies.</summary>
    private const int QueryStrategyCount = 8;

    /// <summary>The maximum stacked subscribe query mutations.</summary>
    private const int MaximumQueryMutations = 3;

    /// <summary>The long query value length.</summary>
    private const int LongQueryValueLength = 5000;

    /// <summary>The percent-junk query strategy.</summary>
    private const int PercentJunkQueryStrategy = 0;

    /// <summary>The character insertion query strategy.</summary>
    private const int InsertCharacterQueryStrategy = 1;

    /// <summary>The segment duplication query strategy.</summary>
    private const int DuplicateSegmentQueryStrategy = 2;

    /// <summary>The segment deletion query strategy.</summary>
    private const int DeleteSegmentQueryStrategy = 3;

    /// <summary>The value replacement query strategy.</summary>
    private const int ReplaceValueQueryStrategy = 4;

    /// <summary>The truncation query strategy.</summary>
    private const int TruncateQueryStrategy = 5;

    /// <summary>The segment swap query strategy.</summary>
    private const int SwapSegmentsQueryStrategy = 6;

    /// <summary>The per-case timeout that turns a hang into a reported failure.</summary>
    private static readonly TimeSpan FuzzCaseTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Malformed or hostile percent-encoded fragments.</summary>
    private static readonly string[] QueryPercentJunk = ["%", "%G0", "%0", "%FF", "%C0%AF", "%ED%A0%80", "%00", "%2", "%%", "%F0%9F", "%E2%80%AE", "%2e%2e%2f"];

    /// <summary>Hostile query characters.</summary>
    private static readonly string[] QueryCharacters = ["&", "=", "?", "#", "+", " ", "/", "\\", "\0", "é", "\U0001F600", "\ud800", ";", "&&", "=="];

    /// <summary>Hostile query values.</summary>
    private static readonly string[] QueryValues =
    [
        string.Empty, "99999999999999999999", "-1", "2147483648", "-9223372036854775809", "1e3", " 42", "0x10", "4", "3", "not-a-guid",
        "00000000-0000-0000-0000-000000000000", "..%2F..", "9999-12-31T23%3A59%3A59%2B14%3A00", new string('a', LongQueryValueLength),
    ];

    /// <summary>
    /// Verifies every mutated golden message either decodes or fails with <see cref="HttpRemoteTransportException"/>, never
    /// hangs, and re-encodes to a stable canonical form when it decodes.
    /// </summary>
    /// <param name="fixture">The golden fixture to mutate.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodDataSource(nameof(GoldenJsonFixtures))]
    public async Task FuzzedGoldenFixtureDecodesOrRejectsWithTypedFailure(string fixture)
    {
        var original = ProtocolGoldenFixtures.ReadBytes(fixture);
        var codec = CreateGoldenCodec();
        List<string> failures = [];
        var accepted = 0;
        for (var caseIndex = 0; caseIndex < CodecFuzzCasesPerFixture && failures.Count < FuzzFailureReportLimit; caseIndex++)
        {
            var random = new ProtocolFuzzRandom(ProtocolFuzzRandom.DeriveSeed(CodecFuzzSeed, fixture, caseIndex));
            var mutated = ProtocolFuzzMutator.Mutate(original, random);
            var result = await RunFuzzCaseAsync(() => CheckGoldenFuzzCase(codec, fixture, mutated));
            accepted += result.Accepted ? 1 : 0;
            AddFuzzFailure(failures, result, ProtocolFuzzRandom.Describe(CodecFuzzSeed, fixture, caseIndex));
        }

        await Assert.That(string.Join(Environment.NewLine, failures)).IsEmpty();
        await Assert.That(accepted).IsGreaterThan(0).And.IsLessThan(CodecFuzzCasesPerFixture);
    }

    /// <summary>
    /// Verifies every mutated subscribe query either parses or fails with <see cref="HttpRemoteTransportException"/>, and
    /// that any query that parses rebuilds from its decoded fields into an equal request.
    /// </summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task FuzzedSubscribeQueryParsesOrRejectsWithTypedFailure()
    {
        var queries = ProtocolGoldenFixtures.ReadLines(GoldenSubscribeQueriesFile);
        var codec = CreateServerCodec();
        List<string> failures = [];
        var accepted = 0;
        for (var caseIndex = 0; caseIndex < QueryFuzzCases && failures.Count < FuzzFailureReportLimit; caseIndex++)
        {
            var random = new ProtocolFuzzRandom(ProtocolFuzzRandom.DeriveSeed(QueryFuzzSeed, GoldenSubscribeQueriesFile, caseIndex));
            var query = MutateQuery(random.Pick(queries), random);
            var result = await RunFuzzCaseAsync(() => CheckQueryFuzzCase(codec, query));
            accepted += result.Accepted ? 1 : 0;
            AddFuzzFailure(failures, result, ProtocolFuzzRandom.Describe(QueryFuzzSeed, GoldenSubscribeQueriesFile, caseIndex));
        }

        await Assert.That(string.Join(Environment.NewLine, failures)).IsEmpty();
        await Assert.That(accepted).IsGreaterThan(0);
    }

    /// <summary>
    /// Verifies an oversized body is rejected as <see cref="HttpTransportFailureKind.PayloadTooLarge"/> by a length check
    /// that allocates far less than the body, so the codec never parses or copies an oversized peer message.
    /// </summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task FuzzedOversizedBodyIsRejectedBeforeParsing()
    {
        var codec = CreateServerCodec();
        var limit = CreateServerLimits().MaximumRequestBytes;
        string[] targets = [.. GoldenJsonFixtures()];
        FuzzDecodeContext context = new(CreateGoldenBatch(), CreateGoldenSnapshotRequest());
        for (var index = 0; index < targets.Length; index++)
        {
            // Warm up each decoder so one-time JIT and first-throw allocations are not charged to a measured case.
            _ = CheckOversizedFuzzCase(codec, targets[index], new byte[limit + 1], context);
        }

        List<string> failures = [];
        for (var caseIndex = 0; caseIndex < OversizeFuzzCases && failures.Count < FuzzFailureReportLimit; caseIndex++)
        {
            var random = new ProtocolFuzzRandom(ProtocolFuzzRandom.DeriveSeed(OversizeFuzzSeed, nameof(HttpProtocolCodec), caseIndex));
            var body = CreateOversizedBody(random, limit);
            var failure = CheckOversizedFuzzCase(codec, random.Pick(targets), body, context);
            if (failure is not null)
            {
                failures.Add($"{ProtocolFuzzRandom.Describe(OversizeFuzzSeed, nameof(HttpProtocolCodec), caseIndex)}: {failure}");
            }
        }

        await Assert.That(string.Join(Environment.NewLine, failures)).IsEmpty();
    }

    /// <summary>Runs one synchronous fuzz case on the thread pool with the per-case timeout.</summary>
    /// <param name="check">The case check.</param>
    /// <returns>The case result.</returns>
    private static async Task<FuzzCaseResult> RunFuzzCaseAsync(Func<FuzzCaseResult> check)
    {
        try
        {
            return await Task.Run(check).WaitAsync(FuzzCaseTimeout).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            return FuzzCaseResult.Fail("the case exceeded the per-case timeout");
        }
    }

    /// <summary>Records a failed case with its reproduction coordinates.</summary>
    /// <param name="failures">The collected failures.</param>
    /// <param name="result">The case result.</param>
    /// <param name="reproduction">The reproduction coordinates.</param>
    private static void AddFuzzFailure(List<string> failures, FuzzCaseResult result, string reproduction)
    {
        if (result.Failure is not null)
        {
            failures.Add($"{reproduction}: {result.Failure}");
        }
    }

    /// <summary>Checks one mutated golden message against the codec invariants.</summary>
    /// <param name="codec">The codec.</param>
    /// <param name="fixture">The fixture name.</param>
    /// <param name="mutated">The mutated bytes.</param>
    /// <returns>The case result.</returns>
    private static FuzzCaseResult CheckGoldenFuzzCase(HttpProtocolCodec codec, string fixture, byte[] mutated)
    {
        object decoded;
        try
        {
            decoded = DecodeFuzzedGolden(codec, fixture, mutated);
        }
        catch (HttpRemoteTransportException)
        {
            return FuzzCaseResult.Rejected;
        }
        catch (SyncBatchValidationException) when (fixture == GoldenPushResponseFile)
        {
            // A push response whose membership does not match the pushed batch is a documented batch validation failure.
            return FuzzCaseResult.Rejected;
        }
        catch (Exception exception)
        {
            return FuzzCaseResult.Fail($"decode threw {exception.GetType().FullName}: {exception.Message}");
        }

        return CheckGoldenRoundTrip(codec, fixture, decoded);
    }

    /// <summary>Checks that an accepted message re-encodes and decodes to the same canonical bytes.</summary>
    /// <param name="codec">The codec.</param>
    /// <param name="fixture">The fixture name.</param>
    /// <param name="decoded">The decoded message.</param>
    /// <returns>The case result.</returns>
    private static FuzzCaseResult CheckGoldenRoundTrip(HttpProtocolCodec codec, string fixture, object decoded)
    {
        try
        {
            var first = EncodeFuzzedGolden(codec, fixture, decoded);
            var second = EncodeFuzzedGolden(codec, fixture, DecodeFuzzedGolden(codec, fixture, first));
            return first.AsSpan().SequenceEqual(second)
                ? FuzzCaseResult.Success
                : FuzzCaseResult.Fail($"round trip changed the canonical form: {Encoding.UTF8.GetString(first)} != {Encoding.UTF8.GetString(second)}");
        }
        catch (Exception exception)
        {
            return FuzzCaseResult.Fail($"an accepted message failed to round trip with {exception.GetType().FullName}: {exception.Message}");
        }
    }

    /// <summary>Decodes a fuzzed message with the decoder that owns its golden fixture.</summary>
    /// <param name="codec">The codec.</param>
    /// <param name="fixture">The fixture name.</param>
    /// <param name="bytes">The message bytes.</param>
    /// <returns>The decoded message.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static object DecodeFuzzedGolden(HttpProtocolCodec codec, string fixture, byte[] bytes) =>
        DecodeFuzzedGolden(codec, fixture, bytes, new(CreateGoldenBatch(), CreateGoldenSnapshotRequest()));

    /// <summary>Decodes a fuzzed message with the decoder that owns its golden fixture.</summary>
    /// <param name="codec">The codec.</param>
    /// <param name="fixture">The fixture name.</param>
    /// <param name="bytes">The message bytes.</param>
    /// <param name="context">The pushed batch and recovery request that bind response decoders.</param>
    /// <returns>The decoded message.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="fixture"/> has no codec mapping.</exception>
    private static object DecodeFuzzedGolden(HttpProtocolCodec codec, string fixture, byte[] bytes, FuzzDecodeContext context) => fixture switch
    {
        GoldenConnectRequestFile => codec.DeserializeConnectRequest(bytes),
        GoldenConnectResponseFile => codec.DeserializeConnectResponse(bytes),
        GoldenPushRequestFile => codec.DeserializePushRequest(bytes),
        GoldenPushResponseFile => codec.DeserializePushResponse(context.Batch, bytes, null),
        GoldenSubscribeResponseFile => codec.DeserializeSubscribeResponse(bytes),
        GoldenAcknowledgeRequestFile => codec.DeserializeAcknowledgement(bytes),
        GoldenSnapshotRecoveryRequestFile => codec.DeserializeSnapshotRecoveryRequest(bytes),
        GoldenSnapshotRecoveredResponseFile or GoldenSnapshotRetentionExpiredResponseFile =>
            codec.DeserializeSnapshotRecoveryResponse(context.SnapshotRequest, bytes),
        _ => throw new ArgumentOutOfRangeException(nameof(fixture), fixture, GoldenUnknownFixtureMessage),
    };

    /// <summary>Encodes a decoded fuzzed message with the encoder that owns its golden fixture.</summary>
    /// <param name="codec">The codec.</param>
    /// <param name="fixture">The fixture name.</param>
    /// <param name="decoded">The decoded message.</param>
    /// <returns>The encoded bytes.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="fixture"/> has no codec mapping.</exception>
    private static byte[] EncodeFuzzedGolden(HttpProtocolCodec codec, string fixture, object decoded) => fixture switch
    {
        GoldenConnectRequestFile => codec.SerializeConnectRequest((TransportConnectRequest)decoded),
        GoldenConnectResponseFile => codec.SerializeConnectResponse((NegotiatedCapabilities)decoded),
        GoldenPushRequestFile => codec.SerializePushRequest((SyncBatch)decoded),
        GoldenPushResponseFile => codec.SerializePushResponse(CreateGoldenBatch(), (RemoteSyncResult)decoded),
        GoldenSubscribeResponseFile => codec.SerializeSubscribeResponse((RemoteEventBatch[])decoded),
        GoldenAcknowledgeRequestFile => codec.SerializeAcknowledgement((ReceiveAcknowledgement)decoded),
        GoldenSnapshotRecoveryRequestFile => codec.SerializeSnapshotRecoveryRequest((RemoteSnapshotRecoveryRequest)decoded),
        GoldenSnapshotRecoveredResponseFile or GoldenSnapshotRetentionExpiredResponseFile =>
            codec.SerializeSnapshotRecoveryResponse(CreateGoldenSnapshotRequest(), (RemoteSnapshotRecoveryResult)decoded),
        _ => throw new ArgumentOutOfRangeException(nameof(fixture), fixture, GoldenUnknownFixtureMessage),
    };

    /// <summary>Checks one mutated subscribe query against the parser invariants.</summary>
    /// <param name="codec">The codec.</param>
    /// <param name="query">The mutated query.</param>
    /// <returns>The case result.</returns>
    private static FuzzCaseResult CheckQueryFuzzCase(HttpProtocolCodec codec, string query)
    {
        HttpSubscribeRequestParseResult parsed;
        try
        {
            parsed = codec.ParseSubscribeRequestWithQueryFields(query);
        }
        catch (HttpRemoteTransportException)
        {
            return FuzzCaseResult.Rejected;
        }
        catch (Exception exception)
        {
            return FuzzCaseResult.Fail($"parse threw {exception.GetType().FullName}: {exception.Message}");
        }

        try
        {
            var reparsed = codec.ParseSubscribeRequestWithQueryFields(BuildQuery(parsed.QueryFields));
            return reparsed.Request == parsed.Request && reparsed.QueryFields.SequenceEqual(parsed.QueryFields)
                ? FuzzCaseResult.Success
                : FuzzCaseResult.Fail($"query round trip changed the request: {query}");
        }
        catch (Exception exception)
        {
            return FuzzCaseResult.Fail($"an accepted query failed to round trip with {exception.GetType().FullName}: {query}");
        }
    }

    /// <summary>Rebuilds an encoded query from decoded fields.</summary>
    /// <param name="fields">The decoded query fields.</param>
    /// <returns>The encoded query.</returns>
    private static string BuildQuery(IReadOnlyList<KeyValuePair<string, string>> fields)
    {
        var builder = new StringBuilder("?");
        for (var index = 0; index < fields.Count; index++)
        {
            _ = builder.Append(index == 0 ? string.Empty : "&")
                .Append(Uri.EscapeDataString(fields[index].Key))
                .Append('=')
                .Append(Uri.EscapeDataString(fields[index].Value));
        }

        return builder.ToString();
    }

    /// <summary>Applies one to three stacked mutations to a subscribe query.</summary>
    /// <param name="query">The golden query.</param>
    /// <param name="random">The case generator.</param>
    /// <returns>The mutated query.</returns>
    private static string MutateQuery(string query, ProtocolFuzzRandom random)
    {
        var mutated = query;
        var count = random.Next(1, MaximumQueryMutations + 1);
        for (var index = 0; index < count; index++)
        {
            mutated = MutateQueryOnce(mutated, random);
        }

        return mutated;
    }

    /// <summary>Applies one subscribe query mutation.</summary>
    /// <param name="query">The query.</param>
    /// <param name="random">The case generator.</param>
    /// <returns>The mutated query.</returns>
    private static string MutateQueryOnce(string query, ProtocolFuzzRandom random)
    {
        var segments = query.TrimStart('?').Split('&');
        var position = random.Next(query.Length + 1);
        return random.Next(QueryStrategyCount) switch
        {
            PercentJunkQueryStrategy => query.Insert(position, random.Pick(QueryPercentJunk)),
            InsertCharacterQueryStrategy => query.Insert(position, random.Pick(QueryCharacters)),
            DuplicateSegmentQueryStrategy => $"{query}&{random.Pick(segments)}",
            DeleteSegmentQueryStrategy => DeleteQuerySegment(segments, random),
            ReplaceValueQueryStrategy => ReplaceQueryValue(segments, random),
            TruncateQueryStrategy => query[..position],
            SwapSegmentsQueryStrategy => ReverseQuerySegments(segments),
            _ => query.Replace("?", string.Empty, StringComparison.Ordinal),
        };
    }

    /// <summary>Deletes one random query segment.</summary>
    /// <param name="segments">The query segments.</param>
    /// <param name="random">The case generator.</param>
    /// <returns>The mutated query.</returns>
    private static string DeleteQuerySegment(string[] segments, ProtocolFuzzRandom random)
    {
        var removed = random.Next(segments.Length);
        List<string> kept = [];
        for (var index = 0; index < segments.Length; index++)
        {
            if (index != removed)
            {
                kept.Add(segments[index]);
            }
        }

        return $"?{string.Join("&", kept)}";
    }

    /// <summary>Reverses the order of the query segments.</summary>
    /// <param name="segments">The query segments.</param>
    /// <returns>The mutated query.</returns>
    private static string ReverseQuerySegments(string[] segments)
    {
        Array.Reverse(segments);
        return $"?{string.Join("&", segments)}";
    }

    /// <summary>Replaces the value of one random query segment.</summary>
    /// <param name="segments">The query segments.</param>
    /// <param name="random">The case generator.</param>
    /// <returns>The mutated query.</returns>
    private static string ReplaceQueryValue(string[] segments, ProtocolFuzzRandom random)
    {
        var index = random.Next(segments.Length);
        var separator = segments[index].IndexOf('=', StringComparison.Ordinal);
        var key = separator < 0 ? segments[index] : segments[index][..separator];
        segments[index] = $"{key}={random.Pick(QueryValues)}";
        return $"?{string.Join("&", segments)}";
    }

    /// <summary>Creates an oversized body of hostile JSON-like bytes.</summary>
    /// <param name="random">The case generator.</param>
    /// <param name="limit">The configured byte limit.</param>
    /// <returns>The oversized body.</returns>
    private static byte[] CreateOversizedBody(ProtocolFuzzRandom random, int limit)
    {
        var length = limit + random.Next(1, limit);
        if (random.OneIn(FuzzCoinOdds))
        {
            return random.NextBytes(length);
        }

        var body = new byte[length];
        body.AsSpan().Fill((byte)(random.OneIn(FuzzCoinOdds) ? '[' : '{'));
        return body;
    }

    /// <summary>Checks one oversized body against every decoder's allocation bound.</summary>
    /// <param name="codec">The codec.</param>
    /// <param name="fixture">The fixture whose decoder receives the body.</param>
    /// <param name="body">The oversized body.</param>
    /// <param name="context">The prebuilt decode context, so the measurement covers only the decoder.</param>
    /// <returns>The failure text, or <see langword="null"/> when the case holds.</returns>
    private static string? CheckOversizedFuzzCase(HttpProtocolCodec codec, string fixture, byte[] body, FuzzDecodeContext context)
    {
        var before = GC.GetAllocatedBytesForCurrentThread();
        HttpTransportFailureKind? kind = null;
        try
        {
            _ = DecodeFuzzedGolden(codec, fixture, body, context);
        }
        catch (HttpRemoteTransportException exception)
        {
            kind = exception.Kind;
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        if (kind != HttpTransportFailureKind.PayloadTooLarge)
        {
            return string.Create(CultureInfo.InvariantCulture, $"{fixture} returned {kind?.ToString() ?? "success"} for a {body.Length}-byte body");
        }

        return allocated > OversizeRejectionAllocationBudget
            ? string.Create(CultureInfo.InvariantCulture, $"{fixture} allocated {allocated} bytes to reject a {body.Length}-byte body")
            : null;
    }

    /// <summary>Describes the outcome of one fuzz case.</summary>
    /// <param name="Accepted">A value indicating whether the input was accepted.</param>
    /// <param name="Failure">The invariant violation, or <see langword="null"/>.</param>
    private readonly record struct FuzzCaseResult(bool Accepted, string? Failure)
    {
        /// <summary>Gets the result for an accepted input that met every invariant.</summary>
        public static FuzzCaseResult Success => new(true, null);

        /// <summary>Gets the result for an input rejected with the documented typed exception.</summary>
        public static FuzzCaseResult Rejected => new(false, null);

        /// <summary>Creates a failed result.</summary>
        /// <param name="failure">The invariant violation.</param>
        /// <returns>The failed result.</returns>
        public static FuzzCaseResult Fail(string failure) => new(false, failure);
    }

    /// <summary>Holds the golden values that bind response decoders to their request.</summary>
    /// <param name="Batch">The pushed batch.</param>
    /// <param name="SnapshotRequest">The snapshot recovery request.</param>
    private sealed record FuzzDecodeContext(SyncBatch Batch, RemoteSnapshotRecoveryRequest SnapshotRequest);
}
