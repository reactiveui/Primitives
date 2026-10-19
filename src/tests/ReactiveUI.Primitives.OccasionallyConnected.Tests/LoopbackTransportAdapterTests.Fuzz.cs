// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>
/// Deterministic, seeded and bounded fuzz tests for <see cref="LoopbackTransportAdapter"/> input validation. Every
/// case derives its own seed from a fixed base seed and the case index, so a failure message is enough to replay it.
/// </summary>
public sealed partial class LoopbackTransportAdapterTests
{
    /// <summary>The fixed base seed for sync batch fuzzing.</summary>
    private const ulong LoopbackBatchFuzzSeed = 0x5EED_100B_0000_0001UL;

    /// <summary>The fixed base seed for acknowledgement fuzzing.</summary>
    private const ulong LoopbackAcknowledgementFuzzSeed = 0x5EED_100B_0000_0002UL;

    /// <summary>The fixed base seed for receive batch fuzzing.</summary>
    private const ulong LoopbackReceiveFuzzSeed = 0x5EED_100B_0000_0003UL;

    /// <summary>The sync batch fuzz case count.</summary>
    private const int LoopbackBatchFuzzCases = 3000;

    /// <summary>The acknowledgement fuzz case count.</summary>
    private const int LoopbackAcknowledgementFuzzCases = 2000;

    /// <summary>The receive batch fuzz case count.</summary>
    private const int LoopbackReceiveFuzzCases = 2000;

    /// <summary>The maximum number of failures collected before a fuzz test stops.</summary>
    private const int LoopbackFuzzFailureLimit = 8;

    /// <summary>The odds that a generated field takes a hostile value.</summary>
    private const int LoopbackFuzzHostileOdds = 6;

    /// <summary>The operations generated above the peer batch limit.</summary>
    private const int LoopbackFuzzExtraOperations = 3;

    /// <summary>The receive event limit configured for fuzzed adapters.</summary>
    private const int LoopbackFuzzMaximumReceiveEvents = 4;

    /// <summary>The metadata entry limit configured for fuzzed adapters.</summary>
    private const int LoopbackFuzzMaximumMetadataEntries = 4;

    /// <summary>The largest generated payload, in bytes.</summary>
    private const int LoopbackFuzzMaximumPayloadBytes = 5000;

    /// <summary>The number of distinct generated identifiers, kept small so duplicates occur.</summary>
    private const int LoopbackFuzzIdentifierSpace = 48;

    /// <summary>The smallest generated enum value.</summary>
    private const int LoopbackFuzzMinimumEnumValue = -2;

    /// <summary>The largest generated enum value, exclusive.</summary>
    private const int LoopbackFuzzMaximumEnumValue = 8;

    /// <summary>The largest generated priority magnitude.</summary>
    private const int LoopbackFuzzPriorityMagnitude = 12;

    /// <summary>The per-case timeout that turns a hang into a reported failure.</summary>
    private static readonly TimeSpan LoopbackFuzzCaseTimeout = TimeSpan.FromSeconds(5);

    /// <summary>A second valid stream used to create mixed-stream batches.</summary>
    private static readonly StreamId OtherStream = new("sensor/humidity");

    /// <summary>Hostile string values.</summary>
    private static readonly string[] LoopbackFuzzStrings =
    [
        string.Empty, " ", "reading", new string('x', OversizedStringLength), "\ud800", "é", "\0", "cursor-1", NextCursor, "..", "\U0001F600",
    ];

    /// <summary>Hostile client sequences.</summary>
    private static readonly long[] LoopbackFuzzSequences = [0, -1, long.MinValue, long.MaxValue];

    /// <summary>
    /// Verifies every generated sync batch is either applied by exactly one hub call or rejected with a documented
    /// typed exception before the hub sees it, and that rejected pushes release their admission slot.
    /// </summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [NotInParallel]
    public async Task FuzzedSyncBatchIsAppliedOnceOrRejectedBeforeHub()
    {
        var hub = new RecordingHub { ApplyHandler = static (batch, _, _) => ValueTask.FromResult(new ServerSyncResult(CreateAcceptedResult(batch), [])) };
        await using var adapter = new LoopbackTransportAdapter(CreateFuzzOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        List<string> failures = [];
        for (var caseIndex = 0; caseIndex < LoopbackBatchFuzzCases && failures.Count < LoopbackFuzzFailureLimit; caseIndex++)
        {
            var random = new LoopbackFuzzRandom(LoopbackFuzzRandom.DeriveSeed(LoopbackBatchFuzzSeed, caseIndex));
            var batch = CreateFuzzBatch(random);
            var callsBefore = hub.ApplyCalls;
            var failure = await RunLoopbackFuzzCaseAsync(async () => _ = await session.PushAsync(batch, CancellationToken.None), () => hub.ApplyCalls - callsBefore);
            AddLoopbackFuzzFailure(failures, failure, LoopbackBatchFuzzSeed, caseIndex);
        }

        var final = await session.PushAsync(CreateBatch(), CancellationToken.None);
        await Assert.That(string.Join(Environment.NewLine, failures)).IsEmpty();
        await Assert.That(final.Operations.Count).IsEqualTo(1);
    }

    /// <summary>
    /// Verifies every generated acknowledgement is either forwarded by exactly one hub call or rejected with a
    /// documented typed exception before the hub sees it, and that rejections release their admission slot.
    /// </summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [NotInParallel]
    public async Task FuzzedReceiveAcknowledgementIsForwardedOnceOrRejectedBeforeHub()
    {
        var hub = new RecordingHub();
        await using var adapter = new LoopbackTransportAdapter(CreateFuzzOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        List<string> failures = [];
        for (var caseIndex = 0; caseIndex < LoopbackAcknowledgementFuzzCases && failures.Count < LoopbackFuzzFailureLimit; caseIndex++)
        {
            var random = new LoopbackFuzzRandom(LoopbackFuzzRandom.DeriveSeed(LoopbackAcknowledgementFuzzSeed, caseIndex));
            var acknowledgement = new ReceiveAcknowledgement(new(CreateFuzzGuid(random)), PickFuzzStream(random), PickFuzzNullableString(random)!);
            var callsBefore = hub.AcknowledgeCalls;
            var failure = await RunLoopbackFuzzCaseAsync(
                async () => await session.AcknowledgeAsync(acknowledgement, CancellationToken.None),
                () => hub.AcknowledgeCalls - callsBefore);
            AddLoopbackFuzzFailure(failures, failure, LoopbackAcknowledgementFuzzSeed, caseIndex);
        }

        await session.AcknowledgeAsync(new(SubscriptionId.New(), Stream, NextCursor), CancellationToken.None);
        await Assert.That(string.Join(Environment.NewLine, failures)).IsEmpty();
    }

    /// <summary>
    /// Verifies every generated receive batch returned by the hub is either delivered unchanged or rejected with a
    /// documented typed exception, never hangs, and always releases the subscription admission slot.
    /// </summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [NotInParallel]
    public async Task FuzzedRemoteEventBatchIsDeliveredOrRejectedWithTypedFailure()
    {
        RemoteEventBatch[] current = [CreateReceiveBatch(CreateRemoteEvent())];
        var hub = new RecordingHub { SubscribeHandler = (_, _, _) => YieldBatches(current[0]) };
        await using var adapter = new LoopbackTransportAdapter(CreateFuzzOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        List<string> failures = [];
        var delivered = 0;
        for (var caseIndex = 0; caseIndex < LoopbackReceiveFuzzCases && failures.Count < LoopbackFuzzFailureLimit; caseIndex++)
        {
            var random = new LoopbackFuzzRandom(LoopbackFuzzRandom.DeriveSeed(LoopbackReceiveFuzzSeed, caseIndex));
            if (!TryCreateFuzzReceiveBatch(random, out var batch))
            {
                continue;
            }

            current[0] = batch;
            var (accepted, failure) = await ReceiveFuzzBatchAsync(session, batch);
            delivered += accepted ? 1 : 0;
            AddLoopbackFuzzFailure(failures, failure, LoopbackReceiveFuzzSeed, caseIndex);
        }

        await Assert.That(string.Join(Environment.NewLine, failures)).IsEmpty();
        await Assert.That(delivered).IsGreaterThan(0);
    }

    /// <summary>Creates adapter options with small bounds and one admission slot per operation kind.</summary>
    /// <param name="hub">The hub.</param>
    /// <returns>The options.</returns>
    private static LoopbackTransportAdapterOptions CreateFuzzOptions(IServerStreamHub hub) =>
        CreateOptions(hub) with
        {
            MaximumConcurrentRequests = 1,
            MaximumConcurrentAcknowledgements = 1,
            MaximumConcurrentSubscriptions = 1,
            MaximumReceiveEvents = LoopbackFuzzMaximumReceiveEvents,
            MaximumCompletedOperations = LoopbackFuzzMaximumReceiveEvents,
            MaximumMetadataEntries = LoopbackFuzzMaximumMetadataEntries,
            MaximumStringBytes = BoundedStringBytes,
        };

    /// <summary>Creates an accepted result for every operation in a batch.</summary>
    /// <param name="batch">The batch.</param>
    /// <returns>The result.</returns>
    private static RemoteSyncResult CreateAcceptedResult(SyncBatch batch)
    {
        var results = new OperationSyncResult[batch.Operations.Count];
        for (var index = 0; index < results.Length; index++)
        {
            results[index] = new(batch.Operations[index].OperationId, OperationResultKind.Accepted, null, "v1");
        }

        return new(batch.BatchId, results, NextCursor, null);
    }

    /// <summary>Records a failed case with its reproduction coordinates.</summary>
    /// <param name="failures">The collected failures.</param>
    /// <param name="failure">The case failure, or <see langword="null"/>.</param>
    /// <param name="seed">The fixed base seed.</param>
    /// <param name="caseIndex">The case index.</param>
    private static void AddLoopbackFuzzFailure(List<string> failures, string? failure, ulong seed, int caseIndex)
    {
        if (failure is not null)
        {
            failures.Add(string.Create(
                CultureInfo.InvariantCulture,
                $"seed=0x{seed:X16} case={caseIndex} caseSeed=0x{LoopbackFuzzRandom.DeriveSeed(seed, caseIndex):X16}: {failure}"));
        }
    }

    /// <summary>Runs one request and checks the typed-failure, timeout and hub-call invariants.</summary>
    /// <param name="action">The request.</param>
    /// <param name="hubCalls">Reads the hub calls made by the request.</param>
    /// <returns>The failure text, or <see langword="null"/> when the case holds.</returns>
    private static async Task<string?> RunLoopbackFuzzCaseAsync(Func<Task> action, Func<int> hubCalls)
    {
        try
        {
            await action().WaitAsync(LoopbackFuzzCaseTimeout);
        }
        catch (TimeoutException)
        {
            return "the request exceeded the per-case timeout";
        }
        catch (Exception exception) when (exception is InvalidOperationException or ArgumentException)
        {
            return hubCalls() == 0 ? null : $"a rejected request reached the hub {hubCalls()} times: {exception.Message}";
        }
        catch (Exception exception)
        {
            return $"threw {exception.GetType().FullName}: {exception.Message}";
        }

        return hubCalls() == 1 ? null : $"an accepted request reached the hub {hubCalls()} times";
    }

    /// <summary>Opens one subscription, reads its first batch and checks the receive invariants.</summary>
    /// <param name="session">The session.</param>
    /// <param name="batch">The batch the hub returns.</param>
    /// <returns>Whether the batch was delivered, and the failure text when an invariant broke.</returns>
    private static async Task<(bool Accepted, string? Failure)> ReceiveFuzzBatchAsync(IRemoteTransportSession session, RemoteEventBatch batch)
    {
        IAsyncEnumerator<RemoteEventBatch> enumerator;
        try
        {
            enumerator = session.SubscribeAsync(new(Stream, SubscriptionId.New(), null, StartPosition.Latest), CancellationToken.None).GetAsyncEnumerator();
        }
        catch (Exception exception)
        {
            return (false, $"opening the subscription threw {exception.GetType().FullName}: {exception.Message}");
        }

        await using (enumerator)
        {
            try
            {
                var moved = await enumerator.MoveNextAsync().AsTask().WaitAsync(LoopbackFuzzCaseTimeout);
                return moved && ReferenceEquals(enumerator.Current, batch) ? (true, null) : (false, "a valid receive batch was not delivered unchanged");
            }
            catch (TimeoutException)
            {
                return (false, "the receive exceeded the per-case timeout");
            }
            catch (Exception exception) when (exception is InvalidOperationException or ArgumentException)
            {
                return (false, null);
            }
            catch (Exception exception)
            {
                return (false, $"receive threw {exception.GetType().FullName}: {exception.Message}");
            }
        }
    }

    /// <summary>Creates a generated sync batch.</summary>
    /// <param name="random">The case generator.</param>
    /// <returns>The batch.</returns>
    private static SyncBatch CreateFuzzBatch(LoopbackFuzzRandom random)
    {
        var operations = new SyncOperation[random.Next(PeerMaximumOperations + LoopbackFuzzExtraOperations)];
        long sequence = 0;
        for (var index = 0; index < operations.Length; index++)
        {
            sequence += random.OneIn(LoopbackFuzzHostileOdds) ? 0 : 1;
            operations[index] = CreateFuzzOperation(random, sequence);
        }

        return new(CreateFuzzGuid(random), operations);
    }

    /// <summary>Creates a generated sync operation.</summary>
    /// <param name="random">The case generator.</param>
    /// <param name="sequence">The ordered client sequence used by well-formed operations.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateFuzzOperation(LoopbackFuzzRandom random, long sequence) => new()
    {
        OperationId = new(CreateFuzzGuid(random)),
        StreamId = PickFuzzStream(random),
        ClientSequence = random.OneIn(LoopbackFuzzHostileOdds) ? random.Pick(LoopbackFuzzSequences) : sequence,
        TimestampUtc = CommittedUtc,
        BaseVersion = random.OneIn(LoopbackFuzzHostileOdds) ? PickFuzzNullableString(random) : null,
        Type = random.OneIn(LoopbackFuzzHostileOdds) ? (SyncOperationType)random.Next(LoopbackFuzzMinimumEnumValue, LoopbackFuzzMaximumEnumValue) : SyncOperationType.Append,
        Payload = random.OneIn(LoopbackFuzzHostileOdds * LoopbackFuzzHostileOdds) ? null! : CreateFuzzPayload(random),
        Policy = CreateFuzzPolicy(random),
        Metadata = CreateFuzzMetadata(random),
    };

    /// <summary>Creates a generated payload envelope.</summary>
    /// <param name="random">The case generator.</param>
    /// <returns>The payload.</returns>
    private static PayloadEnvelope CreateFuzzPayload(LoopbackFuzzRandom random)
    {
        var hostile = random.OneIn(LoopbackFuzzHostileOdds);
        return new(
            hostile ? random.Pick(LoopbackFuzzStrings) : ContractId,
            hostile ? random.Next(LoopbackFuzzMinimumEnumValue, LoopbackFuzzMaximumEnumValue) : 1,
            hostile ? random.Pick(LoopbackFuzzStrings) : PayloadContentType,
            random.NextBytes(hostile ? random.Next(LoopbackFuzzMaximumPayloadBytes) : OperationPayload.Length),
            hostile ? random.Pick(LoopbackFuzzStrings) : "hash");
    }

    /// <summary>Creates a generated operation policy, including a missing policy.</summary>
    /// <param name="random">The case generator.</param>
    /// <returns>The policy.</returns>
    private static OperationPolicy CreateFuzzPolicy(LoopbackFuzzRandom random)
    {
        if (!random.OneIn(LoopbackFuzzHostileOdds))
        {
            return OperationPolicy.Default;
        }

        return random.OneIn(LoopbackFuzzHostileOdds)
            ? null!
            : new(
                (DeliveryGuarantee)random.Next(LoopbackFuzzMinimumEnumValue, LoopbackFuzzMaximumEnumValue),
                (OperationDurability)random.Next(LoopbackFuzzMinimumEnumValue, LoopbackFuzzMaximumEnumValue),
                random.Next(-LoopbackFuzzPriorityMagnitude, LoopbackFuzzPriorityMagnitude),
                (ConflictPolicy)random.Next(LoopbackFuzzMinimumEnumValue, LoopbackFuzzMaximumEnumValue));
    }

    /// <summary>Creates generated metadata, sometimes above the entry or string bounds.</summary>
    /// <param name="random">The case generator.</param>
    /// <returns>The metadata.</returns>
    private static Dictionary<string, string> CreateFuzzMetadata(LoopbackFuzzRandom random)
    {
        var metadata = new Dictionary<string, string>(StringComparer.Ordinal);
        var count = random.OneIn(LoopbackFuzzHostileOdds) ? random.Next(LoopbackFuzzMaximumMetadataEntries * LoopbackFuzzMaximumMetadataEntries) : 1;
        for (var index = 0; index < count; index++)
        {
            var key = random.OneIn(LoopbackFuzzHostileOdds) ? random.Pick(LoopbackFuzzStrings) : string.Create(CultureInfo.InvariantCulture, $"k{index}");
            metadata[key] = random.OneIn(LoopbackFuzzHostileOdds) ? random.Pick(LoopbackFuzzStrings) : "v";
        }

        return metadata;
    }

    /// <summary>Creates a generated receive batch, or reports that the public model rejected the generated shape.</summary>
    /// <param name="random">The case generator.</param>
    /// <param name="batch">The generated batch.</param>
    /// <returns><see langword="true"/> when the public model accepted the shape.</returns>
    private static bool TryCreateFuzzReceiveBatch(LoopbackFuzzRandom random, out RemoteEventBatch batch)
    {
        try
        {
            var events = new RemoteEvent[random.Next(LoopbackFuzzMaximumReceiveEvents + LoopbackFuzzExtraOperations)];
            for (var index = 0; index < events.Length; index++)
            {
                events[index] = CreateFuzzRemoteEvent(random);
            }

            var previous = random.OneIn(LoopbackFuzzHostileOdds) ? random.Pick(LoopbackFuzzStrings) : null;
            var next = random.OneIn(LoopbackFuzzHostileOdds) ? PickFuzzNullableString(random) : NextCursor;
            batch = new(CreateFuzzGuid(random), PickFuzzStream(random), previous, next!, events) { CompletedOperations = CreateFuzzCompletions(random, events) };
            return true;
        }
        catch (Exception exception) when (exception is ArgumentException or InvalidOperationException)
        {
            batch = null!;
            return false;
        }
    }

    /// <summary>Creates a generated remote event.</summary>
    /// <param name="random">The case generator.</param>
    /// <returns>The event.</returns>
    private static RemoteEvent CreateFuzzRemoteEvent(LoopbackFuzzRandom random)
    {
        OperationId? causedBy = random.OneIn(LoopbackFuzzHostileOdds) ? null : new OperationId(CreateFuzzGuid(random));
        var payload = random.OneIn(LoopbackFuzzHostileOdds * LoopbackFuzzHostileOdds) ? null! : CreateFuzzPayload(random);
        var cursor = random.OneIn(LoopbackFuzzHostileOdds) ? PickFuzzNullableString(random) : NextCursor;
        var remoteEvent = new RemoteEvent(CreateFuzzGuid(random), PickFuzzStream(random), cursor!, CommittedUtc, causedBy, payload, CreateFuzzMetadata(random));
        return causedBy is { } operationId && !random.OneIn(LoopbackFuzzHostileOdds)
            ? remoteEvent with { Origin = new(random.OneIn(LoopbackFuzzHostileOdds) ? random.Pick(LoopbackFuzzStrings) : TrustedClientId, operationId) }
            : remoteEvent;
    }

    /// <summary>Creates completion declarations that usually match the events and sometimes do not.</summary>
    /// <param name="random">The case generator.</param>
    /// <param name="events">The events.</param>
    /// <returns>The completions.</returns>
    private static List<RemoteOperationCompletion> CreateFuzzCompletions(LoopbackFuzzRandom random, RemoteEvent[] events)
    {
        List<RemoteOperationCompletion> completions = [];
        for (var index = 0; index < events.Length; index++)
        {
            if (events[index].Origin is not { } origin || random.OneIn(LoopbackFuzzHostileOdds))
            {
                continue;
            }

            Guid[] eventIds = random.OneIn(LoopbackFuzzHostileOdds) ? [CreateFuzzGuid(random)] : [events[index].EventId];
            completions.Add(new(origin, eventIds));
        }

        return completions;
    }

    /// <summary>Picks a stream identifier, sometimes another stream or the default value.</summary>
    /// <param name="random">The case generator.</param>
    /// <returns>The stream identifier.</returns>
    private static StreamId PickFuzzStream(LoopbackFuzzRandom random)
    {
        if (!random.OneIn(LoopbackFuzzHostileOdds))
        {
            return Stream;
        }

        return random.OneIn(LoopbackFuzzHostileOdds) ? default : OtherStream;
    }

    /// <summary>Picks a hostile string or a missing value.</summary>
    /// <param name="random">The case generator.</param>
    /// <returns>The string, or <see langword="null"/>.</returns>
    private static string? PickFuzzNullableString(LoopbackFuzzRandom random) =>
        random.OneIn(LoopbackFuzzHostileOdds) ? null : random.Pick(LoopbackFuzzStrings);

    /// <summary>Creates an identifier from a small space so duplicates and the empty value occur.</summary>
    /// <param name="random">The case generator.</param>
    /// <returns>The identifier.</returns>
    private static Guid CreateFuzzGuid(LoopbackFuzzRandom random)
    {
        var value = random.Next(LoopbackFuzzIdentifierSpace);
        return value == 0 ? Guid.Empty : new(value, 0, 0, new byte[sizeof(long)]);
    }

    /// <summary>A deterministic SplitMix64 generator whose sequence is stable across runtimes.</summary>
    /// <param name="seed">The case seed.</param>
    private sealed class LoopbackFuzzRandom(ulong seed)
    {
        /// <summary>The SplitMix64 state increment.</summary>
        private const ulong GoldenGamma = 0x9E3779B97F4A7C15UL;

        /// <summary>The first SplitMix64 finalizer multiplier.</summary>
        private const ulong FirstMixMultiplier = 0xBF58476D1CE4E5B9UL;

        /// <summary>The second SplitMix64 finalizer multiplier.</summary>
        private const ulong SecondMixMultiplier = 0x94D049BB133111EBUL;

        /// <summary>The first SplitMix64 finalizer shift.</summary>
        private const int FirstMixShift = 30;

        /// <summary>The second SplitMix64 finalizer shift.</summary>
        private const int SecondMixShift = 27;

        /// <summary>The third SplitMix64 finalizer shift.</summary>
        private const int ThirdMixShift = 31;

        /// <summary>The current generator state.</summary>
        private ulong _state = seed;

        /// <summary>Derives a stable per-case seed from a base seed and a case index.</summary>
        /// <param name="baseSeed">The fixed base seed.</param>
        /// <param name="caseIndex">The case index.</param>
        /// <returns>The case seed.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong DeriveSeed(ulong baseSeed, int caseIndex) => Mix(unchecked(baseSeed ^ ((ulong)caseIndex * GoldenGamma)));

        /// <summary>Returns a random value in <c>[0, maxExclusive)</c>.</summary>
        /// <param name="maxExclusive">The exclusive upper bound; must be positive.</param>
        /// <returns>The random value.</returns>
        public int Next(int maxExclusive) => (int)(NextUInt64() % (ulong)maxExclusive);

        /// <summary>Returns a random value in <c>[minInclusive, maxExclusive)</c>.</summary>
        /// <param name="minInclusive">The inclusive lower bound.</param>
        /// <param name="maxExclusive">The exclusive upper bound.</param>
        /// <returns>The random value.</returns>
        public int Next(int minInclusive, int maxExclusive) => minInclusive + Next(maxExclusive - minInclusive);

        /// <summary>Returns <see langword="true"/> with probability one in <paramref name="odds"/>.</summary>
        /// <param name="odds">The odds denominator.</param>
        /// <returns>The random decision.</returns>
        public bool OneIn(int odds) => Next(odds) == 0;

        /// <summary>Picks one item from a list.</summary>
        /// <typeparam name="T">The item type.</typeparam>
        /// <param name="items">The non-empty items.</param>
        /// <returns>The picked item.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Pick<T>(IReadOnlyList<T> items) => items[Next(items.Count)];

        /// <summary>Fills a new byte array with random bytes.</summary>
        /// <param name="length">The array length.</param>
        /// <returns>The random bytes.</returns>
        public byte[] NextBytes(int length)
        {
            var bytes = new byte[length];
            for (var index = 0; index < length; index++)
            {
                bytes[index] = (byte)NextUInt64();
            }

            return bytes;
        }

        /// <summary>Applies the SplitMix64 finalizer.</summary>
        /// <param name="value">The value to mix.</param>
        /// <returns>The mixed value.</returns>
        private static ulong Mix(ulong value)
        {
            var mixed = unchecked((value ^ (value >> FirstMixShift)) * FirstMixMultiplier);
            mixed = unchecked((mixed ^ (mixed >> SecondMixShift)) * SecondMixMultiplier);
            return mixed ^ (mixed >> ThirdMixShift);
        }

        /// <summary>Returns the next 64 random bits.</summary>
        /// <returns>The random value.</returns>
        private ulong NextUInt64()
        {
            _state = unchecked(_state + GoldenGamma);
            return Mix(_state);
        }
    }
}
