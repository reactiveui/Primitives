// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for CRDT codec and contract behavior.</summary>
public sealed class MergeableCodecTests
{
    /// <summary>The first actor.</summary>
    private const string ActorA = "actor-a";

    /// <summary>The second actor.</summary>
    private const string ActorB = "actor-b";

    /// <summary>The third actor.</summary>
    private const string ActorC = "actor-c";

    /// <summary>The alpha element.</summary>
    private const string Alpha = "alpha";

    /// <summary>The beta element.</summary>
    private const string Beta = "beta";

    /// <summary>The alpha element with a longer shared prefix.</summary>
    private const string AlphaPair = "alphaa";

    /// <summary>The gamma element.</summary>
    private const string Gamma = "gamma";

    /// <summary>The first sequence.</summary>
    private const long SequenceOne = 1;

    /// <summary>The second sequence.</summary>
    private const long SequenceTwo = 2;

    /// <summary>The third sequence.</summary>
    private const long SequenceThree = 3;

    /// <summary>The first count value.</summary>
    private const long CountOne = 1;

    /// <summary>The second count value.</summary>
    private const long CountTwo = 2;

    /// <summary>The third count value.</summary>
    private const long CountThree = 3;

    /// <summary>The fifth count value.</summary>
    private const long CountFive = 5;

    /// <summary>The expected PN value.</summary>
    private const long ExpectedPNValue = 1;

    /// <summary>The expected item count for two values.</summary>
    private const int TwoItems = 2;

    /// <summary>The CRDT state payload type.</summary>
    private const byte StatePayload = 1;

    /// <summary>The CRDT input payload type.</summary>
    private const byte InputPayload = 2;

    /// <summary>The first payload index.</summary>
    private const int FirstPayloadIndex = 0;

    /// <summary>The version byte index.</summary>
    private const int VersionIndex = 4;

    /// <summary>The payload type byte index.</summary>
    private const int PayloadTypeIndex = 5;

    /// <summary>The expected item count for one value.</summary>
    private const int OneItem = 1;

    /// <summary>An invalid byte marker.</summary>
    private const byte InvalidMarker = 2;

    /// <summary>An invalid input kind byte.</summary>
    private const byte InvalidInputKind = 99;

    /// <summary>An invalid UTF-8 byte.</summary>
    private const byte InvalidUtf8Byte = 255;

    /// <summary>The encoded header length.</summary>
    private const int HeaderLength = 6;

    /// <summary>The encoded mutation optional actor marker index.</summary>
    private const int MutationActorMarkerIndex = 8;

    /// <summary>The encoded empty state stamp marker index.</summary>
    private const int EmptyStateStampMarkerIndex = 31;

    /// <summary>The Int32 first shift.</summary>
    private const int Int32FirstShift = 24;

    /// <summary>The Int64 first shift.</summary>
    private const int Int64FirstShift = 56;

    /// <summary>The number of bits in one byte.</summary>
    private const int BitsPerByte = 8;

    /// <summary>The timestamp year.</summary>
    private const int TimestampYear = 2026;

    /// <summary>The timestamp month.</summary>
    private const int TimestampMonth = 9;

    /// <summary>The timestamp day.</summary>
    private const int TimestampDay = 13;

    /// <summary>The timestamp hour.</summary>
    private const int TimestampHour = 8;

    /// <summary>The timestamp minute.</summary>
    private const int TimestampMinute = 30;

    /// <summary>The positive offset hour.</summary>
    private const int OffsetHour = 1;

    /// <summary>The register size that forces the writer buffer to grow.</summary>
    private const int ResizeRegisterBytes = 300;

    /// <summary>The encoded size limit used while exercising writer growth.</summary>
    private const int ResizeEncodedBytes = 400;

    /// <summary>The register size limit that still allows alpha text.</summary>
    private const int StampRegisterBytes = 16;

    /// <summary>An invalid lone UTF-16 high surrogate.</summary>
    private const char InvalidHighSurrogate = (char)0xD800;

    /// <summary>Bounds that make otherwise valid payloads too large.</summary>
    private static readonly MergeableBounds TinyBounds = MergeableBounds.Default with
    {
        MaximumCounterComponents = 1,
        MaximumDotBindings = 1,
        MaximumTombstones = 1,
        MaximumElementBytes = 1,
        MaximumRegisterBytes = 1,
        MaximumClientIdUtf8Bytes = 2,
    };

    /// <summary>A reusable valid dot.</summary>
    private static readonly ElementTag ValidDot = Tag(ActorA, SequenceOne);

    /// <summary>Exercises default codec overloads over each supported CRDT shape.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CodecDefaultOverloadsRoundTripAllShapes()
    {
        var stamp = Stamp(ActorA, "00000000-0000-0000-0000-000000000011");
        var growCounter = new MergeableState { Kind = MergeableKind.IncrementOnlyCounter, IncrementOnlyTotalsByClient = CounterComponents(CountThree, CountTwo) };
        var posNegCounter = new MergeableState
        {
            Kind = MergeableKind.Counter,
            IncrementsByClient = new Dictionary<string, long> { [ActorB] = CountOne, [ActorA] = CountThree },
            DecrementsByClient = new Dictionary<string, long> { [ActorB] = CountTwo, [ActorA] = CountOne },
        };
        var observedSet = new MergeableState
        {
            Kind = MergeableKind.AddWinsSet,
            Elements =
            [
                new() { Tag = Tag(ActorB, SequenceTwo), Element = Bytes(Beta) },
                new() { Tag = Tag(ActorA, SequenceOne), Element = Bytes(Alpha) },
            ],
            RemovedTags = [new() { Tag = Tag(ActorC, SequenceThree), Element = Bytes(Gamma) }],
        };
        var register = new MergeableState { Kind = MergeableKind.LastWriterWinsValue, LatestValue = Bytes(Alpha), LatestWriteStamp = stamp };

        var decodedGrowCounter = MergeableCodec.DecodeState(MergeableCodec.EncodeState(growCounter));
        var decodedPosNegCounter = MergeableCodec.DecodeState(MergeableCodec.EncodeState(posNegCounter));
        var decodedObservedSet = MergeableCodec.DecodeState(MergeableCodec.EncodeState(observedSet));
        var decodedRegister = MergeableCodec.DecodeState(MergeableCodec.EncodeState(register));
        var decodedMutation = MergeableCodec.DecodeInput(MergeableCodec.EncodeInput(CreateStampedInput(stamp)));
        var decodedRemove = MergeableCodec.DecodeInput(MergeableCodec.EncodeInput(CreateRemoveInput()));
        var decodedAuthoritative = MergeableCodec.DecodeInput(MergeableCodec.EncodeInput(MergeableInput.ForAuthoritativeState(register)));

        await Assert.That(decodedGrowCounter.Value.Counter).IsEqualTo(CountFive);
        await Assert.That(decodedPosNegCounter.Value.Counter).IsEqualTo(ExpectedPNValue);
        await Assert.That(decodedObservedSet.Value.Elements.Count).IsEqualTo(TwoItems);
        await Assert.That(decodedRegister.LatestWriteStamp).IsNotNull();
        await Assert.That((decodedMutation.Value as MergeableChange)?.LatestWriteStamp).IsNotNull();
        await Assert.That((decodedRemove.Value as MergeableChange)?.TagsToRemove.Count).IsEqualTo(TwoItems);
        await Assert.That((decodedAuthoritative.Value as MergeableState)?.Value.Bytes.Length).IsEqualTo(Bytes(Alpha).Length);
    }

    /// <summary>Rejects foreign wire fields before the decoder selects a case.</summary>
    /// <param name="kind">The original valid case.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(MergeableChangeKind.IncrementOnlyCounterSet)]
    [Arguments(MergeableChangeKind.CounterSet)]
    [Arguments(MergeableChangeKind.AddWinsSetAdd)]
    [Arguments(MergeableChangeKind.AddWinsSetRemove)]
    [Arguments(MergeableChangeKind.LastWriterWinsValueSet)]
    public async Task DecoderRejectsFieldsFromAnotherChangeCase(MergeableChangeKind kind)
    {
        var change = kind switch
        {
            MergeableChangeKind.IncrementOnlyCounterSet => MergeableChange.IncrementOnlyCounterSet(ActorA, CountOne),
            MergeableChangeKind.CounterSet => MergeableChange.CounterSet(ActorA, CountOne, CountOne),
            MergeableChangeKind.AddWinsSetAdd => MergeableChange.AddWinsSetAdd(Bytes(Alpha)),
            MergeableChangeKind.AddWinsSetRemove => MergeableChange.AddWinsSetRemove(Bytes(Alpha), [Tag(ActorA, SequenceOne)]),
            _ => MergeableChange.LastWriterWinsValueSet(Bytes(Alpha), Stamp(ActorA, "00000000-0000-0000-0000-000000000030")),
        };
        var bytes = MergeableCodec.EncodeInput(MergeableInput.ForChange(change));
        var decoded = (MergeableChange)MergeableCodec.DecodeInput(bytes).Value;
        await Assert.That(decoded.ToKind()).IsEqualTo(kind);
        await Assert.That(Convert.ToBase64String(MergeableCodec.EncodeInput(MergeableInput.ForChange(decoded))))
            .IsEqualTo(Convert.ToBase64String(bytes));
        var foreignKind = kind == MergeableChangeKind.AddWinsSetAdd
            ? MergeableChangeKind.IncrementOnlyCounterSet
            : MergeableChangeKind.AddWinsSetAdd;
        await AssertInvalidAsync(() => MergeableCodec.DecodeInput(Mutate(bytes, MutationActorMarkerIndex - 1, (byte)foreignKind)));
    }

    /// <summary>Exercises public factory and value ownership paths.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ContractsOwnCallerCollectionsAndCompareDots()
    {
        var sourceBytes = Bytes(Alpha);
        var dot = Tag(ActorA, SequenceOne);
        var element = new TaggedElement { Tag = dot, Element = sourceBytes };
        var value = new MergeableValue { Kind = MergeableKind.AddWinsSet, Bytes = sourceBytes, Elements = [sourceBytes] };
        var mutation = MergeableChange.LastWriterWinsValueSet(sourceBytes);
        var state = new MergeableState { Kind = MergeableKind.LastWriterWinsValue, LatestValue = sourceBytes };
        sourceBytes[0] = (byte)'z';

        await Assert.That(dot.CompareTo(null)).IsEqualTo(1);
        await Assert.That(dot.CompareTo(Tag(ActorB, SequenceOne))).IsLessThan(0);
        await Assert.That(dot.CompareTo(Tag(ActorA, SequenceTwo))).IsLessThan(0);
        await Assert.That(dot.CompareTo((object)Tag(ActorA, SequenceTwo))).IsLessThan(0);
        await Assert.That(dot.CompareTo(new object())).IsEqualTo(1);
        await Assert.That(MergeableContracts.StateContractId.Length).IsGreaterThan(0);
        await Assert.That(MergeableContracts.InputContractId.Length).IsGreaterThan(0);
        await Assert.That(MergeableContracts.SchemaVersion).IsEqualTo(OneItem);
        await Assert.That(element.ElementLength).IsEqualTo(Bytes(Alpha).Length);
        await Assert.That(element.Element.Span[0]).IsEqualTo((byte)'a');
        await Assert.That(value.Bytes.Span[0]).IsEqualTo((byte)'a');
        await Assert.That(value.Elements[0].Span[0]).IsEqualTo((byte)'a');
        await Assert.That(mutation.Bytes.Span[0]).IsEqualTo((byte)'a');
        await Assert.That(state.LatestValue.Span[0]).IsEqualTo((byte)'a');
    }

    /// <summary>Exercises validation and pure function rejection branches.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PureFunctionsRejectInvalidShapes()
    {
        await AssertInvalidAsync(static () => MergeableFunctions.GetValue(new() { Kind = MergeableKind.None }));
        await AssertInvalidAsync(static () => ApplyLocalAuthoritativeInput());
        await Assert.That(static () => ApplyLocalZeroSequence()).ThrowsExactly<ArgumentOutOfRangeException>();
        await AssertInvalidAsync(static () => ApplyWrongMutationKind());
        await AssertInvalidAsync(static () => ApplyMissingCounterActor());
        await AssertInvalidAsync(static () => ApplyNegativeGrowCounter());
        await AssertInvalidAsync(static () => ApplyNegativePosNegCounter());
        await AssertInvalidAsync(static () => ValidateTooLongRegisterState());
        await AssertInvalidAsync(static () => ApplyOversizedRegister());
        await AssertInvalidAsync(static () => MergeDifferentKinds());
        await AssertInvalidAsync(static () => MergeUnsupportedKinds());
        await AssertInvalidAsync(static () => ValidateTooManyComponents());
        await AssertInvalidAsync(static () => ValidateNegativeComponent());
        await AssertInvalidAsync(static () => ValidateTooManyDotElements());
        await AssertInvalidAsync(static () => ValidateZeroDotSequence());
        await AssertInvalidAsync(static () => ValidateOversizedElement());
        await AssertInvalidAsync(static () => ApplyUnsupportedMutationKind());
        await AssertInvalidAsync(static () => ApplyWhitespaceClientId());
        await AssertInvalidAsync(static () => ApplyTooLongClientId());
        await AssertInvalidAsync(static () => ApplyInvalidUtf16ClientId());
        await AssertInvalidAsync(static () => ConstructOversizedDotElement());
    }

    /// <summary>Exercises successful pure projections and replacements for public overloads.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PureFunctionsProjectReplaceAndMergeSupportedKinds()
    {
        var counter = MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter),
            MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(ActorA, CountOne)),
            ActorA,
            SequenceOne);
        var posNeg = MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.Counter),
            MergeableInput.ForChange(MergeableChange.CounterSet(ActorA, CountThree, CountOne)),
            ActorA,
            SequenceOne);
        var replaced = MergeableFunctions.ReplaceAuthoritativeState(counter);
        var mergedCounter = MergeableFunctions.Merge(counter, new MergeableState
        {
            Kind = MergeableKind.IncrementOnlyCounter,
            IncrementOnlyTotalsByClient = new Dictionary<string, long> { [ActorB] = CountTwo },
        });
        var mergedPosNeg = MergeableFunctions.Merge(posNeg, new MergeableState { Kind = MergeableKind.Counter, DecrementsByClient = new Dictionary<string, long> { [ActorB] = CountOne } });
        MergeableFunctions.ValidateState(counter);

        await Assert.That(replaced.Value.Counter).IsEqualTo(CountOne);
        await Assert.That(mergedCounter.Value.Counter).IsEqualTo(CountThree);
        await Assert.That(mergedPosNeg.Value.Counter).IsEqualTo(CountOne);
    }

    /// <summary>Exercises codec validation and malformed payload branches.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CodecRejectsInvalidInputsAndMalformedPayloads()
    {
        var emptyState = MergeableCodec.EncodeState(MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter));
        var counterInput = MergeableCodec.EncodeInput(MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(ActorA, CountOne)));

        await Assert.That(static () => MergeableCodec.ValidateInput(null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(static () => MergeableInput.ForChange(null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(static () => MergeableInput.ForAuthoritativeState(null!)).ThrowsExactly<ArgumentNullException>();
        await AssertInvalidAsync(static () => ValidateMissingCounterActorInput());
        await AssertInvalidAsync(static () => ValidateNegativeCounterInput());
        await AssertInvalidAsync(static () => ValidateOversizedElementInput());
        await AssertInvalidAsync(static () => ValidateTooManyObservedDotsInput());
        await AssertInvalidAsync(() => MergeableCodec.DecodeState(Mutate(emptyState, FirstPayloadIndex, (byte)'X')));
        await AssertInvalidAsync(() => MergeableCodec.DecodeState(Mutate(emptyState, VersionIndex, InvalidMarker)));
        await AssertInvalidAsync(() => MergeableCodec.DecodeInput(Mutate(emptyState, PayloadTypeIndex, StatePayload)));
        await AssertInvalidAsync(() => MergeableCodec.DecodeState(counterInput));
        await AssertInvalidAsync(static () => DecodeShortPayload());
        await AssertInvalidAsync(static () => MergeableCodec.DecodeInput(WriteHeader(InputPayload, [InvalidInputKind])));
        await AssertInvalidAsync(() => MergeableCodec.DecodeInput(Mutate(counterInput, MutationActorMarkerIndex, InvalidMarker)));
        await AssertInvalidAsync(() => MergeableCodec.DecodeState(Mutate(emptyState, EmptyStateStampMarkerIndex, InvalidMarker)));
        await AssertInvalidAsync(static () => MergeableCodec.DecodeState(WriteNonCanonicalComponents()));
        await AssertInvalidAsync(static () => MergeableCodec.DecodeState(WriteNonCanonicalDotElements()));
        await AssertInvalidAsync(static () => MergeableCodec.DecodeState(WriteSameDotNonCanonicalDotElements()));
        await AssertInvalidAsync(static () => MergeableCodec.DecodeState(WriteSameDotPrefixNonCanonicalDotElements()));
        await AssertInvalidAsync(static () => MergeableCodec.DecodeState(WriteSameDotPrefixRebindDotElements()));
        await AssertInvalidAsync(static () => MergeableCodec.DecodeInput(WriteNonCanonicalObservedDots()));
        await AssertInvalidAsync(static () => MergeableCodec.DecodeState(WriteInvalidUtf8Component()));
        await AssertInvalidAsync(static () => MergeableCodec.DecodeState(WriteNegativeCount()));
        await AssertInvalidAsync(static () => MergeableCodec.DecodeState(WriteTooLongRegister()));
        await AssertInvalidAsync(static () => MergeableCodec.DecodeState(new byte[MergeableBounds.Default.MaximumEncodedBytes + 1]));
        await AssertInvalidAsync(static () => EncodeTooLongStampClient());
        await AssertInvalidAsync(static () => EncodePastPayloadLimit());
        var grownPayload = MergeableCodec.EncodeState(
            new MergeableState { Kind = MergeableKind.LastWriterWinsValue, LatestValue = new byte[ResizeRegisterBytes] },
            MergeableBounds.Default with { MaximumEncodedBytes = ResizeEncodedBytes });

        await Assert.That((MergeableCodec.DecodeInput(counterInput).Value as MergeableChange)?.ClientId).IsEqualTo(ActorA);
        await Assert.That(grownPayload.Length).IsGreaterThan(0);
    }

    /// <summary>Exercises remaining merge and ordering branches.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task MergeHandlesRegisterStampOrderingAndSetIdempotency()
    {
        var earlier = Stamp(ActorA, "00000000-0000-0000-0000-000000000010");
        var later = Stamp(ActorA, "00000000-0000-0000-0000-000000000020");
        var otherClient = Stamp(ActorB, "00000000-0000-0000-0000-000000000010");
        var left = new MergeableState { Kind = MergeableKind.LastWriterWinsValue, LatestValue = Bytes(Alpha), LatestWriteStamp = later };
        var right = new MergeableState { Kind = MergeableKind.LastWriterWinsValue, LatestValue = Bytes(Beta), LatestWriteStamp = earlier };
        var noStamp = new MergeableState { Kind = MergeableKind.LastWriterWinsValue, LatestValue = Bytes(Gamma) };
        var duplicateDot = Tag(ActorA, SequenceOne);
        var existing = new MergeableState { Kind = MergeableKind.AddWinsSet, Elements = [new() { Tag = duplicateDot, Element = Bytes(Alpha) }] };
        var duplicate = new MergeableState { Kind = MergeableKind.AddWinsSet, Elements = [new() { Tag = duplicateDot, Element = Bytes(Alpha) }] };
        var rebind = new MergeableState { Kind = MergeableKind.AddWinsSet, Elements = [new() { Tag = duplicateDot, Element = Bytes(Beta) }] };
        var removed = RemoveObservedDot();

        await Assert.That(MergeableFunctions.CompareWriteStamps(null, null)).IsEqualTo(0);
        await Assert.That(MergeableFunctions.CompareWriteStamps(later, null)).IsEqualTo(1);
        await Assert.That(MergeableFunctions.CompareWriteStamps(null, earlier)).IsEqualTo(-1);
        await Assert.That(MergeableFunctions.CompareWriteStamps(Stamp(ActorA, "00000000-0000-0000-0000-000000000001", TimestampMinute - OneItem), later)).IsLessThan(0);
        await Assert.That(MergeableFunctions.CompareWriteStamps(earlier, later)).IsLessThan(0);
        await Assert.That(MergeableFunctions.CompareWriteStamps(earlier, otherClient)).IsLessThan(0);
        await Assert.That(MergeableFunctions.Merge(left, right).Value.Bytes.Span[0]).IsEqualTo((byte)'a');
        var updatedValue = MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.LastWriterWinsValue),
            CreateStampedInput(later),
            ActorA,
            SequenceOne);
        await Assert.That(updatedValue.Value.Bytes.Span[0]).IsEqualTo((byte)'b');
        await Assert.That(MergeableFunctions.Merge(noStamp, right).Value.Bytes.Span[0]).IsEqualTo((byte)'b');
        await Assert.That(MergeableFunctions.Merge(existing, duplicate).Elements).HasSingleItem();
        await Assert.That(removed.Value.Elements).IsEmpty();
        await Assert.That(removed.RemovedTags).HasSingleItem();
        await AssertInvalidAsync(() => MergeableFunctions.Merge(existing, rebind));
    }

    /// <summary>Asserts an invalid operation exception.</summary>
    /// <param name="action">The action.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertInvalidAsync(Action action) =>
        await Assert.That(action).ThrowsExactly<InvalidOperationException>();

    /// <summary>Creates UTF-8 bytes.</summary>
    /// <param name="value">The value.</param>
    /// <returns>The bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    /// <summary>Creates a CRDT dot.</summary>
    /// <param name="clientId">The client id.</param>
    /// <param name="sequence">The sequence.</param>
    /// <returns>The dot.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ElementTag Tag(string clientId, long sequence) => new() { ClientId = clientId, ClientSequence = sequence };

    /// <summary>Creates a write stamp.</summary>
    /// <param name="clientId">The client id.</param>
    /// <param name="operationId">The operation id.</param>
    /// <returns>The stamp.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ConflictWriteStamp Stamp(string clientId, string operationId) =>
        Stamp(clientId, operationId, TimestampMinute);

    /// <summary>Creates a write stamp.</summary>
    /// <param name="clientId">The client id.</param>
    /// <param name="operationId">The operation id.</param>
    /// <param name="minute">The committed minute.</param>
    /// <returns>The stamp.</returns>
    private static ConflictWriteStamp Stamp(string clientId, string operationId, int minute) =>
        new() { CommittedAtUtc = new(TimestampYear, TimestampMonth, TimestampDay, TimestampHour, minute, 0, TimeSpan.FromHours(OffsetHour)), ClientId = clientId, OperationId = new(new(operationId)) };

    /// <summary>Creates a stamped register input.</summary>
    /// <param name="stamp">The stamp.</param>
    /// <returns>The input.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static MergeableInput CreateStampedInput(ConflictWriteStamp stamp) =>
        MergeableInput.ForChange(MergeableChange.LastWriterWinsValueSet(Bytes(Beta), stamp));

    /// <summary>Creates an OR-set remove input.</summary>
    /// <returns>The input.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static MergeableInput CreateRemoveInput() =>
        MergeableInput.ForChange(MergeableChange.AddWinsSetRemove(Bytes(Alpha), [Tag(ActorA, SequenceOne), Tag(ActorB, SequenceTwo)]));

    /// <summary>Creates counter components.</summary>
    /// <param name="actorAValue">The actor A value.</param>
    /// <param name="actorBValue">The actor B value.</param>
    /// <returns>The components.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Dictionary<string, long> CounterComponents(long actorAValue, long actorBValue)
    {
        Dictionary<string, long> components = [with(comparer: StringComparer.Ordinal)];
        components[ActorA] = actorAValue;
        components[ActorB] = actorBValue;
        return components;
    }

    /// <summary>Creates a two-dot state.</summary>
    /// <returns>The state.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static MergeableState CreateTwoDotState()
    {
        TaggedElement[] dotBindings =
        [
            new() { Tag = ValidDot, Element = Bytes(Alpha) },
            new() { Tag = Tag(ActorB, SequenceOne), Element = Bytes(Beta) },
        ];
        return new() { Kind = MergeableKind.AddWinsSet, Elements = dotBindings };
    }

    /// <summary>Decodes an intentionally short payload.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void DecodeShortPayload()
    {
        byte[] payload = [(byte)'R', (byte)'C'];
        _ = MergeableCodec.DecodeState(payload);
    }

    /// <summary>Mutates one byte in a copy.</summary>
    /// <param name="source">The source.</param>
    /// <param name="index">The index.</param>
    /// <param name="value">The new byte.</param>
    /// <returns>The mutated copy.</returns>
    private static byte[] Mutate(byte[] source, int index, byte value)
    {
        var copy = source.ToArray();
        copy[index] = value;
        return copy;
    }

    /// <summary>Writes a CRDT envelope header and body.</summary>
    /// <param name="payloadType">The payload type.</param>
    /// <param name="body">The body.</param>
    /// <returns>The payload.</returns>
    private static byte[] WriteHeader(byte payloadType, List<byte> body)
    {
        List<byte> bytes = [with(capacity: HeaderLength + body.Count), (byte)'R', (byte)'C', (byte)'D', (byte)'T', 1, payloadType];
        bytes.AddRange(body);
        return [.. bytes];
    }

    /// <summary>Builds a state with noncanonical component keys.</summary>
    /// <returns>The payload.</returns>
    private static byte[] WriteNonCanonicalComponents()
    {
        List<byte> bytes = [with(capacity: 128), (byte)MergeableKind.IncrementOnlyCounter];
        WriteInt32(bytes, TwoItems);
        WriteString(bytes, ActorB);
        WriteInt64(bytes, CountOne);
        WriteString(bytes, ActorA);
        WriteInt64(bytes, CountTwo);
        WriteEmptyStateRemainder(bytes);
        return WriteHeader(StatePayload, bytes);
    }

    /// <summary>Builds a state with noncanonical dot elements.</summary>
    /// <returns>The payload.</returns>
    private static byte[] WriteNonCanonicalDotElements()
    {
        List<byte> bytes = [with(capacity: 128), (byte)MergeableKind.AddWinsSet];
        WriteEmptyComponents(bytes);
        WriteEmptyComponents(bytes);
        WriteEmptyComponents(bytes);
        WriteInt32(bytes, TwoItems);
        WriteDotElement(bytes, Tag(ActorB, SequenceOne), Bytes(Beta));
        WriteDotElement(bytes, Tag(ActorA, SequenceOne), Bytes(Alpha));
        WriteInt32(bytes, 0);
        WriteBytes(bytes, []);
        bytes.Add(0);
        return WriteHeader(StatePayload, bytes);
    }

    /// <summary>Builds a state with same-dot noncanonical element ordering.</summary>
    /// <returns>The payload.</returns>
    private static byte[] WriteSameDotNonCanonicalDotElements()
    {
        List<byte> bytes = [with(capacity: 128), (byte)MergeableKind.AddWinsSet];
        WriteEmptyComponents(bytes);
        WriteEmptyComponents(bytes);
        WriteEmptyComponents(bytes);
        WriteInt32(bytes, TwoItems);
        WriteDotElement(bytes, ValidDot, Bytes(Beta));
        WriteDotElement(bytes, ValidDot, Bytes(Alpha));
        WriteInt32(bytes, 0);
        WriteBytes(bytes, []);
        bytes.Add(0);
        return WriteHeader(StatePayload, bytes);
    }

    /// <summary>Builds a state with same-dot prefix element ordering.</summary>
    /// <returns>The payload.</returns>
    private static byte[] WriteSameDotPrefixNonCanonicalDotElements()
    {
        List<byte> bytes = [with(capacity: 128), (byte)MergeableKind.AddWinsSet];
        WriteEmptyComponents(bytes);
        WriteEmptyComponents(bytes);
        WriteEmptyComponents(bytes);
        WriteInt32(bytes, TwoItems);
        WriteDotElement(bytes, ValidDot, Bytes(AlphaPair));
        WriteDotElement(bytes, ValidDot, Bytes(Alpha));
        WriteInt32(bytes, 0);
        WriteBytes(bytes, []);
        bytes.Add(0);
        return WriteHeader(StatePayload, bytes);
    }

    /// <summary>Builds a state with same-dot prefix element rebinding.</summary>
    /// <returns>The payload.</returns>
    private static byte[] WriteSameDotPrefixRebindDotElements()
    {
        List<byte> bytes = [with(capacity: 128), (byte)MergeableKind.AddWinsSet];
        WriteEmptyComponents(bytes);
        WriteEmptyComponents(bytes);
        WriteEmptyComponents(bytes);
        WriteInt32(bytes, TwoItems);
        WriteDotElement(bytes, ValidDot, Bytes(Alpha));
        WriteDotElement(bytes, ValidDot, Bytes(AlphaPair));
        WriteInt32(bytes, 0);
        WriteBytes(bytes, []);
        bytes.Add(0);
        return WriteHeader(StatePayload, bytes);
    }

    /// <summary>Builds an input with noncanonical observed dots.</summary>
    /// <returns>The payload.</returns>
    private static byte[] WriteNonCanonicalObservedDots()
    {
        List<byte> bytes = [with(capacity: 128), (byte)MergeableInputKind.Change, (byte)MergeableChangeKind.AddWinsSetRemove, 0];
        WriteInt64(bytes, 0);
        WriteInt64(bytes, 0);
        WriteInt64(bytes, 0);
        WriteBytes(bytes, Bytes(Alpha));
        WriteInt32(bytes, TwoItems);
        WriteDot(bytes, Tag(ActorB, SequenceOne));
        WriteDot(bytes, Tag(ActorA, SequenceOne));
        bytes.Add(0);
        return WriteHeader(InputPayload, bytes);
    }

    /// <summary>Builds a payload with an invalid UTF-8 component key.</summary>
    /// <returns>The payload.</returns>
    private static byte[] WriteInvalidUtf8Component()
    {
        List<byte> bytes = [with(capacity: 64), (byte)MergeableKind.IncrementOnlyCounter];
        WriteInt32(bytes, OneItem);
        WriteInt32(bytes, OneItem);
        bytes.Add(InvalidUtf8Byte);
        WriteInt64(bytes, CountOne);
        WriteEmptyStateRemainder(bytes);
        return WriteHeader(StatePayload, bytes);
    }

    /// <summary>Builds a payload with a negative count.</summary>
    /// <returns>The payload.</returns>
    private static byte[] WriteNegativeCount()
    {
        List<byte> bytes = [with(capacity: 32), (byte)MergeableKind.IncrementOnlyCounter];
        WriteInt32(bytes, -1);
        return WriteHeader(StatePayload, bytes);
    }

    /// <summary>Builds a payload whose register length exceeds bounds.</summary>
    /// <returns>The payload.</returns>
    private static byte[] WriteTooLongRegister()
    {
        List<byte> bytes = [with(capacity: 64), (byte)MergeableKind.LastWriterWinsValue];
        WriteEmptyComponents(bytes);
        WriteEmptyComponents(bytes);
        WriteEmptyComponents(bytes);
        WriteInt32(bytes, 0);
        WriteInt32(bytes, 0);
        WriteInt32(bytes, MergeableBounds.Default.MaximumRegisterBytes + 1);
        return WriteHeader(StatePayload, bytes);
    }

    /// <summary>Writes the empty fields after the first component map.</summary>
    /// <param name="bytes">The payload bytes.</param>
    private static void WriteEmptyStateRemainder(List<byte> bytes)
    {
        WriteEmptyComponents(bytes);
        WriteEmptyComponents(bytes);
        WriteInt32(bytes, 0);
        WriteInt32(bytes, 0);
        WriteBytes(bytes, []);
        bytes.Add(0);
    }

    /// <summary>Writes an empty component map.</summary>
    /// <param name="bytes">The payload bytes.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteEmptyComponents(List<byte> bytes) => WriteInt32(bytes, 0);

    /// <summary>Writes one dot element.</summary>
    /// <param name="bytes">The payload bytes.</param>
    /// <param name="dot">The dot.</param>
    /// <param name="element">The element.</param>
    private static void WriteDotElement(List<byte> bytes, ElementTag dot, byte[] element)
    {
        WriteDot(bytes, dot);
        WriteBytes(bytes, element);
    }

    /// <summary>Writes one dot.</summary>
    /// <param name="bytes">The payload bytes.</param>
    /// <param name="dot">The dot.</param>
    private static void WriteDot(List<byte> bytes, ElementTag dot)
    {
        WriteString(bytes, dot.ClientId);
        WriteInt64(bytes, dot.ClientSequence);
    }

    /// <summary>Writes one string.</summary>
    /// <param name="bytes">The payload bytes.</param>
    /// <param name="value">The value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteString(List<byte> bytes, string value) => WriteBytes(bytes, Bytes(value));

    /// <summary>Writes length-prefixed bytes.</summary>
    /// <param name="bytes">The payload bytes.</param>
    /// <param name="value">The value.</param>
    private static void WriteBytes(List<byte> bytes, byte[] value)
    {
        WriteInt32(bytes, value.Length);
        bytes.AddRange(value);
    }

    /// <summary>Writes a big-endian 32-bit integer.</summary>
    /// <param name="bytes">The payload bytes.</param>
    /// <param name="value">The value.</param>
    private static void WriteInt32(List<byte> bytes, int value)
    {
        for (var shift = Int32FirstShift; shift >= 0; shift -= BitsPerByte)
        {
            bytes.Add((byte)((value >> shift) & byte.MaxValue));
        }
    }

    /// <summary>Writes a big-endian 64-bit integer.</summary>
    /// <param name="bytes">The payload bytes.</param>
    /// <param name="value">The value.</param>
    private static void WriteInt64(List<byte> bytes, long value)
    {
        for (var shift = Int64FirstShift; shift >= 0; shift -= BitsPerByte)
        {
            bytes.Add((byte)((value >> shift) & byte.MaxValue));
        }
    }

    /// <summary>Applies an authoritative input locally.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ApplyLocalAuthoritativeInput() =>
        MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter),
            MergeableInput.ForAuthoritativeState(MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter)),
            ActorA,
            SequenceOne,
            MergeableBounds.Default);

    /// <summary>Applies a local mutation with sequence zero.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ApplyLocalZeroSequence() =>
        MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter),
            MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(ActorA, CountOne)),
            ActorA,
            0,
            MergeableBounds.Default);

    /// <summary>Applies a counter mutation to the wrong state kind.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ApplyWrongMutationKind() =>
        MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.Counter),
            MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(ActorA, CountOne)),
            ActorA,
            SequenceOne,
            MergeableBounds.Default);

    /// <summary>Applies a counter mutation without an actor.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ApplyMissingCounterActor() =>
        MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter),
            MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(string.Empty, 0)),
            ActorA,
            SequenceOne,
            MergeableBounds.Default);

    /// <summary>Applies a negative G-counter component.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ApplyNegativeGrowCounter() =>
        MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter),
            MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(ActorA, -1)),
            ActorA,
            SequenceOne,
            MergeableBounds.Default);

    /// <summary>Applies a negative PN-counter component.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ApplyNegativePosNegCounter() =>
        MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.Counter),
            MergeableInput.ForChange(MergeableChange.CounterSet(ActorA, -1, 0)),
            ActorA,
            SequenceOne,
            MergeableBounds.Default);

    /// <summary>Validates an oversized register state.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateTooLongRegisterState() =>
        MergeableFunctions.ValidateState(
            new() { Kind = MergeableKind.LastWriterWinsValue, LatestValue = Bytes(Alpha) },
            TinyBounds with { MaximumClientIdUtf8Bytes = MergeableBounds.Default.MaximumClientIdUtf8Bytes });

    /// <summary>Applies an oversized register mutation.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ApplyOversizedRegister() =>
        MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.LastWriterWinsValue),
            MergeableInput.ForChange(MergeableChange.LastWriterWinsValueSet(Bytes(Alpha))),
            ActorA,
            SequenceOne,
            TinyBounds with { MaximumClientIdUtf8Bytes = MergeableBounds.Default.MaximumClientIdUtf8Bytes });

    /// <summary>Applies an unsupported mutation kind.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ApplyUnsupportedMutationKind() =>
        MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter),
            MergeableCodec.DecodeInput(Mutate(
                MergeableCodec.EncodeInput(MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(ActorA, 0))),
                MutationActorMarkerIndex - 1,
                InvalidInputKind)),
            ActorA,
            SequenceOne,
            MergeableBounds.Default);

    /// <summary>Applies a local input with whitespace client id.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ApplyWhitespaceClientId() =>
        MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter),
            MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(ActorA, CountOne)),
            " ",
            SequenceOne,
            MergeableBounds.Default);

    /// <summary>Applies a local input with an oversized client id.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ApplyTooLongClientId() =>
        MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter),
            MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(ActorA, CountOne)),
            new('a', MergeableBounds.Default.MaximumClientIdUtf8Bytes + 1),
            SequenceOne,
            MergeableBounds.Default);

    /// <summary>Applies a local input with invalid UTF-16 client text.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ApplyInvalidUtf16ClientId() =>
        MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter),
            MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(ActorA, CountOne)),
            new(InvalidHighSurrogate, OneItem),
            SequenceOne,
            MergeableBounds.Default);

    /// <summary>Constructs an oversized dot element at the public boundary.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ConstructOversizedDotElement() =>
        _ = new TaggedElement { Tag = ValidDot, Element = new byte[MergeableBounds.Default.MaximumElementBytes + 1] };

    /// <summary>Encodes a stamp whose client id is too long for the selected payload bounds.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void EncodeTooLongStampClient() =>
        _ = MergeableCodec.EncodeState(
            new MergeableState { Kind = MergeableKind.LastWriterWinsValue, LatestValue = Bytes(Alpha), LatestWriteStamp = Stamp(ActorA, "00000000-0000-0000-0000-000000000030") },
            TinyBounds with { MaximumRegisterBytes = StampRegisterBytes });

    /// <summary>Encodes a state whose payload cannot fit in the selected encoded-byte bounds.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void EncodePastPayloadLimit() =>
        _ = MergeableCodec.EncodeState(MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter), MergeableBounds.Default with { MaximumEncodedBytes = 1 });

    /// <summary>Merges states of different kinds.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void MergeDifferentKinds() =>
        MergeableFunctions.Merge(MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter), MergeableFunctions.Empty(MergeableKind.Counter));

    /// <summary>Merges unsupported state kinds.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void MergeUnsupportedKinds() =>
        MergeableFunctions.Merge(MergeableFunctions.Empty(MergeableKind.None), MergeableFunctions.Empty(MergeableKind.None));

    /// <summary>Validates too many counter components.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateTooManyComponents() =>
        MergeableFunctions.ValidateState(
            new() { Kind = MergeableKind.IncrementOnlyCounter, IncrementOnlyTotalsByClient = CounterComponents(CountOne, CountTwo) },
            TinyBounds);

    /// <summary>Validates a negative counter component.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateNegativeComponent() =>
        MergeableFunctions.ValidateState(
            new() { Kind = MergeableKind.IncrementOnlyCounter, IncrementOnlyTotalsByClient = new Dictionary<string, long> { [ActorA] = -1 } },
            MergeableBounds.Default);

    /// <summary>Validates too many dot elements.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateTooManyDotElements() =>
        MergeableFunctions.ValidateState(
            CreateTwoDotState(),
            TinyBounds);

    /// <summary>Validates a zero dot sequence.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateZeroDotSequence() =>
        MergeableFunctions.ValidateState(
            new() { Kind = MergeableKind.AddWinsSet, Elements = [new() { Tag = Tag(ActorA, 0), Element = Bytes(Alpha) }] },
            MergeableBounds.Default);

    /// <summary>Validates an oversized element.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateOversizedElement() =>
        MergeableFunctions.ValidateState(
            new() { Kind = MergeableKind.AddWinsSet, Elements = [new() { Tag = ValidDot, Element = Bytes(Alpha) }] },
            TinyBounds with { MaximumClientIdUtf8Bytes = MergeableBounds.Default.MaximumClientIdUtf8Bytes });

    /// <summary>Removes an observed dot.</summary>
    /// <returns>The removed state.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static MergeableState RemoveObservedDot()
    {
        var added = MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.AddWinsSet),
            MergeableInput.ForChange(MergeableChange.AddWinsSetAdd(Bytes(Alpha))),
            ActorA,
            SequenceOne,
            MergeableBounds.Default);
        return MergeableFunctions.ApplyLocal(
            added,
            MergeableInput.ForChange(MergeableChange.AddWinsSetRemove(Bytes(Alpha), [ValidDot])),
            ActorA,
            SequenceTwo,
            MergeableBounds.Default);
    }

    /// <summary>Validates a missing actor mutation input.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateMissingCounterActorInput() =>
        MergeableCodec.ValidateInput(MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(string.Empty, 0)), MergeableBounds.Default);

    /// <summary>Validates a negative counter mutation input.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateNegativeCounterInput() =>
        MergeableCodec.ValidateInput(
            MergeableInput.ForChange(MergeableChange.CounterSet(ActorA, -1, 0)),
            MergeableBounds.Default);

    /// <summary>Validates an oversized element mutation input.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateOversizedElementInput() =>
        MergeableCodec.ValidateInput(MergeableInput.ForChange(MergeableChange.AddWinsSetAdd(Bytes(Alpha))), TinyBounds);

    /// <summary>Validates an input with too many observed dots.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateTooManyObservedDotsInput() =>
        MergeableCodec.ValidateInput(
            MergeableInput.ForChange(MergeableChange.AddWinsSetRemove(Bytes("a"), [Tag(ActorA, SequenceOne), Tag(ActorB, SequenceOne)])),
            TinyBounds);
}
