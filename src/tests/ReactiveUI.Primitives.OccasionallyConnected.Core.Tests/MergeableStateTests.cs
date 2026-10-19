// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections;
using System.Runtime.CompilerServices;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="MergeableState"/>.</summary>
public sealed partial class MergeableStateTests
{
    /// <summary>The first actor.</summary>
    private const string ActorA = "actor-a";

    /// <summary>The second actor.</summary>
    private const string ActorB = "actor-b";

    /// <summary>The alpha element text.</summary>
    private const string AlphaText = "alpha";

    /// <summary>The beta element text.</summary>
    private const string BetaText = "beta";

    /// <summary>The left register text.</summary>
    private const string LeftText = "left";

    /// <summary>The right register text.</summary>
    private const string RightText = "right";

    /// <summary>The first durable sequence.</summary>
    private const long FirstSequence = 1;

    /// <summary>The second durable sequence.</summary>
    private const long SecondSequence = 2;

    /// <summary>The dominated G-counter component.</summary>
    private const long DominatedIncrementOnlyTotal = 3;

    /// <summary>The dominant G-counter component.</summary>
    private const long DominantIncrementOnlyTotal = 7;

    /// <summary>The second G-counter component.</summary>
    private const long OtherIncrementOnlyTotal = 4;

    /// <summary>The merged G-counter value.</summary>
    private const long MergedIncrementOnlyCounterValue = 11;

    /// <summary>The PN-counter positive component.</summary>
    private const long PositivePNComponent = 10;

    /// <summary>The dominated PN-counter positive component.</summary>
    private const long DominatedPositivePNComponent = 4;

    /// <summary>The initial PN-counter negative component.</summary>
    private const long InitialNegativePNComponent = 1;

    /// <summary>The dominant PN-counter negative component.</summary>
    private const long DominantNegativePNComponent = 8;

    /// <summary>The resulting PN-counter value.</summary>
    private const long CounterValue = 2;

    /// <summary>The codec counter value.</summary>
    private const long CodecCounterValue = 5;

    /// <summary>The first operation id.</summary>
    private const string FirstOperationId = "00000000-0000-0000-0000-000000000001";

    /// <summary>The second operation id.</summary>
    private const string SecondOperationId = "00000000-0000-0000-0000-000000000002";

    /// <summary>The timestamp year.</summary>
    private const int TimestampYear = 2026;

    /// <summary>The timestamp month.</summary>
    private const int TimestampMonth = 9;

    /// <summary>The timestamp day.</summary>
    private const int TimestampDay = 13;

    /// <summary>The timestamp hour.</summary>
    private const int TimestampHour = 5;

    /// <summary>The zero timestamp part.</summary>
    private const int TimestampZeroPart = 0;

    /// <summary>The first byte index.</summary>
    private const int FirstByteIndex = 0;

    /// <summary>Verifies G-counter updates are absolute, component-wise maxima.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task IncrementOnlyCounterDominatedUpdateIsAcceptedAsNoOp()
    {
        var state = MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter);
        state = MergeableFunctions.ApplyLocal(
            state,
            MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(ActorA, DominantIncrementOnlyTotal)),
            ActorA,
            FirstSequence,
            MergeableBounds.Default);
        state = MergeableFunctions.ApplyLocal(
            state,
            MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(ActorA, DominatedIncrementOnlyTotal)),
            ActorA,
            SecondSequence,
            MergeableBounds.Default);
        state = MergeableFunctions.ApplyLocal(
            state,
            MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(ActorB, OtherIncrementOnlyTotal)),
            ActorB,
            FirstSequence,
            MergeableBounds.Default);

        await Assert.That(state.Value.Counter).IsEqualTo(MergedIncrementOnlyCounterValue);
        await Assert.That(state.IncrementOnlyTotalsByClient[ActorA]).IsEqualTo(DominantIncrementOnlyTotal);
    }

    /// <summary>Verifies PN-counter positive and negative components are independent.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CounterKeepsPositiveAndNegativeComponentsIndependent()
    {
        var state = MergeableFunctions.Empty(MergeableKind.Counter);
        state = MergeableFunctions.ApplyLocal(
            state,
            MergeableInput.ForChange(MergeableChange.CounterSet(ActorA, PositivePNComponent, InitialNegativePNComponent)),
            ActorA,
            FirstSequence,
            MergeableBounds.Default);
        state = MergeableFunctions.ApplyLocal(
            state,
            MergeableInput.ForChange(MergeableChange.CounterSet(ActorA, DominatedPositivePNComponent, DominantNegativePNComponent)),
            ActorA,
            SecondSequence,
            MergeableBounds.Default);

        await Assert.That(state.IncrementsByClient[ActorA]).IsEqualTo(PositivePNComponent);
        await Assert.That(state.DecrementsByClient[ActorA]).IsEqualTo(DominantNegativePNComponent);
        await Assert.That(state.Value.Counter).IsEqualTo(CounterValue);
    }

    /// <summary>Verifies counter actors cannot impersonate a different authenticated client.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CounterMutationsRejectActorMismatch()
    {
        await Assert.That(static () => MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter),
            MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(ActorB, DominantIncrementOnlyTotal)),
            ActorA,
            FirstSequence,
            MergeableBounds.Default)).ThrowsExactly<InvalidOperationException>();

        await Assert.That(static () => MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.Counter),
            MergeableInput.ForChange(MergeableChange.CounterSet(ActorB, PositivePNComponent, InitialNegativePNComponent)),
            ActorA,
            FirstSequence,
            MergeableBounds.Default)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies OR-set remove-before-add retains a tombstone and concurrent unseen adds survive.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AddWinsSetRemoveBeforeAddKeepsObservedDotRemoved()
    {
        var removedDot = new ElementTag { ClientId = ActorA, ClientSequence = FirstSequence };
        var survivorDot = new ElementTag { ClientId = ActorB, ClientSequence = FirstSequence };
        var tombstoned = new MergeableState { Kind = MergeableKind.AddWinsSet, RemovedTags = [new TaggedElement { Tag = removedDot, Element = Bytes(AlphaText) }] };
        var incoming = new MergeableState
        {
            Kind = MergeableKind.AddWinsSet,
            Elements = [new TaggedElement { Tag = removedDot, Element = Bytes(AlphaText) }, new TaggedElement { Tag = survivorDot, Element = Bytes(AlphaText) }],
        };

        var merged = MergeableFunctions.Merge(tombstoned, incoming, MergeableBounds.Default);

        await Assert.That(merged.Value.Elements).HasSingleItem();
        await AssertBytesAsync(merged.Value.Elements[FirstByteIndex], Bytes(AlphaText));
    }

    /// <summary>Verifies the same dot cannot be rebound to different element bytes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AddWinsSetRejectsDotRebind()
    {
        var dot = new ElementTag { ClientId = ActorA, ClientSequence = FirstSequence };
        var dotBindings = new TaggedElement[]
        {
            new() { Tag = dot, Element = Bytes(AlphaText) },
            new() { Tag = dot, Element = Bytes(BetaText) },
        };
        var state = new MergeableState { Kind = MergeableKind.AddWinsSet, Elements = dotBindings };

        await Assert.That(() => MergeableFunctions.ValidateState(state, MergeableBounds.Default)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a tombstone cannot bind an existing dot to a different element.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AddWinsSetRejectsDotRebindAcrossBindingsAndTombstones()
    {
        var dot = new ElementTag { ClientId = ActorA, ClientSequence = FirstSequence };
        var state = new MergeableState
        {
            Kind = MergeableKind.AddWinsSet,
            Elements = [new TaggedElement { Tag = dot, Element = Bytes(AlphaText) }],
            RemovedTags = [new TaggedElement { Tag = dot, Element = Bytes(BetaText) }],
        };
        var removeBeforeAdd = new MergeableState { Kind = MergeableKind.AddWinsSet, RemovedTags = [new TaggedElement { Tag = dot, Element = Bytes(BetaText) }] };
        var incomingAdd = new MergeableState { Kind = MergeableKind.AddWinsSet, Elements = [new TaggedElement { Tag = dot, Element = Bytes(AlphaText) }] };

        await Assert.That(() => MergeableFunctions.ValidateState(state, MergeableBounds.Default)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => MergeableFunctions.Merge(removeBeforeAdd, incomingAdd, MergeableBounds.Default)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies each state kind rejects metadata that belongs to another CRDT family.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ValidateStateRejectsKindForeignMetadata()
    {
        var dot = new ElementTag { ClientId = ActorA, ClientSequence = FirstSequence };
        ConflictWriteStamp stamp = new()
        {
            CommittedAtUtc = new(TimestampYear, TimestampMonth, TimestampDay, TimestampHour, TimestampZeroPart, TimestampZeroPart, TimeSpan.Zero),
            ClientId = ActorA,
            OperationId = new(new Guid(FirstOperationId)),
        };

        await Assert.That(() => MergeableFunctions.ValidateState(
            new()
            {
                Kind = MergeableKind.IncrementOnlyCounter,
                IncrementOnlyTotalsByClient = new Dictionary<string, long> { [ActorA] = DominantIncrementOnlyTotal },
                LatestValue = Bytes(AlphaText),
                LatestWriteStamp = stamp,
            },
            MergeableBounds.Default)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(static () => MergeableFunctions.ValidateState(
            new() { Kind = MergeableKind.Counter, IncrementOnlyTotalsByClient = new Dictionary<string, long> { [ActorA] = DominantIncrementOnlyTotal } },
            MergeableBounds.Default)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(static () => MergeableFunctions.ValidateState(
            new() { Kind = MergeableKind.AddWinsSet, IncrementsByClient = new Dictionary<string, long> { [ActorA] = PositivePNComponent } },
            MergeableBounds.Default)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => MergeableFunctions.ValidateState(
            new() { Kind = MergeableKind.LastWriterWinsValue, Elements = [new TaggedElement { Tag = dot, Element = Bytes(AlphaText) }] },
            MergeableBounds.Default)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies caller-owned collection counts are bounded before trusting custom implementations.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PublicContractsRejectOversizedCallerCollectionsBeforeAllocation()
    {
        var oversizedComponents = new OversizedCounterComponents(MergeableBounds.Default.MaximumCounterComponents + 1);
        var oversizedDotElements = new FixedCountDotElements(MergeableBounds.Default.MaximumDotBindings + 1, FirstSequence, Bytes(AlphaText));
        var growingDots = new FixedCountDots(1, MergeableBounds.Default.MaximumTombstones + 1);

        await Assert.That(() => new MergeableState { Kind = MergeableKind.IncrementOnlyCounter, IncrementOnlyTotalsByClient = oversizedComponents }).ThrowsExactly<InvalidOperationException>();
        await Assert.That(oversizedComponents.WasEnumerated).IsFalse();
        await Assert.That(() => new MergeableState { Kind = MergeableKind.AddWinsSet, Elements = oversizedDotElements }).ThrowsExactly<InvalidOperationException>();
        await Assert.That(oversizedDotElements.WasEnumerated).IsFalse();
        await Assert.That(() => MergeableChange.AddWinsSetRemove(Bytes(AlphaText), growingDots)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(growingDots.WasEnumerated).IsTrue();
    }

    /// <summary>Verifies LWW ordering uses Guid.CompareTo after timestamp and client id.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task LastWriterWinsValueUsesGuidCompareToForTieBreak()
    {
        var timestamp = new DateTimeOffset(TimestampYear, TimestampMonth, TimestampDay, TimestampHour, TimestampZeroPart, TimestampZeroPart, TimeSpan.Zero);
        var left = CreateRegister(LeftText, new() { CommittedAtUtc = timestamp, ClientId = ActorA, OperationId = new(new Guid(FirstOperationId)) });
        var right = CreateRegister(RightText, new() { CommittedAtUtc = timestamp, ClientId = ActorA, OperationId = new(new Guid(SecondOperationId)) });

        var merged = MergeableFunctions.Merge(left, right, MergeableBounds.Default);

        await Assert.That(Encoding.UTF8.GetString(merged.Value.Bytes.Span)).IsEqualTo(RightText);
    }

    /// <summary>Verifies CRDT state payloads roundtrip deterministically and reject trailing data.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CodecRoundTripsDeterministicallyAndRejectsTrailingData()
    {
        var state = MergeableFunctions.ApplyLocal(
            MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter),
            MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet(ActorA, CodecCounterValue)),
            ActorA,
            FirstSequence,
            MergeableBounds.Default);
        var first = MergeableCodec.EncodeState(state, MergeableBounds.Default);
        var second = MergeableCodec.EncodeState(state with { IncrementOnlyTotalsByClient = new Dictionary<string, long> { [ActorA] = CodecCounterValue } }, MergeableBounds.Default);
        var trailing = new byte[first.Length + FirstSequence];
        first.CopyTo(trailing, FirstByteIndex);

        var decoded = MergeableCodec.DecodeState(first, MergeableBounds.Default);

        await AssertBytesAsync(second, first);
        await Assert.That(decoded.Value.Counter).IsEqualTo(CodecCounterValue);
        await Assert.That(() => MergeableCodec.DecodeState(trailing, MergeableBounds.Default)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies byte inputs are copied at public boundaries.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task BytePayloadsAreOwned()
    {
        var bytes = Bytes(AlphaText);
        var mutation = MergeableChange.AddWinsSetAdd(bytes);
        bytes[FirstByteIndex] = (byte)'z';

        await Assert.That(Encoding.UTF8.GetString(mutation.Bytes.Span)).IsEqualTo(AlphaText);
    }

    /// <summary>Creates UTF-8 bytes.</summary>
    /// <param name="value">The value.</param>
    /// <returns>The bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    /// <summary>Asserts byte memory content.</summary>
    /// <param name="actual">The actual bytes.</param>
    /// <param name="expected">The expected bytes.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertBytesAsync(ReadOnlyMemory<byte> actual, byte[] expected)
    {
        await Assert.That(actual.Length).IsEqualTo(expected.Length);
        for (var index = 0; index < expected.Length; index++)
        {
            await Assert.That(actual.Span[index]).IsEqualTo(expected[index]);
        }
    }

    /// <summary>Creates a LWW register state.</summary>
    /// <param name="value">The register value.</param>
    /// <param name="stamp">The write stamp.</param>
    /// <returns>The state.</returns>
    private static MergeableState CreateRegister(string value, ConflictWriteStamp stamp) =>
        new() { Kind = MergeableKind.LastWriterWinsValue, LatestValue = Bytes(value), LatestWriteStamp = stamp };

    /// <summary>Dictionary that reports an attacker-controlled count and fails if enumerated.</summary>
    private sealed class OversizedCounterComponents : IReadOnlyDictionary<string, long>
    {
        /// <summary>Initializes a new instance of the <see cref="OversizedCounterComponents"/> class.</summary>
        /// <param name="count">The reported count.</param>
        public OversizedCounterComponents(int count) => Count = count;

        /// <summary>Gets a value indicating whether enumeration was attempted.</summary>
        public bool WasEnumerated { get; private set; }

        /// <inheritdoc/>
        public IEnumerable<string> Keys => [];

        /// <inheritdoc/>
        public IEnumerable<long> Values => [];

        /// <inheritdoc/>
        public int Count { get; }

        /// <inheritdoc/>
        public long this[string key] => throw new KeyNotFoundException(key);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ContainsKey(string key) => false;

        /// <inheritdoc/>
        public bool TryGetValue(string key, out long value)
        {
            value = 0;
            return false;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<KeyValuePair<string, long>> GetEnumerator()
        {
            WasEnumerated = true;
            throw new InvalidOperationException("Oversized components should be rejected before enumeration.");
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>Tag element list that reports an attacker-controlled count and fails if enumerated.</summary>
    private sealed class FixedCountDotElements : IReadOnlyList<TaggedElement>
    {
        /// <summary>The element bytes.</summary>
        private readonly byte[] _element;

        /// <summary>Initializes a new instance of the <see cref="FixedCountDotElements"/> class.</summary>
        /// <param name="count">The reported count.</param>
        /// <param name="sequence">The dot sequence.</param>
        /// <param name="element">The element bytes.</param>
        public FixedCountDotElements(int count, long sequence, byte[] element)
        {
            Count = count;
            Sequence = sequence;
            _element = element;
        }

        /// <summary>Gets a value indicating whether enumeration was attempted.</summary>
        public bool WasEnumerated { get; private set; }

        /// <inheritdoc/>
        public int Count { get; }

        /// <summary>Gets the dot sequence.</summary>
        private long Sequence { get; }

        /// <inheritdoc/>
        public TaggedElement this[int index] => new() { Tag = new() { ClientId = ActorA, ClientSequence = Sequence + index }, Element = _element };

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<TaggedElement> GetEnumerator()
        {
            WasEnumerated = true;
            throw new InvalidOperationException("Oversized dot elements should be rejected before enumeration.");
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>Tag list that can report one count and enumerate another.</summary>
    private sealed class FixedCountDots : IReadOnlyList<ElementTag>
    {
        /// <summary>The number of dots yielded by enumeration.</summary>
        private readonly int _enumeratedCount;

        /// <summary>Initializes a new instance of the <see cref="FixedCountDots"/> class.</summary>
        /// <param name="reportedCount">The reported count.</param>
        /// <param name="enumeratedCount">The enumerated count.</param>
        public FixedCountDots(int reportedCount, int enumeratedCount)
        {
            Count = reportedCount;
            _enumeratedCount = enumeratedCount;
        }

        /// <summary>Gets a value indicating whether enumeration was attempted.</summary>
        public bool WasEnumerated { get; private set; }

        /// <inheritdoc/>
        public int Count { get; }

        /// <inheritdoc/>
        public ElementTag this[int index] => new() { ClientId = ActorA, ClientSequence = index + FirstSequence };

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<ElementTag> GetEnumerator()
        {
            WasEnumerated = true;
            for (var index = 0; index < _enumeratedCount; index++)
            {
                yield return this[index];
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
