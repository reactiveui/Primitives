// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections;
using System.Runtime.CompilerServices;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="CrdtState"/>.</summary>
public sealed partial class CrdtStateTests
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
    private const long DominatedGCounterComponent = 3;

    /// <summary>The dominant G-counter component.</summary>
    private const long DominantGCounterComponent = 7;

    /// <summary>The second G-counter component.</summary>
    private const long OtherGCounterComponent = 4;

    /// <summary>The merged G-counter value.</summary>
    private const long MergedGCounterValue = 11;

    /// <summary>The PN-counter positive component.</summary>
    private const long PositivePNComponent = 10;

    /// <summary>The dominated PN-counter positive component.</summary>
    private const long DominatedPositivePNComponent = 4;

    /// <summary>The initial PN-counter negative component.</summary>
    private const long InitialNegativePNComponent = 1;

    /// <summary>The dominant PN-counter negative component.</summary>
    private const long DominantNegativePNComponent = 8;

    /// <summary>The resulting PN-counter value.</summary>
    private const long PNCounterValue = 2;

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
    public async Task GCounterDominatedUpdateIsAcceptedAsNoOp()
    {
        var state = CrdtFunctions.Empty(CrdtKind.GCounter);
        state = CrdtFunctions.ApplyLocal(state, CrdtInput.ForMutation(CrdtMutation.GCounterSet(ActorA, DominantGCounterComponent)), ActorA, FirstSequence, CrdtBounds.Default);
        state = CrdtFunctions.ApplyLocal(state, CrdtInput.ForMutation(CrdtMutation.GCounterSet(ActorA, DominatedGCounterComponent)), ActorA, SecondSequence, CrdtBounds.Default);
        state = CrdtFunctions.ApplyLocal(state, CrdtInput.ForMutation(CrdtMutation.GCounterSet(ActorB, OtherGCounterComponent)), ActorB, FirstSequence, CrdtBounds.Default);

        await Assert.That(state.Value.Counter).IsEqualTo(MergedGCounterValue);
        await Assert.That(state.GCounterComponents[ActorA]).IsEqualTo(DominantGCounterComponent);
    }

    /// <summary>Verifies PN-counter positive and negative components are independent.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PNCounterKeepsPositiveAndNegativeComponentsIndependent()
    {
        var state = CrdtFunctions.Empty(CrdtKind.PNCounter);
        state = CrdtFunctions.ApplyLocal(
            state,
            CrdtInput.ForMutation(CrdtMutation.PNCounterSet(ActorA, PositivePNComponent, InitialNegativePNComponent)),
            ActorA,
            FirstSequence,
            CrdtBounds.Default);
        state = CrdtFunctions.ApplyLocal(
            state,
            CrdtInput.ForMutation(CrdtMutation.PNCounterSet(ActorA, DominatedPositivePNComponent, DominantNegativePNComponent)),
            ActorA,
            SecondSequence,
            CrdtBounds.Default);

        await Assert.That(state.PNCounterPositiveComponents[ActorA]).IsEqualTo(PositivePNComponent);
        await Assert.That(state.PNCounterNegativeComponents[ActorA]).IsEqualTo(DominantNegativePNComponent);
        await Assert.That(state.Value.Counter).IsEqualTo(PNCounterValue);
    }

    /// <summary>Verifies counter actors cannot impersonate a different authenticated client.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CounterMutationsRejectActorMismatch()
    {
        await Assert.That(static () => CrdtFunctions.ApplyLocal(
            CrdtFunctions.Empty(CrdtKind.GCounter),
            CrdtInput.ForMutation(CrdtMutation.GCounterSet(ActorB, DominantGCounterComponent)),
            ActorA,
            FirstSequence,
            CrdtBounds.Default)).ThrowsExactly<InvalidOperationException>();

        await Assert.That(static () => CrdtFunctions.ApplyLocal(
            CrdtFunctions.Empty(CrdtKind.PNCounter),
            CrdtInput.ForMutation(CrdtMutation.PNCounterSet(ActorB, PositivePNComponent, InitialNegativePNComponent)),
            ActorA,
            FirstSequence,
            CrdtBounds.Default)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies OR-set remove-before-add retains a tombstone and concurrent unseen adds survive.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ORSetRemoveBeforeAddKeepsObservedDotRemoved()
    {
        var removedDot = new CrdtDot { ClientId = ActorA, ClientSequence = FirstSequence };
        var survivorDot = new CrdtDot { ClientId = ActorB, ClientSequence = FirstSequence };
        var tombstoned = new CrdtState { Kind = CrdtKind.ORSet, Tombstones = [new CrdtDotElement { Dot = removedDot, Element = Bytes(AlphaText) }] };
        var incoming = new CrdtState
        {
            Kind = CrdtKind.ORSet,
            DotBindings = [new CrdtDotElement { Dot = removedDot, Element = Bytes(AlphaText) }, new CrdtDotElement { Dot = survivorDot, Element = Bytes(AlphaText) }],
        };

        var merged = CrdtFunctions.Merge(tombstoned, incoming, CrdtBounds.Default);

        await Assert.That(merged.Value.Elements).HasSingleItem();
        await AssertBytesAsync(merged.Value.Elements[FirstByteIndex], Bytes(AlphaText));
    }

    /// <summary>Verifies the same dot cannot be rebound to different element bytes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ORSetRejectsDotRebind()
    {
        var dot = new CrdtDot { ClientId = ActorA, ClientSequence = FirstSequence };
        var dotBindings = new CrdtDotElement[]
        {
            new() { Dot = dot, Element = Bytes(AlphaText) },
            new() { Dot = dot, Element = Bytes(BetaText) },
        };
        var state = new CrdtState { Kind = CrdtKind.ORSet, DotBindings = dotBindings };

        await Assert.That(() => CrdtFunctions.ValidateState(state, CrdtBounds.Default)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a tombstone cannot bind an existing dot to a different element.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ORSetRejectsDotRebindAcrossBindingsAndTombstones()
    {
        var dot = new CrdtDot { ClientId = ActorA, ClientSequence = FirstSequence };
        var state = new CrdtState
        {
            Kind = CrdtKind.ORSet,
            DotBindings = [new CrdtDotElement { Dot = dot, Element = Bytes(AlphaText) }],
            Tombstones = [new CrdtDotElement { Dot = dot, Element = Bytes(BetaText) }],
        };
        var removeBeforeAdd = new CrdtState { Kind = CrdtKind.ORSet, Tombstones = [new CrdtDotElement { Dot = dot, Element = Bytes(BetaText) }] };
        var incomingAdd = new CrdtState { Kind = CrdtKind.ORSet, DotBindings = [new CrdtDotElement { Dot = dot, Element = Bytes(AlphaText) }] };

        await Assert.That(() => CrdtFunctions.ValidateState(state, CrdtBounds.Default)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => CrdtFunctions.Merge(removeBeforeAdd, incomingAdd, CrdtBounds.Default)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies each state kind rejects metadata that belongs to another CRDT family.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ValidateStateRejectsKindForeignMetadata()
    {
        var dot = new CrdtDot { ClientId = ActorA, ClientSequence = FirstSequence };
        ConflictWriteStamp stamp = new()
        {
            CommittedAtUtc = new(TimestampYear, TimestampMonth, TimestampDay, TimestampHour, TimestampZeroPart, TimestampZeroPart, TimeSpan.Zero),
            ClientId = ActorA,
            OperationId = new(new Guid(FirstOperationId)),
        };

        await Assert.That(() => CrdtFunctions.ValidateState(
            new() { Kind = CrdtKind.GCounter, GCounterComponents = new Dictionary<string, long> { [ActorA] = DominantGCounterComponent }, RegisterValue = Bytes(AlphaText), RegisterStamp = stamp },
            CrdtBounds.Default)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(static () => CrdtFunctions.ValidateState(
            new() { Kind = CrdtKind.PNCounter, GCounterComponents = new Dictionary<string, long> { [ActorA] = DominantGCounterComponent } },
            CrdtBounds.Default)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(static () => CrdtFunctions.ValidateState(
            new() { Kind = CrdtKind.ORSet, PNCounterPositiveComponents = new Dictionary<string, long> { [ActorA] = PositivePNComponent } },
            CrdtBounds.Default)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => CrdtFunctions.ValidateState(
            new() { Kind = CrdtKind.LwwRegister, DotBindings = [new CrdtDotElement { Dot = dot, Element = Bytes(AlphaText) }] },
            CrdtBounds.Default)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies caller-owned collection counts are bounded before trusting custom implementations.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PublicContractsRejectOversizedCallerCollectionsBeforeAllocation()
    {
        var oversizedComponents = new OversizedCounterComponents(CrdtBounds.Default.MaximumCounterComponents + 1);
        var oversizedDotElements = new FixedCountDotElements(CrdtBounds.Default.MaximumDotBindings + 1, FirstSequence, Bytes(AlphaText));
        var growingDots = new FixedCountDots(1, CrdtBounds.Default.MaximumTombstones + 1);

        await Assert.That(() => new CrdtState { Kind = CrdtKind.GCounter, GCounterComponents = oversizedComponents }).ThrowsExactly<InvalidOperationException>();
        await Assert.That(oversizedComponents.WasEnumerated).IsFalse();
        await Assert.That(() => new CrdtState { Kind = CrdtKind.ORSet, DotBindings = oversizedDotElements }).ThrowsExactly<InvalidOperationException>();
        await Assert.That(oversizedDotElements.WasEnumerated).IsFalse();
        await Assert.That(() => CrdtMutation.ORSetRemove(Bytes(AlphaText), growingDots)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(growingDots.WasEnumerated).IsTrue();
    }

    /// <summary>Verifies LWW ordering uses Guid.CompareTo after timestamp and client id.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task LwwRegisterUsesGuidCompareToForTieBreak()
    {
        var timestamp = new DateTimeOffset(TimestampYear, TimestampMonth, TimestampDay, TimestampHour, TimestampZeroPart, TimestampZeroPart, TimeSpan.Zero);
        var left = CreateRegister(LeftText, new() { CommittedAtUtc = timestamp, ClientId = ActorA, OperationId = new(new Guid(FirstOperationId)) });
        var right = CreateRegister(RightText, new() { CommittedAtUtc = timestamp, ClientId = ActorA, OperationId = new(new Guid(SecondOperationId)) });

        var merged = CrdtFunctions.Merge(left, right, CrdtBounds.Default);

        await Assert.That(Encoding.UTF8.GetString(merged.Value.Bytes.Span)).IsEqualTo(RightText);
    }

    /// <summary>Verifies CRDT state payloads roundtrip deterministically and reject trailing data.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CodecRoundTripsDeterministicallyAndRejectsTrailingData()
    {
        var state = CrdtFunctions.ApplyLocal(
            CrdtFunctions.Empty(CrdtKind.GCounter),
            CrdtInput.ForMutation(CrdtMutation.GCounterSet(ActorA, CodecCounterValue)),
            ActorA,
            FirstSequence,
            CrdtBounds.Default);
        var first = CrdtCodec.EncodeState(state, CrdtBounds.Default);
        var second = CrdtCodec.EncodeState(state with { GCounterComponents = new Dictionary<string, long> { [ActorA] = CodecCounterValue } }, CrdtBounds.Default);
        var trailing = new byte[first.Length + FirstSequence];
        first.CopyTo(trailing, FirstByteIndex);

        var decoded = CrdtCodec.DecodeState(first, CrdtBounds.Default);

        await AssertBytesAsync(second, first);
        await Assert.That(decoded.Value.Counter).IsEqualTo(CodecCounterValue);
        await Assert.That(() => CrdtCodec.DecodeState(trailing, CrdtBounds.Default)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies byte inputs are copied at public boundaries.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task BytePayloadsAreOwned()
    {
        var bytes = Bytes(AlphaText);
        var mutation = CrdtMutation.ORSetAdd(bytes);
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
    private static CrdtState CreateRegister(string value, ConflictWriteStamp stamp) =>
        new() { Kind = CrdtKind.LwwRegister, RegisterValue = Bytes(value), RegisterStamp = stamp };

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

    /// <summary>Dot element list that reports an attacker-controlled count and fails if enumerated.</summary>
    private sealed class FixedCountDotElements : IReadOnlyList<CrdtDotElement>
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
        public CrdtDotElement this[int index] => new() { Dot = new() { ClientId = ActorA, ClientSequence = Sequence + index }, Element = _element };

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<CrdtDotElement> GetEnumerator()
        {
            WasEnumerated = true;
            throw new InvalidOperationException("Oversized dot elements should be rejected before enumeration.");
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>Dot list that can report one count and enumerate another.</summary>
    private sealed class FixedCountDots : IReadOnlyList<CrdtDot>
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
        public CrdtDot this[int index] => new() { ClientId = ActorA, ClientSequence = index + FirstSequence };

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<CrdtDot> GetEnumerator()
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
