// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests OR-set limits and causal checkpoint reclamation.</summary>
public sealed partial class CrdtFunctionsTests
{
    /// <summary>The second test sequence and value.</summary>
    private const int Second = 2;

    /// <summary>The third test sequence and value.</summary>
    private const int Third = 3;

    /// <summary>The fourth test sequence and value.</summary>
    private const int Fourth = 4;

    /// <summary>The concurrent actor.</summary>
    private const string OtherClientId = "other";

    /// <summary>Checks merging valid sets has no hidden 4096-element copy limit.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ORSetMergeHonorsConfiguredElementLimitAbove4096()
    {
        const int half = 3000;
        var first = CreateSet(ClientId, 1, half);
        var second = CreateSet(OtherClientId, half + 1, half);
        var bounds = new CrdtBounds { MaximumElements = half * Second };
        var merged = CrdtFunctions.Merge(first, second, bounds);
        await Assert.That(merged.Value.Elements.Count).IsEqualTo(half * Second);
        await AssertSameState(merged, CrdtFunctions.Merge(second, first, bounds));
        await Assert.That(CrdtCodec.DecodeState(CrdtCodec.EncodeState(merged, bounds), bounds).Value.Elements.Count)
            .IsEqualTo(half * Second);
        await Assert.That(() => CrdtFunctions.Merge(first, second, bounds with { MaximumElements = (half * Second) - 1 }))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Checks active-element limits count distinct surviving bytes, not retained dots.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ORSetActiveElementLimitIgnoresRemovedAndDuplicateValues()
    {
        var first = Binding(ClientId, 1, 1);
        var duplicate = Binding(ClientId, Second, 1);
        var removed = Binding(ClientId, Third, Second);
        var state = new CrdtState { Kind = CrdtKind.ORSet, DotBindings = [first, duplicate, removed], Tombstones = [removed] };
        var bounds = new CrdtBounds { MaximumElements = 1 };
        CrdtFunctions.ValidateState(state, bounds);
        await Assert.That(state.Value.Elements).HasSingleItem();
        await Assert.That(() => CrdtFunctions.ValidateState(state with { Tombstones = [] }, bounds))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => CrdtCodec.DecodeState(CrdtCodec.EncodeState(state with { Tombstones = [] }), bounds))
            .ThrowsExactly<InvalidOperationException>();
        var input = CrdtInput.ForMutation(CrdtMutation.ORSetAdd(BitConverter.GetBytes(Third)));
        await Assert.That(() => CrdtFunctions.ApplyLocal(state, input, ClientId, Fourth, bounds))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Checks repeated removals reclaim metadata without accepting stale replicas or operation replay.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ORSetCheckpointSupportsRepeatedRemoveCyclesAndStaleReplay()
    {
        var bounds = new CrdtBounds { MaximumTombstones = 1, MaximumDotBindings = 1, MaximumElements = 1 };
        var state = CrdtFunctions.Empty(CrdtKind.ORSet);
        List<CrdtState> stale = [];
        const int cycles = 20;
        for (var sequence = 1; sequence <= cycles; sequence++)
        {
            var input = CrdtInput.ForMutation(CrdtMutation.ORSetAdd(BitConverter.GetBytes(sequence)));
            state = CrdtFunctions.ApplyLocal(state, input, ClientId, sequence, bounds);
            stale.Add(state);
            state = RemoveBinding(state, state.DotBindings[0], bounds);
            state = CrdtFunctions.CheckpointORSet(state, new Dictionary<string, long> { [ClientId] = sequence }, bounds);
            await Assert.That(state.Tombstones).IsEmpty();
            await Assert.That(state.DotBindings).IsEmpty();
        }

        state = CrdtCodec.DecodeState(CrdtCodec.EncodeState(state, bounds), bounds);
        foreach (var old in stale)
        {
            await AssertSameState(CrdtFunctions.Merge(state, old, bounds), state);
            await AssertSameState(CrdtFunctions.Merge(old, state, bounds), state);
        }

        var replay = CrdtInput.ForMutation(CrdtMutation.ORSetAdd(BitConverter.GetBytes(1)));
        await AssertSameState(CrdtFunctions.ApplyLocal(state, replay, ClientId, 1, bounds), state);
        await Assert.That(state.ORSetFrontier[ClientId]).IsEqualTo((long)cycles);
    }

    /// <summary>Checks checkpoint knowledge preserves live dots, concurrent adds and unclaimed sequence gaps.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ORSetCheckpointPreservesLiveAndConcurrentDots()
    {
        var state = CreateSet(ClientId, 1, Second);
        var live = state.DotBindings[1];
        var removed = RemoveBinding(state, state.DotBindings[0], CrdtBounds.Default);
        var checkpoint = CrdtFunctions.CheckpointORSet(removed, new Dictionary<string, long> { [ClientId] = Second });
        await Assert.That(checkpoint.DotBindings).HasSingleItem();
        await Assert.That(checkpoint.DotBindings[0]).IsEqualTo(live);
        var merged = CrdtFunctions.Merge(checkpoint, state);
        await AssertSameState(merged, checkpoint);
        var future = CreateSet(ClientId, Fourth, 1);
        var gap = CreateSet(ClientId, Third, 1);
        var concurrent = CreateSet(OtherClientId, 1, 1);
        merged = CrdtFunctions.Merge(CrdtFunctions.Merge(CrdtFunctions.Merge(checkpoint, future), gap), concurrent);
        await Assert.That(merged.DotBindings.Count).IsEqualTo(Fourth);
        await Assert.That(merged.Value.Elements.Count).IsEqualTo(Fourth);
        await AssertSameState(
            CrdtFunctions.Merge(CrdtFunctions.Merge(checkpoint, future), state),
            CrdtFunctions.Merge(checkpoint, CrdtFunctions.Merge(future, state)));
        await AssertSameState(CrdtFunctions.Merge(checkpoint, checkpoint), checkpoint);
    }

    /// <summary>Checks remove-before-add checkpoints permanently reject a late removed dot.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ORSetCheckpointRetainsRemoveBeforeAddKnowledge()
    {
        var binding = Binding(ClientId, 1, 1);
        var state = new CrdtState { Kind = CrdtKind.ORSet, Tombstones = [binding] };
        state = CrdtFunctions.CheckpointORSet(state, new Dictionary<string, long> { [ClientId] = 1 });
        await Assert.That(state.Tombstones).IsEmpty();
        var late = new CrdtState { Kind = CrdtKind.ORSet, DotBindings = [binding] };
        await AssertSameState(CrdtFunctions.Merge(state, late), state);
        await AssertSameState(CrdtFunctions.Merge(late, state), state);
    }

    /// <summary>Checks covered removals on a delayed replica reclaim without exhausting the tombstone budget.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ORSetCheckpointMergesDelayedRemovalsWithinBounds()
    {
        var state = CreateSet(ClientId, 1, Second);
        var checkpoint = CrdtFunctions.CheckpointORSet(state, new Dictionary<string, long> { [ClientId] = Second });
        checkpoint = RemoveBinding(checkpoint, checkpoint.DotBindings[0], CrdtBounds.Default);
        var delayed = RemoveBinding(state, state.DotBindings[1], CrdtBounds.Default);
        var bounds = new CrdtBounds { MaximumTombstones = 1 };
        var merged = CrdtFunctions.Merge(checkpoint, delayed, bounds);
        await Assert.That(merged.Tombstones).IsEmpty();
        await Assert.That(merged.DotBindings).IsEmpty();
        await AssertSameState(merged, CrdtFunctions.Merge(delayed, checkpoint, bounds));
        await AssertSameState(CrdtFunctions.Merge(merged, state, bounds), merged);
    }

    /// <summary>Checks reclamation leaves dots outside the proven prefix protected by exact tombstones.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ORSetCheckpointDoesNotInferUnobservedPrefixes()
    {
        var state = CreateSet(ClientId, 1, Second);
        var removed = RemoveBinding(state, state.DotBindings[1], CrdtBounds.Default);
        var checkpoint = CrdtFunctions.CheckpointORSet(removed, new Dictionary<string, long> { [ClientId] = 1 });
        await Assert.That(checkpoint.Tombstones).HasSingleItem();
        await Assert.That(checkpoint.ORSetFrontier[ClientId]).IsEqualTo(1L);
        await AssertSameState(CrdtFunctions.Merge(checkpoint, state), checkpoint);
    }

    /// <summary>Checks malformed, oversized and decreasing frontier knowledge is rejected.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ORSetCheckpointRejectsInvalidFrontier()
    {
        var state = CrdtFunctions.Empty(CrdtKind.ORSet);
        foreach (var value in new long[] { -1, 0 })
        {
            await Assert.That(() => CrdtFunctions.CheckpointORSet(state, new Dictionary<string, long> { [ClientId] = value }))
                .ThrowsExactly<InvalidOperationException>();
        }

        await Assert.That(() => CrdtFunctions.CheckpointORSet(state, new Dictionary<string, long> { [string.Empty] = 1 }))
            .ThrowsExactly<InvalidOperationException>();
        var prefixes = new Dictionary<string, long> { [ClientId] = Second, [OtherClientId] = 1 };
        await Assert.That(() => CrdtFunctions.CheckpointORSet(state, prefixes, new CrdtBounds { MaximumCounterComponents = 1 }))
            .ThrowsExactly<InvalidOperationException>();
        state = CrdtFunctions.CheckpointORSet(state, prefixes);
        await Assert.That(() => CrdtFunctions.CheckpointORSet(state, new Dictionary<string, long> { [ClientId] = 1 }))
            .ThrowsExactly<InvalidOperationException>();
        await AssertSameState(CrdtFunctions.CheckpointORSet(state, new Dictionary<string, long>()), state);
        await Assert.That(() => CrdtFunctions.CheckpointORSet(CrdtFunctions.Empty(CrdtKind.GCounter), prefixes))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => CrdtFunctions.ValidateState(state with { Kind = CrdtKind.LwwRegister }))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Checks checkpoint state and authoritative input persist knowledge while legacy encoding stays at version one.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ORSetCheckpointCodecRoundTripsCausalKnowledge()
    {
        const byte checkpointVersion = 2;
        var legacy = CreateSet(ClientId, 1, Second);
        var legacyBytes = CrdtCodec.EncodeState(legacy);
        await Assert.That(legacyBytes[4]).IsEqualTo((byte)1);
        await AssertSameState(CrdtCodec.DecodeState(legacyBytes), legacy);
        var state = CrdtFunctions.CheckpointORSet(
            RemoveBinding(legacy, legacy.DotBindings[0], CrdtBounds.Default),
            new Dictionary<string, long> { [ClientId] = Second });
        var bytes = CrdtCodec.EncodeState(state);
        await Assert.That(bytes[Fourth]).IsEqualTo(checkpointVersion);
        await AssertSameState(CrdtCodec.DecodeState(bytes), state);
        var inputBytes = CrdtCodec.EncodeInput(CrdtInput.ForAuthoritativeState(state));
        var recovered = CrdtCodec.DecodeInput(inputBytes).State!;
        await AssertSameState(recovered, state);
        await AssertSameState(CrdtFunctions.Merge(recovered, legacy), state);
        await Assert.That(() => CrdtCodec.DecodeState(bytes.AsMemory(0, bytes.Length - 1)))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Checks merge laws across legacy removals and independently checkpointed replicas.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ORSetCheckpointMergesSatisfySemilatticeLaws()
    {
        var initial = CreateSet(ClientId, 1, Second);
        var firstRemoved = RemoveBinding(initial, initial.DotBindings[0], CrdtBounds.Default);
        var secondRemoved = RemoveBinding(initial, initial.DotBindings[1], CrdtBounds.Default);
        var prefix = new Dictionary<string, long> { [ClientId] = Second };
        CrdtState[] states =
        [
            initial,
            firstRemoved,
            secondRemoved,
            CrdtFunctions.CheckpointORSet(firstRemoved, prefix),
            CrdtFunctions.CheckpointORSet(secondRemoved, prefix),
            CreateSet(OtherClientId, 1, 1),
        ];
        foreach (var first in states)
        {
            await AssertSameState(CrdtFunctions.Merge(first, first), first);
            foreach (var second in states)
            {
                await AssertSameState(CrdtFunctions.Merge(first, second), CrdtFunctions.Merge(second, first));
                foreach (var third in states)
                {
                    await AssertSameState(
                        CrdtFunctions.Merge(CrdtFunctions.Merge(first, second), third),
                        CrdtFunctions.Merge(first, CrdtFunctions.Merge(second, third)));
                }
            }
        }
    }

    /// <summary>Checks duplicate local adds are idempotent and rebinding is still rejected.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ORSetLocalAddRetainsDotIdentityAfterCheckpoint()
    {
        var state = CrdtFunctions.CheckpointORSet(
            CreateSet(ClientId, 1, 1),
            new Dictionary<string, long> { [ClientId] = 1 });
        var same = CrdtInput.ForMutation(CrdtMutation.ORSetAdd(BitConverter.GetBytes(1)));
        await AssertSameState(CrdtFunctions.ApplyLocal(state, same, ClientId, 1), state);
        var rebound = CrdtInput.ForMutation(CrdtMutation.ORSetAdd(BitConverter.GetBytes(Second)));
        await Assert.That(() => CrdtFunctions.ApplyLocal(state, rebound, ClientId, 1))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Checks unknown versions and version-two mutations fail closed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ORSetCodecRejectsUnknownVersionAndCheckpointMutation()
    {
        const byte unknownVersion = 3;
        const byte checkpointVersion = 2;
        var stateBytes = CrdtCodec.EncodeState(CrdtFunctions.Empty(CrdtKind.ORSet));
        stateBytes[Fourth] = unknownVersion;
        await Assert.That(() => CrdtCodec.DecodeState(stateBytes)).ThrowsExactly<InvalidOperationException>();
        var inputBytes = CrdtCodec.EncodeInput(CrdtInput.ForMutation(CrdtMutation.ORSetAdd(ReadOnlyMemory<byte>.Empty)));
        inputBytes[Fourth] = checkpointVersion;
        await Assert.That(() => CrdtCodec.DecodeInput(inputBytes)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Creates a set with unique element values.</summary>
    /// <param name="client">The actor.</param>
    /// <param name="start">The first sequence and element.</param>
    /// <param name="count">The binding count.</param>
    /// <returns>The state.</returns>
    private static CrdtState CreateSet(string client, int start, int count) => new()
    {
        Kind = CrdtKind.ORSet,
        DotBindings = Enumerable.Range(start, count).Select(sequence => Binding(client, sequence, sequence)).ToArray(),
    };

    /// <summary>Creates a dot-to-element binding.</summary>
    /// <param name="client">The actor.</param>
    /// <param name="sequence">The durable sequence.</param>
    /// <param name="value">The element value.</param>
    /// <returns>The binding.</returns>
    private static CrdtDotElement Binding(string client, long sequence, int value) =>
        new() { Dot = new() { ClientId = client, ClientSequence = sequence }, Element = BitConverter.GetBytes(value) };

    /// <summary>Removes one observed binding.</summary>
    /// <param name="state">The state.</param>
    /// <param name="binding">The observed binding.</param>
    /// <param name="bounds">The bounds.</param>
    /// <returns>The next state.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static CrdtState RemoveBinding(CrdtState state, CrdtDotElement binding, CrdtBounds bounds) =>
        CrdtFunctions.ApplyLocal(state, CrdtInput.ForMutation(CrdtMutation.ORSetRemove(binding.Element, [binding.Dot])), ClientId, 1, bounds);
}
