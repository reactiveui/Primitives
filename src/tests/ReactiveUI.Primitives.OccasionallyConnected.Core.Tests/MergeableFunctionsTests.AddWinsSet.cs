// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests OR-set limits and causal checkpoint reclamation.</summary>
public sealed partial class MergeableFunctionsTests
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
    public async Task AddWinsSetMergeHonorsConfiguredElementLimitAbove4096()
    {
        const int half = 3000;
        var first = CreateSet(ClientId, 1, half);
        var second = CreateSet(OtherClientId, half + 1, half);
        var bounds = new MergeableBounds { MaximumElements = half * Second };
        var merged = MergeableFunctions.Merge(first, second, bounds);
        await Assert.That(merged.Value.Elements.Count).IsEqualTo(half * Second);
        await AssertSameState(merged, MergeableFunctions.Merge(second, first, bounds));
        await Assert.That(MergeableCodec.DecodeState(MergeableCodec.EncodeState(merged, bounds), bounds).Value.Elements.Count)
            .IsEqualTo(half * Second);
        await Assert.That(() => MergeableFunctions.Merge(first, second, bounds with { MaximumElements = (half * Second) - 1 }))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Checks active-element limits count distinct surviving bytes, not retained dots.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AddWinsSetActiveElementLimitIgnoresRemovedAndDuplicateValues()
    {
        var first = Binding(ClientId, 1, 1);
        var duplicate = Binding(ClientId, Second, 1);
        var removed = Binding(ClientId, Third, Second);
        var state = new MergeableState { Kind = MergeableKind.AddWinsSet, Elements = [first, duplicate, removed], RemovedTags = [removed] };
        var bounds = new MergeableBounds { MaximumElements = 1 };
        MergeableFunctions.ValidateState(state, bounds);
        await Assert.That(state.Value.Elements).HasSingleItem();
        await Assert.That(() => MergeableFunctions.ValidateState(state with { RemovedTags = [] }, bounds))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => MergeableCodec.DecodeState(MergeableCodec.EncodeState(state with { RemovedTags = [] }), bounds))
            .ThrowsExactly<InvalidOperationException>();
        var input = MergeableInput.ForChange(MergeableChange.AddWinsSetAdd(BitConverter.GetBytes(Third)));
        await Assert.That(() => MergeableFunctions.ApplyLocal(state, input, ClientId, Fourth, bounds))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Checks repeated removals reclaim metadata without accepting stale replicas or operation replay.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AddWinsSetCheckpointSupportsRepeatedRemoveCyclesAndStaleReplay()
    {
        var bounds = new MergeableBounds { MaximumTombstones = 1, MaximumDotBindings = 1, MaximumElements = 1 };
        var state = MergeableFunctions.Empty(MergeableKind.AddWinsSet);
        List<MergeableState> stale = [];
        const int cycles = 20;
        for (var sequence = 1; sequence <= cycles; sequence++)
        {
            var input = MergeableInput.ForChange(MergeableChange.AddWinsSetAdd(BitConverter.GetBytes(sequence)));
            state = MergeableFunctions.ApplyLocal(state, input, ClientId, sequence, bounds);
            stale.Add(state);
            state = RemoveBinding(state, state.Elements[0], bounds);
            state = MergeableFunctions.CheckpointAddWinsSet(state, new Dictionary<string, long> { [ClientId] = sequence }, bounds);
            await Assert.That(state.RemovedTags).IsEmpty();
            await Assert.That(state.Elements).IsEmpty();
        }

        state = MergeableCodec.DecodeState(MergeableCodec.EncodeState(state, bounds), bounds);
        foreach (var old in stale)
        {
            await AssertSameState(MergeableFunctions.Merge(state, old, bounds), state);
            await AssertSameState(MergeableFunctions.Merge(old, state, bounds), state);
        }

        var replay = MergeableInput.ForChange(MergeableChange.AddWinsSetAdd(BitConverter.GetBytes(1)));
        await AssertSameState(MergeableFunctions.ApplyLocal(state, replay, ClientId, 1, bounds), state);
        await Assert.That(state.AddWinsSetFrontier[ClientId]).IsEqualTo((long)cycles);
    }

    /// <summary>Checks checkpoint knowledge preserves live dots, concurrent adds and unclaimed sequence gaps.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AddWinsSetCheckpointPreservesLiveAndConcurrentDots()
    {
        var state = CreateSet(ClientId, 1, Second);
        var live = state.Elements[1];
        var removed = RemoveBinding(state, state.Elements[0], MergeableBounds.Default);
        var checkpoint = MergeableFunctions.CheckpointAddWinsSet(removed, new Dictionary<string, long> { [ClientId] = Second });
        await Assert.That(checkpoint.Elements).HasSingleItem();
        await Assert.That(checkpoint.Elements[0]).IsEqualTo(live);
        var merged = MergeableFunctions.Merge(checkpoint, state);
        await AssertSameState(merged, checkpoint);
        var future = CreateSet(ClientId, Fourth, 1);
        var gap = CreateSet(ClientId, Third, 1);
        var concurrent = CreateSet(OtherClientId, 1, 1);
        merged = MergeableFunctions.Merge(MergeableFunctions.Merge(MergeableFunctions.Merge(checkpoint, future), gap), concurrent);
        await Assert.That(merged.Elements.Count).IsEqualTo(Fourth);
        await Assert.That(merged.Value.Elements.Count).IsEqualTo(Fourth);
        await AssertSameState(
            MergeableFunctions.Merge(MergeableFunctions.Merge(checkpoint, future), state),
            MergeableFunctions.Merge(checkpoint, MergeableFunctions.Merge(future, state)));
        await AssertSameState(MergeableFunctions.Merge(checkpoint, checkpoint), checkpoint);
    }

    /// <summary>Checks remove-before-add checkpoints permanently reject a late removed dot.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AddWinsSetCheckpointRetainsRemoveBeforeAddKnowledge()
    {
        var binding = Binding(ClientId, 1, 1);
        var state = new MergeableState { Kind = MergeableKind.AddWinsSet, RemovedTags = [binding] };
        state = MergeableFunctions.CheckpointAddWinsSet(state, new Dictionary<string, long> { [ClientId] = 1 });
        await Assert.That(state.RemovedTags).IsEmpty();
        var late = new MergeableState { Kind = MergeableKind.AddWinsSet, Elements = [binding] };
        await AssertSameState(MergeableFunctions.Merge(state, late), state);
        await AssertSameState(MergeableFunctions.Merge(late, state), state);
    }

    /// <summary>Checks covered removals on a delayed replica reclaim without exhausting the tombstone budget.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AddWinsSetCheckpointMergesDelayedRemovalsWithinBounds()
    {
        var state = CreateSet(ClientId, 1, Second);
        var checkpoint = MergeableFunctions.CheckpointAddWinsSet(state, new Dictionary<string, long> { [ClientId] = Second });
        checkpoint = RemoveBinding(checkpoint, checkpoint.Elements[0], MergeableBounds.Default);
        var delayed = RemoveBinding(state, state.Elements[1], MergeableBounds.Default);
        var bounds = new MergeableBounds { MaximumTombstones = 1 };
        var merged = MergeableFunctions.Merge(checkpoint, delayed, bounds);
        await Assert.That(merged.RemovedTags).IsEmpty();
        await Assert.That(merged.Elements).IsEmpty();
        await AssertSameState(merged, MergeableFunctions.Merge(delayed, checkpoint, bounds));
        await AssertSameState(MergeableFunctions.Merge(merged, state, bounds), merged);
    }

    /// <summary>Checks reclamation leaves dots outside the proven prefix protected by exact tombstones.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AddWinsSetCheckpointDoesNotInferUnobservedPrefixes()
    {
        var state = CreateSet(ClientId, 1, Second);
        var removed = RemoveBinding(state, state.Elements[1], MergeableBounds.Default);
        var checkpoint = MergeableFunctions.CheckpointAddWinsSet(removed, new Dictionary<string, long> { [ClientId] = 1 });
        await Assert.That(checkpoint.RemovedTags).HasSingleItem();
        await Assert.That(checkpoint.AddWinsSetFrontier[ClientId]).IsEqualTo(1L);
        await AssertSameState(MergeableFunctions.Merge(checkpoint, state), checkpoint);
    }

    /// <summary>Checks malformed, oversized and decreasing frontier knowledge is rejected.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AddWinsSetCheckpointRejectsInvalidFrontier()
    {
        var state = MergeableFunctions.Empty(MergeableKind.AddWinsSet);
        foreach (var value in new long[] { -1, 0 })
        {
            await Assert.That(() => MergeableFunctions.CheckpointAddWinsSet(state, new Dictionary<string, long> { [ClientId] = value }))
                .ThrowsExactly<InvalidOperationException>();
        }

        await Assert.That(() => MergeableFunctions.CheckpointAddWinsSet(state, new Dictionary<string, long> { [string.Empty] = 1 }))
            .ThrowsExactly<InvalidOperationException>();
        var prefixes = new Dictionary<string, long> { [ClientId] = Second, [OtherClientId] = 1 };
        await Assert.That(() => MergeableFunctions.CheckpointAddWinsSet(state, prefixes, new MergeableBounds { MaximumCounterComponents = 1 }))
            .ThrowsExactly<InvalidOperationException>();
        state = MergeableFunctions.CheckpointAddWinsSet(state, prefixes);
        await Assert.That(() => MergeableFunctions.CheckpointAddWinsSet(state, new Dictionary<string, long> { [ClientId] = 1 }))
            .ThrowsExactly<InvalidOperationException>();
        await AssertSameState(MergeableFunctions.CheckpointAddWinsSet(state, new Dictionary<string, long>()), state);
        await Assert.That(() => MergeableFunctions.CheckpointAddWinsSet(MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter), prefixes))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => MergeableFunctions.ValidateState(state with { Kind = MergeableKind.LastWriterWinsValue }))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Checks checkpoint state and authoritative input persist knowledge while legacy encoding stays at version one.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AddWinsSetCheckpointCodecRoundTripsCausalKnowledge()
    {
        const byte checkpointVersion = 2;
        var legacy = CreateSet(ClientId, 1, Second);
        var legacyBytes = MergeableCodec.EncodeState(legacy);
        await Assert.That(legacyBytes[4]).IsEqualTo((byte)1);
        await AssertSameState(MergeableCodec.DecodeState(legacyBytes), legacy);
        var state = MergeableFunctions.CheckpointAddWinsSet(
            RemoveBinding(legacy, legacy.Elements[0], MergeableBounds.Default),
            new Dictionary<string, long> { [ClientId] = Second });
        var bytes = MergeableCodec.EncodeState(state);
        await Assert.That(bytes[Fourth]).IsEqualTo(checkpointVersion);
        await AssertSameState(MergeableCodec.DecodeState(bytes), state);
        var inputBytes = MergeableCodec.EncodeInput(MergeableInput.ForAuthoritativeState(state));
        var recovered = (MergeableState)MergeableCodec.DecodeInput(inputBytes).Value;
        await AssertSameState(recovered, state);
        await AssertSameState(MergeableFunctions.Merge(recovered, legacy), state);
        await Assert.That(() => MergeableCodec.DecodeState(bytes.AsMemory(0, bytes.Length - 1)))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Checks merge laws across legacy removals and independently checkpointed replicas.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AddWinsSetCheckpointMergesSatisfySemilatticeLaws()
    {
        var initial = CreateSet(ClientId, 1, Second);
        var firstRemoved = RemoveBinding(initial, initial.Elements[0], MergeableBounds.Default);
        var secondRemoved = RemoveBinding(initial, initial.Elements[1], MergeableBounds.Default);
        var prefix = new Dictionary<string, long> { [ClientId] = Second };
        MergeableState[] states =
        [
            initial,
            firstRemoved,
            secondRemoved,
            MergeableFunctions.CheckpointAddWinsSet(firstRemoved, prefix),
            MergeableFunctions.CheckpointAddWinsSet(secondRemoved, prefix),
            CreateSet(OtherClientId, 1, 1),
        ];
        foreach (var first in states)
        {
            await AssertSameState(MergeableFunctions.Merge(first, first), first);
            foreach (var second in states)
            {
                await AssertSameState(MergeableFunctions.Merge(first, second), MergeableFunctions.Merge(second, first));
                foreach (var third in states)
                {
                    await AssertSameState(
                        MergeableFunctions.Merge(MergeableFunctions.Merge(first, second), third),
                        MergeableFunctions.Merge(first, MergeableFunctions.Merge(second, third)));
                }
            }
        }
    }

    /// <summary>Checks duplicate local adds are idempotent and rebinding is still rejected.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AddWinsSetLocalAddRetainsDotIdentityAfterCheckpoint()
    {
        var state = MergeableFunctions.CheckpointAddWinsSet(
            CreateSet(ClientId, 1, 1),
            new Dictionary<string, long> { [ClientId] = 1 });
        var same = MergeableInput.ForChange(MergeableChange.AddWinsSetAdd(BitConverter.GetBytes(1)));
        await AssertSameState(MergeableFunctions.ApplyLocal(state, same, ClientId, 1), state);
        var rebound = MergeableInput.ForChange(MergeableChange.AddWinsSetAdd(BitConverter.GetBytes(Second)));
        await Assert.That(() => MergeableFunctions.ApplyLocal(state, rebound, ClientId, 1))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Checks unknown versions and version-two mutations fail closed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AddWinsSetCodecRejectsUnknownVersionAndCheckpointMutation()
    {
        const byte unknownVersion = 3;
        const byte checkpointVersion = 2;
        var stateBytes = MergeableCodec.EncodeState(MergeableFunctions.Empty(MergeableKind.AddWinsSet));
        stateBytes[Fourth] = unknownVersion;
        await Assert.That(() => MergeableCodec.DecodeState(stateBytes)).ThrowsExactly<InvalidOperationException>();
        var inputBytes = MergeableCodec.EncodeInput(MergeableInput.ForChange(MergeableChange.AddWinsSetAdd(ReadOnlyMemory<byte>.Empty)));
        inputBytes[Fourth] = checkpointVersion;
        await Assert.That(() => MergeableCodec.DecodeInput(inputBytes)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Creates a set with unique element values.</summary>
    /// <param name="client">The actor.</param>
    /// <param name="start">The first sequence and element.</param>
    /// <param name="count">The binding count.</param>
    /// <returns>The state.</returns>
    private static MergeableState CreateSet(string client, int start, int count) => new()
    {
        Kind = MergeableKind.AddWinsSet,
        Elements = Enumerable.Range(start, count).Select(sequence => Binding(client, sequence, sequence)).ToArray(),
    };

    /// <summary>Creates a dot-to-element binding.</summary>
    /// <param name="client">The actor.</param>
    /// <param name="sequence">The durable sequence.</param>
    /// <param name="value">The element value.</param>
    /// <returns>The binding.</returns>
    private static TaggedElement Binding(string client, long sequence, int value) =>
        new() { Tag = new() { ClientId = client, ClientSequence = sequence }, Element = BitConverter.GetBytes(value) };

    /// <summary>Removes one observed binding.</summary>
    /// <param name="state">The state.</param>
    /// <param name="binding">The observed binding.</param>
    /// <param name="bounds">The bounds.</param>
    /// <returns>The next state.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static MergeableState RemoveBinding(MergeableState state, TaggedElement binding, MergeableBounds bounds) =>
        MergeableFunctions.ApplyLocal(state, MergeableInput.ForChange(MergeableChange.AddWinsSetRemove(binding.Element, [binding.Tag])), ClientId, 1, bounds);
}
