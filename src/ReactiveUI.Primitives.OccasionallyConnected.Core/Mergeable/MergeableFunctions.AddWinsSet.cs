// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

/// <summary>Provides pure CRDT validation, projection, merge, and value helpers.</summary>
public static partial class MergeableFunctions
{
    /// <summary>Reclaims removed OR-set dots while retaining fully observed sequence prefixes.</summary>
    /// <param name="state">The complete OR-set state.</param>
    /// <param name="observedPrefixes">Per-client durable stream sequences through which every operation has been applied.</param>
    /// <returns>The checkpoint state, including active bindings and permanent causal knowledge.</returns>
    /// <exception cref="InvalidOperationException">The state, prefixes, or kind are invalid, or a prefix decreases.</exception>
    /// <remarks>
    /// The caller must prove each prefix is fully observed, not merely the largest received sequence.
    /// Persist and synchronize the returned state atomically. Never reuse client identities with reset sequences.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static MergeableState CheckpointAddWinsSet(MergeableState state, IReadOnlyDictionary<string, long> observedPrefixes) =>
        CheckpointAddWinsSet(state, observedPrefixes, MergeableBounds.Default);

    /// <summary>Reclaims removed OR-set dots while retaining fully observed sequence prefixes.</summary>
    /// <param name="state">The complete OR-set state.</param>
    /// <param name="observedPrefixes">Per-client durable stream sequences through which every operation has been applied.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The checkpoint state, including active bindings and permanent causal knowledge.</returns>
    /// <exception cref="InvalidOperationException">The state, prefixes, or kind are invalid, or a prefix decreases.</exception>
    /// <remarks>
    /// The caller must prove each prefix is fully observed, not merely the largest received sequence.
    /// Persist and synchronize the returned state atomically. Never reuse client identities with reset sequences.
    /// </remarks>
    public static MergeableState CheckpointAddWinsSet(
        MergeableState state,
        IReadOnlyDictionary<string, long> observedPrefixes,
        MergeableBounds bounds)
    {
        ArgumentExceptionHelper.ThrowIfNull(observedPrefixes);
        ValidateKind(state, MergeableKind.AddWinsSet, bounds);
        ValidateFrontier(observedPrefixes, bounds);
        foreach (var prefix in observedPrefixes)
        {
            if (state.AddWinsSetFrontier.TryGetValue(prefix.Key, out var previous) && prefix.Value < previous)
            {
                throw new InvalidOperationException("An OR-set checkpoint cannot decrease causal knowledge.");
            }
        }

        var frontier = MergeComponents(state.AddWinsSetFrontier, observedPrefixes);
        var next = CreateAddWinsSetState(IndexBindings(state.Elements), IndexBindings(state.RemovedTags), frontier);
        ValidateState(next, bounds);
        return next;
    }

    /// <summary>Validates fully observed sequence prefixes.</summary>
    /// <param name="frontier">The prefixes.</param>
    /// <param name="bounds">The bounds.</param>
    /// <exception cref="InvalidOperationException">A prefix or its identity is invalid.</exception>
    private static void ValidateFrontier(IReadOnlyDictionary<string, long> frontier, MergeableBounds bounds)
    {
        ValidateCounterComponents(frontier, bounds);
        foreach (var prefix in frontier)
        {
            if (prefix.Value == 0)
            {
                throw new InvalidOperationException("OR-set checkpoint prefixes must be positive.");
            }
        }
    }

    /// <summary>Checks whether causal knowledge covers a dot.</summary>
    /// <param name="frontier">The fully observed prefixes.</param>
    /// <param name="dot">The dot.</param>
    /// <returns>Whether the dot's sequence has been fully observed.</returns>
    private static bool IsCovered(IReadOnlyDictionary<string, long> frontier, ElementTag dot) =>
        frontier.TryGetValue(dot.ClientId, out var prefix) && dot.ClientSequence <= prefix;

    /// <summary>Indexes retained bindings without copying element bytes.</summary>
    /// <param name="source">The bindings.</param>
    /// <returns>The dot index.</returns>
    private static Dictionary<ElementTag, TaggedElement> IndexBindings(IReadOnlyList<TaggedElement> source)
    {
        Dictionary<ElementTag, TaggedElement> index = [with(capacity: source.Count)];
        foreach (var binding in source)
        {
            AddIndexedBinding(index, binding);
        }

        return index;
    }

    /// <summary>Adds a binding while rejecting dot rebinding.</summary>
    /// <param name="index">The dot index.</param>
    /// <param name="binding">The incoming binding.</param>
    /// <exception cref="InvalidOperationException">A dot is rebound.</exception>
    private static void AddIndexedBinding(Dictionary<ElementTag, TaggedElement> index, TaggedElement binding)
    {
        if (index.TryGetValue(binding.Tag, out var previous) && !BytesEqual(previous.ElementSpan, binding.ElementSpan))
        {
            throw new InvalidOperationException(DotRebindMessage);
        }

        index[binding.Tag] = binding;
    }

    /// <summary>Merges bindings, suppressing dots absent from a checkpoint that already observed them.</summary>
    /// <param name="left">The first state.</param>
    /// <param name="right">The second state.</param>
    /// <returns>The merged binding index.</returns>
    private static Dictionary<ElementTag, TaggedElement> MergeActiveBindings(MergeableState left, MergeableState right)
    {
        var leftBindings = IndexBindings(left.Elements);
        var rightBindings = IndexBindings(right.Elements);
        var merged = IndexBindings(left.Elements);
        foreach (var binding in right.Elements)
        {
            AddIndexedBinding(merged, binding);
        }

        var candidates = SnapshotBindings(merged);
        ValidateNoCrossRebind(candidates, left.RemovedTags);
        ValidateNoCrossRebind(candidates, right.RemovedTags);
        foreach (var binding in candidates)
        {
            if ((!leftBindings.ContainsKey(binding.Tag) && IsCovered(left.AddWinsSetFrontier, binding.Tag))
                || (!rightBindings.ContainsKey(binding.Tag) && IsCovered(right.AddWinsSetFrontier, binding.Tag)))
            {
                _ = merged.Remove(binding.Tag);
            }
        }

        return merged;
    }

    /// <summary>Reclaims covered removals and stores the frontier alongside canonical retained metadata.</summary>
    /// <param name="bindings">The owned mutable binding index.</param>
    /// <param name="tombstones">The owned mutable removal index.</param>
    /// <param name="frontier">The fully observed prefixes.</param>
    /// <returns>The complete state.</returns>
    private static MergeableState CreateAddWinsSetState(
        Dictionary<ElementTag, TaggedElement> bindings,
        Dictionary<ElementTag, TaggedElement> tombstones,
        IReadOnlyDictionary<string, long> frontier)
    {
        var removals = SnapshotBindings(tombstones);
        ValidateNoCrossRebind(SnapshotBindings(bindings), removals);
        foreach (var tombstone in removals)
        {
            if (!IsCovered(frontier, tombstone.Tag))
            {
                continue;
            }

            _ = bindings.Remove(tombstone.Tag);
            _ = tombstones.Remove(tombstone.Tag);
        }

        return new() { Kind = MergeableKind.AddWinsSet, Elements = SortedBindings(bindings), RemovedTags = SortedBindings(tombstones), AddWinsSetFrontier = frontier };
    }

    /// <summary>Snapshots dictionary values before removing entries.</summary>
    /// <param name="bindings">The index.</param>
    /// <returns>The binding references.</returns>
    private static TaggedElement[] SnapshotBindings(Dictionary<ElementTag, TaggedElement> bindings)
    {
        var values = new TaggedElement[bindings.Count];
        bindings.Values.CopyTo(values, 0);
        return values;
    }

    /// <summary>Sorts retained binding references by dot identity.</summary>
    /// <param name="bindings">The index.</param>
    /// <returns>The canonical binding references.</returns>
    private static TaggedElement[] SortedBindings(Dictionary<ElementTag, TaggedElement> bindings)
    {
        var values = SnapshotBindings(bindings);
        Array.Sort(values, static (left, right) => left.Tag.CompareTo(right.Tag));
        return values;
    }

    /// <summary>Sorts active bindings by element bytes without copying payloads.</summary>
    /// <param name="state">The state.</param>
    /// <returns>The sorted active bindings.</returns>
    private static List<TaggedElement> GetSortedActiveBindings(MergeableState state)
    {
        var tombstones = IndexBindings(state.RemovedTags);
        List<TaggedElement> active = [with(capacity: state.Elements.Count)];
        foreach (var binding in state.Elements)
        {
            if (!tombstones.ContainsKey(binding.Tag))
            {
                active.Add(binding);
            }
        }

        active.Sort(static (left, right) => CompareBytes(left.ElementSpan, right.ElementSpan));
        return active;
    }

    /// <summary>Checks derived limits and counter overflow without allocating a public value.</summary>
    /// <param name="state">The state.</param>
    /// <param name="bounds">The bounds.</param>
    /// <exception cref="InvalidOperationException">The active element count exceeds configured bounds.</exception>
    private static void ValidateDerivedValue(MergeableState state, MergeableBounds bounds)
    {
        switch (state.Kind)
        {
            case MergeableKind.IncrementOnlyCounter:
            {
                _ = SumComponents(state.IncrementOnlyTotalsByClient);
                break;
            }

            case MergeableKind.Counter:
            {
                _ = checked(SumComponents(state.IncrementsByClient) - SumComponents(state.DecrementsByClient));
                break;
            }

            case MergeableKind.AddWinsSet:
            {
                ValidateActiveElementCount(state, bounds);
                break;
            }

            default:
            {
                break;
            }
        }
    }

    /// <summary>Validates distinct active elements against the configured count.</summary>
    /// <param name="state">The OR-set state.</param>
    /// <param name="bounds">The bounds.</param>
    /// <exception cref="InvalidOperationException">The active element count exceeds configured bounds.</exception>
    private static void ValidateActiveElementCount(MergeableState state, MergeableBounds bounds)
    {
        var count = 0;
        TaggedElement? previous = null;
        foreach (var binding in GetSortedActiveBindings(state))
        {
            if (previous is not null && BytesEqual(previous.ElementSpan, binding.ElementSpan))
            {
                continue;
            }

            count++;
            if (count > bounds.MaximumElements)
            {
                throw new InvalidOperationException("The CRDT active element count exceeds configured bounds.");
            }

            previous = binding;
        }
    }
}
