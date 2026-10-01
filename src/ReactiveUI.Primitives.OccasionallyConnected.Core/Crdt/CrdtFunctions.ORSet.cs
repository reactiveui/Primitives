// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Crdt;

/// <summary>Provides pure CRDT validation, projection, merge, and value helpers.</summary>
public static partial class CrdtFunctions
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
    public static CrdtState CheckpointORSet(CrdtState state, IReadOnlyDictionary<string, long> observedPrefixes) =>
        CheckpointORSet(state, observedPrefixes, CrdtBounds.Default);

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
    public static CrdtState CheckpointORSet(
        CrdtState state,
        IReadOnlyDictionary<string, long> observedPrefixes,
        CrdtBounds bounds)
    {
        ArgumentExceptionHelper.ThrowIfNull(observedPrefixes);
        ValidateKind(state, CrdtKind.ORSet, bounds);
        ValidateFrontier(observedPrefixes, bounds);
        foreach (var prefix in observedPrefixes)
        {
            if (state.ORSetFrontier.TryGetValue(prefix.Key, out var previous) && prefix.Value < previous)
            {
                throw new InvalidOperationException("An OR-set checkpoint cannot decrease causal knowledge.");
            }
        }

        var frontier = MergeComponents(state.ORSetFrontier, observedPrefixes);
        var next = CreateORSetState(IndexBindings(state.DotBindings), IndexBindings(state.Tombstones), frontier);
        ValidateState(next, bounds);
        return next;
    }

    /// <summary>Validates fully observed sequence prefixes.</summary>
    /// <param name="frontier">The prefixes.</param>
    /// <param name="bounds">The bounds.</param>
    /// <exception cref="InvalidOperationException">A prefix or its identity is invalid.</exception>
    private static void ValidateFrontier(IReadOnlyDictionary<string, long> frontier, CrdtBounds bounds)
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
    private static bool IsCovered(IReadOnlyDictionary<string, long> frontier, CrdtDot dot) =>
        frontier.TryGetValue(dot.ClientId, out var prefix) && dot.ClientSequence <= prefix;

    /// <summary>Indexes retained bindings without copying element bytes.</summary>
    /// <param name="source">The bindings.</param>
    /// <returns>The dot index.</returns>
    private static Dictionary<CrdtDot, CrdtDotElement> IndexBindings(IReadOnlyList<CrdtDotElement> source)
    {
        Dictionary<CrdtDot, CrdtDotElement> index = [with(capacity: source.Count)];
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
    private static void AddIndexedBinding(Dictionary<CrdtDot, CrdtDotElement> index, CrdtDotElement binding)
    {
        if (index.TryGetValue(binding.Dot, out var previous) && !BytesEqual(previous.ElementSpan, binding.ElementSpan))
        {
            throw new InvalidOperationException(DotRebindMessage);
        }

        index[binding.Dot] = binding;
    }

    /// <summary>Merges bindings, suppressing dots absent from a checkpoint that already observed them.</summary>
    /// <param name="left">The first state.</param>
    /// <param name="right">The second state.</param>
    /// <returns>The merged binding index.</returns>
    private static Dictionary<CrdtDot, CrdtDotElement> MergeActiveBindings(CrdtState left, CrdtState right)
    {
        var leftBindings = IndexBindings(left.DotBindings);
        var rightBindings = IndexBindings(right.DotBindings);
        var merged = IndexBindings(left.DotBindings);
        foreach (var binding in right.DotBindings)
        {
            AddIndexedBinding(merged, binding);
        }

        var candidates = SnapshotBindings(merged);
        ValidateNoCrossRebind(candidates, left.Tombstones);
        ValidateNoCrossRebind(candidates, right.Tombstones);
        foreach (var binding in candidates)
        {
            if ((!leftBindings.ContainsKey(binding.Dot) && IsCovered(left.ORSetFrontier, binding.Dot))
                || (!rightBindings.ContainsKey(binding.Dot) && IsCovered(right.ORSetFrontier, binding.Dot)))
            {
                _ = merged.Remove(binding.Dot);
            }
        }

        return merged;
    }

    /// <summary>Reclaims covered removals and stores the frontier alongside canonical retained metadata.</summary>
    /// <param name="bindings">The owned mutable binding index.</param>
    /// <param name="tombstones">The owned mutable removal index.</param>
    /// <param name="frontier">The fully observed prefixes.</param>
    /// <returns>The complete state.</returns>
    private static CrdtState CreateORSetState(
        Dictionary<CrdtDot, CrdtDotElement> bindings,
        Dictionary<CrdtDot, CrdtDotElement> tombstones,
        IReadOnlyDictionary<string, long> frontier)
    {
        var removals = SnapshotBindings(tombstones);
        ValidateNoCrossRebind(SnapshotBindings(bindings), removals);
        foreach (var tombstone in removals)
        {
            if (!IsCovered(frontier, tombstone.Dot))
            {
                continue;
            }

            _ = bindings.Remove(tombstone.Dot);
            _ = tombstones.Remove(tombstone.Dot);
        }

        return new() { Kind = CrdtKind.ORSet, DotBindings = SortedBindings(bindings), Tombstones = SortedBindings(tombstones), ORSetFrontier = frontier };
    }

    /// <summary>Snapshots dictionary values before removing entries.</summary>
    /// <param name="bindings">The index.</param>
    /// <returns>The binding references.</returns>
    private static CrdtDotElement[] SnapshotBindings(Dictionary<CrdtDot, CrdtDotElement> bindings)
    {
        var values = new CrdtDotElement[bindings.Count];
        bindings.Values.CopyTo(values, 0);
        return values;
    }

    /// <summary>Sorts retained binding references by dot identity.</summary>
    /// <param name="bindings">The index.</param>
    /// <returns>The canonical binding references.</returns>
    private static CrdtDotElement[] SortedBindings(Dictionary<CrdtDot, CrdtDotElement> bindings)
    {
        var values = SnapshotBindings(bindings);
        Array.Sort(values, static (left, right) => left.Dot.CompareTo(right.Dot));
        return values;
    }

    /// <summary>Sorts active bindings by element bytes without copying payloads.</summary>
    /// <param name="state">The state.</param>
    /// <returns>The sorted active bindings.</returns>
    private static List<CrdtDotElement> GetSortedActiveBindings(CrdtState state)
    {
        var tombstones = IndexBindings(state.Tombstones);
        List<CrdtDotElement> active = [with(capacity: state.DotBindings.Count)];
        foreach (var binding in state.DotBindings)
        {
            if (!tombstones.ContainsKey(binding.Dot))
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
    private static void ValidateDerivedValue(CrdtState state, CrdtBounds bounds)
    {
        switch (state.Kind)
        {
            case CrdtKind.GCounter:
            {
                _ = SumComponents(state.GCounterComponents);
                break;
            }

            case CrdtKind.PNCounter:
            {
                _ = checked(SumComponents(state.PNCounterPositiveComponents) - SumComponents(state.PNCounterNegativeComponents));
                break;
            }

            case CrdtKind.ORSet:
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
    private static void ValidateActiveElementCount(CrdtState state, CrdtBounds bounds)
    {
        var count = 0;
        CrdtDotElement? previous = null;
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
