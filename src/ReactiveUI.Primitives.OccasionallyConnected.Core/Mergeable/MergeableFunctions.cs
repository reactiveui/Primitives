// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;
using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

/// <summary>Provides pure CRDT validation, projection, merge, and value helpers.</summary>
public static partial class MergeableFunctions
{
    /// <summary>The unsupported kind error message.</summary>
    private const string UnsupportedKindMessage = "The CRDT kind is not supported.";

    /// <summary>The foreign metadata error message.</summary>
    private const string ForeignMetadataMessage = "CRDT state contains metadata for a different kind.";

    /// <summary>The OR-set dot rebind error message.</summary>
    private const string DotRebindMessage = "A CRDT OR-set dot cannot be rebound to another element.";

    /// <summary>Strict UTF-8 used when validating client ids.</summary>
    private static readonly Encoding StrictUtf8 = new UTF8Encoding(false, true);

    /// <summary>Creates an empty CRDT state for a built-in kind.</summary>
    /// <param name="kind">The built-in kind.</param>
    /// <returns>The empty state.</returns>
    public static MergeableState Empty(MergeableKind kind) => new() { Kind = kind };

    /// <summary>Returns the canonical value derived from CRDT metadata.</summary>
    /// <param name="state">The CRDT state.</param>
    /// <returns>The derived value.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    public static MergeableValue GetValue(MergeableState state)
    {
        ArgumentExceptionHelper.ThrowIfNull(state);
        ValidateClosedStateShape(state);
        return state.Kind switch
        {
            MergeableKind.IncrementOnlyCounter => new() { Kind = state.Kind, Counter = SumComponents(state.IncrementOnlyTotalsByClient) },
            MergeableKind.Counter => new() { Kind = state.Kind, Counter = checked(SumComponents(state.IncrementsByClient) - SumComponents(state.DecrementsByClient)) },
            MergeableKind.AddWinsSet => new() { Kind = state.Kind, Elements = GetActiveElements(state) },
            _ => new() { Kind = state.Kind, Bytes = state.LatestValue },
        };
    }

    /// <summary>Applies a local CRDT input.</summary>
    /// <param name="state">The current state.</param>
    /// <param name="input">The input.</param>
    /// <param name="authenticatedClientId">The authenticated client id.</param>
    /// <param name="clientSequence">The durable client sequence.</param>
    /// <returns>The projected state.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static MergeableState ApplyLocal(
        MergeableState state,
        MergeableInput input,
        string authenticatedClientId,
        long clientSequence) =>
        ApplyLocal(state, input, authenticatedClientId, clientSequence, MergeableBounds.Default);

    /// <summary>Applies a local CRDT input.</summary>
    /// <param name="state">The current state.</param>
    /// <param name="input">The input.</param>
    /// <param name="authenticatedClientId">The authenticated client id.</param>
    /// <param name="clientSequence">The durable client sequence.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The projected state.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the client sequence is not positive.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT input is invalid.</exception>
    public static MergeableState ApplyLocal(
        MergeableState state,
        MergeableInput input,
        string authenticatedClientId,
        long clientSequence,
        MergeableBounds bounds)
    {
        ArgumentExceptionHelper.ThrowIfNull(input);
        ArgumentExceptionHelper.ThrowIfNull(bounds);
        bounds.Validate();
        ValidateClientId(authenticatedClientId, bounds);
        ThrowIfNegativeOrZero(clientSequence, nameof(clientSequence));

        MergeableCodec.ValidateInput(input, bounds);
        if (input.Value is not MergeableChange mutation)
        {
            throw new InvalidOperationException("Local CRDT projection requires a mutation input.");
        }

        return ApplyMutation(state, mutation, authenticatedClientId, clientSequence, bounds);
    }

    /// <summary>Validates and returns a complete authoritative replacement state.</summary>
    /// <param name="state">The authoritative state.</param>
    /// <returns>The validated authoritative state.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static MergeableState ReplaceAuthoritativeState(MergeableState state) =>
        ReplaceAuthoritativeState(state, MergeableBounds.Default);

    /// <summary>Validates and returns a complete authoritative replacement state.</summary>
    /// <param name="state">The authoritative state.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The validated authoritative state.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT state is invalid.</exception>
    public static MergeableState ReplaceAuthoritativeState(MergeableState state, MergeableBounds bounds)
    {
        ValidateState(state, bounds);
        return state with { };
    }

    /// <summary>Merges two CRDT states with state-based rules.</summary>
    /// <param name="left">The first state.</param>
    /// <param name="right">The second state.</param>
    /// <returns>The merged state.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static MergeableState Merge(MergeableState left, MergeableState right) =>
        Merge(left, right, MergeableBounds.Default);

    /// <summary>Merges two CRDT states with state-based rules.</summary>
    /// <param name="left">The first state.</param>
    /// <param name="right">The second state.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The merged state.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT states are invalid.</exception>
    public static MergeableState Merge(MergeableState left, MergeableState right, MergeableBounds bounds)
    {
        ValidateSameKind(left, right, bounds);
        return left.Kind switch
        {
            MergeableKind.IncrementOnlyCounter => MergeIncrementOnlyCounter(left, right, bounds),
            MergeableKind.Counter => MergeCounter(left, right, bounds),
            MergeableKind.AddWinsSet => MergeAddWinsSet(left, right, bounds),
            _ => MergeLastWriterWinsValue(left, right, bounds),
        };
    }

    /// <summary>Validates a complete CRDT state.</summary>
    /// <param name="state">The state.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ValidateState(MergeableState state) =>
        ValidateState(state, MergeableBounds.Default);

    /// <summary>Validates a complete CRDT state.</summary>
    /// <param name="state">The state.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT state is invalid.</exception>
    public static void ValidateState(MergeableState state, MergeableBounds bounds)
    {
        ArgumentExceptionHelper.ThrowIfNull(state);
        ArgumentExceptionHelper.ThrowIfNull(bounds);
        bounds.Validate();
        ValidateCounterComponents(state.IncrementOnlyTotalsByClient, bounds);
        ValidateCounterComponents(state.IncrementsByClient, bounds);
        ValidateCounterComponents(state.DecrementsByClient, bounds);
        ValidateDotElements(state.Elements, bounds, bounds.MaximumDotBindings);
        ValidateDotElements(state.RemovedTags, bounds, bounds.MaximumTombstones);
        ValidateNoCrossRebind(state.Elements, state.RemovedTags);
        ValidateFrontier(state.AddWinsSetFrontier, bounds);
        ValidateWriteStamp(state.LatestWriteStamp, bounds);
        if (state.LatestValueLength > bounds.MaximumRegisterBytes)
        {
            throw new InvalidOperationException("The CRDT register value exceeds configured bounds.");
        }

        ValidateClosedStateShape(state);
        ValidateDerivedValue(state, bounds);
    }

    /// <summary>Compares two LWW register stamps using canonical server write ordering.</summary>
    /// <param name="left">The first stamp.</param>
    /// <param name="right">The second stamp.</param>
    /// <returns>The ordering result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    public static int CompareWriteStamps(ConflictWriteStamp? left, ConflictWriteStamp? right)
    {
        if (left is null)
        {
            return right is null ? 0 : -1;
        }

        if (right is null)
        {
            return 1;
        }

        var timestamp = left.CommittedAtUtc.CompareTo(right.CommittedAtUtc);
        if (timestamp != 0)
        {
            return timestamp;
        }

        var client = string.CompareOrdinal(left.ClientId, right.ClientId);
        return client != 0 ? client : left.OperationId.Value.CompareTo(right.OperationId.Value);
    }

    /// <summary>Validates a dot identity.</summary>
    /// <param name="dot">The dot.</param>
    /// <param name="bounds">The bounds.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    internal static void ValidateDot(ElementTag dot, MergeableBounds bounds)
    {
        ArgumentExceptionHelper.ThrowIfNull(dot);
        ValidateClientId(dot.ClientId, bounds);
        if (dot.ClientSequence > 0)
        {
            return;
        }

        throw new InvalidOperationException("CRDT dot sequences must be positive.");
    }

    /// <summary>Validates optional register write provenance.</summary>
    /// <param name="stamp">The optional stamp.</param>
    /// <param name="bounds">The identity bounds.</param>
    /// <exception cref="InvalidOperationException">The write identity is invalid.</exception>
    internal static void ValidateWriteStamp(ConflictWriteStamp? stamp, MergeableBounds bounds)
    {
        if (stamp is null)
        {
            return;
        }

        ValidateClientId(stamp.ClientId, bounds);
        if (stamp.OperationId.Value != Guid.Empty)
        {
            return;
        }

        throw new InvalidOperationException("A CRDT write stamp requires an operation identifier.");
    }

    /// <summary>Validates authenticated client identity text.</summary>
    /// <param name="clientId">The clientId.</param>
    /// <param name="bounds">The bounds.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    internal static void ValidateClientId(string? clientId, MergeableBounds bounds)
    {
        if (string.IsNullOrWhiteSpace(clientId))
        {
            throw new InvalidOperationException("A CRDT client identifier is required.");
        }

        try
        {
            var count = StrictUtf8.GetByteCount(clientId);
            if (count > bounds.MaximumClientIdUtf8Bytes)
            {
                throw new InvalidOperationException("A CRDT client identifier exceeds configured bounds.");
            }
        }
        catch (EncoderFallbackException exception)
        {
            throw new InvalidOperationException("A CRDT client identifier must be valid UTF-8.", exception);
        }
    }

    /// <summary>Applies one mutation to state.</summary>
    /// <param name="state">The state.</param>
    /// <param name="mutation">The mutation.</param>
    /// <param name="authenticatedClientId">The authenticatedClientId.</param>
    /// <param name="clientSequence">The clientSequence.</param>
    /// <param name="bounds">The bounds.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static MergeableState ApplyMutation(
        MergeableState state,
        MergeableChange mutation,
        string authenticatedClientId,
        long clientSequence,
        MergeableBounds bounds)
    {
        ArgumentExceptionHelper.ThrowIfNull(state);
        ArgumentExceptionHelper.ThrowIfNull(mutation);
        return mutation.ToKind() switch
        {
            MergeableChangeKind.IncrementOnlyCounterSet => ApplyIncrementOnlyCounter(state, mutation, authenticatedClientId, bounds),
            MergeableChangeKind.CounterSet => ApplyCounter(state, mutation, authenticatedClientId, bounds),
            MergeableChangeKind.AddWinsSetAdd => ApplyAddWinsSetAdd(state, mutation, authenticatedClientId, clientSequence, bounds),
            MergeableChangeKind.AddWinsSetRemove => ApplyAddWinsSetRemove(state, mutation, bounds),
            _ => ApplyLastWriterWinsValue(state, mutation, bounds),
        };
    }

    /// <summary>Applies a G-counter component update.</summary>
    /// <param name="state">The state.</param>
    /// <param name="mutation">The mutation.</param>
    /// <param name="authenticatedClientId">The authenticated client id.</param>
    /// <param name="bounds">The bounds.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static MergeableState ApplyIncrementOnlyCounter(MergeableState state, MergeableChange mutation, string authenticatedClientId, MergeableBounds bounds)
    {
        ValidateKind(state, MergeableKind.IncrementOnlyCounter, bounds);
        ValidateAuthenticatedActor(authenticatedClientId, mutation.ClientId);

        var components = CopyComponents(state.IncrementOnlyTotalsByClient);
        SetMax(components, authenticatedClientId, mutation.IncrementOnlyTotal);
        var next = state with { IncrementOnlyTotalsByClient = components };
        ValidateState(next, bounds);
        return next;
    }

    /// <summary>Applies a PN-counter component update.</summary>
    /// <param name="state">The state.</param>
    /// <param name="mutation">The mutation.</param>
    /// <param name="authenticatedClientId">The authenticated client id.</param>
    /// <param name="bounds">The bounds.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static MergeableState ApplyCounter(MergeableState state, MergeableChange mutation, string authenticatedClientId, MergeableBounds bounds)
    {
        ValidateKind(state, MergeableKind.Counter, bounds);
        ValidateAuthenticatedActor(authenticatedClientId, mutation.ClientId);

        var positive = CopyComponents(state.IncrementsByClient);
        var negative = CopyComponents(state.DecrementsByClient);
        SetMax(positive, authenticatedClientId, mutation.IncrementTotal);
        SetMax(negative, authenticatedClientId, mutation.DecrementTotal);
        var next = state with { IncrementsByClient = positive, DecrementsByClient = negative };
        ValidateState(next, bounds);
        return next;
    }

    /// <summary>Applies an OR-set add using the durable operation dot.</summary>
    /// <param name="state">The state.</param>
    /// <param name="mutation">The mutation.</param>
    /// <param name="clientId">The clientId.</param>
    /// <param name="sequence">The sequence.</param>
    /// <param name="bounds">The bounds.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static MergeableState ApplyAddWinsSetAdd(MergeableState state, MergeableChange mutation, string clientId, long sequence, MergeableBounds bounds)
    {
        ValidateKind(state, MergeableKind.AddWinsSet, bounds);
        ValidateElementLength(mutation.ByteLength, bounds);
        var dot = new ElementTag { ClientId = clientId, ClientSequence = sequence };
        if (IsCovered(state.AddWinsSetFrontier, dot) && !IndexBindings(state.Elements).ContainsKey(dot))
        {
            return state;
        }

        List<TaggedElement> bindings = [with(capacity: state.Elements.Count + 1)];
        AddRange(bindings, state.Elements);
        var incoming = new TaggedElement { Tag = dot, Element = mutation.Bytes };
        AddOrValidateSameBinding(bindings, incoming);
        var next = state with { Elements = bindings };
        ValidateState(next, bounds);
        return next;
    }

    /// <summary>Applies an OR-set remove by retaining exact observed tombstones.</summary>
    /// <param name="state">The state.</param>
    /// <param name="mutation">The mutation.</param>
    /// <param name="bounds">The bounds.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static MergeableState ApplyAddWinsSetRemove(MergeableState state, MergeableChange mutation, MergeableBounds bounds)
    {
        ValidateKind(state, MergeableKind.AddWinsSet, bounds);
        ValidateElementLength(mutation.ByteLength, bounds);
        var tombstones = IndexBindings(state.RemovedTags);
        for (var index = 0; index < mutation.TagsToRemove.Count; index++)
        {
            var dot = mutation.TagsToRemove[index];
            var candidate = new TaggedElement { Tag = dot, Element = mutation.Bytes };
            AddIndexedBinding(tombstones, candidate);
        }

        var next = CreateAddWinsSetState(IndexBindings(state.Elements), tombstones, state.AddWinsSetFrontier);
        ValidateState(next, bounds);
        return next;
    }

    /// <summary>Applies a LWW register assignment.</summary>
    /// <param name="state">The state.</param>
    /// <param name="mutation">The mutation.</param>
    /// <param name="bounds">The bounds.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static MergeableState ApplyLastWriterWinsValue(MergeableState state, MergeableChange mutation, MergeableBounds bounds)
    {
        ValidateKind(state, MergeableKind.LastWriterWinsValue, bounds);
        var next = state with { LatestValue = mutation.Bytes, LatestWriteStamp = mutation.LatestWriteStamp };
        ValidateState(next, bounds);
        return next;
    }

    /// <summary>Merges grow-only counter states.</summary>
    /// <param name="left">The left.</param>
    /// <param name="right">The right.</param>
    /// <param name="bounds">The bounds.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static MergeableState MergeIncrementOnlyCounter(MergeableState left, MergeableState right, MergeableBounds bounds)
    {
        var components = MergeComponents(left.IncrementOnlyTotalsByClient, right.IncrementOnlyTotalsByClient);
        var state = new MergeableState { Kind = MergeableKind.IncrementOnlyCounter, IncrementOnlyTotalsByClient = components };
        ValidateState(state, bounds);
        return state;
    }

    /// <summary>Merges PN-counter states.</summary>
    /// <param name="left">The left.</param>
    /// <param name="right">The right.</param>
    /// <param name="bounds">The bounds.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static MergeableState MergeCounter(MergeableState left, MergeableState right, MergeableBounds bounds)
    {
        var positive = MergeComponents(left.IncrementsByClient, right.IncrementsByClient);
        var negative = MergeComponents(left.DecrementsByClient, right.DecrementsByClient);
        var state = new MergeableState { Kind = MergeableKind.Counter, IncrementsByClient = positive, DecrementsByClient = negative };
        ValidateState(state, bounds);
        return state;
    }

    /// <summary>Merges observed-remove set states.</summary>
    /// <param name="left">The left.</param>
    /// <param name="right">The right.</param>
    /// <param name="bounds">The bounds.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static MergeableState MergeAddWinsSet(MergeableState left, MergeableState right, MergeableBounds bounds)
    {
        var bindings = MergeActiveBindings(left, right);
        var tombstones = IndexBindings(left.RemovedTags);
        foreach (var tombstone in right.RemovedTags)
        {
            AddIndexedBinding(tombstones, tombstone);
        }

        ValidateNoCrossRebind(SnapshotBindings(bindings), SnapshotBindings(tombstones));
        var frontier = MergeComponents(left.AddWinsSetFrontier, right.AddWinsSetFrontier);
        var state = CreateAddWinsSetState(bindings, tombstones, frontier);
        ValidateState(state, bounds);
        return state;
    }

    /// <summary>Merges LWW register states by canonical write stamp order.</summary>
    /// <param name="left">The left.</param>
    /// <param name="right">The right.</param>
    /// <param name="bounds">The bounds.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static MergeableState MergeLastWriterWinsValue(MergeableState left, MergeableState right, MergeableBounds bounds)
    {
        var order = CompareWriteStamps(left.LatestWriteStamp, right.LatestWriteStamp);
        if (order == 0)
        {
            order = CompareBytes(left.LatestValueSpan, right.LatestValueSpan);
        }

        var winner = order >= 0 ? left : right;
        var stamp = winner.LatestWriteStamp;
        if (stamp is not null)
        {
            stamp = stamp with { CommittedAtUtc = stamp.CommittedAtUtc.ToUniversalTime() };
        }

        var state = new MergeableState { Kind = MergeableKind.LastWriterWinsValue, LatestValue = winner.LatestValue, LatestWriteStamp = stamp };
        ValidateState(state, bounds);
        return state;
    }

    /// <summary>Derives active OR-set elements from bindings minus tombstones.</summary>
    /// <param name="state">The state.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static ReadOnlyCollection<ReadOnlyMemory<byte>> GetActiveElements(MergeableState state)
    {
        List<ReadOnlyMemory<byte>> elements = [];
        var active = GetSortedActiveBindings(state);
        TaggedElement? previous = null;
        foreach (var binding in active)
        {
            if (previous is not null && BytesEqual(previous.ElementSpan, binding.ElementSpan))
            {
                continue;
            }

            elements.Add(binding.ElementSpan.ToArray());
            previous = binding;
        }

        return new(elements);
    }

    /// <summary>Copies counter components.</summary>
    /// <param name="source">The source.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static Dictionary<string, long> CopyComponents(IReadOnlyDictionary<string, long> source)
    {
        Dictionary<string, long> copy = [with(capacity: source.Count, comparer: StringComparer.Ordinal)];
        foreach (var pair in source)
        {
            copy.Add(pair.Key, pair.Value);
        }

        return copy;
    }

    /// <summary>Merges counter components with component-wise maxima.</summary>
    /// <param name="left">The left.</param>
    /// <param name="right">The right.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static Dictionary<string, long> MergeComponents(IReadOnlyDictionary<string, long> left, IReadOnlyDictionary<string, long> right)
    {
        var merged = CopyComponents(left);
        foreach (var pair in right)
        {
            SetMax(merged, pair.Key, pair.Value);
        }

        return merged;
    }

    /// <summary>Sets a component only when the incoming value dominates.</summary>
    /// <param name="components">The components.</param>
    /// <param name="actor">The actor.</param>
    /// <param name="value">The value.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static void SetMax(Dictionary<string, long> components, string actor, long value)
    {
        if (components.TryGetValue(actor, out var current) && current >= value)
        {
            return;
        }

        components[actor] = value;
    }

    /// <summary>Sums components with overflow checking.</summary>
    /// <param name="components">The components.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static long SumComponents(IReadOnlyDictionary<string, long> components)
    {
        long total = 0;
        foreach (var pair in components)
        {
            total = checked(total + pair.Value);
        }

        return total;
    }

    /// <summary>Validates a counter mutation actor matches the authenticated caller.</summary>
    /// <param name="authenticatedClientId">The authenticated client id.</param>
    /// <param name="actor">The mutation actor.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT actor is invalid.</exception>
    private static void ValidateAuthenticatedActor(string authenticatedClientId, string? actor)
    {
        if (string.Equals(authenticatedClientId, actor, StringComparison.Ordinal))
        {
            return;
        }

        throw new InvalidOperationException("A CRDT counter mutation actor must match the authenticated client.");
    }

    /// <summary>Validates state and expected kind.</summary>
    /// <param name="state">The state.</param>
    /// <param name="kind">The kind.</param>
    /// <param name="bounds">The bounds.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static void ValidateKind(MergeableState state, MergeableKind kind, MergeableBounds bounds)
    {
        ValidateState(state, bounds);
        if (state.Kind == kind)
        {
            return;
        }

        throw new InvalidOperationException("The CRDT mutation does not match the state kind.");
    }

    /// <summary>Validates two states share a kind.</summary>
    /// <param name="left">The left.</param>
    /// <param name="right">The right.</param>
    /// <param name="bounds">The bounds.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static void ValidateSameKind(MergeableState left, MergeableState right, MergeableBounds bounds)
    {
        ValidateState(left, bounds);
        ValidateState(right, bounds);
        if (left.Kind == right.Kind)
        {
            return;
        }

        throw new InvalidOperationException("CRDT states must have the same kind.");
    }

    /// <summary>Validates bounded counter components.</summary>
    /// <param name="components">The components.</param>
    /// <param name="bounds">The bounds.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static void ValidateCounterComponents(IReadOnlyDictionary<string, long> components, MergeableBounds bounds)
    {
        if (components.Count > bounds.MaximumCounterComponents)
        {
            throw new InvalidOperationException("The CRDT counter component count exceeds configured bounds.");
        }

        foreach (var pair in components)
        {
            ValidateClientId(pair.Key, bounds);
            if (pair.Value < 0)
            {
                throw new InvalidOperationException("CRDT counter components must be nonnegative.");
            }
        }
    }

    /// <summary>Validates bounded dot element bindings.</summary>
    /// <param name="elements">The elements.</param>
    /// <param name="bounds">The bounds.</param>
    /// <param name="maximumCount">The maximumCount.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static void ValidateDotElements(IReadOnlyList<TaggedElement> elements, MergeableBounds bounds, int maximumCount)
    {
        if (elements.Count > maximumCount)
        {
            throw new InvalidOperationException("The CRDT dot element count exceeds configured bounds.");
        }

        HashSet<ElementTag> seen = [];
        for (var index = 0; index < elements.Count; index++)
        {
            var element = elements[index];
            ValidateDot(element.Tag, bounds);
            ValidateElementLength(element.ElementLength, bounds);
            if (!seen.Add(element.Tag))
            {
                throw new InvalidOperationException("CRDT state dot bindings must be unique.");
            }
        }
    }

    /// <summary>Validates a state carries only metadata used by its CRDT kind.</summary>
    /// <param name="state">The state.</param>
    /// <exception cref="InvalidOperationException">Thrown when unrelated metadata is present.</exception>
    private static void ValidateClosedStateShape(MergeableState state)
    {
        switch (state.Kind)
        {
            case MergeableKind.IncrementOnlyCounter:
            {
                EnsureEmpty(state.AddWinsSetFrontier);
                EnsureEmpty(state.IncrementsByClient);
                EnsureEmpty(state.DecrementsByClient);
                EnsureEmpty(state.Elements);
                EnsureEmpty(state.RemovedTags);
                EnsureNoRegister(state);
                break;
            }

            case MergeableKind.Counter:
            {
                EnsureEmpty(state.AddWinsSetFrontier);
                EnsureEmpty(state.IncrementOnlyTotalsByClient);
                EnsureEmpty(state.Elements);
                EnsureEmpty(state.RemovedTags);
                EnsureNoRegister(state);
                break;
            }

            case MergeableKind.AddWinsSet:
            {
                EnsureEmpty(state.IncrementOnlyTotalsByClient);
                EnsureEmpty(state.IncrementsByClient);
                EnsureEmpty(state.DecrementsByClient);
                EnsureNoRegister(state);
                break;
            }

            case MergeableKind.LastWriterWinsValue:
            {
                EnsureEmpty(state.AddWinsSetFrontier);
                EnsureEmpty(state.IncrementOnlyTotalsByClient);
                EnsureEmpty(state.IncrementsByClient);
                EnsureEmpty(state.DecrementsByClient);
                EnsureEmpty(state.Elements);
                EnsureEmpty(state.RemovedTags);
                break;
            }

            default:
            {
                throw new InvalidOperationException(UnsupportedKindMessage);
            }
        }
    }

    /// <summary>Validates a state does not carry LWW register metadata.</summary>
    /// <param name="state">The state.</param>
    /// <exception cref="InvalidOperationException">Thrown when register metadata is present.</exception>
    private static void EnsureNoRegister(MergeableState state)
    {
        if (state.LatestValueLength == 0 && state.LatestWriteStamp is null)
        {
            return;
        }

        throw new InvalidOperationException(ForeignMetadataMessage);
    }

    /// <summary>Validates a dictionary is empty.</summary>
    /// <param name="items">The items.</param>
    /// <exception cref="InvalidOperationException">Thrown when metadata is present.</exception>
    private static void EnsureEmpty(IReadOnlyDictionary<string, long> items)
    {
        if (items.Count == 0)
        {
            return;
        }

        throw new InvalidOperationException(ForeignMetadataMessage);
    }

    /// <summary>Validates a list is empty.</summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="items">The items.</param>
    /// <exception cref="InvalidOperationException">Thrown when metadata is present.</exception>
    private static void EnsureEmpty<T>(IReadOnlyCollection<T> items)
    {
        if (items.Count == 0)
        {
            return;
        }

        throw new InvalidOperationException(ForeignMetadataMessage);
    }

    /// <summary>Validates retained bindings and tombstones do not bind the same dot to different bytes.</summary>
    /// <param name="bindings">The bindings.</param>
    /// <param name="tombstones">The tombstones.</param>
    /// <exception cref="InvalidOperationException">Thrown when a dot is rebound.</exception>
    private static void ValidateNoCrossRebind(IReadOnlyList<TaggedElement> bindings, IReadOnlyList<TaggedElement> tombstones)
    {
        var indexed = IndexBindings(tombstones);
        foreach (var binding in bindings)
        {
            if (indexed.TryGetValue(binding.Tag, out var tombstone) && !BytesEqual(binding.ElementSpan, tombstone.ElementSpan))
            {
                throw new InvalidOperationException(DotRebindMessage);
            }
        }
    }

    /// <summary>Validates an element byte length.</summary>
    /// <param name="length">The length.</param>
    /// <param name="bounds">The bounds.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static void ValidateElementLength(int length, MergeableBounds bounds)
    {
        if (length <= bounds.MaximumElementBytes)
        {
            return;
        }

        throw new InvalidOperationException("A CRDT element exceeds configured bounds.");
    }

    /// <summary>Adds existing dot elements to a mutable list.</summary>
    /// <param name="target">The target.</param>
    /// <param name="source">The source.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static void AddRange(List<TaggedElement> target, IReadOnlyList<TaggedElement> source)
    {
        for (var index = 0; index < source.Count; index++)
        {
            target.Add(source[index]);
        }
    }

    /// <summary>Adds a dot binding or accepts an idempotent duplicate.</summary>
    /// <param name="target">The target.</param>
    /// <param name="incoming">The incoming.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static void AddOrValidateSameBinding(List<TaggedElement> target, TaggedElement incoming)
    {
        for (var index = 0; index < target.Count; index++)
        {
            var current = target[index];
            if (CompareDots(current.Tag, incoming.Tag) != 0)
            {
                continue;
            }

            if (BytesEqual(current.ElementSpan, incoming.ElementSpan))
            {
                return;
            }

            throw new InvalidOperationException(DotRebindMessage);
        }

        target.Add(incoming);
    }

    /// <summary>Compares two dots.</summary>
    /// <param name="left">The left.</param>
    /// <param name="right">The right.</param>
    /// <returns>The result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CompareDots(ElementTag left, ElementTag right) => left.CompareTo(right);

    /// <summary>Compares two byte spans lexicographically.</summary>
    /// <param name="left">The left.</param>
    /// <param name="right">The right.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static int CompareBytes(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        var shared = Math.Min(left.Length, right.Length);
        for (var index = 0; index < shared; index++)
        {
            var comparison = left[index].CompareTo(right[index]);
            if (comparison != 0)
            {
                return comparison;
            }
        }

        return left.Length.CompareTo(right.Length);
    }

    /// <summary>Determines whether two byte spans are equal.</summary>
    /// <param name="left">The left.</param>
    /// <param name="right">The right.</param>
    /// <returns>The result.</returns>
    private static bool BytesEqual(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right) => CompareBytes(left, right) == 0;

    /// <summary>Throws when a long value is not positive.</summary>
    /// <param name="value">The value to inspect.</param>
    /// <param name="parameterName">The source parameter name.</param>
    /// <exception cref="ArgumentOutOfRangeException">The value is not positive.</exception>
    private static void ThrowIfNegativeOrZero(long value, string parameterName)
    {
        if (value > 0)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(parameterName, value, null);
    }
}
