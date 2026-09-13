// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;
using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Crdt;

/// <summary>Provides pure CRDT validation, projection, merge, and value helpers.</summary>
public static class CrdtFunctions
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
    public static CrdtState Empty(CrdtKind kind) => new() { Kind = kind };

    /// <summary>Returns the canonical value derived from CRDT metadata.</summary>
    /// <param name="state">The CRDT state.</param>
    /// <returns>The derived value.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    public static CrdtValue GetValue(CrdtState state)
    {
        ArgumentExceptionHelper.ThrowIfNull(state);
        ValidateClosedStateShape(state);
        return state.Kind switch
        {
            CrdtKind.GCounter => new() { Kind = state.Kind, Counter = SumComponents(state.GCounterComponents) },
            CrdtKind.PNCounter => new() { Kind = state.Kind, Counter = checked(SumComponents(state.PNCounterPositiveComponents) - SumComponents(state.PNCounterNegativeComponents)) },
            CrdtKind.ORSet => new() { Kind = state.Kind, Elements = GetActiveElements(state) },
            _ => new() { Kind = state.Kind, Bytes = state.RegisterValue },
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
    public static CrdtState ApplyLocal(
        CrdtState state,
        CrdtInput input,
        string authenticatedClientId,
        long clientSequence) =>
        ApplyLocal(state, input, authenticatedClientId, clientSequence, CrdtBounds.Default);

    /// <summary>Applies a local CRDT input.</summary>
    /// <param name="state">The current state.</param>
    /// <param name="input">The input.</param>
    /// <param name="authenticatedClientId">The authenticated client id.</param>
    /// <param name="clientSequence">The durable client sequence.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The projected state.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the client sequence is not positive.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT input is invalid.</exception>
    public static CrdtState ApplyLocal(
        CrdtState state,
        CrdtInput input,
        string authenticatedClientId,
        long clientSequence,
        CrdtBounds bounds)
    {
        ArgumentExceptionHelper.ThrowIfNull(input);
        ArgumentExceptionHelper.ThrowIfNull(bounds);
        bounds.Validate();
        ValidateClientId(authenticatedClientId, bounds);
        ThrowIfNegativeOrZero(clientSequence, nameof(clientSequence));

        CrdtCodec.ValidateInput(input, bounds);
        if (input.Mutation is not { } mutation)
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
    public static CrdtState ReplaceAuthoritativeState(CrdtState state) =>
        ReplaceAuthoritativeState(state, CrdtBounds.Default);

    /// <summary>Validates and returns a complete authoritative replacement state.</summary>
    /// <param name="state">The authoritative state.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The validated authoritative state.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT state is invalid.</exception>
    public static CrdtState ReplaceAuthoritativeState(CrdtState state, CrdtBounds bounds)
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
    public static CrdtState Merge(CrdtState left, CrdtState right) =>
        Merge(left, right, CrdtBounds.Default);

    /// <summary>Merges two CRDT states with state-based rules.</summary>
    /// <param name="left">The first state.</param>
    /// <param name="right">The second state.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <returns>The merged state.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT states are invalid.</exception>
    public static CrdtState Merge(CrdtState left, CrdtState right, CrdtBounds bounds)
    {
        ValidateSameKind(left, right, bounds);
        return left.Kind switch
        {
            CrdtKind.GCounter => MergeGCounter(left, right, bounds),
            CrdtKind.PNCounter => MergePNCounter(left, right, bounds),
            CrdtKind.ORSet => MergeORSet(left, right, bounds),
            _ => MergeLwwRegister(left, right, bounds),
        };
    }

    /// <summary>Validates a complete CRDT state.</summary>
    /// <param name="state">The state.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ValidateState(CrdtState state) =>
        ValidateState(state, CrdtBounds.Default);

    /// <summary>Validates a complete CRDT state.</summary>
    /// <param name="state">The state.</param>
    /// <param name="bounds">The CRDT bounds.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT state is invalid.</exception>
    public static void ValidateState(CrdtState state, CrdtBounds bounds)
    {
        ArgumentExceptionHelper.ThrowIfNull(state);
        ArgumentExceptionHelper.ThrowIfNull(bounds);
        bounds.Validate();
        ValidateCounterComponents(state.GCounterComponents, bounds);
        ValidateCounterComponents(state.PNCounterPositiveComponents, bounds);
        ValidateCounterComponents(state.PNCounterNegativeComponents, bounds);
        ValidateDotElements(state.DotBindings, bounds, bounds.MaximumDotBindings);
        ValidateDotElements(state.Tombstones, bounds, bounds.MaximumTombstones);
        ValidateNoCrossRebind(state.DotBindings, state.Tombstones);
        ValidateWriteStamp(state.RegisterStamp, bounds);
        if (state.RegisterValueLength > bounds.MaximumRegisterBytes)
        {
            throw new InvalidOperationException("The CRDT register value exceeds configured bounds.");
        }

        ValidateClosedStateShape(state);
        _ = GetValue(state);
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
    internal static void ValidateDot(CrdtDot dot, CrdtBounds bounds)
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
    internal static void ValidateWriteStamp(ConflictWriteStamp? stamp, CrdtBounds bounds)
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
    internal static void ValidateClientId(string? clientId, CrdtBounds bounds)
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
    private static CrdtState ApplyMutation(
        CrdtState state,
        CrdtMutation mutation,
        string authenticatedClientId,
        long clientSequence,
        CrdtBounds bounds)
    {
        ArgumentExceptionHelper.ThrowIfNull(state);
        ArgumentExceptionHelper.ThrowIfNull(mutation);
        return mutation.Kind switch
        {
            CrdtMutationKind.GCounterSet => ApplyGCounter(state, mutation, authenticatedClientId, bounds),
            CrdtMutationKind.PNCounterSet => ApplyPNCounter(state, mutation, authenticatedClientId, bounds),
            CrdtMutationKind.ORSetAdd => ApplyORSetAdd(state, mutation, authenticatedClientId, clientSequence, bounds),
            CrdtMutationKind.ORSetRemove => ApplyORSetRemove(state, mutation, bounds),
            _ => ApplyLwwRegister(state, mutation, bounds),
        };
    }

    /// <summary>Applies a G-counter component update.</summary>
    /// <param name="state">The state.</param>
    /// <param name="mutation">The mutation.</param>
    /// <param name="authenticatedClientId">The authenticated client id.</param>
    /// <param name="bounds">The bounds.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static CrdtState ApplyGCounter(CrdtState state, CrdtMutation mutation, string authenticatedClientId, CrdtBounds bounds)
    {
        ValidateKind(state, CrdtKind.GCounter, bounds);
        ValidateAuthenticatedActor(authenticatedClientId, mutation.ActorId);

        var components = CopyComponents(state.GCounterComponents);
        SetMax(components, authenticatedClientId, mutation.GCounterComponent);
        var next = state with { GCounterComponents = components };
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
    private static CrdtState ApplyPNCounter(CrdtState state, CrdtMutation mutation, string authenticatedClientId, CrdtBounds bounds)
    {
        ValidateKind(state, CrdtKind.PNCounter, bounds);
        ValidateAuthenticatedActor(authenticatedClientId, mutation.ActorId);

        var positive = CopyComponents(state.PNCounterPositiveComponents);
        var negative = CopyComponents(state.PNCounterNegativeComponents);
        SetMax(positive, authenticatedClientId, mutation.PNCounterPositiveComponent);
        SetMax(negative, authenticatedClientId, mutation.PNCounterNegativeComponent);
        var next = state with { PNCounterPositiveComponents = positive, PNCounterNegativeComponents = negative };
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
    private static CrdtState ApplyORSetAdd(CrdtState state, CrdtMutation mutation, string clientId, long sequence, CrdtBounds bounds)
    {
        ValidateKind(state, CrdtKind.ORSet, bounds);
        ValidateElementLength(mutation.ByteLength, bounds);
        var dot = new CrdtDot { ClientId = clientId, ClientSequence = sequence };
        List<CrdtDotElement> bindings = [with(capacity: state.DotBindings.Count + 1)];
        AddRange(bindings, state.DotBindings);
        var incoming = new CrdtDotElement { Dot = dot, Element = mutation.Bytes };
        AddOrValidateSameBinding(bindings, incoming);
        var next = state with { DotBindings = bindings };
        ValidateState(next, bounds);
        return next;
    }

    /// <summary>Applies an OR-set remove by retaining exact observed tombstones.</summary>
    /// <param name="state">The state.</param>
    /// <param name="mutation">The mutation.</param>
    /// <param name="bounds">The bounds.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static CrdtState ApplyORSetRemove(CrdtState state, CrdtMutation mutation, CrdtBounds bounds)
    {
        ValidateKind(state, CrdtKind.ORSet, bounds);
        ValidateElementLength(mutation.ByteLength, bounds);
        List<CrdtDotElement> tombstones = [with(capacity: state.Tombstones.Count + mutation.ObservedDots.Count)];
        AddRange(tombstones, state.Tombstones);
        for (var index = 0; index < mutation.ObservedDots.Count; index++)
        {
            var dot = mutation.ObservedDots[index];
            var candidate = new CrdtDotElement { Dot = dot, Element = mutation.Bytes };
            AddOrValidateSameBinding(tombstones, candidate);
        }

        var next = state with { Tombstones = tombstones };
        ValidateState(next, bounds);
        return next;
    }

    /// <summary>Applies a LWW register assignment.</summary>
    /// <param name="state">The state.</param>
    /// <param name="mutation">The mutation.</param>
    /// <param name="bounds">The bounds.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static CrdtState ApplyLwwRegister(CrdtState state, CrdtMutation mutation, CrdtBounds bounds)
    {
        ValidateKind(state, CrdtKind.LwwRegister, bounds);
        var next = state with { RegisterValue = mutation.Bytes, RegisterStamp = mutation.RegisterStamp };
        ValidateState(next, bounds);
        return next;
    }

    /// <summary>Merges grow-only counter states.</summary>
    /// <param name="left">The left.</param>
    /// <param name="right">The right.</param>
    /// <param name="bounds">The bounds.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static CrdtState MergeGCounter(CrdtState left, CrdtState right, CrdtBounds bounds)
    {
        var components = MergeComponents(left.GCounterComponents, right.GCounterComponents);
        var state = new CrdtState { Kind = CrdtKind.GCounter, GCounterComponents = components };
        ValidateState(state, bounds);
        return state;
    }

    /// <summary>Merges PN-counter states.</summary>
    /// <param name="left">The left.</param>
    /// <param name="right">The right.</param>
    /// <param name="bounds">The bounds.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static CrdtState MergePNCounter(CrdtState left, CrdtState right, CrdtBounds bounds)
    {
        var positive = MergeComponents(left.PNCounterPositiveComponents, right.PNCounterPositiveComponents);
        var negative = MergeComponents(left.PNCounterNegativeComponents, right.PNCounterNegativeComponents);
        var state = new CrdtState { Kind = CrdtKind.PNCounter, PNCounterPositiveComponents = positive, PNCounterNegativeComponents = negative };
        ValidateState(state, bounds);
        return state;
    }

    /// <summary>Merges observed-remove set states.</summary>
    /// <param name="left">The left.</param>
    /// <param name="right">The right.</param>
    /// <param name="bounds">The bounds.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static CrdtState MergeORSet(CrdtState left, CrdtState right, CrdtBounds bounds)
    {
        List<CrdtDotElement> bindings = [with(capacity: left.DotBindings.Count + right.DotBindings.Count)];
        List<CrdtDotElement> tombstones = [with(capacity: left.Tombstones.Count + right.Tombstones.Count)];
        AddRange(bindings, left.DotBindings);
        AddRange(tombstones, left.Tombstones);
        AddMergedDotElements(bindings, right.DotBindings);
        AddMergedDotElements(tombstones, right.Tombstones);
        var state = new CrdtState { Kind = CrdtKind.ORSet, DotBindings = bindings, Tombstones = tombstones };
        ValidateState(state, bounds);
        return state;
    }

    /// <summary>Merges LWW register states by canonical write stamp order.</summary>
    /// <param name="left">The left.</param>
    /// <param name="right">The right.</param>
    /// <param name="bounds">The bounds.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static CrdtState MergeLwwRegister(CrdtState left, CrdtState right, CrdtBounds bounds)
    {
        var winner = CompareWriteStamps(left.RegisterStamp, right.RegisterStamp) >= 0 ? left : right;
        var state = new CrdtState { Kind = CrdtKind.LwwRegister, RegisterValue = winner.RegisterValue, RegisterStamp = winner.RegisterStamp };
        ValidateState(state, bounds);
        return state;
    }

    /// <summary>Derives active OR-set elements from bindings minus tombstones.</summary>
    /// <param name="state">The state.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static ReadOnlyCollection<ReadOnlyMemory<byte>> GetActiveElements(CrdtState state)
    {
        List<ReadOnlyMemory<byte>> elements = [];
        for (var index = 0; index < state.DotBindings.Count; index++)
        {
            var binding = state.DotBindings[index];
            if (ContainsDotElement(state.Tombstones, binding) || ContainsBytes(elements, binding.ElementSpan))
            {
                continue;
            }

            elements.Add(binding.ElementSpan.ToArray());
        }

        elements.Sort(CompareMemory);
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
    private static void ValidateKind(CrdtState state, CrdtKind kind, CrdtBounds bounds)
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
    private static void ValidateSameKind(CrdtState left, CrdtState right, CrdtBounds bounds)
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
    private static void ValidateCounterComponents(IReadOnlyDictionary<string, long> components, CrdtBounds bounds)
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
    private static void ValidateDotElements(IReadOnlyList<CrdtDotElement> elements, CrdtBounds bounds, int maximumCount)
    {
        if (elements.Count > maximumCount)
        {
            throw new InvalidOperationException("The CRDT dot element count exceeds configured bounds.");
        }

        HashSet<CrdtDot> seen = [];
        for (var index = 0; index < elements.Count; index++)
        {
            var element = elements[index];
            ValidateDot(element.Dot, bounds);
            ValidateElementLength(element.ElementLength, bounds);
            if (!seen.Add(element.Dot))
            {
                throw new InvalidOperationException("CRDT state dot bindings must be unique.");
            }
        }
    }

    /// <summary>Validates a state carries only metadata used by its CRDT kind.</summary>
    /// <param name="state">The state.</param>
    /// <exception cref="InvalidOperationException">Thrown when unrelated metadata is present.</exception>
    private static void ValidateClosedStateShape(CrdtState state)
    {
        switch (state.Kind)
        {
            case CrdtKind.GCounter:
            {
                EnsureEmpty(state.PNCounterPositiveComponents);
                EnsureEmpty(state.PNCounterNegativeComponents);
                EnsureEmpty(state.DotBindings);
                EnsureEmpty(state.Tombstones);
                EnsureNoRegister(state);
                break;
            }

            case CrdtKind.PNCounter:
            {
                EnsureEmpty(state.GCounterComponents);
                EnsureEmpty(state.DotBindings);
                EnsureEmpty(state.Tombstones);
                EnsureNoRegister(state);
                break;
            }

            case CrdtKind.ORSet:
            {
                EnsureEmpty(state.GCounterComponents);
                EnsureEmpty(state.PNCounterPositiveComponents);
                EnsureEmpty(state.PNCounterNegativeComponents);
                EnsureNoRegister(state);
                break;
            }

            case CrdtKind.LwwRegister:
            {
                EnsureEmpty(state.GCounterComponents);
                EnsureEmpty(state.PNCounterPositiveComponents);
                EnsureEmpty(state.PNCounterNegativeComponents);
                EnsureEmpty(state.DotBindings);
                EnsureEmpty(state.Tombstones);
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
    private static void EnsureNoRegister(CrdtState state)
    {
        if (state.RegisterValueLength == 0 && state.RegisterStamp is null)
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
    private static void ValidateNoCrossRebind(IReadOnlyList<CrdtDotElement> bindings, IReadOnlyList<CrdtDotElement> tombstones)
    {
        for (var bindingIndex = 0; bindingIndex < bindings.Count; bindingIndex++)
        {
            var binding = bindings[bindingIndex];
            for (var tombstoneIndex = 0; tombstoneIndex < tombstones.Count; tombstoneIndex++)
            {
                var tombstone = tombstones[tombstoneIndex];
                if (CompareDots(binding.Dot, tombstone.Dot) == 0 && !BytesEqual(binding.ElementSpan, tombstone.ElementSpan))
                {
                    throw new InvalidOperationException(DotRebindMessage);
                }
            }
        }
    }

    /// <summary>Validates an element byte length.</summary>
    /// <param name="length">The length.</param>
    /// <param name="bounds">The bounds.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static void ValidateElementLength(int length, CrdtBounds bounds)
    {
        if (length <= bounds.MaximumElementBytes)
        {
            return;
        }

        throw new InvalidOperationException("A CRDT element exceeds configured bounds.");
    }

    /// <summary>Adds dot elements while keeping dot rebinding impossible.</summary>
    /// <param name="target">The target.</param>
    /// <param name="source">The source.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static void AddMergedDotElements(List<CrdtDotElement> target, IReadOnlyList<CrdtDotElement> source)
    {
        for (var index = 0; index < source.Count; index++)
        {
            AddOrValidateSameBinding(target, source[index]);
        }
    }

    /// <summary>Adds existing dot elements to a mutable list.</summary>
    /// <param name="target">The target.</param>
    /// <param name="source">The source.</param>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static void AddRange(List<CrdtDotElement> target, IReadOnlyList<CrdtDotElement> source)
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
    private static void AddOrValidateSameBinding(List<CrdtDotElement> target, CrdtDotElement incoming)
    {
        for (var index = 0; index < target.Count; index++)
        {
            var current = target[index];
            if (CompareDots(current.Dot, incoming.Dot) != 0)
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

    /// <summary>Determines whether a dot element list contains an exact dot and element match.</summary>
    /// <param name="elements">The elements.</param>
    /// <param name="value">The value.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static bool ContainsDotElement(IReadOnlyList<CrdtDotElement> elements, CrdtDotElement value)
    {
        for (var index = 0; index < elements.Count; index++)
        {
            var element = elements[index];
            if (CompareDots(element.Dot, value.Dot) == 0 && BytesEqual(element.ElementSpan, value.ElementSpan))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Determines whether a mutable element list contains the supplied bytes.</summary>
    /// <param name="elements">The elements.</param>
    /// <param name="value">The value.</param>
    /// <returns>The result.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the CRDT data is invalid.</exception>
    private static bool ContainsBytes(List<ReadOnlyMemory<byte>> elements, ReadOnlySpan<byte> value)
    {
        for (var index = 0; index < elements.Count; index++)
        {
            if (BytesEqual(elements[index].Span, value))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Compares two dots.</summary>
    /// <param name="left">The left.</param>
    /// <param name="right">The right.</param>
    /// <returns>The result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CompareDots(CrdtDot left, CrdtDot right) => left.CompareTo(right);

    /// <summary>Compares two byte memory values.</summary>
    /// <param name="left">The left.</param>
    /// <param name="right">The right.</param>
    /// <returns>The result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CompareMemory(ReadOnlyMemory<byte> left, ReadOnlyMemory<byte> right) => CompareBytes(left.Span, right.Span);

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
