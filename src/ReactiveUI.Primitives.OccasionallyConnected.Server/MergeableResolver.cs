// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Resolves built-in CRDT operations against complete server state payloads.</summary>
[System.Diagnostics.DebuggerDisplay("{_options.Kind,nq}")]
public sealed class MergeableResolver : IConflictResolver
{
    /// <summary>The resolver options.</summary>
    private readonly MergeableResolverOptions _options;

    /// <summary>Initializes a new instance of the <see cref="MergeableResolver"/> class.</summary>
    /// <param name="options">The resolver options.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    public MergeableResolver(MergeableResolverOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        MergeableServerGuards.ValidateKind(options.Kind, nameof(options.Kind));
        ArgumentExceptionHelper.ThrowIfNull(options.VersionFactory);
        ArgumentExceptionHelper.ThrowIfNull(options.Bounds);
        options.Bounds.Validate();
        _options = options;
    }

    /// <inheritdoc/>
    public ValueTask<ConflictResolutionResult> ResolveAsync(
        ConflictContext context,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(context);
        cancellationToken.ThrowIfCancellationRequested();
        var operationId = context.Incoming.Count == 0 ? new OperationId(Guid.Empty) : context.Incoming[0].OperationId;
        if (!TryGetTrustedCandidate(context, out var candidateWrite))
        {
            return new(Rejected(operationId, "crdt-missing-server-provenance", context.Current.Version));
        }

        var operation = context.Incoming[0];
        return new(ResolveDecoded(context, operation, candidateWrite));
    }

    /// <summary>Creates a rejected result.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="reasonCode">The stable reason code.</param>
    /// <param name="serverVersion">The current server version.</param>
    /// <returns>The rejected result.</returns>
    private static ConflictResolutionResult Rejected(OperationId operationId, string reasonCode, string serverVersion) =>
        new([], [new(operationId, reasonCode, true)], [], [], serverVersion);

    /// <summary>Validates trusted server provenance for the incoming operation.</summary>
    /// <param name="context">The conflict context.</param>
    /// <param name="candidateWrite">The trusted candidate write when provenance matches.</param>
    /// <returns>Whether provenance is trusted and matching.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool TryGetTrustedCandidate(
        ConflictContext context,
        [NotNullWhen(true)] out ConflictWriteStamp? candidateWrite)
    {
        if (context.Incoming.Count == 1
            && context.Server is { CandidateWrite: var serverWrite }
            && serverWrite.ClientId == context.Client.ClientId
            && serverWrite.OperationId == context.Incoming[0].OperationId)
        {
            candidateWrite = serverWrite;
            return true;
        }

        candidateWrite = null;
        return false;
    }

    /// <summary>Returns whether a mutation belongs to a CRDT kind.</summary>
    /// <param name="mutationKind">The mutation kind.</param>
    /// <param name="kind">The CRDT kind.</param>
    /// <returns>Whether the mutation belongs to the registered kind.</returns>
    private static bool MutationMatchesKind(MergeableChangeKind mutationKind, MergeableKind kind)
    {
        if (kind == MergeableKind.IncrementOnlyCounter)
        {
            return mutationKind == MergeableChangeKind.IncrementOnlyCounterSet;
        }

        if (kind == MergeableKind.Counter)
        {
            return mutationKind == MergeableChangeKind.CounterSet;
        }

        return kind == MergeableKind.AddWinsSet
            ? mutationKind is MergeableChangeKind.AddWinsSetAdd or MergeableChangeKind.AddWinsSetRemove
            : mutationKind == MergeableChangeKind.LastWriterWinsValueSet;
    }

    /// <summary>Binds server-owned write provenance to mutations that carry write stamps.</summary>
    /// <param name="input">The decoded input.</param>
    /// <param name="candidateWrite">The trusted candidate write stamp.</param>
    /// <returns>The trusted input.</returns>
    private static MergeableInput BindTrustedInput(MergeableInput input, ConflictWriteStamp candidateWrite) =>
        input.Value is MergeableChange mutation && mutation.ToKind() == MergeableChangeKind.LastWriterWinsValueSet
            ? MergeableInput.ForChange(MergeableChange.LastWriterWinsValueSet(mutation.Bytes, candidateWrite))
            : input;

    /// <summary>Resolves after trusted operation provenance is available.</summary>
    /// <param name="context">The conflict context.</param>
    /// <param name="operation">The operation.</param>
    /// <param name="candidateWrite">The trusted candidate write.</param>
    /// <returns>The conflict resolution result.</returns>
    private ConflictResolutionResult ResolveDecoded(
        ConflictContext context,
        SyncOperation operation,
        ConflictWriteStamp candidateWrite)
    {
        if (!MergeableServerPayloads.TryDecodeState(context.Current.State, _options.Kind, _options.Bounds, out var current, out var stateReason))
        {
            return Rejected(operation.OperationId, stateReason, context.Current.Version);
        }

        if (!MergeableServerPayloads.TryDecodeInput(operation.Payload, _options.Bounds, out var input, out var inputReason))
        {
            return Rejected(operation.OperationId, inputReason, context.Current.Version);
        }

        if (input.Value is not MergeableChange mutation)
        {
            return Rejected(operation.OperationId, "crdt-input-kind-mismatch", context.Current.Version);
        }

        return MutationMatchesKind(mutation.ToKind(), _options.Kind)
            ? ResolveAccepted(context, operation, current, input, candidateWrite)
            : Rejected(operation.OperationId, "crdt-state-kind-mismatch", context.Current.Version);
    }

    /// <summary>Resolves a decoded CRDT mutation.</summary>
    /// <param name="context">The conflict context.</param>
    /// <param name="operation">The operation.</param>
    /// <param name="current">The current CRDT state.</param>
    /// <param name="input">The decoded CRDT input.</param>
    /// <param name="candidateWrite">The trusted write stamp.</param>
    /// <returns>The conflict resolution result.</returns>
    private ConflictResolutionResult ResolveAccepted(
        ConflictContext context,
        SyncOperation operation,
        MergeableState current,
        MergeableInput input,
        ConflictWriteStamp candidateWrite)
    {
        PayloadEnvelope payload;
        try
        {
            var trusted = BindTrustedInput(input, candidateWrite);
            var candidate = MergeableFunctions.ApplyLocal(current, trusted, candidateWrite.ClientId, operation.ClientSequence, _options.Bounds);
            var resolved = MergeableFunctions.Merge(current, candidate, _options.Bounds);
            payload = MergeableServerPayloads.CreateState(resolved, _options.Bounds);
        }
        catch (Exception exception) when (exception is InvalidOperationException or ArgumentException or OverflowException)
        {
            return Rejected(operation.OperationId, "crdt-invalid-mutation", context.Current.Version);
        }

        var version = _options.VersionFactory.CreateNextVersion(context, operation);
        return new([operation.OperationId], [], [new(operation.OperationId, "crdt.merge", payload)], [], version);
    }
}
