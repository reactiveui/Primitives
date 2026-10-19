// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Resolves conflicts using trusted server write stamps.</summary>
[System.Diagnostics.DebuggerDisplay("LastWriterWinsResolver")]
public sealed class LastWriterWinsResolver : IConflictResolver
{
    /// <summary>The reason returned when trusted server provenance is missing.</summary>
    private const string MissingServerWriteProvenanceReason = "missing-server-write-provenance";

    /// <summary>The reason returned when the candidate write loses to the current write.</summary>
    private const string StaleWriteReason = "lww-stale-write";

    /// <summary>The resolver options.</summary>
    private readonly LastWriterWinsResolverOptions _options;

    /// <summary>Initializes a new instance of the <see cref="LastWriterWinsResolver"/> class.</summary>
    /// <param name="options">The resolver options.</param>
    public LastWriterWinsResolver(LastWriterWinsResolverOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        options.Validate();
        _options = options;
    }

    /// <inheritdoc/>
    public ValueTask<ConflictResolutionResult> ResolveAsync(
        ConflictContext context,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(context);
        cancellationToken.ThrowIfCancellationRequested();
        var operation = context.Incoming.Count == 1 ? context.Incoming[0] : null;
        if (operation is null || context.Server is not { } server
            || server.CandidateWrite.ClientId != context.Client.ClientId
            || server.CandidateWrite.OperationId != operation.OperationId)
        {
            var operationId = operation?.OperationId ?? new OperationId(Guid.Empty);
            return new(Reject(operationId, MissingServerWriteProvenanceReason, false, context.Current.Version));
        }

        var isBaseVersionMatched = IsBaseVersionMatched(context, operation);
        if (ShouldAccept(server, isBaseVersionMatched))
        {
            var nextVersion = _options.VersionFactory.CreateNextVersion(context, operation);
            return new(Accept(operation, nextVersion, isBaseVersionMatched));
        }

        return new(new ConflictResolutionResult(
            [],
            [new(operation.OperationId, StaleWriteReason, true)],
            [],
            [],
            context.Current.Version));
    }

    /// <summary>Checks whether the operation base version matches the current state.</summary>
    /// <param name="context">The conflict context.</param>
    /// <param name="operation">The operation.</param>
    /// <returns>Whether the base version matches or is unconditional.</returns>
    private static bool IsBaseVersionMatched(ConflictContext context, SyncOperation operation) =>
        operation.BaseVersion is null || StringComparer.Ordinal.Equals(operation.BaseVersion, context.Current.Version);

    /// <summary>Checks whether a write should be accepted.</summary>
    /// <param name="server">The trusted server context.</param>
    /// <param name="isBaseVersionMatched">Whether the operation base version already matched.</param>
    /// <returns>Whether the write is accepted.</returns>
    private static bool ShouldAccept(ConflictServerContext server, bool isBaseVersionMatched) =>
        isBaseVersionMatched || server.CurrentWrite is null || IsNewer(server.CandidateWrite, server.CurrentWrite);

    /// <summary>Compares two trusted write stamps.</summary>
    /// <param name="candidate">The candidate write.</param>
    /// <param name="current">The current write.</param>
    /// <returns>Whether the candidate follows the current write.</returns>
    private static bool IsNewer(ConflictWriteStamp candidate, ConflictWriteStamp current)
    {
        var timeOrder = candidate.CommittedAtUtc.CompareTo(current.CommittedAtUtc);
        if (timeOrder != 0)
        {
            return timeOrder > 0;
        }

        var clientOrder = StringComparer.Ordinal.Compare(candidate.ClientId, current.ClientId);
        return clientOrder != 0 ? clientOrder > 0 : candidate.OperationId.Value.CompareTo(current.OperationId.Value) > 0;
    }

    /// <summary>Creates an accepted result.</summary>
    /// <param name="operation">The accepted operation.</param>
    /// <param name="serverVersion">The accepted server version.</param>
    /// <param name="baseVersionMatched">Whether the base version already matched.</param>
    /// <returns>The accepted conflict resolution.</returns>
    private static ConflictResolutionResult Accept(
        SyncOperation operation,
        string serverVersion,
        bool baseVersionMatched) =>
        new(
            [operation.OperationId],
            [],
            baseVersionMatched ? [] : [new(operation.OperationId, "lww.accepted", operation.Payload)],
            [],
            serverVersion);

    /// <summary>Creates a rejected result.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="reasonCode">The stable reason code.</param>
    /// <param name="mayResubmit">Whether the operation may be resubmitted.</param>
    /// <param name="serverVersion">The server version.</param>
    /// <returns>The rejected conflict resolution.</returns>
    private static ConflictResolutionResult Reject(
        OperationId operationId,
        string reasonCode,
        bool mayResubmit,
        string serverVersion) =>
        new([], [new(operationId, reasonCode, mayResubmit)], [], [], serverVersion);
}
