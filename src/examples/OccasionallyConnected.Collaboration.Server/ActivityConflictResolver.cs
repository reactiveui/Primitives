// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server;

/// <summary>Resolves activity updates and custom operations with schema validation and canonical server payloads.</summary>
[System.Diagnostics.DebuggerDisplay("Activity conflict resolver")]
internal sealed class ActivityConflictResolver : IConflictResolver
{
    /// <summary>The reason returned when trusted server provenance is missing.</summary>
    private const string MissingServerProvenanceReason = "activity-missing-server-provenance";

    /// <summary>The reason returned when the operation type is not an activity mutation.</summary>
    private const string OperationTypeMismatchReason = "activity-operation-type-mismatch";

    /// <summary>The reason returned when the incoming operation count is not supported.</summary>
    private const string OperationCountMismatchReason = "activity-operation-count-mismatch";

    /// <summary>The resolution code used when an activity payload is canonicalized.</summary>
    private const string AcceptedResolutionCode = "activity.custom.canonicalized";

    /// <summary>The version factory used for accepted activity writes.</summary>
    private readonly ActivityVersionFactory _versionFactory = new();

    /// <inheritdoc/>
    public ValueTask<ConflictResolutionResult> ResolveAsync(
        ConflictContext context,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        cancellationToken.ThrowIfCancellationRequested();
        var operation = context.Incoming.Count == 1 ? context.Incoming[0] : null;
        if (operation is null)
        {
            return ValueTask.FromResult(Reject(new(Guid.Empty), OperationCountMismatchReason, context.Current.Version));
        }

        if (!HasTrustedServerProvenance(context, operation))
        {
            return ValueTask.FromResult(Reject(operation.OperationId, MissingServerProvenanceReason, context.Current.Version));
        }

        if (operation.Type is not (SyncOperationType.Custom or SyncOperationType.Update))
        {
            return ValueTask.FromResult(Reject(operation.OperationId, OperationTypeMismatchReason, context.Current.Version));
        }

        if (!ActivityPayloads.TryReadInput(operation.Payload, out var input, out var reasonCode))
        {
            return ValueTask.FromResult(Reject(operation.OperationId, reasonCode, context.Current.Version));
        }

        var version = _versionFactory.CreateNextVersion(context, operation);
        var canonical = ActivityPayloads.CreateCanonical(input, context.Client, context, operation, version);
        return ValueTask.FromResult(new ConflictResolutionResult(
            [operation.OperationId],
            [],
            [new(operation.OperationId, AcceptedResolutionCode, canonical)],
            [],
            version));
    }

    /// <summary>Creates a rejected activity resolution.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="reasonCode">The stable rejection reason.</param>
    /// <param name="serverVersion">The current server version.</param>
    /// <returns>The rejected resolution.</returns>
    private static ConflictResolutionResult Reject(
        OperationId operationId,
        string reasonCode,
        string serverVersion) =>
        new([], [new(operationId, reasonCode, true)], [], [], serverVersion);

    /// <summary>Validates trusted server write provenance for the activity operation.</summary>
    /// <param name="context">The conflict context.</param>
    /// <param name="operation">The operation being resolved.</param>
    /// <returns>Whether trusted provenance matches the operation and caller.</returns>
    private static bool HasTrustedServerProvenance(ConflictContext context, SyncOperation operation) =>
        context.Server is { CandidateWrite: var write }
        && write.OperationId == operation.OperationId
        && string.Equals(write.ClientId, context.Client.ClientId, StringComparison.Ordinal);
}
