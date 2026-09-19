// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Concrete authorized server stream hub facade over the internal server journal and processor.</summary>
public sealed partial class ServerStreamHub
{
    /// <summary>The logical byte width counted for Int32 and enum scalar values.</summary>
    private const long SnapshotInt32LogicalBytes = 4;

    /// <summary>The logical byte width counted for Int64 scalar values.</summary>
    private const long SnapshotInt64LogicalBytes = 8;

    /// <summary>The logical byte width counted for Guid scalar values.</summary>
    private const long SnapshotGuidLogicalBytes = 16;

    /// <summary>The logical byte width counted for DateTimeOffset scalar values.</summary>
    private const long SnapshotDateTimeOffsetLogicalBytes = 16;

    /// <summary>The stable unsupported snapshot recovery reason.</summary>
    private const string SnapshotUnsupportedReason = "snapshot.unsupported";

    /// <summary>The stable retained provenance failure reason.</summary>
    private const string SnapshotRetentionExpiredReason = "snapshot.retention_expired";

    /// <summary>The stable validation failure reason.</summary>
    private const string SnapshotValidationRejectedReason = "snapshot.validation_rejected";

    /// <summary>The stable ambiguous replay proof failure reason.</summary>
    private const string SnapshotAmbiguousPendingOperationReason = "snapshot.ambiguous_pending_operation";

    /// <summary>The stable capacity failure reason.</summary>
    private const string SnapshotCapacityExceededReason = "snapshot.capacity_exceeded";

    /// <summary>The stable concurrent change failure reason.</summary>
    private const string SnapshotConcurrentChangeReason = "snapshot.concurrent_change";

    /// <inheritdoc/>
    public async ValueTask<RemoteSnapshotRecoveryResult> GetSnapshotAsync(
        RemoteSnapshotRecoveryRequest request,
        ServerAuthenticatedClient client,
        CancellationToken cancellationToken)
    {
        EnterActiveCall();
        try
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeCancellation.Token);
            var token = linked.Token;
            ValidateSnapshotRecoveryRequest(request, client, _options.SnapshotRecoveryLimits);
            token.ThrowIfCancellationRequested();

            var materializer = _options.SnapshotRecoveryMaterializer;
            if (materializer is null)
            {
                return CreateNonRecoveredResult(RemoteSnapshotRecoveryStatus.UnsupportedProjection, SnapshotUnsupportedReason);
            }

            var authorizationPolicy = _options.SnapshotRecoveryAuthorizationPolicy!;
            var publicScope = await authorizationPolicy.AuthorizeSnapshotRecoveryAsync(client, request, token).ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
            var scope = ValidateScope(client, request.StreamId, publicScope);
            var streamKey = new ServerStreamKey(scope.TenantId, request.StreamId);
            var subscription = new ServerSubscriptionIdentity(streamKey, scope.ClientId, request.SubscriptionId);
            var view = ReadSnapshotRecoveryView(_snapshotRecoveryJournal, streamKey, subscription, request, _options.SnapshotRecoveryLimits);

            var capturedState = view.Snapshot.State;
            if (view.SubscriptionState is null || !HasRetainedExpiredCursorProof(view) || capturedState is null)
            {
                return CreateNonRecoveredResult(RemoteSnapshotRecoveryStatus.RetentionExpired, SnapshotRetentionExpiredReason);
            }

            if (!ServerSnapshotRecoveryJournalOperations.ReplayOnlyProofsAreAccepted(view, request))
            {
                return CreateNonRecoveredResult(RemoteSnapshotRecoveryStatus.AmbiguousPendingOperation, SnapshotAmbiguousPendingOperationReason);
            }

            var frontierCursor = CreateCapturedFrontierCursor(view.Snapshot);
            var observedAtUtc = _options.TimeProvider.GetUtcNow();
            var dispositions = CreatePublicOperationDispositions(view);
            var context = CreateMaterializationContext(request, scope, capturedState, frontierCursor, observedAtUtc);
            var materialization = await materializer.MaterializeAsync(context, token).ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
            var offerContext = new SnapshotRecoveryOfferContext
            {
                Request = request,
                StreamKey = streamKey,
                Subscription = subscription,
                View = view,
                CapturedServerVersion = capturedState.Version,
                FrontierCursor = frontierCursor,
                ObservedAtUtc = observedAtUtc,
                OperationDispositions = dispositions,
            };
            return materialization is null
                ? CreateNonRecoveredResult(RemoteSnapshotRecoveryStatus.ValidationRejected, SnapshotValidationRejectedReason)
                : OfferMaterializedSnapshot(_snapshotRecoveryJournal, _options.SnapshotRecoveryLimits, offerContext, materialization);
        }
        finally
        {
            ReleaseActiveCall();
        }
    }

    /// <summary>Maps the closed set of durable journal offer results to public snapshot recovery results.</summary>
    /// <param name="offer">The durable offer result.</param>
    /// <param name="recovered">The recovered result that was offered.</param>
    /// <returns>The public remote recovery result.</returns>
    /// <remarks>
    /// Concrete journals may return validation rejection for invalid identity or checkpoint fences. The hub builds those
    /// fences internally, but this boundary keeps the fail-closed durable policy explicit and testable.
    /// </remarks>
    internal static RemoteSnapshotRecoveryResult MapOfferResult(ServerSnapshotOfferResult offer, RemoteSnapshotRecoveryResult recovered) =>
        offer.Status switch
        {
            ServerSnapshotOfferStatus.Offered or ServerSnapshotOfferStatus.AlreadyOffered => recovered,
            ServerSnapshotOfferStatus.MissingSubscription => CreateNonRecoveredResult(
                RemoteSnapshotRecoveryStatus.RetentionExpired,
                SnapshotRetentionExpiredReason),
            ServerSnapshotOfferStatus.ConcurrentChange => CreateNonRecoveredResult(
                RemoteSnapshotRecoveryStatus.RetryableConcurrentChange,
                SnapshotConcurrentChangeReason),
            ServerSnapshotOfferStatus.CapacityExceeded => CreateNonRecoveredResult(
                RemoteSnapshotRecoveryStatus.CapacityExceeded,
                SnapshotCapacityExceededReason),
            _ => CreateNonRecoveredResult(RemoteSnapshotRecoveryStatus.ValidationRejected, SnapshotValidationRejectedReason),
        };

    /// <summary>Reads the retained snapshot recovery view for one trusted subscription.</summary>
    /// <param name="journal">The snapshot recovery journal.</param>
    /// <param name="streamKey">The trusted stream key.</param>
    /// <param name="subscription">The trusted subscription identity.</param>
    /// <param name="request">The recovery request.</param>
    /// <param name="limits">The snapshot recovery limits.</param>
    /// <returns>The captured snapshot recovery view.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static ServerSnapshotRecoveryView ReadSnapshotRecoveryView(
        IServerSnapshotRecoveryJournal journal,
        ServerStreamKey streamKey,
        ServerSubscriptionIdentity subscription,
        RemoteSnapshotRecoveryRequest request,
        SnapshotRecoveryLimits limits) =>
        journal.ReadSnapshotRecoveryView(new() { StreamKey = streamKey, Subscription = subscription, RecoveryRequest = request, Limits = limits });

    /// <summary>Validates snapshot recovery shape before authorization or journal lookup.</summary>
    /// <param name="request">The recovery request.</param>
    /// <param name="client">The authenticated client.</param>
    /// <param name="limits">The snapshot recovery limits.</param>
    private static void ValidateSnapshotRecoveryRequest(
        RemoteSnapshotRecoveryRequest request,
        ServerAuthenticatedClient client,
        SnapshotRecoveryLimits limits)
    {
        ValidateClient(client);
        SnapshotRecoveryValidator.Validate(request, limits);
    }

    /// <summary>Checks whether the requested expired cursor is proven by retained subscription offers.</summary>
    /// <param name="view">The captured snapshot recovery view.</param>
    /// <returns>Whether retained provenance is sufficient to recover.</returns>
    private static bool HasRetainedExpiredCursorProof(ServerSnapshotRecoveryView view) =>
        view.RequestedExpiredCursor is null || view.ExpiredCursorOffer is not null;

    /// <summary>Creates the captured complete receive frontier cursor for a snapshot view.</summary>
    /// <param name="snapshot">The captured stream snapshot.</param>
    /// <returns>The captured frontier cursor.</returns>
    private static string CreateCapturedFrontierCursor(ServerCommitSnapshot snapshot) =>
        snapshot.LastCursor ?? ServerReceiveGroupCursor.Create(snapshot.StreamKey, snapshot.LastGroupSequence);

    /// <summary>Builds a materializer context from trusted authorization and captured server state.</summary>
    /// <param name="request">The recovery request.</param>
    /// <param name="scope">The trusted recovery scope.</param>
    /// <param name="capturedState">The captured canonical server state.</param>
    /// <param name="frontierCursor">The captured frontier cursor.</param>
    /// <param name="observedAtUtc">The captured observation time.</param>
    /// <returns>The materializer context.</returns>
    private static ServerSnapshotMaterializationContext CreateMaterializationContext(
        RemoteSnapshotRecoveryRequest request,
        ServerOperationScope scope,
        ServerState capturedState,
        string frontierCursor,
        DateTimeOffset observedAtUtc) =>
        new()
        {
            TenantId = scope.TenantId,
            ClientId = scope.ClientId,
            StreamId = request.StreamId,
            SubscriptionId = request.SubscriptionId,
            FrontierCursor = frontierCursor,
            CapturedServerState = capturedState,
            CapturedServerVersion = capturedState.Version,
            ClientStateContractId = request.ClientStateContractId,
            ClientStateSchemaVersion = request.ClientStateSchemaVersion,
            SnapshotFormatVersion = request.SnapshotFormatVersion,
            MaximumResponseBytes = request.MaximumResponseBytes,
            ObservedAtUtc = observedAtUtc,
        };

    /// <summary>Maps trusted server operation dispositions to public recovery dispositions.</summary>
    /// <param name="view">The captured snapshot recovery view.</param>
    /// <returns>The public disposition list.</returns>
    private static SnapshotOperationDisposition[] CreatePublicOperationDispositions(ServerSnapshotRecoveryView view)
    {
        var source = view.OperationDispositions;
        var dispositions = new SnapshotOperationDisposition[source.Count];
        for (var index = 0; index < dispositions.Length; index++)
        {
            var disposition = source[index];
            dispositions[index] = new() { OperationId = disposition.OperationId, Kind = disposition.Kind, Result = disposition.Result };
        }

        return dispositions;
    }

    /// <summary>Maps materializer statuses to remote snapshot recovery statuses.</summary>
    /// <param name="status">The materialization status.</param>
    /// <returns>The remote recovery status.</returns>
    private static RemoteSnapshotRecoveryStatus MapMaterializationStatus(ServerSnapshotMaterializationStatus status) =>
        status switch
        {
            ServerSnapshotMaterializationStatus.UnsupportedProjection => RemoteSnapshotRecoveryStatus.UnsupportedProjection,
            ServerSnapshotMaterializationStatus.CapacityExceeded => RemoteSnapshotRecoveryStatus.CapacityExceeded,
            ServerSnapshotMaterializationStatus.ValidationRejected => RemoteSnapshotRecoveryStatus.ValidationRejected,
            _ => RemoteSnapshotRecoveryStatus.ValidationRejected,
        };

    /// <summary>Creates and validates a non-recovered materialization result.</summary>
    /// <param name="request">The recovery request.</param>
    /// <param name="status">The mapped remote status.</param>
    /// <param name="reasonCode">The materializer reason code.</param>
    /// <param name="limits">The snapshot recovery limits.</param>
    /// <returns>The validated non-recovered result.</returns>
    private static RemoteSnapshotRecoveryResult CreateMaterializationNonRecoveredResult(
        RemoteSnapshotRecoveryRequest request,
        RemoteSnapshotRecoveryStatus status,
        string? reasonCode,
        SnapshotRecoveryLimits limits)
    {
        var result = CreateNonRecoveredResult(status, reasonCode);
        try
        {
            SnapshotRecoveryValidator.Validate(request, result, limits);
            return result;
        }
        catch (EncoderFallbackException)
        {
            return CreateNonRecoveredResult(RemoteSnapshotRecoveryStatus.ValidationRejected, SnapshotValidationRejectedReason);
        }
        catch (ArgumentException)
        {
            return status == RemoteSnapshotRecoveryStatus.CapacityExceeded
                ? CreateNonRecoveredResult(RemoteSnapshotRecoveryStatus.CapacityExceeded, SnapshotCapacityExceededReason)
                : CreateNonRecoveredResult(RemoteSnapshotRecoveryStatus.ValidationRejected, SnapshotValidationRejectedReason);
        }
    }

    /// <summary>Validates a recovered result and maps structural failures to bounded remote statuses.</summary>
    /// <param name="request">The recovery request.</param>
    /// <param name="result">The recovered result.</param>
    /// <param name="checkpoint">The known recovered checkpoint.</param>
    /// <param name="limits">The snapshot recovery limits.</param>
    /// <param name="validationResult">The mapped non-recovered result when validation fails.</param>
    /// <returns>Whether validation succeeded.</returns>
    private static bool TryValidateRecoveredResult(
        RemoteSnapshotRecoveryRequest request,
        RemoteSnapshotRecoveryResult result,
        RemoteSnapshotCheckpoint checkpoint,
        SnapshotRecoveryLimits limits,
        out RemoteSnapshotRecoveryResult validationResult)
    {
        try
        {
            SnapshotRecoveryValidator.Validate(request, result, limits);
            validationResult = result;
            return true;
        }
        catch (EncoderFallbackException)
        {
            validationResult = CreateNonRecoveredResult(RemoteSnapshotRecoveryStatus.ValidationRejected, SnapshotValidationRejectedReason);
            return false;
        }
        catch (ArgumentException)
        {
            validationResult = IsRecoveredResultCapacityExceeded(request, result, checkpoint)
                ? CreateNonRecoveredResult(RemoteSnapshotRecoveryStatus.CapacityExceeded, SnapshotCapacityExceededReason)
                : CreateNonRecoveredResult(RemoteSnapshotRecoveryStatus.ValidationRejected, SnapshotValidationRejectedReason);
            return false;
        }
    }

    /// <summary>Determines whether a failed recovered-result validation is caused by bounded size.</summary>
    /// <param name="request">The recovery request.</param>
    /// <param name="result">The recovered result.</param>
    /// <param name="checkpoint">The known recovered checkpoint.</param>
    /// <returns>Whether capacity caused the failed validation.</returns>
    private static bool IsRecoveredResultCapacityExceeded(
        RemoteSnapshotRecoveryRequest request,
        RemoteSnapshotRecoveryResult result,
        RemoteSnapshotCheckpoint checkpoint)
    {
        var logicalBytes = EstimateRecoveredResultLogicalBytes(result, checkpoint);

        return logicalBytes > request.MaximumResponseBytes;
    }

    /// <summary>Estimates recovered result logical bytes using the public validator's scalar accounting.</summary>
    /// <param name="result">The recovered result.</param>
    /// <param name="checkpoint">The known recovered checkpoint.</param>
    /// <returns>The estimated logical byte count.</returns>
    private static long EstimateRecoveredResultLogicalBytes(
        RemoteSnapshotRecoveryResult result,
        RemoteSnapshotCheckpoint checkpoint)
    {
        var bytes = SnapshotInt32LogicalBytes + EstimateTextBytes(result.ReasonCode);
        bytes += EstimateTextBytes(checkpoint.StreamId.Value)
            + SnapshotGuidLogicalBytes
            + EstimateTextBytes(checkpoint.FrontierCursor)
            + EstimateTextBytes(checkpoint.ServerVersion)
            + SnapshotInt32LogicalBytes
            + SnapshotDateTimeOffsetLogicalBytes
            + EstimatePayloadBytes(checkpoint.ClientState);

        bytes += SnapshotInt32LogicalBytes;
        for (var index = 0; index < result.OperationDispositions.Count; index++)
        {
            bytes += EstimateDispositionBytes(result.OperationDispositions[index]);
        }

        return bytes;
    }

    /// <summary>Estimates one operation disposition's logical byte count.</summary>
    /// <param name="disposition">The disposition.</param>
    /// <returns>The estimated logical byte count.</returns>
    private static long EstimateDispositionBytes(SnapshotOperationDisposition disposition) =>
        disposition.Result is null
            ? SnapshotGuidLogicalBytes + SnapshotInt32LogicalBytes
            : SnapshotGuidLogicalBytes + SnapshotInt32LogicalBytes + SnapshotGuidLogicalBytes + SnapshotInt32LogicalBytes
                + EstimateTextBytes(disposition.Result.ReasonCode)
                + EstimateTextBytes(disposition.Result.ServerVersion);

    /// <summary>Estimates a payload envelope's logical byte count.</summary>
    /// <param name="payload">The payload.</param>
    /// <returns>The estimated logical byte count.</returns>
    private static long EstimatePayloadBytes(PayloadEnvelope payload) =>
        SnapshotInt32LogicalBytes
        + SnapshotInt64LogicalBytes
        + EstimateTextBytes(payload.ContractId)
        + EstimateTextBytes(payload.ContentType)
        + EstimateTextBytes(payload.PayloadHash)
        + payload.PayloadLength;

    /// <summary>Estimates UTF-8 protocol bytes for a possibly null value.</summary>
    /// <param name="value">The protocol value.</param>
    /// <returns>The UTF-8 byte count, or zero for null.</returns>
    private static long EstimateTextBytes(string? value) =>
        value is null ? 0 : Encoding.UTF8.GetByteCount(value);

    /// <summary>Creates a structurally simple non-recovered snapshot result.</summary>
    /// <param name="status">The non-recovered status.</param>
    /// <param name="reasonCode">The stable reason code.</param>
    /// <returns>The non-recovered result.</returns>
    private static RemoteSnapshotRecoveryResult CreateNonRecoveredResult(RemoteSnapshotRecoveryStatus status, string? reasonCode) =>
        new() { Status = status, ReasonCode = reasonCode };

    /// <summary>Creates, validates, and durably offers a materialized snapshot result.</summary>
    /// <param name="snapshotRecoveryJournal">The snapshot recovery journal.</param>
    /// <param name="limits">The snapshot recovery limits.</param>
    /// <param name="context">The trusted offer context.</param>
    /// <param name="materialization">The materialization result.</param>
    /// <returns>The remote snapshot recovery result.</returns>
    private static RemoteSnapshotRecoveryResult OfferMaterializedSnapshot(
        IServerSnapshotRecoveryJournal snapshotRecoveryJournal,
        SnapshotRecoveryLimits limits,
        SnapshotRecoveryOfferContext context,
        ServerSnapshotMaterializationResult materialization)
    {
        if (materialization.Status != ServerSnapshotMaterializationStatus.Materialized)
        {
            return CreateMaterializationNonRecoveredResult(
                context.Request,
                MapMaterializationStatus(materialization.Status),
                materialization.ReasonCode,
                limits);
        }

        var clientState = materialization.ClientState;
        if (clientState is null
            || clientState.SchemaVersion <= 0
            || !string.Equals(clientState.ContractId, context.Request.ClientStateContractId, StringComparison.Ordinal)
            || clientState.SchemaVersion != context.Request.ClientStateSchemaVersion)
        {
            return CreateNonRecoveredResult(RemoteSnapshotRecoveryStatus.ValidationRejected, SnapshotValidationRejectedReason);
        }

        if (clientState.PayloadLength > limits.MaximumPayloadBytes)
        {
            return CreateNonRecoveredResult(RemoteSnapshotRecoveryStatus.CapacityExceeded, SnapshotCapacityExceededReason);
        }

        var result = new RemoteSnapshotRecoveryResult
        {
            Status = RemoteSnapshotRecoveryStatus.Recovered,
            Checkpoint = new()
            {
                StreamId = context.Request.StreamId,
                SubscriptionId = context.Request.SubscriptionId,
                FrontierCursor = context.FrontierCursor,
                ServerVersion = context.CapturedServerVersion,
                SnapshotFormatVersion = context.Request.SnapshotFormatVersion,
                ClientState = clientState,
                ObservedAtUtc = context.ObservedAtUtc,
            },
            OperationDispositions = context.OperationDispositions,
        };

        if (!TryValidateRecoveredResult(context.Request, result, result.Checkpoint, limits, out var validationResult))
        {
            return validationResult;
        }

        var offer = snapshotRecoveryJournal.TryOfferSnapshot(new()
        {
            StreamKey = context.StreamKey,
            Subscription = context.Subscription,
            View = context.View,
            RecoveryRequest = context.Request,
            RecoveryResult = result,
            Limits = limits,
        });

        return MapOfferResult(offer, result);
    }

    /// <summary>Groups captured inputs needed to validate and offer a materialized snapshot.</summary>
    private sealed record SnapshotRecoveryOfferContext
    {
        /// <summary>Gets the recovery request.</summary>
        internal required RemoteSnapshotRecoveryRequest Request { get; init; }

        /// <summary>Gets the trusted stream key.</summary>
        internal required ServerStreamKey StreamKey { get; init; }

        /// <summary>Gets the trusted subscription identity.</summary>
        internal required ServerSubscriptionIdentity Subscription { get; init; }

        /// <summary>Gets the captured snapshot recovery view.</summary>
        internal required ServerSnapshotRecoveryView View { get; init; }

        /// <summary>Gets the captured server version.</summary>
        internal required string CapturedServerVersion { get; init; }

        /// <summary>Gets the captured frontier cursor.</summary>
        internal required string FrontierCursor { get; init; }

        /// <summary>Gets the captured observation time.</summary>
        internal required DateTimeOffset ObservedAtUtc { get; init; }

        /// <summary>Gets the public operation dispositions.</summary>
        internal required IReadOnlyList<SnapshotOperationDisposition> OperationDispositions { get; init; }
    }
}
