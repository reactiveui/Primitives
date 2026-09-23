// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Upload pump implementation for <see cref="SyncEngine"/>.</summary>
internal sealed partial class SyncEngine
{
    /// <summary>Runs one acquired upload attempt and releases scheduler ownership when complete.</summary>
    /// <param name="acquisition">The acquired stream head.</param>
    /// <param name="context">The acquired upload attempt context.</param>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The upload attempt task.</returns>
    private async Task RunUploadAttemptAsync(
        FairStreamAcquisition acquisition,
        UploadAttemptContext context,
        CancellationToken cancellationToken)
    {
        UploadReschedule? reschedule = null;
        try
        {
            reschedule = await RunUploadAttemptCoreAsync(
                acquisition,
                context,
                cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            PublishUploadAttemptFault(acquisition.StreamId, exception);
        }
        finally
        {
            try
            {
                CompleteUploadAcquisitionSafely(acquisition, reschedule);
            }
            finally
            {
                await ReleaseSharedSessionLeaseAsync(context.SessionLease).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Runs the leased upload attempt body.</summary>
    /// <param name="acquisition">The acquired stream head.</param>
    /// <param name="context">The acquired upload attempt context.</param>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The optional reschedule request.</returns>
    private async Task<UploadReschedule?> RunUploadAttemptCoreAsync(
        FairStreamAcquisition acquisition,
        UploadAttemptContext context,
        CancellationToken cancellationToken)
    {
        var lease = await LeaseOneBatchAsync(
                acquisition.StreamId,
                context.MaximumOperations,
                context.MaximumBytes,
                context.AttemptOptions.LeaseRenewalDuration,
                cancellationToken)
            .ConfigureAwait(false);
        return lease is null
            ? null
            : await RunLeasedUploadAttemptAsync(lease, context, cancellationToken)
                .ConfigureAwait(false);
    }

    /// <summary>Runs a leased upload attempt after outbox ownership has been acquired.</summary>
    /// <param name="lease">The active lease.</param>
    /// <param name="context">The acquired upload attempt context.</param>
    /// <param name="cancellationToken">The engine-owned stop token.</param>
    /// <returns>The optional reschedule request.</returns>
    private async Task<UploadReschedule?> RunLeasedUploadAttemptAsync(
        LeasedOperationBatch lease,
        UploadAttemptContext context,
        CancellationToken cancellationToken)
    {
        var head = context.Head;
        var streamId = lease.Operations[0].StreamId;
        var leasedCapabilities = await TryNegotiateLeasedUploadCapabilitiesAsync(lease, context, streamId).ConfigureAwait(false);
        if (leasedCapabilities is null)
        {
            return null;
        }

        var maximumOperations = Math.Min(context.MaximumOperations, leasedCapabilities.MaximumBatchOperations);
        var maximumBytes = Math.Min(context.MaximumBytes, leasedCapabilities.MaximumBatchBytes);
        var attemptOptions = context.AttemptOptions with
        {
            MaximumOperations = maximumOperations,
            MaximumEncodedSizeBytes = maximumBytes,
        };
        var requiresDurableRetryAnchor = leasedCapabilities.EffectiveExactlyOnceWindow is not null;
        var retryOptions = CreateUploadRetryOptions(leasedCapabilities);

        if (head.DeadLetterOversizedHead)
        {
            await DeadLetterOversizedLeaseHeadAsync(lease).ConfigureAwait(false);
            return CreateImmediateReschedule(head, maximumOperations: 0);
        }

        var planned = await PlanLeasedUploadAsync(lease, head, maximumOperations, maximumBytes).ConfigureAwait(false);
        if (!planned.Handled
            && !await TryPrepareUploadRetryAnchorsAsync(lease, streamId, leasedCapabilities, retryOptions).ConfigureAwait(false))
        {
            return null;
        }

        var execution = new PreparedUploadExecution(
            context.Preparer,
            attemptOptions,
            retryOptions,
            requiresDurableRetryAnchor,
            context.SessionLease.Generation);
        return planned.Handled ? planned.Reschedule : await ExecutePreparedUploadAsync(lease, execution, head, streamId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Creates retry options bounded by the normalized exactly-once effect window.</summary>
    /// <param name="capabilities">The normalized leased upload capabilities.</param>
    /// <returns>The retry options for the leased upload attempt.</returns>
    private RetryOptions CreateUploadRetryOptions(NegotiatedCapabilities capabilities)
    {
        var retryOptions = _options.Options.Retry;
        return capabilities.EffectiveExactlyOnceWindow is { } effectWindow && effectWindow < retryOptions.MaximumRetryAge
            ? retryOptions with { MaximumRetryAge = effectWindow }
            : retryOptions;
    }

    /// <summary>Persists and validates retry anchors before remote upload effects can occur.</summary>
    /// <param name="lease">The active lease.</param>
    /// <param name="streamId">The leased stream identity.</param>
    /// <param name="capabilities">The normalized leased upload capabilities.</param>
    /// <param name="retryOptions">The retry options bounded by leased capabilities.</param>
    /// <returns><see langword="true"/> when the upload may proceed.</returns>
    private async Task<bool> TryPrepareUploadRetryAnchorsAsync(
        LeasedOperationBatch lease,
        StreamId streamId,
        NegotiatedCapabilities capabilities,
        RetryOptions retryOptions) =>
        capabilities.EffectiveExactlyOnceWindow is null
        || await TryPrepareExactlyOnceUploadRetryAnchorsAsync(lease, streamId, retryOptions).ConfigureAwait(false);

    /// <summary>Persists and validates exactly-once retry anchors for every operation in a leased batch.</summary>
    /// <param name="lease">The active lease.</param>
    /// <param name="streamId">The leased stream identity.</param>
    /// <param name="retryOptions">The retry options bounded by leased capabilities.</param>
    /// <returns><see langword="true"/> when the upload may proceed.</returns>
    private async Task<bool> TryPrepareExactlyOnceUploadRetryAnchorsAsync(
        LeasedOperationBatch lease,
        StreamId streamId,
        RetryOptions retryOptions)
    {
        for (var i = 0; i < lease.Operations.Count; i++)
        {
            var operation = lease.Operations[i];
            if (!await TryPrepareUploadRetryAnchorAsync(lease.LeaseId, streamId, operation, retryOptions).ConfigureAwait(false))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Persists and validates one exactly-once retry anchor before remote effects can occur.</summary>
    /// <param name="leaseId">The active lease identifier.</param>
    /// <param name="streamId">The leased stream identity.</param>
    /// <param name="operation">The leased operation.</param>
    /// <param name="retryOptions">The retry options bounded by leased capabilities.</param>
    /// <returns><see langword="true"/> when the operation may proceed.</returns>
    private async Task<bool> TryPrepareUploadRetryAnchorAsync(
        Guid leaseId,
        StreamId streamId,
        SyncOperation operation,
        RetryOptions retryOptions)
    {
        var retryState = await TryGetUploadRetryAnchorAsync(leaseId, streamId, operation.OperationId).ConfigureAwait(false);
        if (retryState.Faulted)
        {
            return false;
        }

        var anchor = retryState.RetryState ?? await TryCreateUploadRetryAnchorAsync(leaseId, streamId, operation).ConfigureAwait(false);
        return anchor is not null && await TryValidateUploadRetryAnchorAgeAsync(leaseId, streamId, anchor, retryOptions).ConfigureAwait(false);
    }

    /// <summary>Reads one durable retry anchor and releases the lease on lookup failure.</summary>
    /// <param name="leaseId">The active lease identifier.</param>
    /// <param name="streamId">The leased stream identity.</param>
    /// <param name="operationId">The leased operation identity.</param>
    /// <returns>The lookup result.</returns>
    private async Task<UploadRetryAnchorLookup> TryGetUploadRetryAnchorAsync(Guid leaseId, StreamId streamId, OperationId operationId)
    {
        try
        {
            var retryState = await _options.Store.GetRetryStateAsync(operationId, CancellationToken.None).ConfigureAwait(false);
            return new(Faulted: false, RetryState: retryState);
        }
        catch (Exception exception)
        {
            await ReleaseFaultedUploadLeaseAsync(leaseId, streamId, exception).ConfigureAwait(false);
            return new(Faulted: true, RetryState: null);
        }
    }

    /// <summary>Creates a new retry anchor for never-attempted exactly-once work.</summary>
    /// <param name="leaseId">The active lease identifier.</param>
    /// <param name="streamId">The leased stream identity.</param>
    /// <param name="operation">The leased operation.</param>
    /// <returns>The created retry anchor, or null when the lease has been faulted.</returns>
    private async Task<RetryState?> TryCreateUploadRetryAnchorAsync(Guid leaseId, StreamId streamId, SyncOperation operation)
    {
        var status = await TryGetUploadRetryAnchorStatusAsync(leaseId, streamId, operation.OperationId).ConfigureAwait(false);
        if (status.Faulted || !CanCreateFreshUploadRetryAnchor(operation, streamId, status.Status))
        {
            await ReleaseMissingUploadRetryAnchorAsync(leaseId, streamId, status.Faulted).ConfigureAwait(false);
            return null;
        }

        var nowUtc = _options.TimeProvider.GetUtcNow();
        var retryState = RetryState.Start(nowUtc);
        return await TrySaveUploadRetryAnchorAsync(leaseId, streamId, operation.OperationId, retryState).ConfigureAwait(false) ? retryState : null;
    }

    /// <summary>Reads durable status used to decide whether a missing retry anchor can be created.</summary>
    /// <param name="leaseId">The active lease identifier.</param>
    /// <param name="streamId">The leased stream identity.</param>
    /// <param name="operationId">The leased operation identity.</param>
    /// <returns>The status lookup result.</returns>
    private async Task<UploadStatusLookup> TryGetUploadRetryAnchorStatusAsync(Guid leaseId, StreamId streamId, OperationId operationId)
    {
        try
        {
            var status = await _options.Store.GetOperationStatusAsync(operationId, CancellationToken.None).ConfigureAwait(false);
            return new(Faulted: false, Status: status);
        }
        catch (Exception exception)
        {
            await ReleaseFaultedUploadLeaseAsync(leaseId, streamId, exception).ConfigureAwait(false);
            return new(Faulted: true, Status: null);
        }
    }

    /// <summary>Releases a lease when an exact upload retry anchor is missing after an attempt.</summary>
    /// <param name="leaseId">The active lease identifier.</param>
    /// <param name="streamId">The leased stream identity.</param>
    /// <param name="alreadyFaulted">Whether the lookup failure already released and faulted the lease.</param>
    /// <returns>The release task.</returns>
    private Task ReleaseMissingUploadRetryAnchorAsync(Guid leaseId, StreamId streamId, bool alreadyFaulted) =>
        alreadyFaulted
            ? Task.CompletedTask
            : ReleaseFaultedUploadLeaseAsync(
                leaseId,
                streamId,
                new InvalidOperationException("The exactly-once upload retry anchor is missing for an attempted operation."));

    /// <summary>Saves a durable retry anchor and releases the lease on persistence failure.</summary>
    /// <param name="leaseId">The active lease identifier.</param>
    /// <param name="streamId">The leased stream identity.</param>
    /// <param name="operationId">The leased operation identity.</param>
    /// <param name="retryState">The retry anchor to save.</param>
    /// <returns><see langword="true"/> when the anchor was saved.</returns>
    private async Task<bool> TrySaveUploadRetryAnchorAsync(Guid leaseId, StreamId streamId, OperationId operationId, RetryState retryState)
    {
        try
        {
            await _options.Store.SaveRetryStateAsync(operationId, retryState, CancellationToken.None).ConfigureAwait(false);
            return true;
        }
        catch (Exception exception)
        {
            await ReleaseFaultedUploadLeaseAsync(leaseId, streamId, exception).ConfigureAwait(false);
            return false;
        }
    }

    /// <summary>Validates that a durable retry anchor is still inside the effective exactly-once window.</summary>
    /// <param name="leaseId">The active lease identifier.</param>
    /// <param name="streamId">The leased stream identity.</param>
    /// <param name="retryState">The retry anchor.</param>
    /// <param name="retryOptions">The retry options bounded by leased capabilities.</param>
    /// <returns><see langword="true"/> when the anchor is still usable.</returns>
    private async Task<bool> TryValidateUploadRetryAnchorAgeAsync(
        Guid leaseId,
        StreamId streamId,
        RetryState retryState,
        RetryOptions retryOptions)
    {
        var nowUtc = _options.TimeProvider.GetUtcNow();
        if (nowUtc - retryState.StartedUtc < retryOptions.MaximumRetryAge)
        {
            return true;
        }

        await ReleaseFaultedUploadLeaseAsync(
                leaseId,
                streamId,
                new InvalidOperationException("The exactly-once upload retry window has expired."))
            .ConfigureAwait(false);
        return false;
    }

    /// <summary>Validates and normalizes every leased operation against the acquired session before remote attempt work starts.</summary>
    /// <param name="lease">The active lease.</param>
    /// <param name="context">The upload attempt context.</param>
    /// <param name="streamId">The leased stream identity.</param>
    /// <returns>The normalized leased capabilities, or <see langword="null"/> when the lease is incompatible.</returns>
    private async Task<NegotiatedCapabilities?> TryNegotiateLeasedUploadCapabilitiesAsync(
        LeasedOperationBatch lease,
        UploadAttemptContext context,
        StreamId streamId)
    {
        NegotiatedCapabilities? leasedCapabilities = null;
        for (var index = 0; index < lease.Operations.Count; index++)
        {
            try
            {
                var operationCapabilities = CapabilityNegotiator.Negotiate(CreateUploadCapabilityRequest(
                    lease.Operations[index].Policy,
                    context.NegotiatedCapabilities));
                leasedCapabilities = leasedCapabilities is null
                    ? operationCapabilities
                    : CombineLeasedUploadCapabilities(leasedCapabilities, operationCapabilities);
            }
            catch (Exception exception) when (IsCapabilityValidationException(exception))
            {
                await ReleaseIncompatibleUploadLeaseAsync(lease.LeaseId, streamId, exception).ConfigureAwait(false);
                return null;
            }
        }

        return leasedCapabilities;
    }

    /// <summary>Creates the shared capability negotiation request for one leased upload policy.</summary>
    /// <param name="policy">The leased operation policy.</param>
    /// <param name="peerOffer">The capabilities negotiated by the acquired session.</param>
    /// <returns>The negotiation request.</returns>
    private CapabilityNegotiationRequest CreateUploadCapabilityRequest(OperationPolicy policy, NegotiatedCapabilities peerOffer) =>
        new()
        {
            Options = _options.Options,
            Policy = policy,
            StoreCapabilities = _options.Store.Capabilities,
            TransportCapabilities = _options.Transport.Capabilities,
            PeerOffer = peerOffer,
            RequiresConcurrentDrain = _options.Options.MaxConcurrentStreams > 1,
        };

    /// <summary>Releases an incompatible upload lease and publishes the validation fault without retrying.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="streamId">The leased stream identity.</param>
    /// <param name="exception">The capability validation failure.</param>
    /// <returns>The release task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Task ReleaseIncompatibleUploadLeaseAsync(Guid leaseId, StreamId streamId, Exception exception) =>
        ReleaseFaultedUploadLeaseAsync(leaseId, streamId, exception);

    /// <summary>Releases a failed upload lease and publishes the upload fault without retrying.</summary>
    /// <param name="leaseId">The lease identifier.</param>
    /// <param name="streamId">The leased stream identity.</param>
    /// <param name="exception">The upload failure.</param>
    /// <returns>The release task.</returns>
    private async Task ReleaseFaultedUploadLeaseAsync(Guid leaseId, StreamId streamId, Exception exception)
    {
        try
        {
            await _options.Store.ReleaseLeaseAsync(leaseId, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception releaseException)
        {
            PublishFault("OC.Engine.UploadLeaseRelease", "A faulted upload lease release failed.", streamId, releaseException);
        }

        PublishUploadAttemptFault(streamId, exception);
    }
}
