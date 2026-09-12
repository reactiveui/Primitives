// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Negotiates only capabilities supported by every participant.</summary>
internal static class CapabilityNegotiator
{
    /// <summary>The implemented wire protocol major version.</summary>
    private const int ProtocolMajor = 1;

    /// <summary>The implemented wire protocol minor version.</summary>
    private const int ProtocolMinor = 0;

    /// <summary>The features understood by this runtime.</summary>
    private const RemoteTransportCapabilities KnownFeatures = RemoteTransportCapabilities.BatchPush
        | RemoteTransportCapabilities.CursorResume | RemoteTransportCapabilities.ReceiveAcknowledgements
        | RemoteTransportCapabilities.ServerIdempotency | RemoteTransportCapabilities.AtomicApplyAndAcknowledge
        | RemoteTransportCapabilities.StreamingReceive;

    /// <summary>The remote capabilities necessary for exactly-once effect.</summary>
    private const RemoteTransportCapabilities ExactlyOnceFeatures = RemoteTransportCapabilities.ServerIdempotency
        | RemoteTransportCapabilities.AtomicApplyAndAcknowledge | RemoteTransportCapabilities.ReceiveAcknowledgements;

    /// <summary>The store capabilities necessary for exactly-once effect.</summary>
    private const LocalStoreCapabilities ExactlyOnceStoreFeatures = LocalStoreCapabilities.AtomicLocalCommit
        | LocalStoreCapabilities.AtomicRemoteApply | LocalStoreCapabilities.DurableInbox | LocalStoreCapabilities.DurableLocalCommit;

    /// <summary>Negotiates one stream before any synchronization work starts.</summary>
    /// <param name="request">The stream requirements and capability offers.</param>
    /// <returns>The negotiated capabilities.</returns>
    /// <exception cref="ArgumentNullException">A required configuration reference is missing.</exception>
    /// <exception cref="InvalidOperationException">The offered capabilities cannot satisfy the requirements.</exception>
    internal static NegotiatedCapabilities Negotiate(CapabilityNegotiationRequest request)
    {
        ValidateRequest(request);
        var offer = request.PeerOffer;
        var features = offer.Features & request.TransportCapabilities & KnownFeatures;
        ValidateStoreRequirements(request);
        ValidateRemoteRequirements(request, features);
        ValidateInboxRetention(request);

        return offer with
        {
            ProtocolVersion = new(ProtocolMajor, Math.Min(ProtocolMinor, offer.ProtocolVersion.Minor)),
            Features = features,
            MaximumBatchOperations = (features & RemoteTransportCapabilities.BatchPush) != 0
                ? Math.Min(request.Options.Batching.MaximumOperations, offer.MaximumBatchOperations)
                : 1,
            MaximumBatchBytes = Math.Min(request.Options.Batching.MaximumBytes, offer.MaximumBatchBytes),
            EffectiveExactlyOnceWindow = GetExactlyOnceWindow(request),
        };
    }

    /// <summary>Validates required references, configuration, and peer limits.</summary>
    /// <param name="request">The negotiation request.</param>
    /// <exception cref="InvalidOperationException">The offer has an incompatible version or invalid limits.</exception>
    private static void ValidateRequest(CapabilityNegotiationRequest request)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        ArgumentExceptionHelper.ThrowIfNull(request.Options);
        ArgumentExceptionHelper.ThrowIfNull(request.Policy);
        ArgumentExceptionHelper.ThrowIfNull(request.PeerOffer);
        ArgumentExceptionHelper.ThrowIfNull(request.PeerOffer.ProtocolVersion);
        request.Options.Validate();
        request.Policy.Validate(request.Options.MinimumPriority, request.Options.MaximumPriority);

        var offer = request.PeerOffer;
        if (offer.ProtocolVersion.Major != ProtocolMajor)
        {
            throw new InvalidOperationException("The peer has no compatible protocol major version.");
        }

        if (offer.MaximumBatchOperations <= 0 || offer.MaximumBatchBytes <= 0)
        {
            throw new InvalidOperationException("The peer's batch count and byte limits must be positive.");
        }

        if (offer.ServerIdempotencyRetention is not { } retention)
        {
            return;
        }

        ValidateRetention(retention);
    }

    /// <summary>Checks store capabilities before accepting stream requirements.</summary>
    /// <param name="request">The negotiation request.</param>
    private static void ValidateStoreRequirements(CapabilityNegotiationRequest request)
    {
        var required = LocalStoreCapabilities.None;
        if (request.Policy.Durability == OperationDurability.Durable)
        {
            required |= LocalStoreCapabilities.AtomicLocalCommit | LocalStoreCapabilities.DurableLocalCommit;
        }

        if (request.Policy.DeliveryGuarantee == DeliveryGuarantee.ExactlyOnce)
        {
            required |= ExactlyOnceStoreFeatures;
        }

        if (request.RequiresCursorResume)
        {
            required |= LocalStoreCapabilities.AtomicRemoteApply;
        }

        if (request.RequiresConcurrentDrain)
        {
            required |= LocalStoreCapabilities.LeasedOutbox;
        }

        if (request.RequiresMultipleProcesses)
        {
            required |= LocalStoreCapabilities.MultiProcessCoordination;
        }

        if (request.Options.Security.RequireAuthenticatedEncryptionAtRest)
        {
            required |= LocalStoreCapabilities.AuthenticatedEncryptionAtRest;
        }

        RequireStoreFeatures(request.StoreCapabilities, required);
    }

    /// <summary>Checks the intersection of peer and transport capabilities.</summary>
    /// <param name="request">The negotiation request.</param>
    /// <param name="features">The supported feature intersection.</param>
    /// <exception cref="InvalidOperationException">A required remote capability is absent.</exception>
    private static void ValidateRemoteRequirements(CapabilityNegotiationRequest request, RemoteTransportCapabilities features)
    {
        var required = RemoteTransportCapabilities.None;
        if (request.Policy.DeliveryGuarantee != DeliveryGuarantee.AtMostOnce)
        {
            required |= RemoteTransportCapabilities.ServerIdempotency;
        }

        if (request.Policy.DeliveryGuarantee == DeliveryGuarantee.ExactlyOnce)
        {
            required |= ExactlyOnceFeatures;
        }

        if (request.RequiresCursorResume)
        {
            required |= RemoteTransportCapabilities.CursorResume;
        }

        if ((features & required) == required)
        {
            return;
        }

        throw new InvalidOperationException($"The peer and transport must both support: {required & ~features}.");
    }

    /// <summary>Checks the peer's minimum durable inbox retention requirement.</summary>
    /// <param name="request">The negotiation request.</param>
    /// <exception cref="InvalidOperationException">The client's inbox cannot satisfy the peer requirement.</exception>
    private static void ValidateInboxRetention(CapabilityNegotiationRequest request)
    {
        if (request.PeerOffer.ClientInboxRetentionRequired is not { } required)
        {
            return;
        }

        ValidateRetention(required);
        RequireStoreFeatures(request.StoreCapabilities, LocalStoreCapabilities.DurableInbox);
        if (request.Options.Retention.InboxDeduplicationRetention >= required)
        {
            return;
        }

        throw new InvalidOperationException("The client's inbox retention is shorter than the peer requires.");
    }

    /// <summary>Computes the bounded effect guarantee without trusting a peer-supplied effective window.</summary>
    /// <param name="request">The negotiation request.</param>
    /// <returns>The effective exactly-once window, or null for another delivery guarantee.</returns>
    /// <exception cref="InvalidOperationException">Exactly-once delivery has no server retention commitment.</exception>
    private static TimeSpan? GetExactlyOnceWindow(CapabilityNegotiationRequest request)
    {
        if (request.Policy.DeliveryGuarantee != DeliveryGuarantee.ExactlyOnce)
        {
            return null;
        }

        if (request.PeerOffer.ServerIdempotencyRetention is not { } serverRetention)
        {
            throw new InvalidOperationException("Exactly-once effects require an explicit server idempotency retention window.");
        }

        var clientRetention = request.Options.Retention.InboxDeduplicationRetention;
        return serverRetention < clientRetention ? serverRetention : clientRetention;
    }

    /// <summary>Requires every requested store capability.</summary>
    /// <param name="available">The advertised store capabilities.</param>
    /// <param name="required">The capabilities required for this stream.</param>
    /// <exception cref="InvalidOperationException">The store cannot satisfy the stream.</exception>
    private static void RequireStoreFeatures(LocalStoreCapabilities available, LocalStoreCapabilities required)
    {
        if ((available & required) == required)
        {
            return;
        }

        throw new InvalidOperationException($"The store must support: {required & ~available}.");
    }

    /// <summary>Rejects invalid or unbounded retention offers.</summary>
    /// <param name="retention">The offered retention interval.</param>
    /// <exception cref="InvalidOperationException">The interval is not positive and finite.</exception>
    private static void ValidateRetention(TimeSpan retention)
    {
        if (retention > TimeSpan.Zero && retention != TimeSpan.MaxValue)
        {
            return;
        }

        throw new InvalidOperationException("Peer retention intervals must be positive and finite.");
    }
}
