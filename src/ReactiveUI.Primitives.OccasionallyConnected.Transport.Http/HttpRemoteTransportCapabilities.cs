// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Validates HTTP transport capability negotiation.</summary>
internal static class HttpRemoteTransportCapabilities
{
    /// <summary>Validates the negotiated capabilities against the request and adapter support.</summary>
    /// <param name="request">The original connect request.</param>
    /// <param name="capabilities">The negotiated capabilities.</param>
    /// <param name="adapterCapabilities">The adapter capabilities.</param>
    /// <exception cref="HttpRemoteTransportException">The negotiation response is incompatible or malformed.</exception>
    internal static void ValidateNegotiation(
        TransportConnectRequest request,
        NegotiatedCapabilities capabilities,
        RemoteTransportCapabilities adapterCapabilities)
    {
        if (capabilities.ProtocolVersion < request.SupportedProtocolVersions.Minimum
            || capabilities.ProtocolVersion > request.SupportedProtocolVersions.Maximum)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.SchemaIncompatible);
        }

        if ((capabilities.Features & ~adapterCapabilities) != RemoteTransportCapabilities.None)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        if (capabilities.MaximumBatchOperations <= 0 || capabilities.MaximumBatchBytes <= 0)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation);
        }

        foreach (var guarantee in request.RequiredGuarantees)
        {
            ValidateRequiredGuarantee(guarantee, capabilities.Features);
        }
    }

    /// <summary>Validates that a requested guarantee is supported by negotiated features.</summary>
    /// <param name="guarantee">The delivery guarantee.</param>
    /// <param name="features">The negotiated features.</param>
    /// <exception cref="HttpRemoteTransportException">The negotiated features do not satisfy the requested guarantee.</exception>
    private static void ValidateRequiredGuarantee(DeliveryGuarantee guarantee, RemoteTransportCapabilities features)
    {
        if (guarantee is DeliveryGuarantee.AtMostOnce)
        {
            return;
        }

        if (guarantee is DeliveryGuarantee.AtLeastOnce
            && Has(features, RemoteTransportCapabilities.BatchPush | RemoteTransportCapabilities.ServerIdempotency))
        {
            return;
        }

        if (guarantee is DeliveryGuarantee.ExactlyOnce
            && Has(features, RemoteTransportCapabilities.BatchPush | RemoteTransportCapabilities.ServerIdempotency
                | RemoteTransportCapabilities.AtomicApplyAndAcknowledge | RemoteTransportCapabilities.ReceiveAcknowledgements))
        {
            return;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.SchemaIncompatible);
    }

    /// <summary>Determines whether all required flags are present.</summary>
    /// <param name="features">The available features.</param>
    /// <param name="required">The required features.</param>
    /// <returns>Whether all required flags are present.</returns>
    private static bool Has(RemoteTransportCapabilities features, RemoteTransportCapabilities required) => (features & required) == required;
}
