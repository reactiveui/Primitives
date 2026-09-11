// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a remote transport connection request.</summary>
[System.Diagnostics.DebuggerDisplay("{Client,nq}")]
public sealed record TransportConnectRequest
{
    /// <summary>Initializes a new instance of the <see cref="TransportConnectRequest"/> class.</summary>
    /// <param name="supportedProtocolVersions">The protocol version range supported by the client.</param>
    /// <param name="client">The client identity.</param>
    /// <param name="requiredGuarantees">The delivery guarantees required by the client.</param>
    public TransportConnectRequest(
        VersionRange supportedProtocolVersions,
        ClientIdentity client,
        IReadOnlyCollection<DeliveryGuarantee> requiredGuarantees)
    {
        SupportedProtocolVersions = supportedProtocolVersions;
        Client = client;
        RequiredGuarantees = CollectionCopy.Collection(requiredGuarantees);
    }

    /// <summary>Gets the protocol version range supported by the client.</summary>
    public VersionRange SupportedProtocolVersions { get; }

    /// <summary>Gets the client identity.</summary>
    public ClientIdentity Client { get; }

    /// <summary>Gets the delivery guarantees required by the client.</summary>
    public IReadOnlyCollection<DeliveryGuarantee> RequiredGuarantees { get; }
}
