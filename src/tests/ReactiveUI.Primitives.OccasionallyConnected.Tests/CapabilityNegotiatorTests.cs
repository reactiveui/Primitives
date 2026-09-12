// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests capability negotiation before stream startup.</summary>
public sealed class CapabilityNegotiatorTests
{
    /// <summary>The count limit offered by the peer.</summary>
    private const int PeerCount = 20;

    /// <summary>The encoded byte limit offered by the peer.</summary>
    private const long PeerBytes = 4096;

    /// <summary>The server retention in days.</summary>
    private const int ServerRetentionDays = 14;

    /// <summary>All currently defined remote capabilities.</summary>
    private const RemoteTransportCapabilities RemoteFeatures = RemoteTransportCapabilities.BatchPush
        | RemoteTransportCapabilities.CursorResume | RemoteTransportCapabilities.ReceiveAcknowledgements
        | RemoteTransportCapabilities.ServerIdempotency | RemoteTransportCapabilities.AtomicApplyAndAcknowledge
        | RemoteTransportCapabilities.StreamingReceive;

    /// <summary>All currently defined local capabilities.</summary>
    private const LocalStoreCapabilities StoreFeatures = LocalStoreCapabilities.AtomicLocalCommit
        | LocalStoreCapabilities.AtomicRemoteApply | LocalStoreCapabilities.DurableInbox | LocalStoreCapabilities.LeasedOutbox
        | LocalStoreCapabilities.MultiProcessCoordination | LocalStoreCapabilities.AuthenticatedEncryptionAtRest;

    /// <summary>Verifies the exactly-once window is bounded by actual client retention.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ExactlyOnceUsesShorterClientWindow()
    {
        var request = CreateRequest();
        var actual = CapabilityNegotiator.Negotiate(request);
        await Assert.That(actual.EffectiveExactlyOnceWindow).IsEqualTo(request.Options.Retention.InboxDeduplicationRetention);
        await Assert.That(actual.MaximumBatchOperations).IsEqualTo(PeerCount);
        await Assert.That(actual.MaximumBatchBytes).IsEqualTo(PeerBytes);
    }

    /// <summary>Verifies each local exactly-once prerequisite is mandatory.</summary>
    /// <param name="missing">The missing capability.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments(LocalStoreCapabilities.AtomicLocalCommit)]
    [Arguments(LocalStoreCapabilities.AtomicRemoteApply)]
    [Arguments(LocalStoreCapabilities.DurableInbox)]
    public async Task ExactlyOnceRejectsMissingStoreCapability(LocalStoreCapabilities missing)
    {
        var request = CreateRequest() with { StoreCapabilities = StoreFeatures & ~missing };
        await Assert.That(() => CapabilityNegotiator.Negotiate(request)).Throws<InvalidOperationException>();
    }

    /// <summary>Verifies each remote exactly-once prerequisite is supported by both adapter and peer.</summary>
    /// <param name="missing">The missing capability.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments(RemoteTransportCapabilities.ServerIdempotency)]
    [Arguments(RemoteTransportCapabilities.AtomicApplyAndAcknowledge)]
    [Arguments(RemoteTransportCapabilities.ReceiveAcknowledgements)]
    public async Task ExactlyOnceRejectsMissingTransportCapability(RemoteTransportCapabilities missing)
    {
        var request = CreateRequest() with { TransportCapabilities = RemoteFeatures & ~missing };
        await Assert.That(() => CapabilityNegotiator.Negotiate(request)).Throws<InvalidOperationException>();
        request = CreateRequest() with { PeerOffer = CreateRequest().PeerOffer with { Features = RemoteFeatures & ~missing } };
        await Assert.That(() => CapabilityNegotiator.Negotiate(request)).Throws<InvalidOperationException>();
    }

    /// <summary>Verifies an unbatched transport gets a single-operation limit.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task MissingBatchPushDisablesBatching()
    {
        var request = CreateRequest() with { TransportCapabilities = RemoteFeatures & ~RemoteTransportCapabilities.BatchPush };
        var actual = CapabilityNegotiator.Negotiate(request);
        await Assert.That(actual.MaximumBatchOperations).IsEqualTo(1);
        await Assert.That(actual.Features).IsEqualTo(request.TransportCapabilities);
    }

    /// <summary>Verifies requirements cannot be silently weakened.</summary>
    /// <param name="scenario">The unsupported startup configuration.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments("durable")]
    [Arguments("resume-store")]
    [Arguments("resume-peer")]
    [Arguments("leases")]
    [Arguments("processes")]
    [Arguments("encryption")]
    [Arguments("idempotency")]
    public async Task RejectsUnsupportedStartupRequirement(string scenario)
    {
        var request = CreateRequest() with { Policy = OperationPolicy.Default };
        request = scenario switch
        {
            "durable" => request with { StoreCapabilities = StoreFeatures & ~LocalStoreCapabilities.AtomicLocalCommit },
            "resume-store" => request with { RequiresCursorResume = true, StoreCapabilities = StoreFeatures & ~LocalStoreCapabilities.AtomicRemoteApply },
            "resume-peer" => request with { RequiresCursorResume = true, TransportCapabilities = RemoteFeatures & ~RemoteTransportCapabilities.CursorResume },
            "leases" => request with { RequiresConcurrentDrain = true, StoreCapabilities = StoreFeatures & ~LocalStoreCapabilities.LeasedOutbox },
            "processes" => request with { RequiresMultipleProcesses = true, StoreCapabilities = StoreFeatures & ~LocalStoreCapabilities.MultiProcessCoordination },
            "encryption" => request with
            {
                StoreCapabilities = StoreFeatures & ~LocalStoreCapabilities.AuthenticatedEncryptionAtRest,
                Options = request.Options with { Security = request.Options.Security with { RequireAuthenticatedEncryptionAtRest = true } },
            },
            _ => request with { TransportCapabilities = RemoteFeatures & ~RemoteTransportCapabilities.ServerIdempotency },
        };

        await Assert.That(() => CapabilityNegotiator.Negotiate(request)).Throws<InvalidOperationException>();
    }

    /// <summary>Verifies all optional requirements succeed when their capabilities are present.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task SupportsExplicitStoreAndCursorRequirements()
    {
        var request = CreateRequest() with
        {
            RequiresCursorResume = true,
            RequiresConcurrentDrain = true,
            RequiresMultipleProcesses = true,
            Options = OccasionallyConnectedOptions.Default with
            {
                Security = new() { RequireAuthenticatedEncryptionAtRest = true },
            },
        };
        var actual = CapabilityNegotiator.Negotiate(request);
        await Assert.That(actual.Features).IsEqualTo(RemoteFeatures);
    }

    /// <summary>Verifies volatile at-most-once operation does not acquire unsupported guarantees.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task VolatileAtMostOnceNeedsNoDurableCapabilities()
    {
        var request = CreateRequest() with
        {
            Policy = OperationPolicy.Default with { Durability = OperationDurability.Volatile, DeliveryGuarantee = DeliveryGuarantee.AtMostOnce },
            StoreCapabilities = LocalStoreCapabilities.None,
            TransportCapabilities = RemoteTransportCapabilities.None,
            PeerOffer = CreateRequest().PeerOffer with { ClientInboxRetentionRequired = null },
        };
        var actual = CapabilityNegotiator.Negotiate(request);
        await Assert.That(actual.Features).IsEqualTo(RemoteTransportCapabilities.None);
        await Assert.That(actual.EffectiveExactlyOnceWindow).IsNull();
        await Assert.That(actual.MaximumBatchOperations).IsEqualTo(1);
    }

    /// <summary>Verifies the shortest server window limits exactly-once effects.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ServerWindowLimitsTheGuarantee()
    {
        var request = CreateRequest();
        request = request with { PeerOffer = request.PeerOffer with { ServerIdempotencyRetention = TimeSpan.FromDays(1) } };
        var actual = CapabilityNegotiator.Negotiate(request);
        await Assert.That(actual.EffectiveExactlyOnceWindow).IsEqualTo(TimeSpan.FromDays(1));
    }

    /// <summary>Verifies a peer-required inbox window must have actual durable deduplication storage.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task RequiredInboxRetentionNeedsDurableInbox()
    {
        var request = CreateRequest() with
        {
            Policy = OperationPolicy.Default,
            StoreCapabilities = StoreFeatures & ~LocalStoreCapabilities.DurableInbox,
        };
        await Assert.That(() => CapabilityNegotiator.Negotiate(request)).Throws<InvalidOperationException>();
    }

    /// <summary>Verifies invalid and unavailable server retention cannot create a guarantee.</summary>
    /// <param name="retentionTicks">The peer retention, or null when omitted.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments(null)]
    [Arguments(0L)]
    [Arguments(-1L)]
    [Arguments(long.MaxValue)]
    public async Task ExactlyOnceRejectsInvalidServerRetention(long? retentionTicks)
    {
        var request = CreateRequest();
        request = request with
        {
            PeerOffer = request.PeerOffer with
            {
                ServerIdempotencyRetention = retentionTicks is { } ticks ? TimeSpan.FromTicks(ticks) : null,
            },
        };
        await Assert.That(() => CapabilityNegotiator.Negotiate(request)).Throws<InvalidOperationException>();
    }

    /// <summary>Verifies peer inbox requirements are validated independently of the publish guarantee.</summary>
    /// <param name="retentionTicks">The peer's required inbox duration.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments(0L)]
    [Arguments(-1L)]
    [Arguments(long.MaxValue)]
    [Arguments(long.MaxValue - 1)]
    public async Task RejectsInvalidOrUnsatisfiedInboxRequirement(long retentionTicks)
    {
        var request = CreateRequest() with { Policy = OperationPolicy.Default };
        request = request with { PeerOffer = request.PeerOffer with { ClientInboxRetentionRequired = TimeSpan.FromTicks(retentionTicks) } };
        await Assert.That(() => CapabilityNegotiator.Negotiate(request)).Throws<InvalidOperationException>();
    }

    /// <summary>Verifies optional retention absence never preserves an untrusted guarantee claim.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ClearsPeerGuaranteeClaimForAtLeastOnce()
    {
        var request = CreateRequest() with { Policy = OperationPolicy.Default };
        request = request with
        {
            PeerOffer = request.PeerOffer with
            {
                ServerIdempotencyRetention = null,
                ClientInboxRetentionRequired = null,
                EffectiveExactlyOnceWindow = TimeSpan.MaxValue,
            },
        };
        var actual = CapabilityNegotiator.Negotiate(request);
        await Assert.That(actual.EffectiveExactlyOnceWindow).IsNull();
        await Assert.That(actual.ServerIdempotencyRetention).IsNull();
        await Assert.That(actual.ClientInboxRetentionRequired).IsNull();
    }

    /// <summary>Verifies malformed offers cannot bypass size or protocol limits.</summary>
    /// <param name="scenario">The malformed offer field.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments("count")]
    [Arguments("bytes")]
    [Arguments("major")]
    public async Task RejectsMalformedOffer(string scenario)
    {
        var request = CreateRequest();
        var offer = request.PeerOffer;
        offer = scenario switch
        {
            "count" => offer with { MaximumBatchOperations = 0 },
            "bytes" => offer with { MaximumBatchBytes = 0 },
            _ => offer with { ProtocolVersion = new(2, 0) },
        };
        request = request with { PeerOffer = offer };
        await Assert.That(() => CapabilityNegotiator.Negotiate(request)).Throws<InvalidOperationException>();
    }

    /// <summary>Verifies local limits and the supported minor protocol constrain a newer peer.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task IntersectsLimitsAndIgnoresUnknownOptionalFeatures()
    {
        var request = CreateRequest();
        request = request with
        {
            Options = request.Options with { Batching = new() { MaximumOperations = 1, MaximumBytes = 1 } },
            PeerOffer = request.PeerOffer with { ProtocolVersion = new(1, 1), Features = (RemoteTransportCapabilities)int.MaxValue },
            TransportCapabilities = (RemoteTransportCapabilities)int.MaxValue,
        };
        var actual = CapabilityNegotiator.Negotiate(request);
        await Assert.That(actual.ProtocolVersion).IsEqualTo(new(1, 0));
        await Assert.That(actual.MaximumBatchOperations).IsEqualTo(1);
        await Assert.That(actual.MaximumBatchBytes).IsEqualTo(1);
        await Assert.That(actual.Features).IsEqualTo(RemoteFeatures);
        await Assert.That(request.PeerOffer.ProtocolVersion).IsEqualTo(new(1, 1));
    }

    /// <summary>Verifies required references are guarded before negotiation.</summary>
    /// <param name="scenario">The missing reference.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments("request")]
    [Arguments("options")]
    [Arguments("policy")]
    [Arguments("offer")]
    [Arguments("version")]
    public async Task RejectsMissingRequiredReference(string scenario)
    {
        var request = CreateRequest();
        request = scenario switch
        {
            "request" => null!,
            "options" => request with { Options = null! },
            "policy" => request with { Policy = null! },
            "offer" => request with { PeerOffer = null! },
            _ => request with { PeerOffer = request.PeerOffer with { ProtocolVersion = null! } },
        };
        await Assert.That(() => CapabilityNegotiator.Negotiate(request)).Throws<ArgumentNullException>();
    }

    /// <summary>Creates a supported, authenticated exactly-once offer.</summary>
    /// <returns>The negotiation request.</returns>
    private static CapabilityNegotiationRequest CreateRequest() => new()
    {
        Options = OccasionallyConnectedOptions.Default,
        Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce },
        StoreCapabilities = StoreFeatures,
        TransportCapabilities = RemoteFeatures,
        PeerOffer = new(new Version(1, 0), RemoteFeatures, PeerCount, PeerBytes, TimeSpan.FromDays(ServerRetentionDays), TimeSpan.FromDays(1)),
    };
}
