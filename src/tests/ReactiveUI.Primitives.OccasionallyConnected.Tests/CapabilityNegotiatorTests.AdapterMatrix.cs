// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.IO;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Startup negotiation against the capability sets advertised by the shipped adapters.</summary>
public sealed partial class CapabilityNegotiatorTests
{
    /// <summary>The remote capabilities advertised by the HTTP reference transport and endpoint.</summary>
    private const RemoteTransportCapabilities HttpTransportFeatures = RemoteTransportCapabilities.BatchPush
        | RemoteTransportCapabilities.CursorResume | RemoteTransportCapabilities.ReceiveAcknowledgements
        | RemoteTransportCapabilities.ServerIdempotency | RemoteTransportCapabilities.AtomicApplyAndAcknowledge
        | RemoteTransportCapabilities.SnapshotRecovery;

    /// <summary>Identifies a shipped local store adapter.</summary>
    public enum ShippedStore
    {
        /// <summary>The in-memory reference store.</summary>
        InMemory = 0,

        /// <summary>The SQLite durable store.</summary>
        Sqlite = 1,
    }

    /// <summary>Verifies startup accepts or rejects each delivery policy exactly as the shipped store and HTTP capabilities allow.</summary>
    /// <param name="store">The shipped store.</param>
    /// <param name="guarantee">The requested delivery guarantee.</param>
    /// <param name="durability">The requested durability.</param>
    /// <param name="expectAccepted">Whether negotiation must succeed.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments(ShippedStore.InMemory, DeliveryGuarantee.AtMostOnce, OperationDurability.Volatile, true)]
    [Arguments(ShippedStore.InMemory, DeliveryGuarantee.AtLeastOnce, OperationDurability.Volatile, true)]
    [Arguments(ShippedStore.InMemory, DeliveryGuarantee.AtLeastOnce, OperationDurability.Durable, false)]
    [Arguments(ShippedStore.InMemory, DeliveryGuarantee.ExactlyOnce, OperationDurability.Durable, false)]
    [Arguments(ShippedStore.Sqlite, DeliveryGuarantee.AtMostOnce, OperationDurability.Durable, true)]
    [Arguments(ShippedStore.Sqlite, DeliveryGuarantee.AtLeastOnce, OperationDurability.Durable, true)]
    [Arguments(ShippedStore.Sqlite, DeliveryGuarantee.ExactlyOnce, OperationDurability.Durable, true)]
    public async Task ShippedAdapterCapabilitiesDecideStartup(
        ShippedStore store,
        DeliveryGuarantee guarantee,
        OperationDurability durability,
        bool expectAccepted)
    {
        var storeCapabilities = await GetShippedStoreCapabilitiesAsync(store);
        var request = CreateRequest() with
        {
            Policy = OperationPolicy.Default with { DeliveryGuarantee = guarantee, Durability = durability },
            StoreCapabilities = storeCapabilities,
            TransportCapabilities = HttpTransportFeatures,
            PeerOffer = CreateRequest().PeerOffer with { Features = RemoteFeatures, ClientInboxRetentionRequired = null },
        };

        if (!expectAccepted)
        {
            await Assert.That(() => CapabilityNegotiator.Negotiate(request)).ThrowsExactly<InvalidOperationException>();
            return;
        }

        var negotiated = CapabilityNegotiator.Negotiate(request);
        await Assert.That(negotiated.Features & RemoteTransportCapabilities.StreamingReceive).IsEqualTo(RemoteTransportCapabilities.None);
        await Assert.That(negotiated.EffectiveExactlyOnceWindow.HasValue).IsEqualTo(guarantee == DeliveryGuarantee.ExactlyOnce);
    }

    /// <summary>Verifies neither shipped store can satisfy multi-process coordination or encryption at rest requirements.</summary>
    /// <param name="store">The shipped store.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments(ShippedStore.InMemory)]
    [Arguments(ShippedStore.Sqlite)]
    public async Task ShippedStoresRejectUnadvertisedStartupRequirements(ShippedStore store)
    {
        var storeCapabilities = await GetShippedStoreCapabilitiesAsync(store);
        var baseline = CreateRequest() with
        {
            Policy = OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.AtMostOnce, Durability = OperationDurability.Volatile },
            StoreCapabilities = storeCapabilities,
            TransportCapabilities = HttpTransportFeatures,
            PeerOffer = CreateRequest().PeerOffer with { ClientInboxRetentionRequired = null },
        };
        var multiProcess = baseline with { RequiresMultipleProcesses = true };
        var encrypted = baseline with
        {
            Options = baseline.Options with { Security = baseline.Options.Security with { RequireAuthenticatedEncryptionAtRest = true } },
        };

        await Assert.That(() => CapabilityNegotiator.Negotiate(multiProcess)).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => CapabilityNegotiator.Negotiate(encrypted)).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Reads the capabilities advertised by a shipped store instance.</summary>
    /// <param name="store">The shipped store.</param>
    /// <returns>The advertised capabilities.</returns>
    private static async Task<LocalStoreCapabilities> GetShippedStoreCapabilitiesAsync(ShippedStore store)
    {
        if (store == ShippedStore.InMemory)
        {
            await using var memory = new InMemoryLocalStoreAdapter();
            return memory.Capabilities;
        }

        var databasePath = Path.Combine(SqliteTestDirectory.Create("oc-capability-matrix-").FullName, "store.db");
        await using var sqlite = new SqliteLocalStoreAdapter(databasePath);
        return sqlite.Capabilities;
    }
}
