// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests stream family consistency for CRDT remote snapshots.</summary>
public sealed partial class CrdtLocalProjectionTests
{
    /// <summary>Verifies a remote snapshot cannot change the registered CRDT family.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RemoteSnapshotCannotChangeStreamFamily()
    {
        var projection = new CrdtLocalProjection(ClientId, CrdtKind.GCounter);
        var input = CrdtInput.ForAuthoritativeState(CrdtFunctions.Empty(CrdtKind.ORSet));
        var serializer = new CrdtPayloadSerializer();
        var payload = await serializer.SerializeAsync(CrdtContracts.InputContractId, CrdtContracts.SchemaVersion, input, CancellationToken.None);
        var remoteEvent = new RemoteEvent(Guid.NewGuid(), new StreamId("counter"), CursorOne, DateTimeOffset.UnixEpoch, null, payload, new Dictionary<string, string>());
        await Assert.That(() => projection.ApplyRemote(projection.InitialState, input, remoteEvent))
            .ThrowsExactly<InvalidOperationException>();
    }
}
