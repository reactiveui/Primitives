// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests stream family consistency for CRDT remote snapshots.</summary>
public sealed partial class MergeableLocalProjectionTests
{
    /// <summary>Verifies a remote snapshot cannot change the registered CRDT family.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RemoteSnapshotCannotChangeStreamFamily()
    {
        var projection = new MergeableLocalProjection(ClientId, MergeableKind.IncrementOnlyCounter);
        var input = MergeableInput.ForAuthoritativeState(MergeableFunctions.Empty(MergeableKind.AddWinsSet));
        var serializer = new MergeablePayloadSerializer();
        var payload = await serializer.SerializeAsync(MergeableContracts.InputContractId, MergeableContracts.SchemaVersion, input, CancellationToken.None);
        var remoteEvent = new RemoteEvent(Guid.NewGuid(), new StreamId("counter"), CursorOne, DateTimeOffset.UnixEpoch, null, payload, new Dictionary<string, string>());
        await Assert.That(() => projection.ApplyRemote(projection.InitialState, input, remoteEvent))
            .ThrowsExactly<InvalidOperationException>();
    }
}
