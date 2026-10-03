// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="PublishReceipt"/>.</summary>
public sealed class PublishReceiptTests
{
    /// <summary>Verifies the operation state is retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsState()
    {
        var operationId = OperationId.New();
        var savedAt = DateTimeOffset.UnixEpoch;
        var receipt = new PublishReceipt(operationId, 1, SyncOperationState.SavedLocally, savedAt);

        await Assert.That(receipt.OperationId).IsEqualTo(operationId);
        await Assert.That(receipt.ClientSequence).IsEqualTo(1);
        await Assert.That(receipt.State).IsEqualTo(SyncOperationState.SavedLocally);
        await Assert.That(receipt.SavedAtUtc).IsEqualTo(savedAt);
    }
}
