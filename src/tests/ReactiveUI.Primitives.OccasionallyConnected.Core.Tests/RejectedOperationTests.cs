// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="RejectedOperation"/>.</summary>
public sealed class RejectedOperationTests
{
    /// <summary>Verifies resubmission policy is retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsResubmissionPolicy()
    {
        var operationId = OperationId.New();
        var rejected = new RejectedOperation(operationId, "OC.Rejected", false);

        await Assert.That(rejected.OperationId).IsEqualTo(operationId);
        await Assert.That(rejected.ReasonCode).IsEqualTo("OC.Rejected");
        await Assert.That(rejected.MayResubmit).IsFalse();
    }
}
