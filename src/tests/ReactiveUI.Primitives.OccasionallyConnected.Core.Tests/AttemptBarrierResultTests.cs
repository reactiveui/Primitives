// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="AttemptBarrierResult"/>.</summary>
public sealed class AttemptBarrierResultTests
{
    /// <summary>The attempted send count.</summary>
    private const int AttemptNumber = 3;

    /// <summary>Verifies send eligibility and reason are retained.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRetainsSendEligibilityAndReason()
    {
        var result = new AttemptBarrierResult(OperationId.New(), AttemptNumber, false, "OC.AtMostOnceAmbiguous");
        await Assert.That(result.MaySend).IsFalse();
        await Assert.That(result.ReasonCode).IsEqualTo("OC.AtMostOnceAmbiguous");
    }
}
