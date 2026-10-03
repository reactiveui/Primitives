// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Tests for <see cref="CrdtLoopbackScenarioShape"/>.</summary>
public sealed class CrdtLoopbackScenarioShapeTests
{
    /// <summary>The expected CRDT kind count.</summary>
    private const int ExpectedKindCount = 4;

    /// <summary>Verifies the scenario advertises only volatile in-memory loopback capabilities.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AdvertisesOnlyVolatileLoopbackCapabilities()
    {
        await Assert.That(HasCapability(RemoteTransportCapabilities.BatchPush)).IsTrue();
        await Assert.That(HasCapability(RemoteTransportCapabilities.CursorResume)).IsTrue();
        await Assert.That(HasCapability(RemoteTransportCapabilities.ReceiveAcknowledgements)).IsTrue();
        await Assert.That(HasCapability(RemoteTransportCapabilities.ServerIdempotency)).IsTrue();
        await Assert.That(HasCapability(RemoteTransportCapabilities.StreamingReceive)).IsTrue();
        await Assert.That(HasCapability(RemoteTransportCapabilities.AtomicApplyAndAcknowledge)).IsFalse();
        await Assert.That(HasCapability(RemoteTransportCapabilities.SnapshotRecovery)).IsFalse();
        await Assert.That(CrdtLoopbackScenarioShape.CoveredKinds).Count().IsEqualTo(ExpectedKindCount);
        await Assert.That(CrdtLoopbackScenarioShape.CoveredKinds).Contains(CrdtKind.GCounter);
        await Assert.That(CrdtLoopbackScenarioShape.CoveredKinds).Contains(CrdtKind.PNCounter);
        await Assert.That(CrdtLoopbackScenarioShape.CoveredKinds).Contains(CrdtKind.ORSet);
        await Assert.That(CrdtLoopbackScenarioShape.CoveredKinds).Contains(CrdtKind.LwwRegister);
    }

    /// <summary>Gets whether the volatile capability set contains a capability.</summary>
    /// <param name="capability">The capability to inspect.</param>
    /// <returns>Whether the capability is advertised.</returns>
    private static bool HasCapability(RemoteTransportCapabilities capability) =>
        (CrdtLoopbackScenarioShape.VolatileLoopbackCapabilities & capability) == capability;
}
