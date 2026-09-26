// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Hosting.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedHealthMonitor"/>.</summary>
public sealed class OccasionallyConnectedHealthMonitorTests
{
    /// <summary>Verifies the monitor reports the latest state and stops observing when disposed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task MonitorTracksLatestStateUntilDisposed()
    {
        var context = new HostingTestDoubles.RecordingContext();
        var monitor = new OccasionallyConnectedHealthMonitor(context, TimeProvider.System);

        var initial = monitor.Current;
        context.Publish(HostingTestDoubles.CreateState(SyncLifecycleStatus.Online, 0, reasonCode: null));
        var online = monitor.Current;
        monitor.OnCompleted();
        monitor.Dispose();
        context.Publish(HostingTestDoubles.CreateState(SyncLifecycleStatus.Faulted, 0, reasonCode: null));

        await Assert.That(initial.LifecycleStatus).IsEqualTo(SyncLifecycleStatus.Created);
        await Assert.That(online.Status).IsEqualTo(OccasionallyConnectedHealthStatus.Healthy);
        await Assert.That(monitor.Current.Status).IsEqualTo(OccasionallyConnectedHealthStatus.Healthy);
        await Assert.That(context.SubscriberCount).IsEqualTo(0);
    }

    /// <summary>Verifies invalid arguments are rejected.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task MonitorRejectsNullArguments()
    {
        var context = new HostingTestDoubles.RecordingContext();
        IOccasionallyConnectedContext? missingContext = null;
        TimeProvider? missingClock = null;
        using var monitor = new OccasionallyConnectedHealthMonitor(context, TimeProvider.System);
        SyncState? missingState = null;
        Exception? missingError = null;

        await Assert.That(() => new OccasionallyConnectedHealthMonitor(missingContext!, TimeProvider.System)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => new OccasionallyConnectedHealthMonitor(context, missingClock!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => monitor.OnNext(missingState!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => monitor.OnError(missingError!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => monitor.OnError(new InvalidOperationException("ignored"))).ThrowsNothing();
    }
}
