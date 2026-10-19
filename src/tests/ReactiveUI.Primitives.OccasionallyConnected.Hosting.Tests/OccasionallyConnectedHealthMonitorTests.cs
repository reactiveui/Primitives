// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Hosting.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedHealthMonitor"/>.</summary>
public sealed class OccasionallyConnectedHealthMonitorTests
{
    /// <summary>The pending operation count retained by a terminal fault.</summary>
    private const int PendingOperationCount = 7;

    /// <summary>The encoded byte count retained by a terminal fault.</summary>
    private const long PendingByteCount = 70;

    /// <summary>The number of concurrent reads and ignored updates in the terminal-state check.</summary>
    private const int ConcurrentReadCount = 64;

    /// <summary>The exception detail that must not appear in a health report.</summary>
    private const string SensitiveFailureDetail = "sensitive failure detail";

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
        monitor.Dispose();
        context.Publish(HostingTestDoubles.CreateState(SyncLifecycleStatus.Faulted, 0, reasonCode: null));

        await Assert.That(typeof(IObserver<SyncState>).IsAssignableFrom(typeof(OccasionallyConnectedHealthMonitor))).IsFalse();
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
        await Assert.That(() => context.Publish(missingState!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => context.Fail(missingError!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => context.Fail(new InvalidOperationException("ignored"))).ThrowsNothing();
    }

    /// <summary>Verifies an error before the first state replaces the initial report with a stable fault.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ErrorBeforeFirstStateLatchesFaultWithoutExposingException()
    {
        var context = new HostingTestDoubles.RecordingContext();
        using var monitor = new OccasionallyConnectedHealthMonitor(context, TimeProvider.System);

        context.Fail(new InvalidOperationException(SensitiveFailureDetail));

        var report = monitor.Current;
        await Assert.That(report.Status).IsEqualTo(OccasionallyConnectedHealthStatus.Unhealthy);
        await Assert.That(report.LifecycleStatus).IsEqualTo(SyncLifecycleStatus.Faulted);
        await Assert.That(report.ReasonCode).IsEqualTo(OccasionallyConnectedHealth.FaultedReasonCode);
        await Assert.That(report.ReasonCode).DoesNotContain("sensitive");
    }

    /// <summary>Verifies an error after an online state preserves pending counts and cannot be overwritten.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ErrorAfterOnlineStatePreservesPendingWorkAndIgnoresLaterStates()
    {
        var context = new HostingTestDoubles.RecordingContext();
        using var monitor = new OccasionallyConnectedHealthMonitor(context, TimeProvider.System);
        context.Publish(HostingTestDoubles.CreateState(SyncLifecycleStatus.Online, PendingOperationCount, reasonCode: null));

        context.Fail(new InvalidOperationException(SensitiveFailureDetail));
        context.Fail(new InvalidOperationException("later failure detail"));
        context.Publish(HostingTestDoubles.CreateState(SyncLifecycleStatus.Online, 0, reasonCode: null));

        var report = monitor.Current;
        await Assert.That(report.Status).IsEqualTo(OccasionallyConnectedHealthStatus.Unhealthy);
        await Assert.That(report.LifecycleStatus).IsEqualTo(SyncLifecycleStatus.Faulted);
        await Assert.That(report.PendingOperations).IsEqualTo(PendingOperationCount);
        await Assert.That(report.PendingBytes).IsEqualTo(PendingByteCount);
        await Assert.That(report.ReasonCode).IsEqualTo(OccasionallyConnectedHealth.FaultedReasonCode);
        await Assert.That(report.ReasonCode).DoesNotContain("sensitive");
    }

    /// <summary>Verifies concurrent reads remain faulted while later states arrive.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConcurrentReadsRemainFaultedAfterTerminalError()
    {
        var context = new HostingTestDoubles.RecordingContext();
        using var monitor = new OccasionallyConnectedHealthMonitor(context, TimeProvider.System);
        context.Fail(new InvalidOperationException(SensitiveFailureDetail));
        var unhealthyReadCount = 0;

        _ = Parallel.For(0, ConcurrentReadCount, _ =>
        {
            context.Publish(HostingTestDoubles.CreateState(SyncLifecycleStatus.Online, 0, reasonCode: null));
            if (monitor.Current.Status == OccasionallyConnectedHealthStatus.Unhealthy)
            {
                _ = Interlocked.Increment(ref unhealthyReadCount);
            }
        });

        await Assert.That(unhealthyReadCount).IsEqualTo(ConcurrentReadCount);
        await Assert.That(monitor.Current.LifecycleStatus).IsEqualTo(SyncLifecycleStatus.Faulted);
    }
}
