// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedHealth"/>.</summary>
public sealed class OccasionallyConnectedHealthTests
{
    /// <summary>The pending operation count used by degraded cases.</summary>
    private const int PendingCount = 3;

    /// <summary>The pending byte count used by degraded cases.</summary>
    private const long PendingSize = 128;

    /// <summary>The seconds between the state change and evaluation.</summary>
    private const int AgeSeconds = 5;

    /// <summary>The circuit retry delay in seconds.</summary>
    private const int CircuitRetrySeconds = 9;

    /// <summary>The time the evaluated state changed.</summary>
    private static readonly DateTimeOffset ChangedAt = DateTimeOffset.UnixEpoch;

    /// <summary>The evaluation time.</summary>
    private static readonly DateTimeOffset EvaluatedAt = ChangedAt.AddSeconds(AgeSeconds);

    /// <summary>Verifies online and synchronizing states are healthy even while work is in flight.</summary>
    /// <param name="status">The lifecycle status.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(SyncLifecycleStatus.Online)]
    [Arguments(SyncLifecycleStatus.Synchronizing)]
    public async Task ConnectedStatesAreHealthy(SyncLifecycleStatus status)
    {
        var report = OccasionallyConnectedHealth.Evaluate(CreateState(status, PendingCount, reasonCode: null), EvaluatedAt);

        await Assert.That(report.Status).IsEqualTo(OccasionallyConnectedHealthStatus.Healthy);
        await Assert.That(report.ReasonCode).IsNull();
        await Assert.That(report.PendingOperations).IsEqualTo(PendingCount);
        await Assert.That(report.PendingBytes).IsEqualTo(PendingSize);
        await Assert.That(report.StateAge).IsEqualTo(TimeSpan.FromSeconds(AgeSeconds));
    }

    /// <summary>Verifies disconnected states are healthy without pending work and degraded with it.</summary>
    /// <param name="status">The lifecycle status.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(SyncLifecycleStatus.Offline)]
    [Arguments(SyncLifecycleStatus.Connecting)]
    [Arguments(SyncLifecycleStatus.Degraded)]
    public async Task DisconnectedStatesDegradeOnlyWithPendingWork(SyncLifecycleStatus status)
    {
        var idle = OccasionallyConnectedHealth.Evaluate(CreateState(status, 0, reasonCode: null), EvaluatedAt);
        var pending = OccasionallyConnectedHealth.Evaluate(CreateState(status, PendingCount, reasonCode: null), EvaluatedAt);
        var reported = OccasionallyConnectedHealth.Evaluate(CreateState(status, PendingCount, SyncEngine.TransportUnavailableReasonCode), EvaluatedAt);
        var bytesOnly = OccasionallyConnectedHealth.Evaluate(CreateState(status, 0, reasonCode: null) with { PendingBytes = PendingSize }, EvaluatedAt);

        await Assert.That(idle.Status).IsEqualTo(OccasionallyConnectedHealthStatus.Healthy);
        await Assert.That(idle.ReasonCode).IsNull();
        await Assert.That(pending.Status).IsEqualTo(OccasionallyConnectedHealthStatus.Degraded);
        await Assert.That(pending.ReasonCode).IsEqualTo(OccasionallyConnectedHealth.PendingRemoteWorkReasonCode);
        await Assert.That(reported.ReasonCode).IsEqualTo(SyncEngine.TransportUnavailableReasonCode);
        await Assert.That(bytesOnly.Status).IsEqualTo(OccasionallyConnectedHealthStatus.Degraded);
    }

    /// <summary>Verifies an open circuit breaker degrades health even without pending work.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OpenCircuitIsDegraded()
    {
        var retryAfter = TimeSpan.FromSeconds(CircuitRetrySeconds);
        var state = CreateState(SyncLifecycleStatus.Offline, 0, SyncEngine.CircuitOpenReasonCode) with { RetryAfter = retryAfter };

        var report = OccasionallyConnectedHealth.Evaluate(state, EvaluatedAt);

        await Assert.That(report.Status).IsEqualTo(OccasionallyConnectedHealthStatus.Degraded);
        await Assert.That(report.ReasonCode).IsEqualTo(SyncEngine.CircuitOpenReasonCode);
        await Assert.That(report.RetryAfter).IsEqualTo(retryAfter);
    }

    /// <summary>Verifies faulted states are unhealthy and keep their specific reason.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FaultedStatesAreUnhealthy()
    {
        var specific = OccasionallyConnectedHealth.Evaluate(CreateState(SyncLifecycleStatus.Faulted, 0, SyncEngine.ConnectFaultCode), EvaluatedAt);
        var general = OccasionallyConnectedHealth.Evaluate(CreateState(SyncLifecycleStatus.Faulted, 0, reasonCode: null), EvaluatedAt);

        await Assert.That(specific.Status).IsEqualTo(OccasionallyConnectedHealthStatus.Unhealthy);
        await Assert.That(specific.ReasonCode).IsEqualTo(SyncEngine.ConnectFaultCode);
        await Assert.That(general.Status).IsEqualTo(OccasionallyConnectedHealthStatus.Unhealthy);
        await Assert.That(general.ReasonCode).IsEqualTo(OccasionallyConnectedHealth.FaultedReasonCode);
    }

    /// <summary>Verifies states outside the running lifecycle are degraded as not running.</summary>
    /// <param name="status">The lifecycle status.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(SyncLifecycleStatus.Created)]
    [Arguments(SyncLifecycleStatus.Initializing)]
    [Arguments(SyncLifecycleStatus.Stopping)]
    [Arguments(SyncLifecycleStatus.Stopped)]
    public async Task InactiveStatesAreNotRunning(SyncLifecycleStatus status)
    {
        var report = OccasionallyConnectedHealth.Evaluate(CreateState(status, 0, reasonCode: null), EvaluatedAt);

        await Assert.That(report.Status).IsEqualTo(OccasionallyConnectedHealthStatus.Degraded);
        await Assert.That(report.ReasonCode).IsEqualTo(OccasionallyConnectedHealth.NotRunningReasonCode);
        await Assert.That(report.LifecycleStatus).IsEqualTo(status);
    }

    /// <summary>Verifies a clock earlier than the state change reports a zero age.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EvaluateClampsNegativeAge()
    {
        var report = OccasionallyConnectedHealth.Evaluate(CreateState(SyncLifecycleStatus.Online, 0, reasonCode: null), ChangedAt.AddSeconds(-1));

        await Assert.That(report.StateAge).IsEqualTo(TimeSpan.Zero);
    }

    /// <summary>Verifies a missing state is rejected.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task EvaluateRejectsNullState()
    {
        SyncState? missing = null;

        await Assert.That(() => OccasionallyConnectedHealth.Evaluate(missing!, EvaluatedAt)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Creates a synchronization state.</summary>
    /// <param name="status">The lifecycle status.</param>
    /// <param name="pendingOperations">The pending operation count.</param>
    /// <param name="reasonCode">The reason code.</param>
    /// <returns>The state.</returns>
    private static SyncState CreateState(SyncLifecycleStatus status, int pendingOperations, string? reasonCode) =>
        new(status, NetworkAvailable: false, pendingOperations, pendingOperations == 0 ? 0 : PendingSize, ChangedAt, null, null, reasonCode);
}
