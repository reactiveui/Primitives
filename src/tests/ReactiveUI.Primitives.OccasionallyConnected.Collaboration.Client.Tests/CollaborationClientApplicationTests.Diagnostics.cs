// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client.Tests;

/// <content>Operation and fault diagnostics tests for <see cref="CollaborationClientApplication"/>.</content>
public sealed partial class CollaborationClientApplicationTests
{
    /// <summary>The number of statuses used to cross the retained-state limit.</summary>
    private const int StatusProbeCount = 20;

    /// <summary>The number of faults used to cross the retained-fault limit.</summary>
    private const int FaultProbeCount = 70;

    /// <summary>The expected maximum retained fault count.</summary>
    private const int MaximumRetainedFaultCount = 64;

    /// <summary>The terminal fault code used by diagnostics output tests.</summary>
    private const string StoppedFaultCode = "stopped";

    /// <summary>Verifies diagnostics wait for the current command's terminal operation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DiagnosticsIgnoreOtherOperationsAndCaptureCurrentSynchronization()
    {
        var expectedId = OperationId.New();
        var otherId = OperationId.New();
        var diagnostics = new CollaborationClientApplication.PublishDiagnosticsObserver();
        diagnostics.OnNext(CreateOperationStatus(otherId, SyncOperationState.Synchronized));
        diagnostics.OnNext(CreateOperationStatus(expectedId, SyncOperationState.SavedLocally));
        diagnostics.TrackOperation(expectedId);
        diagnostics.OnNext(CreateOperationStatus(otherId, SyncOperationState.Rejected));
        diagnostics.OnNext(CreateSyncState(SyncLifecycleStatus.Online));
        diagnostics.OnNext(CreateOperationStatus(expectedId, SyncOperationState.Synchronized));

        var result = await diagnostics.WaitForTerminalOperationAsync(CancellationToken.None).ConfigureAwait(false);
        var snapshot = diagnostics.Snapshot();

        await Assert.That(result.Status?.OperationId).IsEqualTo(expectedId);
        await Assert.That(result.Status?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(snapshot.LatestSyncStatus).IsEqualTo(SyncLifecycleStatus.Online);
        await Assert.That(snapshot.FaultCount).IsEqualTo(0);
    }

    /// <summary>Verifies a terminal status emitted before receipt tracking is still observed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DiagnosticsReplayTerminalOperationObservedBeforeTracking()
    {
        var expectedId = OperationId.New();
        var diagnostics = new CollaborationClientApplication.PublishDiagnosticsObserver();
        for (var index = 0; index < StatusProbeCount; index++)
        {
            diagnostics.OnNext(CreateOperationStatus(OperationId.New(), SyncOperationState.SavedLocally));
        }

        diagnostics.OnNext(CreateOperationStatus(expectedId, SyncOperationState.Rejected));
        diagnostics.TrackOperation(expectedId);

        var result = await diagnostics.WaitForTerminalOperationAsync(CancellationToken.None).ConfigureAwait(false);

        await Assert.That(result.Status?.OperationId).IsEqualTo(expectedId);
        await Assert.That(result.Status?.State).IsEqualTo(SyncOperationState.Rejected);
    }

    /// <summary>Verifies transient and unrelated faults do not end the current command.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DiagnosticsCompleteOnMatchingPermanentFaultWithBoundedCount()
    {
        var expectedId = OperationId.New();
        var diagnostics = new CollaborationClientApplication.PublishDiagnosticsObserver();
        diagnostics.TrackOperation(expectedId);
        for (var index = 0; index < FaultProbeCount; index++)
        {
            diagnostics.OnNext(CreateFault("retry", expectedId, FaultSeverity.Warning, true));
        }

        diagnostics.OnNext(CreateFault("other", OperationId.New(), FaultSeverity.Error, false));
        diagnostics.OnNext(CreateFault(StoppedFaultCode, expectedId, FaultSeverity.Critical, false));

        var result = await diagnostics.WaitForTerminalOperationAsync(CancellationToken.None).ConfigureAwait(false);
        var snapshot = diagnostics.Snapshot();

        await Assert.That(result.Status).IsNull();
        await Assert.That(snapshot.FaultCount).IsEqualTo(MaximumRetainedFaultCount);
        await Assert.That(snapshot.LatestFault?.Code).IsEqualTo(StoppedFaultCode);
    }

    /// <summary>Verifies a permanent global fault observed before tracking ends the command.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DiagnosticsReplayGlobalFaultObservedBeforeTracking()
    {
        var diagnostics = new CollaborationClientApplication.PublishDiagnosticsObserver();
        diagnostics.OnNext(CreateFault("global", null, FaultSeverity.Error, false));
        diagnostics.TrackOperation(OperationId.New());

        var result = await diagnostics.WaitForTerminalOperationAsync(CancellationToken.None).ConfigureAwait(false);

        await Assert.That(result.Status).IsNull();
        await Assert.That(diagnostics.Snapshot().LatestFault?.Code).IsEqualTo("global");
    }

    /// <summary>Verifies a terminal observable error remains visible to the CLI wait.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DiagnosticsPropagateTerminalObservableError()
    {
        var diagnostics = new CollaborationClientApplication.PublishDiagnosticsObserver();
        var error = new InvalidOperationException("operation observer failed");
        diagnostics.TrackOperation(OperationId.New());
        diagnostics.OnError(error);

        var observed = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            diagnostics.WaitForTerminalOperationAsync(CancellationToken.None));

        await Assert.That(ReferenceEquals(observed, error)).IsTrue();
    }

    /// <summary>Verifies terminal fault and absent sync state are printed using stable redacted fields.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DiagnosticsSummaryPrintsFaultCodeWithoutRawMessage()
    {
        var diagnostics = new CollaborationClientApplication.PublishDiagnosticsObserver();
        diagnostics.OnNext(CreateFault(StoppedFaultCode, OperationId.New(), FaultSeverity.Error, false));
        diagnostics.OnCompleted();
        await using var output = new StringWriter(CultureInfo.InvariantCulture);

        await CollaborationClientApplication.WriteDiagnosticsSummaryAsync(output, diagnostics).ConfigureAwait(false);

        var text = output.ToString();
        await Assert.That(text.Contains("sync: none", StringComparison.Ordinal)).IsTrue();
        await Assert.That(text.Contains("faults: 1", StringComparison.Ordinal)).IsTrue();
        await Assert.That(text.Contains($"fault: {StoppedFaultCode}", StringComparison.Ordinal)).IsTrue();
        await Assert.That(text.Contains("Exception", StringComparison.Ordinal)).IsFalse();
    }

    /// <summary>Verifies watch observer output formats views and redacts raw terminal errors.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PrintingObserverFormatsViewAndRedactsTerminalError()
    {
        await using var output = new StringWriter(CultureInfo.InvariantCulture);
        var observer = new CollaborationClientApplication.PrintingActivityObserver(output);
        var view = ActivityView.Empty() with { Status = "ready", Title = "Shared title", Details = "Shared details" };

        observer.OnNext(view);
        observer.OnError(new InvalidOperationException("raw secret detail"));
        observer.OnCompleted();

        var text = output.ToString();
        await Assert.That(text.Contains("ready | Shared title | Shared details | activity-v0", StringComparison.Ordinal)).IsTrue();
        await Assert.That(text.Contains("fault: ActivityObservationFailed", StringComparison.Ordinal)).IsTrue();
        await Assert.That(text.Contains("raw secret detail", StringComparison.Ordinal)).IsFalse();
    }

    /// <summary>Creates a deterministic operation status.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="state">The operation state.</param>
    /// <returns>The status.</returns>
    private static SyncOperationStatus CreateOperationStatus(OperationId operationId, SyncOperationState state) =>
        new(operationId, ActivityContracts.StreamId, state, 0, DateTimeOffset.UnixEpoch, null);

    /// <summary>Creates a deterministic synchronization state.</summary>
    /// <param name="status">The synchronization status.</param>
    /// <returns>The state.</returns>
    private static SyncState CreateSyncState(SyncLifecycleStatus status) =>
        new(status, true, 0, 0, DateTimeOffset.UnixEpoch, null, null, null);

    /// <summary>Creates a classified fault for diagnostic tests.</summary>
    /// <param name="code">The stable fault code.</param>
    /// <param name="operationId">The optional operation identifier.</param>
    /// <param name="severity">The severity.</param>
    /// <param name="transient">Whether retry may recover the fault.</param>
    /// <returns>The fault.</returns>
    private static OccasionallyConnectedFault CreateFault(
        string code,
        OperationId? operationId,
        FaultSeverity severity,
        bool transient) =>
        new(code, code, DateTimeOffset.UnixEpoch, ActivityContracts.StreamId, operationId, null)
        { Category = FaultCategory.Transport, Severity = severity, IsTransient = transient };
}
