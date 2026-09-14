// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Extensions.Time.Testing;
using OccasionallyConnected.DurableOutbox;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Examples.Tests;

/// <summary>Tests for <see cref="DurableOutboxApplication"/>.</summary>
public sealed partial class DurableOutboxApplicationTests
{
    /// <summary>The sample device identifier used by durable workflow tests.</summary>
    private const string DeviceId = "device-a";

    /// <summary>The sample reading unit used by durable workflow tests.</summary>
    private const string Unit = "C";

    /// <summary>The store identity used by the example app.</summary>
    private const string StoreIdentity = "durable-outbox-example";

    /// <summary>The first persisted test reading.</summary>
    private const double FirstReading = 21.5;

    /// <summary>The second persisted test reading.</summary>
    private const double SecondReading = 22.25;

    /// <summary>The at-most-once test reading.</summary>
    private const double AtMostOnceReading = 19.75;

    /// <summary>The unsupported exactly-once test reading.</summary>
    private const double ExactlyOnceReading = 18.0;

    /// <summary>The single subscription test reading.</summary>
    private const double SubscriptionReading = 20.0;

    /// <summary>The retained first reading that is later rejected.</summary>
    private const double RejectedReading = 20.0;

    /// <summary>The retained second reading that remains after rejection.</summary>
    private const double RetainedReading = 25.0;

    /// <summary>The terminal receipt test reading.</summary>
    private const double TerminalReceiptReading = 20.0;

    /// <summary>The accepted reading that should remain after a later rejection.</summary>
    private const double AcceptedBeforeRejectReading = 31.0;

    /// <summary>The later rejected reading that must be removed from optimistic state.</summary>
    private const double LaterRejectedReading = 32.0;

    /// <summary>The third capacity test reading.</summary>
    private const double ThirdCapacityReading = 23.0;

    /// <summary>The fourth capacity test reading.</summary>
    private const double FourthCapacityReading = 24.0;

    /// <summary>The fifth capacity test reading.</summary>
    private const double FifthCapacityReading = 25.0;

    /// <summary>The invalid command exit code.</summary>
    private const int InvalidCommandExitCode = 2;

    /// <summary>The initial store schema version.</summary>
    private const int StoreSchemaVersion = 1;

    /// <summary>The store client identity used by the example app.</summary>
    private const string StoreClientId = "durable-outbox-client";

    /// <summary>The payload contract for reading operations.</summary>
    private const string ReadingContract = "example.temperature-reading";

    /// <summary>The payload contract for snapshots.</summary>
    private const string SnapshotContract = "example.temperature-snapshot";

    /// <summary>The payload schema version used by both sample contracts.</summary>
    private const int PayloadSchemaVersion = 1;

    /// <summary>The snapshot format version used by the example projection.</summary>
    private const int SnapshotFormatVersion = 1;

    /// <summary>The default priority used by direct durable-store seed operations.</summary>
    private const int DefaultPriority = 0;

    /// <summary>The first direct durable-store client sequence.</summary>
    private const long FirstClientSequence = 1;

    /// <summary>The second direct durable-store client sequence.</summary>
    private const long SecondClientSequence = 2;

    /// <summary>The empty direct durable-store snapshot revision.</summary>
    private const long EmptySnapshotRevision = 0;

    /// <summary>The first direct durable-store snapshot revision.</summary>
    private const long FirstSnapshotRevision = 1;

    /// <summary>The direct durable-store reading count after one operation.</summary>
    private const int OneReadingCount = 1;

    /// <summary>The direct durable-store reading count after two operations.</summary>
    private const int TwoReadingCount = 2;

    /// <summary>The maximum payload bytes leased by the sample.</summary>
    private const long LeaseCapacityBytes = 64L * 1024L;

    /// <summary>The clock start year for lease-expiry workflow tests.</summary>
    private const int ClockStartYear = 2026;

    /// <summary>The clock start month for lease-expiry workflow tests.</summary>
    private const int ClockStartMonth = 1;

    /// <summary>The clock start day for lease-expiry workflow tests.</summary>
    private const int ClockStartDay = 1;

    /// <summary>The number of minutes advanced past the sample lease duration.</summary>
    private const int LeaseExpiryAdvanceMinutes = 6;

    /// <summary>The synchronized status text printed by retained operation status.</summary>
    private const string SynchronizedStatusText = "state=Synchronized";

    /// <summary>The synchronized state text printed by local simulation output.</summary>
    private const string SynchronizedOutputText = "state: Synchronized";

    /// <summary>The first attempt text printed by local simulation output.</summary>
    private const string AttemptOneOutputText = "attempt: 1";

    /// <summary>The missing pending operation message printed by local simulation failures.</summary>
    private const string MissingPendingOperationMessage = "No pending operation was available";

    /// <summary>The message printed when no active lease can be acquired for simulation.</summary>
    private const string LeaseUnavailableMessage = "No lease could be acquired for the pending operation.";

    /// <summary>The message printed when recovered pending work has no durable status row.</summary>
    private const string MissingDurableStatusMessage = "The pending operation has no durable status.";

    /// <summary>The message printed when the next durable send attempt cannot be represented.</summary>
    private const string AttemptOverflowMessage = "The next durable attempt number would overflow.";

    /// <summary>The open failure message used by disposal-mask tests.</summary>
    private const string OpenFailureMessage = "sample open failed after durable initialization";

    /// <summary>The cleanup failure message used by disposal-mask tests.</summary>
    private const string DisposeFailureMessage = "sample dispose failed after open failure";

    /// <summary>The message printed when rejected recovered work has no durable snapshot.</summary>
    private const string MissingSnapshotMessage = "The rejected operation cannot rebuild a snapshot because no durable snapshot exists.";

    /// <summary>The message printed when rejected recovered work has no authoritative checkpoint.</summary>
    private const string MissingAuthoritativeSnapshotMessage =
        "The rejected operation cannot rebuild a snapshot because no authoritative checkpoint exists.";

    /// <summary>The snapshot output line for one retained reading.</summary>
    private const string SingleReadingCountOutput = "reading-count: 1";

    /// <summary>Verifies appending readings uses real SQLite durable state across separate command executions.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAppendCommandRunsTwice_ThenSubscriptionOperationSequenceAndSnapshotPersist()
    {
        using var database = ExampleDatabase.Create();
        var first = await RunAsync(new AppendReadingCommand(database.Path, DeviceId, FirstReading, DeliveryGuarantee.AtLeastOnce));
        var second = await RunAsync(new AppendReadingCommand(database.Path, DeviceId, SecondReading, DeliveryGuarantee.AtLeastOnce));
        var status = await RunAsync(new InspectCommand(database.Path, InspectView.Status));

        await Assert.That(first.ExitCode).IsEqualTo(0);
        await Assert.That(second.ExitCode).IsEqualTo(0);
        await Assert.That(status.StandardOutput).Contains("subscription:");
        await Assert.That(status.StandardOutput).Contains("pending: 2");
        await Assert.That(status.StandardOutput).Contains("next-client-sequence: 3");
        await Assert.That(status.StandardOutput).Contains("last-reading: 22.25");
        await Assert.That(status.StandardOutput).Contains("local receipt persisted before any server acknowledgement");
    }

    /// <summary>Verifies at-most-once lost-response ambiguity is terminal and restart-visible.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAtMostOnceAttemptLosesResponse_ThenReopenShowsAmbiguityAndDoesNotRetry()
    {
        using var database = ExampleDatabase.Create();
        var append = await RunAsync(new AppendReadingCommand(database.Path, DeviceId, AtMostOnceReading, DeliveryGuarantee.AtMostOnce));
        var attempt = await RunAsync(new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.LostResponse));
        var reopen = await RunAsync(new InspectCommand(database.Path, InspectView.Pending));
        var retry = await RunAsync(new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.LostResponse));

        await Assert.That(attempt.StandardOutput).Contains("state: Ambiguous");
        await Assert.That(reopen.StandardOutput).Contains("state=Ambiguous");
        await Assert.That(retry.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(retry.StandardError).Contains("AtMostOnce ambiguous operations are not retried");
    }

    /// <summary>Verifies the deterministic demo uses and cleans only an owned temporary directory.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDemoRuns_ThenItCleansTheOwnedTemporaryDirectory()
    {
        var result = await DurableOutboxApplication.RunAsync(["--demo"], CancellationToken.None);

        await Assert.That(result.ExitCode).IsEqualTo(0);
        await Assert.That(result.StandardOutput).Contains("demo database cleaned:");
        await Assert.That(result.StandardOutput).Contains("owned directory removed: True");
    }

    /// <summary>Verifies unsupported exactly-once claims fail before local mutation.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenExactlyOnceIsRequested_ThenApplicationRejectsUnsupportedCombination()
    {
        using var database = ExampleDatabase.Create();
        var result = await RunAsync(new AppendReadingCommand(database.Path, DeviceId, ExactlyOnceReading, DeliveryGuarantee.ExactlyOnce));
        var inspect = await RunAsync(new InspectCommand(database.Path, InspectView.Status));

        await Assert.That(result.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(result.StandardError)
            .Contains("ExactlyOnce effect requires a server idempotency ledger and atomic apply-plus-ack");
        await Assert.That(inspect.StandardOutput).Contains("pending: 0");
    }

    /// <summary>Verifies the bounded subscription view reuses the durable subscription identity after reopen.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSubscribeCommandRuns_ThenItPrintsBoundedRecoveredSubscriptionEntries()
    {
        using var database = ExampleDatabase.Create();
        _ = await RunAsync(new AppendReadingCommand(database.Path, DeviceId, SubscriptionReading, DeliveryGuarantee.AtLeastOnce));
        var result = await RunAsync(new InspectCommand(database.Path, InspectView.Subscription, Take: 2));

        await Assert.That(result.ExitCode).IsEqualTo(0);
        await Assert.That(result.StandardOutput).Contains("subscription-entry: 1");
        await Assert.That(result.StandardOutput).Contains("local-reading-count=1");
        await Assert.That(result.StandardOutput).DoesNotContain("subscription-entry: 3");
    }

    /// <summary>Verifies rejected work is removed from the optimistic snapshot without dropping later local work.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenHeadOperationIsRejected_ThenReopenShowsSnapshotRebuiltWithoutRejectedReading()
    {
        using var database = ExampleDatabase.Create();
        var first = await RunAsync(new AppendReadingCommand(database.Path, DeviceId, RejectedReading, DeliveryGuarantee.AtLeastOnce));
        _ = await RunAsync(new AppendReadingCommand(database.Path, DeviceId, RetainedReading, DeliveryGuarantee.AtLeastOnce));

        var rejected = await RunAsync(new SimulateAttemptCommand(database.Path, first.OperationId, SimulatedAttemptOutcome.Rejected));
        var snapshot = await RunAsync(new InspectCommand(database.Path, InspectView.Snapshot));

        await Assert.That(rejected.ExitCode).IsEqualTo(0);
        await Assert.That(rejected.StandardOutput).Contains("state: Rejected");
        await Assert.That(snapshot.StandardOutput).Contains(SingleReadingCountOutput);
        await Assert.That(snapshot.StandardOutput).Contains("last-reading: 25");
        await Assert.That(snapshot.StandardOutput).Contains("total-reading: 25");
    }

    /// <summary>Verifies rejection fails closed when recovered pending work has no durable snapshot.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRejectedOperationHasNoSnapshot_ThenApplicationFailsClosedBeforeApplyingResult()
    {
        using var database = ExampleDatabase.Create();
        StoreAdapterObserver observer = new();
        DurableOutboxApplication application = new(
            CreateClock(),
            (databasePath, options) => new ObservingStoreAdapter(
                new SqliteLocalStoreAdapter(databasePath, options),
                observer,
                recoveryFactory: RecoverWithoutSnapshotAsync));
        var append = await RunAsync(
            application,
            new AppendReadingCommand(database.Path, DeviceId, FirstReading, DeliveryGuarantee.AtLeastOnce));

        var result = await RunAsync(
            application,
            new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.Rejected));

        await Assert.That(result.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(result.OperationId).IsEqualTo(append.OperationId);
        await Assert.That(result.StandardError).Contains(MissingSnapshotMessage);
        await Assert.That(observer.ApplyResultCalls).IsEqualTo(0);
    }

    /// <summary>Verifies rejection fails closed when a direct durable-store seed lacks an authoritative checkpoint.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRejectedOperationHasNoAuthoritativeSnapshot_ThenApplicationFailsClosedBeforeApplyingResult()
    {
        using var database = ExampleDatabase.Create();
        var serializer = CreateAppSerializer();
        SyncOperation first;
        await using (SqliteLocalStoreAdapter adapter = new(database.Path))
        {
            await adapter.InitializeAsync(
                new(StoreIdentity, StoreSchemaVersion, RequireAuthenticatedEncryptionAtRest: false) { ClientId = StoreClientId },
                CancellationToken.None);
            _ = await adapter.GetOrCreateSubscriptionIdAsync(
                DurableOutboxApplication.TemperatureStream,
                preferredId: null,
                CancellationToken.None);
            first = await CreateDirectStoreOperationAsync(serializer, FirstClientSequence, RejectedReading);
            _ = await adapter.CommitLocalOperationAsync(
                first,
                await CreateDirectStoreSnapshotAsync(serializer, OneReadingCount, RejectedReading, RejectedReading, EmptySnapshotRevision),
                CancellationToken.None);
            var second = await CreateDirectStoreOperationAsync(serializer, SecondClientSequence, RetainedReading);
            _ = await adapter.CommitLocalOperationAsync(
                second,
                await CreateDirectStoreSnapshotAsync(
                    serializer,
                    TwoReadingCount,
                    RetainedReading,
                    RejectedReading + RetainedReading,
                    FirstSnapshotRevision),
                CancellationToken.None);
        }

        StoreAdapterObserver observer = new();
        DurableOutboxApplication application = new(
            CreateClock(),
            (databasePath, options) => new ObservingStoreAdapter(
                new SqliteLocalStoreAdapter(databasePath, options),
                observer));

        var rejected = await RunAsync(
            application,
            new SimulateAttemptCommand(database.Path, first.OperationId, SimulatedAttemptOutcome.Rejected));
        var status = await RunAsync(application, new InspectCommand(database.Path, InspectView.Status, OperationId: first.OperationId));
        var snapshot = await RunAsync(application, new InspectCommand(database.Path, InspectView.Snapshot));

        await Assert.That(rejected.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(rejected.StandardError).Contains(MissingAuthoritativeSnapshotMessage);
        await Assert.That(rejected.StandardOutput).DoesNotContain("state: Rejected");
        await Assert.That(observer.ApplyResultCalls).IsEqualTo(0);
        await Assert.That(status.StandardOutput).Contains("pending: 2");
        await Assert.That(snapshot.StandardOutput).Contains("reading-count: 2");
        await Assert.That(snapshot.StandardOutput).Contains("last-reading: 25");
        await Assert.That(snapshot.StandardOutput).Contains("total-reading: 45");
    }

    /// <summary>Verifies a failed open disposes the SQLite adapter so a later open can acquire ownership.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenOpenFailsBecauseClientIdentityDiffers_ThenLaterOpenDoesNotSeeLeakedOwnership()
    {
        using var database = ExampleDatabase.Create();
        await using (var adapter = new SqliteLocalStoreAdapter(database.Path))
        {
            await adapter.InitializeAsync(
                new(StoreIdentity, StoreSchemaVersion, false) { ClientId = "different-client" },
                CancellationToken.None);
        }

        var firstFailure = await RunAsync(new InspectCommand(database.Path, InspectView.Status));
        var secondFailure = await RunAsync(new InspectCommand(database.Path, InspectView.Status));

        await Assert.That(firstFailure.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(firstFailure.StandardError).Contains("bound to another client identity");
        await Assert.That(secondFailure.StandardError).DoesNotContain("already owned");
    }

    /// <summary>Verifies asynchronously completed failed-open cleanup preserves the original open failure.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenOpenFailureCleanupCompletesAsynchronously_ThenApplicationReportsOriginalOpenFailure()
    {
        using var database = ExampleDatabase.Create();
        DurableOutboxApplication application = new(
            CreateClock(),
            static (databasePath, options) => new ThrowingDisposeOnInitializeFailureStoreAdapter(
                new SqliteLocalStoreAdapter(databasePath, options),
                failDispose: false));

        var result = await RunAsync(application, new InspectCommand(database.Path, InspectView.Status));

        await Assert.That(result.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(result.StandardError).Contains(OpenFailureMessage);
        await Assert.That(result.StandardError).DoesNotContain(DisposeFailureMessage);
    }

    /// <summary>Verifies a synchronous failed-open dispose exception cannot replace the original open failure.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenOpenFailureCleanupThrowsSynchronously_ThenApplicationReportsOriginalOpenFailure()
    {
        using var database = ExampleDatabase.Create();
        SqliteLocalStoreAdapter? inner = null;
        try
        {
            DurableOutboxApplication application = new(
                CreateClock(),
                (databasePath, options) =>
                {
                    var adapter = new SqliteLocalStoreAdapter(databasePath, options);
                    inner = adapter;
                    return new ObservingStoreAdapter(
                        new ThrowingDisposeOnInitializeFailureStoreAdapter(adapter, failDispose: false),
                        new(),
                        disposeFactory: CreateSynchronousDisposeFailure);
                });

            var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
                () => new InspectCommand(database.Path, InspectView.Status)
                    .ExecuteAsync(application, CancellationToken.None)
                    .AsTask());
            var stackTrace = exception?.StackTrace ?? string.Empty;

            await Assert.That(exception?.Message).IsEqualTo(OpenFailureMessage);
            await Assert.That(stackTrace).Contains(nameof(ThrowingDisposeOnInitializeFailureStoreAdapter.InitializeAsync));
            await Assert.That(stackTrace).DoesNotContain(nameof(CreateSynchronousDisposeFailure));
        }
        finally
        {
            if (inner is not null)
            {
                await inner.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>Verifies failed-open cleanup cannot replace the original open failure shown to the user.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenOpenFailureCleanupFails_ThenApplicationReportsOriginalOpenFailure()
    {
        using var database = ExampleDatabase.Create();
        DurableOutboxApplication application = new(
            CreateClock(),
            static (databasePath, options) => new ThrowingDisposeOnInitializeFailureStoreAdapter(
                new SqliteLocalStoreAdapter(databasePath, options)));

        var result = await RunAsync(application, new InspectCommand(database.Path, InspectView.Status));

        await Assert.That(result.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(result.StandardError).Contains(OpenFailureMessage);
        await Assert.That(result.StandardError).DoesNotContain(DisposeFailureMessage);
    }

    /// <summary>Verifies terminal operation status remains inspectable after reopen.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAcceptedOperationIsInspectedAfterReopen_ThenTerminalReceiptIsPrintedById()
    {
        using var database = ExampleDatabase.Create();
        var append = await RunAsync(
            new AppendReadingCommand(database.Path, DeviceId, TerminalReceiptReading, DeliveryGuarantee.AtLeastOnce));
        _ = await RunAsync(new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.Accepted));

        var status = await RunAsync(new InspectCommand(database.Path, InspectView.Status, OperationId: append.OperationId));

        await Assert.That(status.ExitCode).IsEqualTo(0);
        await Assert.That(status.StandardOutput).Contains($"operation-status: operation={append.OperationId.Value}");
        await Assert.That(status.StandardOutput).Contains(SynchronizedStatusText);
    }

    /// <summary>Verifies missing retained operation status is still printed as an explicit unknown receipt.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenMissingOperationStatusIsInspected_ThenUnknownStatusIsPrintedById()
    {
        using var database = ExampleDatabase.Create();
        OperationId missing = new(Guid.NewGuid());

        var status = await RunAsync(new InspectCommand(database.Path, InspectView.Status, OperationId: missing));

        await Assert.That(status.ExitCode).IsEqualTo(0);
        await Assert.That(status.StandardOutput).Contains($"operation-status: operation={missing.Value} state=unknown attempt=0");
        await Assert.That(status.StandardOutput).DoesNotContain("reason=");
    }

    /// <summary>Verifies pending inspect output remains diagnostic when a composed store loses a status row.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenPendingStatusIsMissingDuringInspect_ThenUnknownPendingStatusIsPrinted()
    {
        using var database = ExampleDatabase.Create();
        DurableOutboxApplication application = new(
            CreateClock(),
            static (databasePath, options) => new ObservingStoreAdapter(
                new SqliteLocalStoreAdapter(databasePath, options),
                new(),
                statusFactory: static (_, _, cancellationToken) =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    return new((SyncOperationStatus?)null);
                }));
        var append = await RunAsync(
            application,
            new AppendReadingCommand(database.Path, DeviceId, FirstReading, DeliveryGuarantee.AtLeastOnce));

        var status = await RunAsync(application, new InspectCommand(database.Path, InspectView.Pending));

        await Assert.That(status.ExitCode).IsEqualTo(0);
        await Assert.That(status.StandardOutput)
            .Contains($"pending-operation: operation={append.OperationId.Value} sequence=1 guarantee=AtLeastOnce state=unknown attempt=0");
        await Assert.That(status.StandardOutput).DoesNotContain("reason=");
    }

    /// <summary>Verifies a terminal receipt is not treated as pending work during a later local simulation.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenTerminalOperationIsSimulatedAgain_ThenCommandReportsMissingPendingOperation()
    {
        using var database = ExampleDatabase.Create();
        var append = await RunAsync(
            new AppendReadingCommand(database.Path, DeviceId, TerminalReceiptReading, DeliveryGuarantee.AtLeastOnce));
        _ = await RunAsync(new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.Accepted));

        var retry = await RunAsync(new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.Accepted));

        await Assert.That(retry.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(retry.OperationId).IsEqualTo(append.OperationId);
        await Assert.That(retry.StandardError).Contains(MissingPendingOperationMessage);
    }

    /// <summary>Verifies rejecting later work preserves earlier synchronized authoritative state.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAcceptedReadingPrecedesRejectedReading_ThenReopenKeepsAcceptedSnapshot()
    {
        using var database = ExampleDatabase.Create();
        var accepted = await RunAsync(
            new AppendReadingCommand(database.Path, DeviceId, AcceptedBeforeRejectReading, DeliveryGuarantee.AtLeastOnce));
        _ = await RunAsync(new SimulateAttemptCommand(database.Path, accepted.OperationId, SimulatedAttemptOutcome.Accepted));
        var rejected = await RunAsync(
            new AppendReadingCommand(database.Path, DeviceId, LaterRejectedReading, DeliveryGuarantee.AtLeastOnce));

        _ = await RunAsync(new SimulateAttemptCommand(database.Path, rejected.OperationId, SimulatedAttemptOutcome.Rejected));
        var snapshot = await RunAsync(new InspectCommand(database.Path, InspectView.Snapshot));

        await Assert.That(snapshot.ExitCode).IsEqualTo(0);
        await Assert.That(snapshot.StandardOutput).Contains(SingleReadingCountOutput);
        await Assert.That(snapshot.StandardOutput).Contains("last-reading: 31");
        await Assert.That(snapshot.StandardOutput).Contains("total-reading: 31");
    }

    /// <summary>Verifies the sample refuses to lease later operations before earlier durable work is drained.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLaterOperationIsRequestedBeforeEarlierOperation_ThenLeaseOrderingIsPreserved()
    {
        using var database = ExampleDatabase.Create();
        var earlier = await RunAsync(new AppendReadingCommand(database.Path, DeviceId, FirstReading, DeliveryGuarantee.AtLeastOnce));
        var later = await RunAsync(new AppendReadingCommand(database.Path, DeviceId, SecondReading, DeliveryGuarantee.AtLeastOnce));

        var blocked = await RunAsync(new SimulateAttemptCommand(database.Path, later.OperationId, SimulatedAttemptOutcome.Accepted));
        var firstAccepted = await RunAsync(
            new SimulateAttemptCommand(database.Path, earlier.OperationId, SimulatedAttemptOutcome.Accepted));
        var secondAccepted = await RunAsync(
            new SimulateAttemptCommand(database.Path, later.OperationId, SimulatedAttemptOutcome.Accepted));
        var status = await RunAsync(new InspectCommand(database.Path, InspectView.Status));

        await Assert.That(blocked.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(blocked.StandardError).Contains("The requested operation is not the next leased operation");
        await Assert.That(firstAccepted.StandardOutput).Contains(SynchronizedOutputText);
        await Assert.That(secondAccepted.StandardOutput).Contains(SynchronizedOutputText);
        await Assert.That(status.StandardOutput).Contains("pending: 0");
    }

    /// <summary>Verifies an empty operation id selects the next pending durable operation.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSimulationOmitsOperationId_ThenNextPendingOperationIsSelected()
    {
        using var database = ExampleDatabase.Create();
        var earlier = await RunAsync(
            new AppendReadingCommand(database.Path, DeviceId, FirstReading, DeliveryGuarantee.AtLeastOnce));
        var later = await RunAsync(
            new AppendReadingCommand(database.Path, DeviceId, SecondReading, DeliveryGuarantee.AtLeastOnce));

        var accepted = await RunAsync(
            new SimulateAttemptCommand(database.Path, default, SimulatedAttemptOutcome.Accepted));
        var earlierStatus = await RunAsync(
            new InspectCommand(database.Path, InspectView.Status, OperationId: earlier.OperationId));
        var laterStatus = await RunAsync(
            new InspectCommand(database.Path, InspectView.Status, OperationId: later.OperationId));

        await Assert.That(accepted.ExitCode).IsEqualTo(0);
        await Assert.That(accepted.OperationId).IsEqualTo(earlier.OperationId);
        await Assert.That(earlierStatus.StandardOutput).Contains(SynchronizedStatusText);
        await Assert.That(laterStatus.StandardOutput).Contains("state=QueuedForUpload");
    }

    /// <summary>Verifies at-least-once operations can retry after a lost response and controlled lease expiry.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAtLeastOnceResponseIsLostAndLeaseExpires_ThenRetryCanSynchronize()
    {
        using var database = ExampleDatabase.Create();
        DateTimeOffset start = new(ClockStartYear, ClockStartMonth, ClockStartDay, 0, 0, 0, TimeSpan.Zero);
        FakeTimeProvider clock = new(start);
        DurableOutboxApplication application = new(clock);
        var append = await RunAsync(
            application,
            new AppendReadingCommand(database.Path, DeviceId, FirstReading, DeliveryGuarantee.AtLeastOnce));
        var lost = await RunAsync(
            application,
            new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.LostResponse));
        clock.Advance(TimeSpan.FromMinutes(LeaseExpiryAdvanceMinutes));

        var accepted = await RunAsync(
            application,
            new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.Accepted));
        var status = await RunAsync(
            application,
            new InspectCommand(database.Path, InspectView.Status, OperationId: append.OperationId));

        await Assert.That(lost.StandardOutput).Contains(AttemptOneOutputText);
        await Assert.That(accepted.ExitCode).IsEqualTo(0);
        await Assert.That(accepted.StandardOutput).Contains("attempt: 2");
        await Assert.That(status.StandardOutput).Contains(SynchronizedStatusText);
    }

    /// <summary>Verifies at-least-once ambiguity is durable but waits for the current lease before retry.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAtLeastOnceResponseIsLostBeforeLeaseExpires_ThenRetryReportsActiveLease()
    {
        using var database = ExampleDatabase.Create();
        var clock = CreateClock();
        DurableOutboxApplication application = new(clock);
        var append = await RunAsync(
            application,
            new AppendReadingCommand(database.Path, DeviceId, FirstReading, DeliveryGuarantee.AtLeastOnce));
        var lost = await RunAsync(
            application,
            new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.LostResponse));

        var retry = await RunAsync(
            application,
            new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.Accepted));

        await Assert.That(lost.StandardOutput).Contains("state: Uploading");
        await Assert.That(lost.StandardOutput).Contains("reason: OC.AttemptAmbiguous");
        await Assert.That(lost.StandardOutput).Contains("server acknowledgement: not recorded");
        await Assert.That(retry.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(retry.OperationId).IsEqualTo(append.OperationId);
        await Assert.That(retry.StandardError).Contains(LeaseUnavailableMessage);
    }

    /// <summary>Verifies lost-response output preserves the receipt when status disappears after the barrier.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenLostResponseStatusDisappearsAfterBarrier_ThenUnknownStateReceiptIsPrinted()
    {
        using var database = ExampleDatabase.Create();
        StoreAdapterObserver observer = new();
        DurableOutboxApplication application = new(
            CreateClock(),
            (databasePath, options) => new ObservingStoreAdapter(
                new SqliteLocalStoreAdapter(databasePath, options),
                observer,
                (inner, operationId, cancellationToken) => observer.BeginAttemptCalls == 0
                    ? inner.GetOperationStatusAsync(operationId, cancellationToken)
                    : new((SyncOperationStatus?)null)));
        var append = await RunAsync(
            application,
            new AppendReadingCommand(database.Path, DeviceId, FirstReading, DeliveryGuarantee.AtLeastOnce));

        var lost = await RunAsync(
            application,
            new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.LostResponse));

        await Assert.That(lost.ExitCode).IsEqualTo(0);
        await Assert.That(lost.OperationId).IsEqualTo(append.OperationId);
        await Assert.That(lost.StandardOutput).Contains(AttemptOneOutputText);
        await Assert.That(lost.StandardOutput).Contains("state: unknown");
        await Assert.That(lost.StandardOutput).Contains("reason: OC.AttemptAmbiguous");
    }

    /// <summary>Verifies accepted-response output preserves the receipt when terminal status disappears after apply.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAcceptedStatusDisappearsAfterApply_ThenUnknownStateReceiptIsPrinted()
    {
        using var database = ExampleDatabase.Create();
        StoreAdapterObserver observer = new();
        DurableOutboxApplication application = new(
            CreateClock(),
            (databasePath, options) => new ObservingStoreAdapter(
                new SqliteLocalStoreAdapter(databasePath, options),
                observer,
                (inner, operationId, cancellationToken) => observer.ApplyResultCalls == 0
                    ? inner.GetOperationStatusAsync(operationId, cancellationToken)
                    : new((SyncOperationStatus?)null)));
        var append = await RunAsync(
            application,
            new AppendReadingCommand(database.Path, DeviceId, FirstReading, DeliveryGuarantee.AtLeastOnce));

        var accepted = await RunAsync(
            application,
            new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.Accepted));

        await Assert.That(accepted.ExitCode).IsEqualTo(0);
        await Assert.That(accepted.OperationId).IsEqualTo(append.OperationId);
        await Assert.That(accepted.StandardOutput).Contains(AttemptOneOutputText);
        await Assert.That(accepted.StandardOutput).Contains("state: unknown");
    }

    /// <summary>Verifies a durable barrier denial is surfaced when another sender wins the same attempt race.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAttemptBarrierIsDenied_ThenApplicationReleasesLeaseAndReportsStoreReason()
    {
        using var database = ExampleDatabase.Create();
        AttemptPreemption preemption = new();
        DurableOutboxApplication application = new(
            CreateClock(),
            (databasePath, options) => new PreemptingAttemptStoreAdapter(
                new SqliteLocalStoreAdapter(databasePath, options),
                preemption));
        var append = await RunAsync(
            application,
            new AppendReadingCommand(database.Path, DeviceId, FirstReading, DeliveryGuarantee.AtLeastOnce));

        var denied = await RunAsync(
            application,
            new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.Accepted));
        var retry = await RunAsync(
            application,
            new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.Accepted));

        await Assert.That(denied.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(denied.OperationId).IsEqualTo(append.OperationId);
        await Assert.That(denied.StandardError).Contains("The durable store denied the send attempt: OC.AttemptNotAdvanced");
        await Assert.That(retry.ExitCode).IsEqualTo(0);
        await Assert.That(retry.StandardOutput).Contains("attempt: 2");
        await Assert.That(retry.StandardOutput).Contains(SynchronizedOutputText);
    }

    /// <summary>Verifies a store denial without a durable reason still preserves the operation receipt.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenAttemptBarrierIsDeniedWithoutReason_ThenUnknownReasonIsPrinted()
    {
        using var database = ExampleDatabase.Create();
        DurableOutboxApplication application = new(
            CreateClock(),
            static (databasePath, options) => new ObservingStoreAdapter(
                new SqliteLocalStoreAdapter(databasePath, options),
                new(),
                attemptFactory: static (_, _, operationId, nextAttempt, cancellationToken) =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    return new(new AttemptBarrierResult(operationId, nextAttempt, MaySend: false, ReasonCode: null));
                }));
        var append = await RunAsync(
            application,
            new AppendReadingCommand(database.Path, DeviceId, FirstReading, DeliveryGuarantee.AtLeastOnce));

        var denied = await RunAsync(
            application,
            new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.Accepted));

        await Assert.That(denied.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(denied.OperationId).IsEqualTo(append.OperationId);
        await Assert.That(denied.StandardError).Contains("The durable store denied the send attempt: unknown");
    }

    /// <summary>Verifies recovered pending work without durable status fails closed before recording a send barrier.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenPendingOperationStatusIsMissing_ThenSimulationFailsClosedBeforeAttemptBarrier()
    {
        using var database = ExampleDatabase.Create();
        StoreAdapterObserver observer = new();
        DurableOutboxApplication application = new(
            CreateClock(),
            (databasePath, options) => new ObservingStoreAdapter(
                new SqliteLocalStoreAdapter(databasePath, options),
                observer,
                statusFactory: static (_, _, cancellationToken) =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    return new((SyncOperationStatus?)null);
                }));
        var append = await RunAsync(
            application,
            new AppendReadingCommand(database.Path, DeviceId, FirstReading, DeliveryGuarantee.AtLeastOnce));

        var result = await RunAsync(
            application,
            new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.Accepted));

        await Assert.That(result.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(result.OperationId).IsEqualTo(append.OperationId);
        await Assert.That(result.StandardError).Contains(MissingDurableStatusMessage);
        await Assert.That(observer.BeginAttemptCalls).IsEqualTo(0);
    }

    /// <summary>Verifies an exhausted durable attempt counter fails closed before wrapping to a negative attempt.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenDurableAttemptCounterIsExhausted_ThenSimulationFailsClosedBeforeAttemptBarrier()
    {
        using var database = ExampleDatabase.Create();
        StoreAdapterObserver observer = new();
        DurableOutboxApplication application = new(
            CreateClock(),
            (databasePath, options) => new ObservingStoreAdapter(
                new SqliteLocalStoreAdapter(databasePath, options),
                observer,
                statusFactory: static (_, operationId, cancellationToken) =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    return new(CreateExhaustedAttemptStatus(operationId));
                }));
        var append = await RunAsync(
            application,
            new AppendReadingCommand(database.Path, DeviceId, FirstReading, DeliveryGuarantee.AtLeastOnce));

        var result = await RunAsync(
            application,
            new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.Accepted));

        await Assert.That(result.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(result.OperationId).IsEqualTo(append.OperationId);
        await Assert.That(result.StandardError).Contains(AttemptOverflowMessage);
        await Assert.That(observer.BeginAttemptCalls).IsEqualTo(0);
    }

    /// <summary>Verifies terminal receipts free bounded pending capacity for later durable work.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenTerminalReceiptExists_ThenPendingCapacityAllowsNewWorkAfterReopen()
    {
        using var database = ExampleDatabase.Create();
        var first = await RunAsync(new AppendReadingCommand(database.Path, DeviceId, FirstReading, DeliveryGuarantee.AtLeastOnce));
        _ = await RunAsync(new AppendReadingCommand(database.Path, DeviceId, SecondReading, DeliveryGuarantee.AtLeastOnce));
        _ = await RunAsync(new AppendReadingCommand(database.Path, DeviceId, ThirdCapacityReading, DeliveryGuarantee.AtLeastOnce));
        _ = await RunAsync(new AppendReadingCommand(database.Path, DeviceId, FourthCapacityReading, DeliveryGuarantee.AtLeastOnce));
        var full = await RunAsync(new AppendReadingCommand(database.Path, DeviceId, FifthCapacityReading, DeliveryGuarantee.AtLeastOnce));

        _ = await RunAsync(new SimulateAttemptCommand(database.Path, first.OperationId, SimulatedAttemptOutcome.Accepted));
        var admitted = await RunAsync(
            new AppendReadingCommand(database.Path, DeviceId, FifthCapacityReading, DeliveryGuarantee.AtLeastOnce));
        var status = await RunAsync(new InspectCommand(database.Path, InspectView.Status));

        await Assert.That(full.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(full.StandardError).Contains("capacity is 4");
        await Assert.That(admitted.ExitCode).IsEqualTo(0);
        await Assert.That(status.StandardOutput).Contains("pending: 4");
    }

    /// <summary>Verifies missing operations are reported without mutating durable state.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSimulateAttemptFindsNoPendingOperation_ThenCommandReportsMissingOperation()
    {
        using var database = ExampleDatabase.Create();
        OperationId missing = new(Guid.NewGuid());

        var emptyDefault = await RunAsync(new SimulateAttemptCommand(database.Path, default, SimulatedAttemptOutcome.Accepted));
        var missingExplicit = await RunAsync(new SimulateAttemptCommand(database.Path, missing, SimulatedAttemptOutcome.Rejected));

        await Assert.That(emptyDefault.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(emptyDefault.StandardError).Contains(MissingPendingOperationMessage);
        await Assert.That(missingExplicit.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(missingExplicit.StandardError).Contains(MissingPendingOperationMessage);
    }

    /// <summary>Verifies a non-ambiguous status cannot substitute for missing recovered pending work.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRecoveredOperationIsNotPendingButStatusExists_ThenCommandReportsMissingOperation()
    {
        using var database = ExampleDatabase.Create();
        DurableOutboxApplication application = new(
            CreateClock(),
            static (databasePath, options) => new ObservingStoreAdapter(
                new SqliteLocalStoreAdapter(databasePath, options),
                new(),
                statusFactory: static (_, operationId, cancellationToken) =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    return new(new SyncOperationStatus(
                        operationId,
                        DurableOutboxApplication.TemperatureStream,
                        SyncOperationState.QueuedForUpload,
                        0,
                        DateTimeOffset.UnixEpoch,
                        ReasonCode: null));
                },
                recoveryFactory: static async (inner, streamId, subscriptionId, cancellationToken) =>
                {
                    var recovered = await inner.RecoverStreamAsync(streamId, subscriptionId, cancellationToken)
                        .ConfigureAwait(false);
                    return new(
                        recovered.SubscriptionId,
                        recovered.ServerCursor,
                        recovered.Snapshot,
                        [],
                        recovered.DeadLetters,
                        recovered.NextClientSequence) { ReplayOperations = recovered.ReplayOperations };
                }));
        var append = await RunAsync(
            application,
            new AppendReadingCommand(database.Path, DeviceId, FirstReading, DeliveryGuarantee.AtLeastOnce));

        var result = await RunAsync(
            application,
            new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.Accepted));

        await Assert.That(result.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(result.OperationId).IsEqualTo(append.OperationId);
        await Assert.That(result.StandardError).Contains(MissingPendingOperationMessage);
    }

    /// <summary>Verifies ambiguous status fallback preserves the at-most-once ambiguity guard.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRecoveredOperationIsMissingButStatusIsAmbiguous_ThenAmbiguityIsReported()
    {
        using var database = ExampleDatabase.Create();
        DurableOutboxApplication application = new(
            CreateClock(),
            static (databasePath, options) => new ObservingStoreAdapter(
                new SqliteLocalStoreAdapter(databasePath, options),
                new(),
                statusFactory: static (_, operationId, cancellationToken) =>
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    return new(new SyncOperationStatus(
                        operationId,
                        DurableOutboxApplication.TemperatureStream,
                        SyncOperationState.Ambiguous,
                        1,
                        DateTimeOffset.UnixEpoch,
                        ReasonCode: "OC.AttemptAmbiguous"));
                },
                recoveryFactory: static async (inner, streamId, subscriptionId, cancellationToken) =>
                {
                    var recovered = await inner.RecoverStreamAsync(streamId, subscriptionId, cancellationToken)
                        .ConfigureAwait(false);
                    return new(
                        recovered.SubscriptionId,
                        recovered.ServerCursor,
                        recovered.Snapshot,
                        [],
                        recovered.DeadLetters,
                        recovered.NextClientSequence) { ReplayOperations = recovered.ReplayOperations };
                }));
        var append = await RunAsync(
            application,
            new AppendReadingCommand(database.Path, DeviceId, FirstReading, DeliveryGuarantee.AtLeastOnce));

        var result = await RunAsync(
            application,
            new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.Accepted));

        await Assert.That(result.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(result.OperationId).IsEqualTo(append.OperationId);
        await Assert.That(result.StandardError).Contains("AtMostOnce ambiguous operations are not retried");
    }

    /// <summary>Verifies an unexpired durable lease prevents a second local simulation from acquiring the same operation.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenOperationIsAlreadyLeased_ThenSimulationReportsNoLeaseAvailable()
    {
        using var database = ExampleDatabase.Create();
        var clock = CreateClock();
        DurableOutboxApplication application = new(clock);
        var append = await RunAsync(
            application,
            new AppendReadingCommand(database.Path, DeviceId, FirstReading, DeliveryGuarantee.AtLeastOnce));
        await LeaseFirstPendingOperationAsync(database.Path, clock);

        var blocked = await RunAsync(
            application,
            new SimulateAttemptCommand(database.Path, append.OperationId, SimulatedAttemptOutcome.Accepted));

        await Assert.That(blocked.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(blocked.StandardError).Contains(LeaseUnavailableMessage);
    }

    /// <summary>Verifies invalid direct inspect options are converted to process-style command failures.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenInspectTakeIsInvalid_ThenApplicationReturnsCommandFailure()
    {
        using var database = ExampleDatabase.Create();

        var result = await RunAsync(new InspectCommand(database.Path, InspectView.Subscription, Take: 0));

        await Assert.That(result.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(result.StandardError).Contains("The subscription take count must be positive.");
    }

    /// <summary>Verifies expected command exceptions are converted to process-style failures.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenCommandThrowsExpectedException_ThenApplicationReturnsCommandFailure()
    {
        var unsupported = await RunAsync(new ThrowingCommand(new NotSupportedException("unsupported sample path")));
        var invalidArgument = await RunAsync(new ThrowingCommand(new ArgumentException("invalid sample argument")));

        await Assert.That(unsupported.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(unsupported.StandardError).Contains("unsupported sample path");
        await Assert.That(invalidArgument.ExitCode).IsEqualTo(InvalidCommandExitCode);
        await Assert.That(invalidArgument.StandardError).Contains("invalid sample argument");
    }
}
