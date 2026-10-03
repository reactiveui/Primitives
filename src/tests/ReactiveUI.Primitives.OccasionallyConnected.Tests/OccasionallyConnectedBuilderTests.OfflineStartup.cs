// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.IO;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Offline startup tests for contexts composed by <see cref="OccasionallyConnectedBuilder"/>.</content>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>The short retry delay used by real-clock offline startup tests.</summary>
    private static readonly TimeSpan OfflineStartupRetryDelay = TimeSpan.FromMilliseconds(50);

    /// <summary>Verifies a context starts offline, commits locally, reconnects, and synchronizes the offline write.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ContextStartsOfflineCommitsLocallyAndSynchronizesAfterReconnect()
    {
        var databasePath = Path.Combine(SqliteTestDirectory.Create("oc-offline-start-").FullName, RecoveredUploadDatabaseFileName);
        await using var store = new SqliteLocalStoreAdapter(databasePath);
        await using var transport = new RecoveredUploadTransportAdapter();
        transport.ConnectFailures.Enqueue(new IOException("endpoint unreachable"));
        transport.ConnectFailures.Enqueue(new IOException("endpoint still unreachable"));
        var states = new RecoveredUploadDiagnosticObserver<SyncState>();
        await using var context = CreateReadyBuilder(store, transport)
            .ConfigureOptions(static options => options with
            {
                Retry = options.Retry with { MinimumDelay = OfflineStartupRetryDelay, MaximumDelay = OfflineStartupRetryDelay },
            })
            .Build();
        using var stateSubscription = context.SyncStates.Subscribe(states);
        var stream = context.GetOrCreateStream(CreateDefinition());

        await context.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        var receipt = await stream.PublishAsync(new(1), null, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await context.SyncEngine.AwaitSynchronizedAsync(receipt.OperationId, GuardTimeout, TimeProvider.System, CancellationToken.None);

        var status = await store.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);
        await Assert.That(receipt.State).IsEqualTo(SyncOperationState.SavedLocally);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Synchronized);
        await Assert.That(transport.ConnectFailures).IsEmpty();
        await WaitForConditionAsync(() => states.Values.Exists(static state => state.Status == SyncLifecycleStatus.Online));
    }
}
