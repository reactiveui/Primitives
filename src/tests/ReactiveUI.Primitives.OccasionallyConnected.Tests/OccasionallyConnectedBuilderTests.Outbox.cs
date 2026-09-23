// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.IO;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests public outbox configuration for <see cref="OccasionallyConnectedBuilder"/>.</summary>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>The byte budget allowing the small counter operations used by these tests.</summary>
    private const long ConfiguredOutboxBytes = 4096;

    /// <summary>A different operation limit used to detect conflicting configuration.</summary>
    private const int ConflictingOperationLimit = 2;

    /// <summary>The second stream sharing the configured outbox.</summary>
    private static readonly StreamId OtherOutboxStream = new("builder/outbox-other");

    /// <summary>Verifies public limits reject another stream before persistence when the shared outbox is full.</summary>
    /// <param name="explicitInitialization">Whether store initialization is supplied explicitly.</param>
    /// <param name="explicitLimits">Whether supplied initialization has equivalent outbox limits.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task ConfiguredOutboxRejectsAcrossStreamsBeforePersistence(bool explicitInitialization, bool explicitLimits)
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var outbox = new OutboxOptions { MaxOperations = 1, MaxBytes = ConfiguredOutboxBytes };
        var builder = CreateReadyBuilder(store, transport).UseOptions(OccasionallyConnectedOptions.Default with { Outbox = outbox });
        if (explicitInitialization)
        {
            _ = builder.UseStoreInitialization(new(StoreIdentity, 1, false) { Outbox = explicitLimits ? outbox with { } : null });
        }

        await using var context = builder.Build();
        var first = context.GetOrCreateStream(CreateDefinition());
        var second = context.GetOrCreateStream(CreateDefinition(OtherOutboxStream));
        var firstOptions = CreateVolatilePublishOptions() with { AdmissionStrategy = BufferStrategy.Reject };
        var secondOptions = firstOptions with { StreamId = OtherOutboxStream };
        var receipt = await first.PublishAsync(new(1), firstOptions, CancellationToken.None);

        await Assert.That(async () => await second.PublishAsync(new(1), secondOptions, CancellationToken.None))
            .ThrowsExactly<QueueCapacityExceededException>();

        var retained = await store.RecoverStreamAsync(Stream, first.SubscriptionId, CancellationToken.None);
        var rejected = await store.RecoverStreamAsync(OtherOutboxStream, second.SubscriptionId, CancellationToken.None);
        await Assert.That(retained.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(retained.PendingOperations[0].OperationId).IsEqualTo(receipt.OperationId);
        await Assert.That(rejected.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(rejected.NextClientSequence).IsEqualTo(1L);
        await Assert.That(rejected.Snapshot).IsNull();
        await Assert.That(store.Initialization?.Outbox).IsEqualTo(outbox);
    }

    /// <summary>Verifies conflicting store limits cannot weaken public context configuration.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task BuildRejectsConflictingExplicitOutboxLimits()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var outbox = new OutboxOptions { MaxOperations = 1, MaxBytes = ConfiguredOutboxBytes };
        var initialization = new LocalStoreInitialization(StoreIdentity, 1, false) { Outbox = outbox with { MaxOperations = ConflictingOperationLimit } };
        var builder = CreateReadyBuilder(store, transport)
            .UseOptions(OccasionallyConnectedOptions.Default with { Outbox = outbox })
            .UseStoreInitialization(initialization);
        OccasionallyConnectedContext? unexpected = null;
        try
        {
            await Assert.That(() => unexpected = builder.Build()).ThrowsExactly<InvalidOperationException>();
        }
        finally
        {
            if (unexpected is not null)
            {
                await unexpected.DisposeAsync();
            }
        }
    }

    /// <summary>Verifies a durable acknowledgement frees shared SQLite capacity for another stream's publisher.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConfiguredOutboxDurableAcknowledgementAdmitsWaitingStream()
    {
        var databasePath = Path.Combine(SqliteTestDirectory.Create("oc-shared-outbox-").FullName, RecoveredUploadDatabaseFileName);
        await using var store = new SqliteLocalStoreAdapter(databasePath);
        await using var transport = new RecoveredUploadTransportAdapter();
        var outbox = new OutboxOptions { MaxOperations = 1, MaxBytes = ConfiguredOutboxBytes };
        await using var context = CreateReadyBuilder(store, transport)
            .UseOptions(OccasionallyConnectedOptions.Default with { Outbox = outbox })
            .Build();
        var first = context.GetOrCreateStream(CreateDefinition());
        var second = context.GetOrCreateStream(CreateDefinition(OtherOutboxStream));
        var firstReceipt = await first.PublishAsync(new(1), null, CancellationToken.None);
        using CancellationTokenSource cancellation = new();
        var waiting = second.PublishAsync(new(1), null, cancellation.Token).AsTask();
        try
        {
            await Assert.That(waiting.IsCompleted).IsFalse();
            await context.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            var secondReceipt = await waiting.WaitAsync(GuardTimeout);
            await context.SyncEngine.AwaitSynchronizedAsync(firstReceipt.OperationId, GuardTimeout, TimeProvider.System, CancellationToken.None);
            await context.SyncEngine.AwaitSynchronizedAsync(secondReceipt.OperationId, GuardTimeout, TimeProvider.System, CancellationToken.None);

            var firstStatus = await store.GetOperationStatusAsync(firstReceipt.OperationId, CancellationToken.None);
            var secondStatus = await store.GetOperationStatusAsync(secondReceipt.OperationId, CancellationToken.None);
            await Assert.That(firstStatus?.State).IsEqualTo(SyncOperationState.Synchronized);
            await Assert.That(secondStatus?.State).IsEqualTo(SyncOperationState.Synchronized);
            await Assert.That(secondReceipt.OperationId).IsNotEqualTo(firstReceipt.OperationId);
            var firstRecovered = await store.RecoverStreamAsync(Stream, first.SubscriptionId, CancellationToken.None);
            var secondRecovered = await store.RecoverStreamAsync(OtherOutboxStream, second.SubscriptionId, CancellationToken.None);
            await Assert.That(firstRecovered.PendingOperations).IsEmpty();
            await Assert.That(secondRecovered.PendingOperations).IsEmpty();
        }
        finally
        {
            await cancellation.CancelAsync();
            await ObserveExpectedStartupCancellationAsync(waiting);
        }
    }
}
