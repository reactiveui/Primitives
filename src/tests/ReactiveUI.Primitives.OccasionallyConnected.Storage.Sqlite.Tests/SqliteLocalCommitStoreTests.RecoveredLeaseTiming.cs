// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalCommitStore"/>.</summary>
/// <content>Recovered pending-upload timing across durable retries and leases.</content>
public sealed partial class SqliteLocalCommitStoreTests
{
    /// <summary>The active lease deadline, in minutes after the initial clock.</summary>
    private const int RecoveredLeaseExpiryMinute = 2;

    /// <summary>The later retry deadline, in minutes after the initial clock.</summary>
    private const int RecoveredLaterRetryMinute = 3;

    /// <summary>Verifies recovery honors whichever future blocker ends last.</summary>
    /// <param name="retryMinute">The retry due minute relative to the initial clock.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    public async Task WhenRetryAndActiveLeaseOverlap_ThenRecoveryAndLeasingWaitForLaterDeadline(int retryMinute)
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        SubscriptionId subscriptionId;
        OperationId operationId;
        Guid leaseId;
        using (var store = CreateInitializedStore(database.Path, clock))
        {
            subscriptionId = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
            var operation = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText);
            operationId = operation.OperationId;
            var lease = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(RecoveredLeaseExpiryMinute))));
            leaseId = lease.LeaseId;
            await store.SaveRetryStateAsync(
                operationId,
                RetryState.Start(clock.GetUtcNow()) with { DueUtc = clock.GetUtcNow().AddMinutes(retryMinute) },
                CancellationToken.None);
        }

        var expectedNotBeforeUtc = clock.GetUtcNow().AddMinutes(Math.Max(RecoveredLeaseExpiryMinute, retryMinute));
        using var reopened = CreateInitializedStore(database.Path, clock);
        var recovered = reopened.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        var blocked = await LeaseSingleBatch(reopened, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));

        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.PendingOperations[0].OperationId).IsEqualTo(operationId);
        await Assert.That(recovered.PendingUploadNotBeforeUtc).IsEqualTo(expectedNotBeforeUtc);
        await Assert.That(blocked).IsNull();

        clock.Advance(TimeSpan.FromMinutes(Math.Max(RecoveredLeaseExpiryMinute, retryMinute)).Subtract(TimeSpan.FromTicks(1)));
        var justBefore = await LeaseSingleBatch(reopened, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));
        await Assert.That(justBefore).IsNull();

        clock.Advance(TimeSpan.FromTicks(1));
        var available = RequireBatch(await LeaseSingleBatch(reopened, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        await Assert.That(available.Operations[0].OperationId).IsEqualTo(operationId);
        await Assert.That(available.LeaseId).IsNotEqualTo(leaseId);
    }

    /// <summary>Verifies an expired lease no longer delays a future retry.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenLeaseExpiresBeforeRetry_ThenRecoveryUsesRetryDeadline()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        SubscriptionId subscriptionId;
        OperationId operationId;
        var retryDueUtc = clock.GetUtcNow().AddMinutes(RecoveredLaterRetryMinute);
        using (var store = CreateInitializedStore(database.Path, clock))
        {
            subscriptionId = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
            operationId = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText).OperationId;
            _ = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
            await store.SaveRetryStateAsync(
                operationId,
                RetryState.Start(clock.GetUtcNow()) with { DueUtc = retryDueUtc },
                CancellationToken.None);
        }

        clock.Advance(TimeSpan.FromMinutes(RecoveredLeaseExpiryMinute));

        using var reopened = CreateInitializedStore(database.Path, clock);
        var recovered = reopened.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        var blocked = await LeaseSingleBatch(reopened, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));
        await Assert.That(recovered.PendingUploadNotBeforeUtc).IsEqualTo(retryDueUtc);
        await Assert.That(blocked).IsNull();

        clock.Advance(TimeSpan.FromMinutes(1));
        var available = RequireBatch(await LeaseSingleBatch(reopened, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        await Assert.That(available.Operations[0].OperationId).IsEqualTo(operationId);
    }

    /// <summary>Verifies expired lease and elapsed retry impose no recovery delay.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenLeaseAndRetryHaveExpired_ThenRecoveryCanLeaseImmediately()
    {
        using var database = TempDatabase.Create();
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        SubscriptionId subscriptionId;
        OperationId operationId;
        using (var store = CreateInitializedStore(database.Path, clock))
        {
            subscriptionId = store.GetOrCreateSubscriptionId(Stream, null, CancellationToken.None);
            operationId = CommitOperation(store, Stream, clientSequence: 1, OperationPayloadText).OperationId;
            _ = RequireBatch(await LeaseSingleBatch(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
            await store.SaveRetryStateAsync(
                operationId,
                RetryState.Start(clock.GetUtcNow()) with { DueUtc = clock.GetUtcNow().AddMinutes(1) },
                CancellationToken.None);
        }

        clock.Advance(TimeSpan.FromMinutes(RecoveredLeaseExpiryMinute));

        using var reopened = CreateInitializedStore(database.Path, clock);
        var recovered = reopened.RecoverStream(Stream, subscriptionId, CancellationToken.None);
        var available = RequireBatch(await LeaseSingleBatch(reopened, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));

        await Assert.That(recovered.PendingUploadNotBeforeUtc).IsNull();
        await Assert.That(available.Operations[0].OperationId).IsEqualTo(operationId);
    }
}
