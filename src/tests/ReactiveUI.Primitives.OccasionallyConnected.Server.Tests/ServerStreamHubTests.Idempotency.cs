// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Operation idempotency tests for <see cref="ServerStreamHub"/>.</summary>
public sealed partial class ServerStreamHubTests
{
    /// <summary>The batch identifier used when a client retries an operation in a new batch.</summary>
    private static readonly Guid RetryBatchId = Guid.Parse("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee");

    /// <summary>The bounded wait applied to every subscription move in public-flow tests.</summary>
    private static readonly TimeSpan MoveTimeout = TimeSpan.FromSeconds(10);

    /// <summary>Verifies an in-memory hub returns the original result for a repeated operation identifier.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ApplyOperationsAsyncInMemoryReturnsOriginalResultForRepeatedOperationId()
    {
        var domain = new RecordingDomainHandler();
        await using var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), domain));
        var operation = Operation(1, PayloadA);

        var first = await hub.ApplyOperationsAsync(Batch(operation), new(Tenant, Client), CancellationToken.None);
        var sameBatch = await hub.ApplyOperationsAsync(Batch(operation), new(Tenant, Client), CancellationToken.None);
        var retryBatch = await hub.ApplyOperationsAsync(new(RetryBatchId, [operation]), new(Tenant, Client), CancellationToken.None);

        await Assert.That(first.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(first.Result.Operations[0].ServerVersion).IsEqualTo(FirstVersion);
        await Assert.That(sameBatch.Result.Operations[0]).IsEqualTo(first.Result.Operations[0]);
        await Assert.That(retryBatch.Result.Operations[0]).IsEqualTo(first.Result.Operations[0]);
        await Assert.That(retryBatch.Result.BatchId).IsEqualTo(RetryBatchId);
        await Assert.That(domain.CallCount).IsEqualTo(SingleCount);
        await AssertSingleCanonicalEventAsync(hub, operation.OperationId);
    }

    /// <summary>Verifies a SQLite hub returns the original result for a repeated operation identifier after reopening.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ApplyOperationsAsyncSqliteReturnsOriginalResultForRepeatedOperationIdAfterReopen()
    {
        using var database = new SqliteLease();
        var operation = Operation(1, PayloadA);
        var firstDomain = new RecordingDomainHandler();
        ServerSyncResult first;
        await using (var firstHub = ServerStreamHub.CreateSqlite(database.Path, Options(new AllowPolicy(Tenant), firstDomain)))
        {
            first = await firstHub.ApplyOperationsAsync(Batch(operation), new(Tenant, Client), CancellationToken.None);
            var sameHubRetry = await firstHub.ApplyOperationsAsync(Batch(operation), new(Tenant, Client), CancellationToken.None);
            await Assert.That(sameHubRetry.Result.Operations[0]).IsEqualTo(first.Result.Operations[0]);
        }

        var reopenedDomain = new RecordingDomainHandler();
        await using var reopened = ServerStreamHub.CreateSqlite(database.Path, Options(new AllowPolicy(Tenant), reopenedDomain));
        var replayed = await reopened.ApplyOperationsAsync(new(RetryBatchId, [operation]), new(Tenant, Client), CancellationToken.None);

        await Assert.That(first.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(replayed.Result.Operations[0]).IsEqualTo(first.Result.Operations[0]);
        await Assert.That(firstDomain.CallCount).IsEqualTo(SingleCount);
        await Assert.That(reopenedDomain.CallCount).IsEqualTo(0);
        await AssertSingleCanonicalEventAsync(reopened, operation.OperationId);
    }

    /// <summary>Verifies a repeated operation identifier with different content does not create a second effect.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ApplyOperationsAsyncRejectsRepeatedOperationIdWithDifferentPayloadWithoutEffects()
    {
        var domain = new RecordingDomainHandler();
        await using var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), domain));
        var operation = Operation(1, PayloadA);
        _ = await hub.ApplyOperationsAsync(Batch(operation), new(Tenant, Client), CancellationToken.None);

        var changed = await hub.ApplyOperationsAsync(
            new(RetryBatchId, [operation with { Payload = Payload(PayloadB) }]),
            new(Tenant, Client),
            CancellationToken.None);

        await Assert.That(changed.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Rejected);
        await Assert.That(changed.ProducedEvents).IsEmpty();
        await Assert.That(domain.CallCount).IsEqualTo(SingleCount);
        await AssertSingleCanonicalEventAsync(hub, operation.OperationId);
    }

    /// <summary>Reads the whole stream from the start and asserts it holds exactly one canonical event.</summary>
    /// <param name="hub">The hub.</param>
    /// <param name="operationId">The operation expected to have caused the event.</param>
    /// <returns>A task that represents the asynchronous assertion.</returns>
    private static async Task AssertSingleCanonicalEventAsync(ServerStreamHub hub, OperationId operationId)
    {
        var page = await ReadFirstBatchAsync(hub, new(Tenant, Client), SubscriptionId.New());

        await Assert.That(page.Events).Count().IsEqualTo(SingleCount);
        await Assert.That(page.Events[0].CausedByOperationId).IsEqualTo(operationId);
        await Assert.That(page.CompletedOperations).Count().IsEqualTo(SingleCount);
    }

    /// <summary>Moves a subscription enumerator under a bounded wait.</summary>
    /// <param name="enumerator">The subscription enumerator.</param>
    /// <returns>The next page.</returns>
    /// <exception cref="InvalidOperationException">The subscription completed without a page.</exception>
    private static async Task<RemoteEventBatch> NextPageAsync(IAsyncEnumerator<RemoteEventBatch> enumerator)
    {
        var hasPage = await enumerator.MoveNextAsync().AsTask().WaitAsync(MoveTimeout);
        return hasPage ? enumerator.Current : throw new InvalidOperationException("The subscription completed before the expected page.");
    }
}
