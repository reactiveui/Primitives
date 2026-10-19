// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Last-writer-wins conflict tests for <see cref="ServerStreamHub"/>.</summary>
public sealed partial class ServerStreamHubTests
{
    /// <summary>A client whose identifier sorts before <see cref="Client"/>.</summary>
    private const string EarlierClient = "client-0";

    /// <summary>A client whose identifier sorts after <see cref="Client"/>.</summary>
    private const string LaterClient = "client-z";

    /// <summary>The second committed server version produced by the test version factory.</summary>
    private const string CanonicalSecondVersion = "v2";

    /// <summary>The stale-write reason documented by <see cref="LastWriterWinsResolver"/>.</summary>
    private const string StaleWriteReason = "lww-stale-write";

    /// <summary>The third operation seed.</summary>
    private const int ThirdOperationSeed = 3;

    /// <summary>The expected event count after two accepted writes.</summary>
    private const int TwoEvents = 2;

    /// <summary>Verifies a stale write that loses the trusted write order is rejected with the canonical server version.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ApplyOperationsAsyncRejectsLosingStaleWriteWithCanonicalVersionDeterministically()
    {
        var domain = new RecordingDomainHandler();
        await using var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), domain));
        var winner = LastWriterWinsOperation(1, PayloadA);
        var stale = LastWriterWinsOperation(SecondOperationSeed, PayloadB);
        _ = await hub.ApplyOperationsAsync(Batch(winner), new(Tenant, Client), CancellationToken.None);

        var rejected = await hub.ApplyOperationsAsync(Batch(stale), new(Tenant, EarlierClient), CancellationToken.None);
        var repeated = await hub.ApplyOperationsAsync(new(RetryBatchId, [stale]), new(Tenant, EarlierClient), CancellationToken.None);

        var result = rejected.Result.Operations[0];
        await Assert.That(result.Kind).IsEqualTo(OperationResultKind.Rejected);
        await Assert.That(result.ReasonCode).IsEqualTo(StaleWriteReason);
        await Assert.That(result.ServerVersion).IsEqualTo(FirstVersion);
        await Assert.That(rejected.ProducedEvents).IsEmpty();
        await Assert.That(repeated.Result.Operations[0]).IsEqualTo(result);
        await Assert.That(domain.CallCount).IsEqualTo(SingleCount);
        await AssertSingleCanonicalEventAsync(hub, winner.OperationId);
    }

    /// <summary>Verifies a stale write that wins the trusted write order is accepted once with the next canonical version.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ApplyOperationsAsyncAcceptsWinningStaleWriteOnceWithNextCanonicalVersion()
    {
        var domain = new RecordingDomainHandler();
        await using var hub = ServerStreamHub.CreateInMemory(Options(new AllowPolicy(Tenant), domain));
        var first = LastWriterWinsOperation(1, PayloadA);
        var stale = LastWriterWinsOperation(SecondOperationSeed, PayloadB);
        _ = await hub.ApplyOperationsAsync(Batch(first), new(Tenant, Client), CancellationToken.None);

        var accepted = await hub.ApplyOperationsAsync(Batch(stale), new(Tenant, LaterClient), CancellationToken.None);
        var repeated = await hub.ApplyOperationsAsync(new(RetryBatchId, [stale]), new(Tenant, LaterClient), CancellationToken.None);

        var result = accepted.Result.Operations[0];
        await Assert.That(result.Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(result.ServerVersion).IsEqualTo(CanonicalSecondVersion);
        await Assert.That(repeated.Result.Operations[0]).IsEqualTo(result);
        await Assert.That(domain.CallCount).IsEqualTo(TwoEvents);
        var page = await ReadFirstBatchAsync(hub, new(Tenant, Client), SubscriptionId.New());
        await Assert.That(page.Events).Count().IsEqualTo(TwoEvents);
        await Assert.That(page.Events[1].CausedByOperationId).IsEqualTo(stale.OperationId);
    }

    /// <summary>Verifies a rejected stale write keeps its terminal result after the stream advances and the SQLite hub reopens.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ApplyOperationsAsyncSqliteKeepsRejectedStaleWriteResultAfterStreamAdvancesAndReopen()
    {
        using var database = new SqliteLease();
        var stale = LastWriterWinsOperation(SecondOperationSeed, PayloadB);
        OperationSyncResult rejected;
        await using (var hub = ServerStreamHub.CreateSqlite(database.Path, Options(new AllowPolicy(Tenant), new RecordingDomainHandler())))
        {
            _ = await hub.ApplyOperationsAsync(Batch(LastWriterWinsOperation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
            rejected = (await hub.ApplyOperationsAsync(Batch(stale), new(Tenant, EarlierClient), CancellationToken.None)).Result.Operations[0];
            var advance = LastWriterWinsOperation(ThirdOperationSeed, PayloadA) with { ClientSequence = SecondOperationSeed, BaseVersion = FirstVersion };
            var advanced = await hub.ApplyOperationsAsync(Batch(advance), new(Tenant, Client), CancellationToken.None);
            await Assert.That(advanced.Result.Operations[0].ServerVersion).IsEqualTo(CanonicalSecondVersion);
        }

        var domain = new RecordingDomainHandler();
        await using var reopened = ServerStreamHub.CreateSqlite(database.Path, Options(new AllowPolicy(Tenant), domain));
        var repeated = await reopened.ApplyOperationsAsync(new(RetryBatchId, [stale]), new(Tenant, EarlierClient), CancellationToken.None);

        await Assert.That(rejected.Kind).IsEqualTo(OperationResultKind.Rejected);
        await Assert.That(rejected.ServerVersion).IsEqualTo(FirstVersion);
        await Assert.That(repeated.Result.Operations[0]).IsEqualTo(rejected);
        await Assert.That(domain.CallCount).IsEqualTo(0);
    }

    /// <summary>Creates a last-writer-wins operation whose base version is the initial version.</summary>
    /// <param name="seed">The deterministic operation seed.</param>
    /// <param name="payload">The payload text.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation LastWriterWinsOperation(int seed, string payload) =>
        Operation(seed, payload) with
        {
            ClientSequence = 1,
            BaseVersion = InitialVersion,
            Policy = OperationPolicy.Default with { ConflictPolicy = ConflictPolicy.LastWriterWins },
        };
}
