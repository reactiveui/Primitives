// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Replay paging tests for <see cref="ServerStreamHub"/>.</summary>
public sealed partial class ServerStreamHubTests
{
    /// <summary>The number of operations published by paging tests.</summary>
    private const int PagedOperationCount = 5;

    /// <summary>The number of pages read before the mid-stream acknowledgement.</summary>
    private const int PagesBeforeRestart = 3;

    /// <summary>The number of pages acknowledged before the restart.</summary>
    private const int AcknowledgedPages = 2;

    /// <summary>The payload length used by logical-byte paging tests.</summary>
    private const int LargePayloadLength = 1000;

    /// <summary>A receive budget that fits one large event but never three.</summary>
    private const long LargeEventPageBudget = 2500;

    /// <summary>The maximum events a large-payload page may carry under the budget.</summary>
    private const int MaximumLargeEventsPerPage = 2;

    /// <summary>The minimum pages needed to deliver all large-payload events under the budget.</summary>
    private const int MinimumLargeEventPages = 3;

    /// <summary>Verifies a SQLite hub pages replay, resumes from the acknowledged cursor after reopen and delivers every event once.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncSqlitePagesAndResumesFromAcknowledgedCursorAfterReopen()
    {
        using var database = new SqliteLease();
        var subscriptionId = SubscriptionId.New();
        var operations = UnconditionalOperations(PagedOperationCount, PayloadA);
        var delivered = new List<Guid>();
        string acknowledgedCursor;
        await using (var hub = ServerStreamHub.CreateSqlite(database.Path, SingleEventPageOptions()))
        {
            await PublishEachAsync(hub, operations);
            var enumerable = hub.SubscribeStreamAsync(Subscribe(subscriptionId, null), new(Tenant, Client), CancellationToken.None);
            await using var enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None);
            var pages = new List<RemoteEventBatch>();
            for (var index = 0; index < PagesBeforeRestart; index++)
            {
                pages.Add(await NextPageAsync(enumerator));
            }

            await AssertCursorChainAsync(pages, null);
            acknowledgedCursor = pages[AcknowledgedPages - 1].NextCursor;
            await hub.AcknowledgeAsync(new(subscriptionId, Stream, acknowledgedCursor), new(Tenant, Client), CancellationToken.None);
            delivered.AddRange(pages.Take(AcknowledgedPages).SelectMany(static page => page.Events).Select(static item => item.EventId));
        }

        await using var reopened = ServerStreamHub.CreateSqlite(database.Path, SingleEventPageOptions());
        var resumed = reopened.SubscribeStreamAsync(Subscribe(subscriptionId, acknowledgedCursor), new(Tenant, Client), CancellationToken.None);
        await using var resumedEnumerator = resumed.GetAsyncEnumerator(CancellationToken.None);
        var resumedPages = new List<RemoteEventBatch>();
        for (var index = AcknowledgedPages; index < PagedOperationCount; index++)
        {
            resumedPages.Add(await NextPageAsync(resumedEnumerator));
        }

        await AssertCursorChainAsync(resumedPages, acknowledgedCursor);
        delivered.AddRange(resumedPages.SelectMany(static page => page.Events).Select(static item => item.EventId));
        await reopened.AcknowledgeAsync(new(subscriptionId, Stream, resumedPages[^1].NextCursor), new(Tenant, Client), CancellationToken.None);
        await Assert.That(delivered).IsEquivalentTo([.. operations.Select(static item => item.OperationId.Value)], EqualityComparer<Guid>.Default, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    /// <summary>Verifies the receive logical-byte budget splits replay into gapless pages.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncSplitsReplayByLogicalByteBudgetWithoutGaps()
    {
        await using var hub = ServerStreamHub.CreateInMemory(
            Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with { MaximumReceiveLogicalBytes = LargeEventPageBudget });
        SyncOperation[] operations =
        [
            .. UnconditionalOperations(PagedOperationCount, PayloadA).Select(static item => item with { Payload = LargePayload() }),
        ];
        await PublishEachAsync(hub, operations);
        var enumerable = hub.SubscribeStreamAsync(Subscribe(SubscriptionId.New(), null), new(Tenant, Client), CancellationToken.None);
        await using var enumerator = enumerable.GetAsyncEnumerator(CancellationToken.None);
        var pages = new List<RemoteEventBatch>();
        var delivered = new List<Guid>();

        while (delivered.Count < PagedOperationCount && pages.Count < PagedOperationCount)
        {
            var page = await NextPageAsync(enumerator);
            pages.Add(page);
            delivered.AddRange(page.Events.Select(static item => item.EventId));
        }

        await Assert.That(pages.Count).IsGreaterThanOrEqualTo(MinimumLargeEventPages);
        await Assert.That(pages.Max(static page => page.Events.Count)).IsLessThanOrEqualTo(MaximumLargeEventsPerPage);
        await Assert.That(pages.Min(static page => page.Events.Count)).IsGreaterThanOrEqualTo(SingleCount);
        await AssertCursorChainAsync(pages, null);
        await Assert.That(delivered).IsEquivalentTo([.. operations.Select(static item => item.OperationId.Value)], EqualityComparer<Guid>.Default, TUnit.Assertions.Enums.CollectionOrdering.Matching);
    }

    /// <summary>Asserts every page links to the previous page cursor.</summary>
    /// <param name="pages">The pages in delivery order.</param>
    /// <param name="initialCursor">The cursor the first page must continue from.</param>
    /// <returns>A task that represents the asynchronous assertion.</returns>
    private static async Task AssertCursorChainAsync(List<RemoteEventBatch> pages, string? initialCursor)
    {
        var previous = initialCursor;
        foreach (var page in pages)
        {
            await Assert.That(page.StreamId).IsEqualTo(Stream);
            await Assert.That(page.PreviousCursor).IsEqualTo(previous);
            await Assert.That(page.NextCursor).IsNotEqualTo(previous);
            previous = page.NextCursor;
        }
    }

    /// <summary>Publishes each operation in its own batch.</summary>
    /// <param name="hub">The hub.</param>
    /// <param name="operations">The operations.</param>
    /// <returns>A task that represents the asynchronous publish.</returns>
    private static async Task PublishEachAsync(ServerStreamHub hub, IReadOnlyList<SyncOperation> operations)
    {
        foreach (var operation in operations)
        {
            var result = await hub.ApplyOperationsAsync(
                new(operation.OperationId.Value, [operation]),
                new(Tenant, Client),
                CancellationToken.None);
            await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        }
    }

    /// <summary>Creates unconditional operations with increasing client sequences.</summary>
    /// <param name="count">The number of operations.</param>
    /// <param name="payload">The payload text.</param>
    /// <returns>The operations.</returns>
    private static SyncOperation[] UnconditionalOperations(int count, string payload) =>
        [.. Enumerable.Range(1, count).Select(seed => Operation(seed, payload) with { BaseVersion = null })];

    /// <summary>Creates a payload whose body dominates the receive logical-byte accounting.</summary>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope LargePayload() =>
        new(Contract, 1, ContentType, new byte[LargePayloadLength], PayloadA);

    /// <summary>Creates hub options that deliver one event per receive page.</summary>
    /// <returns>The hub options.</returns>
    private static ServerStreamHubOptions SingleEventPageOptions() =>
        Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with { MaximumReceiveEvents = SingleCount };

    /// <summary>Creates a subscribe request that replays from the start of the stream.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="cursor">The resume cursor.</param>
    /// <returns>The subscribe request.</returns>
    private static RemoteSubscribeRequest Subscribe(SubscriptionId subscriptionId, string? cursor) =>
        new(Stream, subscriptionId, cursor, StartPosition.FromSequence(0));
}
