// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="ServerStreamHub"/>.</summary>
/// <content>Proves one publish wakes all subscribers without advancing a poll timer.</content>
public sealed partial class ServerStreamHubTests
{
    /// <summary>The number of subscribers that must wake in one publish epoch.</summary>
    private const int BroadcastSubscriberCount = 3;

    /// <summary>The bounded test wait without firing a poll timer.</summary>
    private const int BroadcastWaitSeconds = 5;

    /// <summary>The elapsed ticks needed to expire an idle subscription binding.</summary>
    private const int IdleBindingElapsedTicks = 2;

    /// <summary>Verifies a replaced idle Latest binding reports a gap instead of silently skipping the waking event.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task SubscribeStreamAsyncReportsGapAfterIdleLatestBindingIsReplaced()
    {
        using var database = new SqliteLease();
        var clock = new BroadcastTimeProvider(Start, SingleCount);
        var options = Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
        {
            TimeProvider = clock,
            EmptyPollDelay = TimeSpan.FromMinutes(LongPollMinutes),
            JournalLimits = new ServerCommitJournalLimits { SubscriptionRetention = TimeSpan.FromTicks(SingleCount) },
        };
        await using var hub = ServerStreamHub.CreateSqlite(database.Path, options);
        await using var enumerator = hub.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.Latest),
            new(Tenant, Client),
            CancellationToken.None).GetAsyncEnumerator();
        var pending = enumerator.MoveNextAsync().AsTask();
        await clock.AllWaiting;
        clock.SetUtcNow(Start.AddTicks(IdleBindingElapsedTicks));
        using (var journal = new SqliteServerCommitJournal(
            database.Path,
            new() { TimeProvider = clock, SubscriptionRetention = TimeSpan.FromTicks(SingleCount) }))
        {
            _ = journal.Compact();
        }

        _ = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
        _ = await Assert.ThrowsExactlyAsync<RemoteSubscriptionRetentionGapException>(
            () => pending.WaitAsync(TimeSpan.FromSeconds(BroadcastWaitSeconds)));
        await Assert.That(enumerator.Current.Events).IsEmpty();
    }

    /// <summary>Verifies all idle subscribers receive an immediate broadcast in the same timer epoch.</summary>
    /// <param name="sqlite">Whether to use the durable journal.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SubscribeStreamAsyncBroadcastsPublishToEveryIdleSubscriber(bool sqlite)
    {
        using var database = new SqliteLease();
        var clock = new BroadcastTimeProvider(Start, BroadcastSubscriberCount);
        var options = Options(new AllowPolicy(Tenant), new RecordingDomainHandler()) with
        {
            TimeProvider = clock,
            EmptyPollDelay = TimeSpan.FromMinutes(LongPollMinutes),
            MaximumActiveCalls = BroadcastSubscriberCount,
        };
        await using var hub = sqlite ? ServerStreamHub.CreateSqlite(database.Path, options) : ServerStreamHub.CreateInMemory(options);
        var enumerators = Enumerable.Range(0, BroadcastSubscriberCount)
            .Select(_ => hub.SubscribeStreamAsync(
                new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
                new(Tenant, Client),
                CancellationToken.None).GetAsyncEnumerator())
            .ToArray();
        try
        {
            var moves = enumerators.Select(static enumerator => enumerator.MoveNextAsync().AsTask()).ToArray();
            await clock.AllWaiting;
            _ = await hub.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(Tenant, Client), CancellationToken.None);
            var results = await Task.WhenAll(moves).WaitAsync(TimeSpan.FromSeconds(BroadcastWaitSeconds));
            foreach (var result in results)
            {
                await Assert.That(result).IsTrue();
            }

            foreach (var enumerator in enumerators)
            {
                await Assert.That(enumerator.Current.Events).Count().IsEqualTo(1);
            }
        }
        finally
        {
            foreach (var enumerator in enumerators)
            {
                await enumerator.DisposeAsync();
            }
        }
    }

    /// <summary>Signals when all subscribers wait, but never fires poll timers.</summary>
    /// <param name="utcNow">The journal clock.</param>
    /// <param name="subscriberCount">The expected idle subscribers.</param>
    private sealed class BroadcastTimeProvider(DateTimeOffset utcNow, int subscriberCount) : TimeProvider
    {
        /// <summary>Completes after all empty-poll timers are installed.</summary>
        private readonly TaskCompletionSource<bool> _waiting = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The created timer count.</summary>
        private int _timers;

        /// <summary>The controlled journal clock.</summary>
        private DateTimeOffset _utcNow = utcNow;

        /// <summary>Gets the deterministic wait signal.</summary>
        internal Task AllWaiting => _waiting.Task;

        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow() => _utcNow;

        /// <inheritdoc/>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            if (Interlocked.Increment(ref _timers) == subscriberCount)
            {
                _ = _waiting.TrySetResult(true);
            }

            return new FixedTimer();
        }

        /// <summary>Advances the journal clock while its timers remain paused.</summary>
        /// <param name="value">The next journal time.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetUtcNow(DateTimeOffset value) => _utcNow = value;
    }
}
