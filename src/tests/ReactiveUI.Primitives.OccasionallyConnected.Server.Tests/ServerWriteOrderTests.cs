// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests the deterministic order of competing server writes.</summary>
public sealed class ServerWriteOrderTests
{
    /// <summary>The first operation identifier.</summary>
    private static readonly OperationId FirstOperation = new(Guid.Parse("00000000-0000-0000-0000-000000000001"));

    /// <summary>The last operation identifier.</summary>
    private static readonly OperationId LastOperation = new(Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff"));

    /// <summary>Verifies a later server timestamp wins despite lower client and operation identifiers.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task LaterServerCommitWinsBeforeIdentityTieBreakers()
    {
        var committed = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var current = new ServerWriteStamp(committed, "z", LastOperation);
        var candidate = new ServerWriteStamp(committed.AddTicks(1), "a", FirstOperation);

        await Assert.That(ServerWriteOrder.IsNewer(candidate, current)).IsTrue();
        await Assert.That(ServerWriteOrder.IsNewer(candidate: current, current: candidate)).IsFalse();
    }

    /// <summary>Verifies equal timestamps use ordinal client order before operation identifiers.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task EqualCommitTimesUseOrdinalClientIdentity()
    {
        var current = new ServerWriteStamp(DateTimeOffset.UnixEpoch, "Z", LastOperation);
        var candidate = new ServerWriteStamp(DateTimeOffset.UnixEpoch, "a", FirstOperation);

        await Assert.That(ServerWriteOrder.IsNewer(candidate, current)).IsTrue();
        await Assert.That(ServerWriteOrder.IsNewer(candidate: current, current: candidate)).IsFalse();
    }

    /// <summary>Verifies an identical timestamp and client are resolved by operation identity.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SameClientAndTimeUseOperationIdentity()
    {
        var current = new ServerWriteStamp(DateTimeOffset.UnixEpoch, "client", FirstOperation);
        var candidate = current with { OperationId = LastOperation };

        await Assert.That(ServerWriteOrder.IsNewer(candidate, current)).IsTrue();
        await Assert.That(ServerWriteOrder.IsNewer(candidate: current, current: candidate)).IsFalse();
        await Assert.That(ServerWriteOrder.IsNewer(current, current)).IsFalse();
    }

    /// <summary>Verifies time zone offsets cannot change the winner for an identical instant.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task EqualInstantsWithDifferentOffsetsHaveTheSameOrder()
    {
        var current = new ServerWriteStamp(DateTimeOffset.UnixEpoch, "client", FirstOperation);
        var candidate = current with { CommittedAtUtc = current.CommittedAtUtc.ToOffset(TimeSpan.FromHours(1)) };

        await Assert.That(ServerWriteOrder.IsNewer(candidate, current)).IsFalse();
        await Assert.That(ServerWriteOrder.IsNewer(candidate: current, current: candidate)).IsFalse();
        await Assert.That(ServerWriteOrder.IsNewer(candidate with { OperationId = LastOperation }, current)).IsTrue();
    }

    /// <summary>Verifies all permutations converge to the same winner across clients and timestamp ties.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CompetingWritesConvergeRegardlessOfComparisonOrder()
    {
        var first = new ServerWriteStamp(DateTimeOffset.UnixEpoch, "z", LastOperation);
        var second = new ServerWriteStamp(DateTimeOffset.UnixEpoch.AddTicks(1), "a", FirstOperation);
        var winner = second with { OperationId = LastOperation };
        ServerWriteStamp[][] permutations =
        [
            [first, second, winner],
            [first, winner, second],
            [second, first, winner],
            [second, winner, first],
            [winner, first, second],
            [winner, second, first],
        ];

        foreach (var permutation in permutations)
        {
            var selected = permutation[0];
            foreach (var stamp in permutation)
            {
                if (ServerWriteOrder.IsNewer(stamp, selected))
                {
                    selected = stamp;
                }
            }

            await Assert.That(selected).IsEqualTo(winner);
        }
    }
}
