// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpReplayRetentionBudget"/>.</summary>
public sealed class HttpReplayRetentionBudgetTests
{
    /// <summary>The single retained byte count.</summary>
    private const long SingleByte = 1;

    /// <summary>The double retained byte count.</summary>
    private const long DoubleByte = 2;

    /// <summary>The larger retained byte count.</summary>
    private const long LargerByteCount = 4;

    /// <summary>The zero retained byte count.</summary>
    private const long ZeroBytes = 0;

    /// <summary>The negative retained byte count.</summary>
    private const long NegativeBytes = -1;

    /// <summary>The bounded update/dispose race attempt count.</summary>
    private const int RaceAttempts = 32;

    /// <summary>Verifies non-positive retained byte limits are rejected.</summary>
    /// <param name="maximumBytes">The invalid retained byte limit.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(ZeroBytes)]
    [Arguments(NegativeBytes)]
    public async Task ConstructorRejectsNonPositiveMaximum(long maximumBytes) =>
        await Assert.That(() => new HttpReplayRetentionBudget(maximumBytes)).ThrowsExactly<ArgumentOutOfRangeException>();

    /// <summary>Verifies negative reservations are rejected before retained byte state changes.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReserveRejectsNegativeByteCountAndPreservesRetainedBytes()
    {
        HttpReplayRetentionBudget budget = new(SingleByte);

        await Assert.That(() => budget.Reserve(NegativeBytes)).ThrowsExactly<ArgumentOutOfRangeException>();

        await Assert.That(budget.RetainedBytes).IsEqualTo(ZeroBytes);
    }

    /// <summary>Verifies insufficient remaining capacity is reported without releasing existing owners.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReserveRejectsCapacityOverflowAndPreservesExistingLease()
    {
        HttpReplayRetentionBudget budget = new(DoubleByte);
        using var lease = budget.Reserve(DoubleByte);

        await Assert.That(() => budget.Reserve(SingleByte)).ThrowsExactly<HttpRemoteTransportException>();

        await Assert.That(budget.RetainedBytes).IsEqualTo(DoubleByte);
    }

    /// <summary>Verifies disposing a retained-byte lease repeatedly releases its bytes once.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task LeaseDisposeIsIdempotentAndReleasesRetainedBytesOnce()
    {
        HttpReplayRetentionBudget budget = new(DoubleByte);
        var lease = budget.Reserve(DoubleByte);

        lease.Dispose();
        lease.Dispose();

        await Assert.That(budget.RetainedBytes).IsEqualTo(ZeroBytes);
    }

    /// <summary>Verifies transferred reservations keep retained bytes until the retained lease is disposed.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReservationTransferKeepsRetainedBytesUntilTransferredLeaseDisposes()
    {
        HttpReplayRetentionBudget budget = new(DoubleByte);
        var reservation = budget.Reserve(DoubleByte);
        var lease = reservation.Transfer();

        reservation.Dispose();
        await Assert.That(budget.RetainedBytes).IsEqualTo(DoubleByte);

        lease.Dispose();
        lease.Dispose();

        await Assert.That(budget.RetainedBytes).IsEqualTo(ZeroBytes);
    }

    /// <summary>Verifies transferred reservations cannot transfer the retained lease again.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ReservationTransferRejectsRepeatedTransfer()
    {
        HttpReplayRetentionBudget budget = new(SingleByte);
        using var reservation = budget.Reserve(SingleByte);
        using var lease = reservation.Transfer();

        await Assert.That(() => reservation.Transfer()).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies an existing lease can grow and shrink its reservation before disposal.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task UpdateGrowsAndShrinksExistingLeaseReservation()
    {
        HttpReplayRetentionBudget budget = new(LargerByteCount);
        using var lease = budget.Reserve(DoubleByte);

        budget.Update(lease, LargerByteCount);

        await Assert.That(budget.RetainedBytes).IsEqualTo(LargerByteCount);

        budget.Update(lease, SingleByte);

        await Assert.That(budget.RetainedBytes).IsEqualTo(SingleByte);
    }

    /// <summary>Verifies a lease cannot grow into capacity consumed after it shrank.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task UpdateRejectsGrowthWhenReleasedCapacityWasReused()
    {
        HttpReplayRetentionBudget budget = new(LargerByteCount);
        using var shrinkingLease = budget.Reserve(DoubleByte);

        budget.Update(shrinkingLease, SingleByte);
        using var consumingLease = budget.Reserve(LargerByteCount - SingleByte);

        await Assert.That(() => budget.Update(shrinkingLease, DoubleByte)).ThrowsExactly<HttpRemoteTransportException>();
        await Assert.That(budget.RetainedBytes).IsEqualTo(LargerByteCount);
    }

    /// <summary>Verifies negative lease updates are rejected without changing retained bytes.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task UpdateRejectsNegativeByteCountAndPreservesRetainedBytes()
    {
        HttpReplayRetentionBudget budget = new(DoubleByte);
        using var lease = budget.Reserve(SingleByte);

        await Assert.That(() => budget.Update(lease, NegativeBytes)).ThrowsExactly<ArgumentOutOfRangeException>();

        await Assert.That(budget.RetainedBytes).IsEqualTo(SingleByte);
    }

    /// <summary>Verifies a lease from another budget cannot mutate this budget.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task UpdateRejectsForeignLeaseAndPreservesBothBudgets()
    {
        HttpReplayRetentionBudget owner = new(DoubleByte);
        HttpReplayRetentionBudget foreign = new(DoubleByte);
        using var lease = owner.Reserve(SingleByte);

        await Assert.That(() => foreign.Update(lease, DoubleByte)).ThrowsExactly<ArgumentException>();

        await Assert.That(owner.RetainedBytes).IsEqualTo(SingleByte);
        await Assert.That(foreign.RetainedBytes).IsEqualTo(ZeroBytes);
    }

    /// <summary>Verifies updating a disposed lease cannot resurrect retained bytes.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task UpdateAfterDisposePreservesReleasedReservation()
    {
        HttpReplayRetentionBudget budget = new(LargerByteCount);
        var lease = budget.Reserve(SingleByte);

        lease.Dispose();
        await Assert.That(budget.RetainedBytes).IsEqualTo(ZeroBytes);

        await Assert.That(() => budget.Update(lease, LargerByteCount)).ThrowsExactly<ObjectDisposedException>();

        await Assert.That(budget.RetainedBytes).IsEqualTo(ZeroBytes);
    }

    /// <summary>Verifies racing an update with disposal cannot leave retained bytes behind.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConcurrentUpdateAndDisposeLeaveFinalRetainedBytesAtZero()
    {
        for (var attempt = 0; attempt < RaceAttempts; attempt++)
        {
            HttpReplayRetentionBudget budget = new(LargerByteCount);
            var lease = budget.Reserve(SingleByte);
            var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var update = Task.Run(async () =>
            {
                await start.Task.ConfigureAwait(false);
                try
                {
                    budget.Update(lease, LargerByteCount);
                }
                catch (ObjectDisposedException)
                {
                    return;
                }
            });
            var dispose = Task.Run(async () =>
            {
                await start.Task.ConfigureAwait(false);
                lease.Dispose();
            });

            start.SetResult();
            await Task.WhenAll(update, dispose);

            await Assert.That(budget.RetainedBytes).IsEqualTo(ZeroBytes);
        }
    }
}
