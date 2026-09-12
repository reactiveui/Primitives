// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Verifies retry scheduling and irreversible delivery decisions.</summary>
public sealed partial class InMemoryLocalStoreAdapterTests
{
    /// <summary>Verifies a delayed stream head blocks later operations while other streams progress.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task RetryDueTimeBlocksOnlyItsStreamUntilDue()
    {
        var clock = new ManualTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = await CreateInitializedStoreAsync(clock);
        var first = await CommitOperationAsync(store, Stream, 1, "first");
        _ = await CommitOperationAsync(store, Stream, SecondClientSequence, "second");
        var other = await CommitOperationAsync(store, OtherStream, 1, "other");
        var due = clock.GetUtcNow().AddMinutes(1);
        await store.SaveRetryStateAsync(first.OperationId, RetryState.Start(clock.GetUtcNow()) with { DueUtc = due }, CancellationToken.None);
        var blocked = await LeaseSingleBatchAsync(store, new(Stream, LeaseOperationLimit, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));
        await Assert.That(blocked).IsNull();
        var available = RequireBatch(await LeaseSingleBatchAsync(store, new(null, LeaseOperationLimit, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        await Assert.That(available.Operations[0].OperationId).IsEqualTo(other.OperationId);
        clock.Advance(TimeSpan.FromMinutes(1));
        var ready = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, LeaseOperationLimit, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        await Assert.That(ready.Operations.Count).IsEqualTo(ExpectedLeasedOperationCount);
        await Assert.That(ready.Operations[0].OperationId).IsEqualTo(first.OperationId);
    }

    /// <summary>Verifies malformed retry state is rejected before storage mutation.</summary>
    /// <param name="attempts">The scheduled attempt count.</param>
    /// <param name="delayTicks">The previous delay ticks.</param>
    /// <param name="authentication">The authentication state value.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(-1, 0, 0)]
    [Arguments(0, -1, 0)]
    [Arguments(0, 0, 2)]
    public async Task InvalidRetryStateIsRejectedWithoutMutation(int attempts, long delayTicks, int authentication)
    {
        await using var store = await CreateInitializedStoreAsync();
        var operation = await CommitOperationAsync(store, Stream, 1, "local");
        var state = RetryState.Start(DateTimeOffset.UnixEpoch) with
        {
            TransientAttemptCount = attempts,
            PreviousDelay = TimeSpan.FromTicks(delayTicks),
            AuthenticationState = (RetryAuthenticationState)authentication,
        };
        Func<Task> save = async () => await store.SaveRetryStateAsync(operation.OperationId, state, CancellationToken.None);
        await Assert.That(save).Throws<ArgumentException>();
        await Assert.That(await store.GetRetryStateAsync(operation.OperationId, CancellationToken.None)).IsNull();
    }

    /// <summary>Verifies settled or conflicted operations cannot be rescheduled implicitly.</summary>
    /// <param name="kind">The server result.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(OperationResultKind.Accepted)]
    [Arguments(OperationResultKind.Rejected)]
    [Arguments(OperationResultKind.Conflict)]
    public async Task SettledOperationRejectsRetryScheduling(OperationResultKind kind)
    {
        await using var store = await CreateInitializedStoreAsync();
        var operation = await CommitOperationAsync(store, Stream, 1, "local");
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        await store.ApplySyncResultAsync(lease.LeaseId, new(lease.LeaseId, [new(operation.OperationId, kind, null, null)], null, null), CancellationToken.None);
        Func<Task> save = async () => await store.SaveRetryStateAsync(operation.OperationId, RetryState.Start(DateTimeOffset.UnixEpoch), CancellationToken.None);
        await Assert.That(save).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a retryable server response cannot re-lease an attempted at-most-once operation.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task AtMostOnceRetryableResultKeepsLaterStreamOperationsBlocked()
    {
        await using var store = await CreateInitializedStoreAsync();
        var operation = await CommitOperationAsync(store, Stream, 1, "first", OperationPolicy.Default with { DeliveryGuarantee = DeliveryGuarantee.AtMostOnce });
        _ = await CommitOperationAsync(store, Stream, SecondClientSequence, "second");
        var lease = RequireBatch(await LeaseSingleBatchAsync(store, new(Stream, 1, DefaultLeaseBytes, TimeSpan.FromMinutes(1))));
        _ = await store.TryBeginRemoteAttemptAsync(lease.LeaseId, operation.OperationId, 1, CancellationToken.None);
        await store.ApplySyncResultAsync(lease.LeaseId, new(lease.LeaseId, [new(operation.OperationId, OperationResultKind.Retryable, null, null)], null, null), CancellationToken.None);
        var next = await LeaseSingleBatchAsync(store, new(Stream, LeaseOperationLimit, DefaultLeaseBytes, TimeSpan.FromMinutes(1)));
        await Assert.That(next).IsNull();
        Func<Task> reschedule = async () => await store.SaveRetryStateAsync(operation.OperationId, RetryState.Start(DateTimeOffset.UnixEpoch), CancellationToken.None);
        await Assert.That(reschedule).ThrowsExactly<InvalidOperationException>();
    }
}
