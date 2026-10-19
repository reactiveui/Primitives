// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <content>Tests manual receive-retry time during lost-ACK proof collection.</content>
public sealed partial class DurableHttpLostAckScenarioTests
{
    /// <summary>The late receive retry delay.</summary>
    private const int ReceiveRetryMilliseconds = 200;

    /// <summary>The bounded observation wait.</summary>
    private const int ClockGuardSeconds = 5;

    /// <summary>Verifies a receive retry registered after writer retry time still wakes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ObserverConvergenceAdvancesLateReceiveRetryTimer()
    {
        var clock = new DurableHttpLostAckScenario.MutableTimeProvider(DateTimeOffset.UnixEpoch);
        var observed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var timer = clock.CreateTimer(
            _ => observed.TrySetResult(),
            null,
            TimeSpan.FromMilliseconds(ReceiveRetryMilliseconds),
            Timeout.InfiniteTimeSpan);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(ClockGuardSeconds));

        await DurableHttpLostAckScenario.AdvanceClockUntilObservedAsync(observed.Task, clock, cancellation.Token);

        await Assert.That(observed.Task.IsCompletedSuccessfully).IsTrue();
        await Assert.That(clock.GetUtcNow()).IsGreaterThanOrEqualTo(DateTimeOffset.UnixEpoch.AddMilliseconds(ReceiveRetryMilliseconds));
    }

    /// <summary>Verifies scenario cancellation bounds proof waits without swallowing it.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ObserverConvergencePreservesCancellation()
    {
        var clock = new DurableHttpLostAckScenario.MutableTimeProvider(DateTimeOffset.UnixEpoch);
        var observed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();

        await Assert.That(() => DurableHttpLostAckScenario.AdvanceClockUntilObservedAsync(observed.Task, clock, cancellation.Token))
            .Throws<OperationCanceledException>();
        await Assert.That(clock.GetUtcNow()).IsEqualTo(DateTimeOffset.UnixEpoch);
    }

    /// <summary>Verifies driving receive retry time preserves a failed durable writer proof.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WriterSynchronizationPreservesDurableProofFailure()
    {
        var clock = new DurableHttpLostAckScenario.MutableTimeProvider(DateTimeOffset.UnixEpoch);
        var expected = new InvalidOperationException("The durable writer proof failed.");
        var proof = Task.FromException<ClientStoreProof>(expected);

        var failure = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => DurableHttpLostAckScenario.AdvanceClockUntilObservedAsync(proof, clock, CancellationToken.None));

        await Assert.That(failure).IsSameReferenceAs(expected);
        await Assert.That(clock.GetUtcNow()).IsEqualTo(DateTimeOffset.UnixEpoch);
    }
}
