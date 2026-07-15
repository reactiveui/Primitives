// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Tests for <see cref="FromAsyncSubscription{T}"/>'s synchronous-completion path when the external
/// cancellation token forwards a terminal error while the task factory is still running: the subscription is
/// already completed by the time the completed task is inspected, so it forwards nothing further.
/// </summary>
public sealed class FromAsyncSubscriptionTests
{
    /// <summary>The value produced by the task factory that completes synchronously.</summary>
    private const int FactoryValue = 11;

    /// <summary>
    /// Verifies that when the external token is cancelled while the factory runs, the forwarded cancellation is
    /// the only notification and the already-complete task the factory returned is not forwarded a second time.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ExternalCancellationDuringTheFactoryForwardsCancellationAndDropsTheCompletedResult()
    {
        using CancellationTokenSource externalCancellation = new();
        RecordingWitness<int> witness = new();

        FromAsyncSubscription<int> subscription = new(
            witness,
            _ =>
            {
                externalCancellation.Cancel();
                return Task.FromResult(FactoryValue);
            },
            externalCancellation.Token);

        using var handle = subscription.Start();

        await Assert.That(witness.Values.Count).IsEqualTo(0);
        await Assert.That(witness.Completed).IsEqualTo(0);
        await Assert.That(witness.Errors.Count).IsEqualTo(1);
        await Assert.That(witness.Errors[0]).IsTypeOf<TaskCanceledException>();
    }
}
