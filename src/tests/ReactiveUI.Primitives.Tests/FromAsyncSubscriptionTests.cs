// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for the synchronous-completion path of <see cref="FromAsyncSubscription{T}"/>.</summary>
public sealed class FromAsyncSubscriptionTests
{
    /// <summary>The value produced by the task factory that completes synchronously.</summary>
    private const int FactoryValue = 11;

    /// <summary>Cancellation raised while the factory runs is the only notification, and the task result is dropped.</summary>
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
