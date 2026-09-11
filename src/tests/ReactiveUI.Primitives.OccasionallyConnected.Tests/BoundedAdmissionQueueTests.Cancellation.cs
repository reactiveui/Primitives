// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests admission cancellation at the producer boundary.</summary>
public sealed partial class BoundedAdmissionQueueTests
{
    /// <summary>The value whose admission is cancelled.</summary>
    private const string CancelledValue = "cancelled";

    /// <summary>Verifies custom non-dropping policies can wait in producer order.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CustomBlockingPolicyPreservesDurableProducerOrder()
    {
        using var queue = new BoundedAdmissionQueue<string>(new(OneItem, OneByte, TwoBlockedProducers), static (_, _) => BoundedAdmissionDecision.Block);
        await queue.EnqueueAsync(ResidentValue, OneByte, durable: true, control: false);
        var first = queue.EnqueueAsync(FirstDataValue, OneByte, durable: true, control: false, BufferStrategy.Custom);
        var second = queue.EnqueueAsync(AdmittedValue, OneByte, durable: true, control: false, BufferStrategy.Custom);
        await Assert.That(first.IsCompleted).IsFalse();
        await Assert.That(second.IsCompleted).IsFalse();
        await Assert.That(queue.TryDequeue(out var resident)).IsTrue();
        await Assert.That(resident.Value).IsEqualTo(ResidentValue);
        await Assert.That((await first.WaitAsync(GuardTimeout)).Item.Value).IsEqualTo(FirstDataValue);
        await Assert.That(second.IsCompleted).IsFalse();
        await Assert.That(queue.TryDequeue(out var next)).IsTrue();
        await Assert.That(next.Value).IsEqualTo(FirstDataValue);
        await Assert.That((await second.WaitAsync(GuardTimeout)).Item.Value).IsEqualTo(AdmittedValue);
    }

    /// <summary>Verifies disposal releases admitted references and resets queue capacity counters.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeReleasesAdmittedCapacity()
    {
        var queue = new BoundedAdmissionQueue<string>(new(OneItem, OneByte, OneBlockedProducer));
        await queue.EnqueueAsync(ResidentValue, OneByte, durable: true, control: false);
        queue.Dispose();
        await Assert.That(queue.Count).IsEqualTo(0);
        await Assert.That(queue.Bytes).IsEqualTo(0);
    }

    /// <summary>Verifies cancellation during an unlocked custom decision cannot evict or admit work.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CancellationDuringCustomDecisionPreservesResidentWork()
    {
        using var cancellation = new CancellationTokenSource();
        using var queue = new BoundedAdmissionQueue<string>(new(OneItem, OneByte, OneBlockedProducer), (_, _) =>
        {
            cancellation.Cancel();
            return BoundedAdmissionDecision.DropOldest(OneItem);
        });
        await queue.EnqueueAsync(ResidentValue, OneByte, durable: false, control: false);
        await Assert.That(() => queue.EnqueueAsync(CancelledValue, OneByte, durable: false, control: false, BufferStrategy.Custom, cancellation.Token)).ThrowsExactly<OperationCanceledException>();
        await Assert.That(queue.TryDequeue(out var resident)).IsTrue();
        await Assert.That(resident.Value).IsEqualTo(ResidentValue);
    }

    /// <summary>Verifies cancellation prevents admission for every strategy.</summary>
    /// <param name="strategy">The configured admission strategy.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(BufferStrategy.Block)]
    [Arguments(BufferStrategy.Reject)]
    [Arguments(BufferStrategy.DropOldest)]
    [Arguments(BufferStrategy.DropNewest)]
    [Arguments(BufferStrategy.Custom)]
    public async Task PreCancelledPublishNeverConsumesCapacity(BufferStrategy strategy)
    {
        using var queue = new BoundedAdmissionQueue<string>(new(OneItem, OneByte, OneBlockedProducer), static (_, _) => BoundedAdmissionDecision.DropNewest);
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        await Assert.That(() => queue.EnqueueAsync(CancelledValue, OneByte, durable: false, control: false, strategy, cancellation.Token)).ThrowsExactly<OperationCanceledException>();
        await Assert.That(queue.Count).IsEqualTo(0);
        await Assert.That(queue.Bytes).IsEqualTo(0);
    }

    /// <summary>Verifies waiting publishers retain the cancellation token that stopped admission.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CancelledWaiterPreservesTheCallerToken()
    {
        using var queue = new BoundedAdmissionQueue<string>(new(OneItem, OneByte, OneBlockedProducer));
        using var cancellation = new CancellationTokenSource();
        await queue.EnqueueAsync(ResidentValue, OneByte, durable: true, control: false);
        var waiting = queue.EnqueueAsync(CancelledValue, OneByte, durable: true, control: false, BufferStrategy.Block, cancellation.Token);
        await cancellation.CancelAsync();
        var exception = await Assert.ThrowsExactlyAsync<TaskCanceledException>(() => waiting);
        await Assert.That(exception!.CancellationToken).IsEqualTo(cancellation.Token);
        await Assert.That(queue.Count).IsEqualTo(OneItem);
    }
}
