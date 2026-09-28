// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Public <c>PublishAsync</c> custom <see cref="IBufferOverflowPolicy"/> tests for built contexts.</content>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>Verifies the builder rejects a null buffer overflow policy.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UseBufferOverflowPolicyRejectsNull() =>
        await Assert.That(static () => CreateBuilder().UseBufferOverflowPolicy(NullReference<IBufferOverflowPolicy>()))
            .ThrowsExactly<ArgumentNullException>();

    /// <summary>
    /// Verifies a registered custom policy receives metadata-only non-durable candidates oldest first and can evict the
    /// selected candidate to admit a durable publication.
    /// </summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PublicCustomPolicyEvictsSelectedNonDurableCandidate()
    {
        var policy = new ScriptedOverflowPolicy(static context => BufferOverflowDecision.Evict(context.Candidates[^1].OperationId));
        await using var store = CreatePublicAdmissionStore("oc-overflow-custom-evict-");
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateOverflowBuilder(store, transport).UseBufferOverflowPolicy(policy).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var volatileReject = CreatePublicPublishOptions(BufferStrategy.Reject, durable: false);

        var first = await stream.PublishAsync(new(1), volatileReject, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        var second = await stream.PublishAsync(new(OverflowSecondDelta), volatileReject, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        _ = await stream.PublishAsync(
                new(OverflowThirdDelta),
                CreatePublicPublishOptions(BufferStrategy.Custom, durable: true),
                CancellationToken.None)
            .AsTask()
            .WaitAsync(GuardTimeout);

        await AssertPendingValuesAsync(store, stream.SubscriptionId, 1, OverflowThirdDelta);
        await AssertDeadLetteredAsync(store, second.OperationId, CustomEvictedReasonCode);
        var observed = policy.Contexts.Single();
        await Assert.That(observed.StreamId).IsEqualTo(Stream);
        await Assert.That(observed.IncomingDurable).IsTrue();
        await Assert.That(observed.Candidates.Count).IsEqualTo(OverflowSecondDelta);
        await Assert.That(observed.Candidates[0].OperationId).IsEqualTo(first.OperationId);
        await Assert.That(observed.Candidates[1].OperationId).IsEqualTo(second.OperationId);
        await Assert.That(observed.Candidates[1].ClientSequence).IsEqualTo(second.ClientSequence);
        await Assert.That(observed.Candidates[1].PayloadBytes).IsEqualTo((long)OverflowSecondDelta);
    }

    /// <summary>Verifies a custom Reject decision fails with a capacity error and reports the overflow.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PublicCustomPolicyRejectReportsOverflow()
    {
        var policy = new ScriptedOverflowPolicy(static _ => BufferOverflowDecision.Reject);
        await using var store = CreatePublicAdmissionStore("oc-overflow-custom-reject-");
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateOverflowBuilder(store, transport).UseBufferOverflowPolicy(policy).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var faults = new RecoveredUploadDiagnosticObserver<OccasionallyConnectedFault>();
        using var faultSubscription = stream.Faults.Subscribe(faults);
        var custom = CreatePublicPublishOptions(BufferStrategy.Custom, durable: false);

        _ = await stream.PublishAsync(new(1), custom, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        _ = await stream.PublishAsync(new(OverflowSecondDelta), custom, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.ThrowsExactlyAsync<QueueCapacityExceededException>(
            () => stream.PublishAsync(new(OverflowThirdDelta), custom, CancellationToken.None).AsTask().WaitAsync(GuardTimeout));
        await AssertPendingValuesAsync(store, stream.SubscriptionId, 1, OverflowSecondDelta);
        await WaitForConditionAsync(() => faults.Values.Count == 1);
        await Assert.That(faults.Values[0].Code).IsEqualTo(OutboxOverflowFaultCode);
    }

    /// <summary>Verifies a custom Block decision waits for capacity until the caller cancels, without evicting work.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PublicCustomPolicyBlockWaitsForCapacity()
    {
        var policy = new ScriptedOverflowPolicy(static _ => BufferOverflowDecision.Block);
        await using var store = CreatePublicAdmissionStore("oc-overflow-custom-block-");
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateOverflowBuilder(store, transport).UseBufferOverflowPolicy(policy).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var custom = CreatePublicPublishOptions(BufferStrategy.Custom, durable: false);
        _ = await stream.PublishAsync(new(1), custom, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        _ = await stream.PublishAsync(new(OverflowSecondDelta), custom, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        using CancellationTokenSource cancellation = new();
        var blocked = stream.PublishAsync(new(OverflowThirdDelta), custom, cancellation.Token).AsTask();
        await WaitForConditionAsync(() => policy.Contexts.Count != 0);
        await Assert.That(blocked.IsCompleted).IsFalse();
        await cancellation.CancelAsync();

        await Assert.That(async () => await blocked.WaitAsync(GuardTimeout)).Throws<OperationCanceledException>();
        await AssertPendingValuesAsync(store, stream.SubscriptionId, 1, OverflowSecondDelta);
    }

    /// <summary>Verifies selections of durable, unknown, or leased operations are policy errors that evict nothing.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PublicCustomPolicyRejectsIneligibleSelections()
    {
        OperationId? selection = null;
        var policy = new ScriptedOverflowPolicy(_ => BufferOverflowDecision.Evict(selection!.Value));
        await using var store = CreatePublicAdmissionStore("oc-overflow-custom-invalid-");
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateOverflowBuilder(store, transport).UseBufferOverflowPolicy(policy).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var custom = CreatePublicPublishOptions(BufferStrategy.Custom, durable: false);
        var durable = await stream.PublishAsync(new(1), CreatePublicPublishOptions(BufferStrategy.Reject, durable: true), CancellationToken.None)
            .AsTask()
            .WaitAsync(GuardTimeout);
        var volatileOperation = await stream.PublishAsync(new(OverflowSecondDelta), custom, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        selection = durable.OperationId;
        await Assert.That(async () => await stream.PublishAsync(new(OverflowThirdDelta), custom, CancellationToken.None).AsTask().WaitAsync(GuardTimeout))
            .ThrowsExactly<InvalidOperationException>();
        selection = new OperationId(Guid.NewGuid());
        await Assert.That(async () => await stream.PublishAsync(new(OverflowThirdDelta), custom, CancellationToken.None).AsTask().WaitAsync(GuardTimeout))
            .ThrowsExactly<InvalidOperationException>();
        selection = volatileOperation.OperationId;
        var lease = await LeaseHeadAsync(store);
        await Assert.That(async () => await stream.PublishAsync(new(OverflowThirdDelta), custom, CancellationToken.None).AsTask().WaitAsync(GuardTimeout))
            .ThrowsExactly<InvalidOperationException>();
        await store.ReleaseLeaseAsync(lease.LeaseId, CancellationToken.None);

        await AssertPendingValuesAsync(store, stream.SubscriptionId, 1, OverflowSecondDelta);
    }

    /// <summary>Verifies custom admission stays unavailable for custom conflict policies even when a policy is registered.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PublicCustomPolicyDoesNotEnableCustomConflictPolicy()
    {
        var policy = new ScriptedOverflowPolicy(static _ => BufferOverflowDecision.Reject);
        await using var store = CreatePublicAdmissionStore("oc-overflow-custom-conflict-");
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateOverflowBuilder(store, transport).UseBufferOverflowPolicy(policy).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var options = CreatePublicPublishOptions(BufferStrategy.Custom, durable: false) with { ConflictPolicy = ConflictPolicy.Custom };

        await Assert.That(async () => await stream.PublishAsync(new(1), options, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a throwing policy fails the publication and preserves the committed outbox.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PublicCustomPolicyFailurePreservesPendingOperations()
    {
        var policy = new ScriptedOverflowPolicy(static _ => throw new InvalidOperationException("Policy failure."));
        await using var store = CreatePublicAdmissionStore("oc-overflow-custom-throw-");
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateOverflowBuilder(store, transport).UseBufferOverflowPolicy(policy).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var custom = CreatePublicPublishOptions(BufferStrategy.Custom, durable: false);

        _ = await stream.PublishAsync(new(1), custom, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        _ = await stream.PublishAsync(new(OverflowSecondDelta), custom, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(async () => await stream.PublishAsync(new(OverflowThirdDelta), custom, CancellationToken.None).AsTask().WaitAsync(GuardTimeout))
            .ThrowsExactly<InvalidOperationException>();
        await AssertPendingValuesAsync(store, stream.SubscriptionId, 1, OverflowSecondDelta);
        await Assert.That(policy.Contexts.Count).IsEqualTo(1);
    }

    /// <summary>Verifies a policy that returns no decision cannot change the committed outbox.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task PublicCustomPolicyWithoutDecisionPreservesPendingOperations()
    {
        var policy = new ScriptedOverflowPolicy(static _ => null!);
        await using var store = CreatePublicAdmissionStore("oc-overflow-custom-null-");
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateOverflowBuilder(store, transport).UseBufferOverflowPolicy(policy).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var custom = CreatePublicPublishOptions(BufferStrategy.Custom, durable: false);

        _ = await stream.PublishAsync(new(1), custom, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        _ = await stream.PublishAsync(new(OverflowSecondDelta), custom, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

        await Assert.That(async () => await stream.PublishAsync(new(OverflowThirdDelta), custom, CancellationToken.None).AsTask().WaitAsync(GuardTimeout))
            .ThrowsExactly<InvalidOperationException>();
        await AssertPendingValuesAsync(store, stream.SubscriptionId, 1, OverflowSecondDelta);
        await Assert.That(policy.Contexts.Count).IsEqualTo(1);
    }
}
