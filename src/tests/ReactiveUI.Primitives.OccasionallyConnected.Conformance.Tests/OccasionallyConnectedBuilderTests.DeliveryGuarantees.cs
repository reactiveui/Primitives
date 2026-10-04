// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;
using TUnit.Core.Helpers;

namespace ReactiveUI.Primitives.OccasionallyConnected.Conformance.Tests;

/// <summary>
/// End-to-end delivery-guarantee tests for contexts built by <see cref="OccasionallyConnectedBuilder"/> over a SQLite store,
/// a real transport and a SQLite <see cref="Server.ServerStreamHub"/> that loses committed push responses on request.
/// </summary>
/// <remarks>Bounds competing real client/server database fixtures while preserving each test's fault interleavings.</remarks>
[ParallelLimiter<ProcessorCountParallelLimit>]
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>The number of fake-time steps pumped after a terminal outcome to prove no resend follows.</summary>
    private const int SettleSteps = 20;

    /// <summary>The minimum pushes that prove a resend after one lost acknowledgement.</summary>
    private const int ResendPushes = 2;

    /// <summary>The short exactly-once window used by expiry tests.</summary>
    private static readonly TimeSpan ShortRetention = TimeSpan.FromSeconds(3);

    /// <summary>The extra fake time that moves a retry anchor past the short window.</summary>
    private static readonly TimeSpan PastShortRetention = ShortRetention + TimeSpan.FromSeconds(1);

    /// <summary>Verifies a lost at-most-once acknowledgement ends as terminal ambiguous without a resend.</summary>
    /// <param name="transport">The transport selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(LoopbackTransport)]
    [Arguments(HttpTransport)]
    public async Task AtMostOnceLostAcknowledgementStopsAsAmbiguousWithoutResend(int transport)
    {
        await using var stack = await DeliveryStack.StartAsync(transport, CreateOptions(ExactlyOnceExpiryBehavior.StopAndReport), LongRetention);
        stack.DropPushResponses(true);

        var receipt = await stack.ClientStream.PublishAsync(new(1), CreatePublishOptions(DeliveryGuarantee.AtMostOnce), CancellationToken.None);
        await PumpUntilAsync(
            stack.Clock,
            () => new(stack.HasFault(SyncReasonCodes.AtMostOnceAmbiguous)),
            $"{TransportName(transport)} at-most-once ambiguous fault");
        stack.DropPushResponses(false);
        await PumpStepsAsync(stack.Clock, SettleSteps);

        var status = await stack.GetStatusAsync(receipt.OperationId);
        var failure = await Assert.That(async () => await stack.Context.SyncEngine.AwaitSynchronizedAsync(receipt.OperationId, GuardTimeout))
            .ThrowsExactly<SyncOperationFailedException>();
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.Ambiguous);
        await Assert.That(failure?.State).IsEqualTo(SyncOperationState.Ambiguous);
        await Assert.That(stack.Peer.Pushed.Count).IsEqualTo(1);
        await Assert.That(stack.Domain.EffectsFor(receipt.OperationId)).IsEqualTo(1);
        await Assert.That(stack.States.Values.Any(value => value.OperationId == receipt.OperationId && value.State == SyncOperationState.Ambiguous)).IsTrue();
    }

    /// <summary>Verifies a lost at-least-once acknowledgement resends the same operation and the server applies it once.</summary>
    /// <param name="transport">The transport selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(LoopbackTransport)]
    [Arguments(HttpTransport)]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task AtLeastOnceLostAcknowledgementResendsSameOperationWithOneServerEffect(int transport) =>
        AssertLostAcknowledgementResendsOnceAsync(transport, DeliveryGuarantee.AtLeastOnce);

    /// <summary>Verifies a lost exactly-once acknowledgement inside the window synchronizes with one server effect.</summary>
    /// <param name="transport">The transport selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(LoopbackTransport)]
    [Arguments(HttpTransport)]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ExactlyOnceLostAcknowledgementSynchronizesWithOneServerEffect(int transport) =>
        AssertLostAcknowledgementResendsOnceAsync(transport, DeliveryGuarantee.ExactlyOnce);

    /// <summary>Verifies an exactly-once operation that outlives its window stops as guarantee-expired and stays stopped after reopen.</summary>
    /// <param name="transport">The transport selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(LoopbackTransport)]
    [Arguments(HttpTransport)]
    public async Task ExactlyOnceWindowExpiryStopsAndReportsGuaranteeExpired(int transport)
    {
        await using var stack = await DeliveryStack.StartAsync(transport, CreateOptions(ExactlyOnceExpiryBehavior.StopAndReport), ShortRetention);
        stack.DropPushResponses(true);

        var receipt = await stack.ClientStream.PublishAsync(new(1), CreatePublishOptions(DeliveryGuarantee.ExactlyOnce), CancellationToken.None);
        await WaitForFirstLostAcknowledgementAsync(stack, transport);
        stack.Clock.Advance(PastShortRetention);
        await PumpUntilAsync(
            stack.Clock,
            async () => (await stack.GetStatusAsync(receipt.OperationId))?.State == SyncOperationState.GuaranteeExpired,
            $"{TransportName(transport)} guarantee expiry");
        await PumpUntilAsync(
            stack.Clock,
            () => new(stack.States.Values.Any(value =>
                value.OperationId == receipt.OperationId && value.ReasonCode == SyncReasonCodes.GuaranteeExpired)),
            $"{TransportName(transport)} guarantee-expired state event");
        stack.DropPushResponses(false);
        var pushesAtExpiry = stack.Peer.Pushed.Count;
        await PumpStepsAsync(stack.Clock, SettleSteps);

        var failure = await Assert.That(async () => await stack.Context.SyncEngine.AwaitSynchronizedAsync(receipt.OperationId, GuardTimeout))
            .ThrowsExactly<SyncOperationFailedException>();
        var fault = stack.Faults.Values.Single(static value => value.Code == SyncReasonCodes.GuaranteeExpired);
        await Assert.That(failure?.State).IsEqualTo(SyncOperationState.GuaranteeExpired);
        await Assert.That(failure?.ReasonCode).IsEqualTo(SyncReasonCodes.GuaranteeExpired);
        await Assert.That(fault.OperationId).IsEqualTo(receipt.OperationId);
        await Assert.That(stack.HasFault(SyncReasonCodes.GuaranteeDowngraded)).IsFalse();
        await Assert.That(stack.Peer.Pushed.Count).IsEqualTo(pushesAtExpiry);
        await Assert.That(stack.Peer.Pushed.All(value => value == receipt.OperationId)).IsTrue();
        await Assert.That(stack.Domain.EffectsFor(receipt.OperationId)).IsEqualTo(1);
        await Assert.That(stack.States.Values.Any(value => value.OperationId == receipt.OperationId && value.ReasonCode == SyncReasonCodes.GuaranteeExpired)).IsTrue();

        await stack.DisposeClientAsync();
        await using var reopened = new SqliteLocalStoreAdapter(stack.ClientDatabasePath, new() { TimeProvider = stack.Clock });
        await reopened.InitializeAsync(new(StoreIdentity, 1, RequireAuthenticatedEncryptionAtRest: false) { ClientId = Client }, CancellationToken.None);
        var persisted = await reopened.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None);
        await Assert.That(persisted?.State).IsEqualTo(SyncOperationState.GuaranteeExpired);
        await Assert.That(persisted?.ReasonCode).IsEqualTo(SyncReasonCodes.GuaranteeExpired);
    }

    /// <summary>Verifies an explicit at-least-once fallback emits the downgrade fault before retrying and then synchronizes once.</summary>
    /// <param name="transport">The transport selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(LoopbackTransport)]
    [Arguments(HttpTransport)]
    public async Task ExactlyOnceWindowExpiryFallsBackToAtLeastOnceAfterDowngradeFault(int transport)
    {
        await using var stack = await DeliveryStack.StartAsync(transport, CreateOptions(ExactlyOnceExpiryBehavior.FallbackToAtLeastOnce), ShortRetention);
        stack.DropPushResponses(true);

        var receipt = await stack.ClientStream.PublishAsync(new(1), CreatePublishOptions(DeliveryGuarantee.ExactlyOnce), CancellationToken.None);
        await WaitForFirstLostAcknowledgementAsync(stack, transport);
        var pushesBeforeExpiry = stack.Peer.Pushed.Count;
        stack.Clock.Advance(PastShortRetention);
        await PumpUntilAsync(stack.Clock, () => new(stack.HasFault(SyncReasonCodes.GuaranteeDowngraded)), $"{TransportName(transport)} downgrade fault");
        stack.DropPushResponses(false);
        await PumpUntilAsync(
            stack.Clock,
            async () => (await stack.GetStatusAsync(receipt.OperationId))?.State == SyncOperationState.Synchronized,
            $"{TransportName(transport)} synchronization after downgrade");

        await stack.Context.SyncEngine.AwaitSynchronizedAsync(receipt.OperationId, GuardTimeout);
        var fault = stack.Faults.Values.Single(static value => value.Code == SyncReasonCodes.GuaranteeDowngraded);
        await Assert.That(fault.OperationId).IsEqualTo(receipt.OperationId);
        await Assert.That(fault.Severity).IsEqualTo(FaultSeverity.Warning);
        await Assert.That(stack.HasFault(SyncReasonCodes.GuaranteeExpired)).IsFalse();
        await Assert.That(pushesBeforeExpiry).IsEqualTo(1);
        await Assert.That(stack.Peer.Pushed.Count).IsGreaterThan(pushesBeforeExpiry);
        await Assert.That(stack.Peer.Pushed.All(value => value == receipt.OperationId)).IsTrue();
        await Assert.That(stack.Domain.EffectsFor(receipt.OperationId)).IsEqualTo(1);
        await Assert.That(stack.States.Values.Any(value => value.OperationId == receipt.OperationId && value.ReasonCode == SyncReasonCodes.GuaranteeDowngraded)).IsTrue();
    }

    /// <summary>Verifies a persisted downgrade survives a client restart without a second fault or a guarantee expiry.</summary>
    /// <param name="transport">The transport selector.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(LoopbackTransport)]
    [Arguments(HttpTransport)]
    public async Task ExactlyOnceDowngradeSurvivesClientRestart(int transport)
    {
        var options = CreateOptions(ExactlyOnceExpiryBehavior.FallbackToAtLeastOnce);
        await using var first = await DeliveryStack.StartAsync(transport, options, ShortRetention);
        first.DropPushResponses(true);
        var receipt = await first.ClientStream.PublishAsync(new(1), CreatePublishOptions(DeliveryGuarantee.ExactlyOnce), CancellationToken.None);
        await WaitForFirstLostAcknowledgementAsync(first, transport);
        first.Clock.Advance(PastShortRetention);
        await PumpUntilAsync(first.Clock, () => new(first.HasFault(SyncReasonCodes.GuaranteeDowngraded)), $"{TransportName(transport)} downgrade fault");

        await using var second = await first.RestartClientAsync(options);
        var droppedBeforeRestart = second.DroppedResponses;
        await PumpUntilAsync(second.Clock, () => new(second.DroppedResponses > droppedBeforeRestart), $"{TransportName(transport)} resend after restart");
        second.Clock.Advance(PastShortRetention);
        var droppedAfterWindow = second.DroppedResponses;
        await PumpUntilAsync(second.Clock, () => new(second.DroppedResponses > droppedAfterWindow), $"{TransportName(transport)} resend after the window");
        second.DropPushResponses(false);
        await PumpUntilAsync(
            second.Clock,
            async () => (await second.GetStatusAsync(receipt.OperationId))?.State == SyncOperationState.Synchronized,
            $"{TransportName(transport)} synchronization after restart");

        await Assert.That(second.HasFault(SyncReasonCodes.GuaranteeDowngraded)).IsFalse();
        await Assert.That(second.HasFault(SyncReasonCodes.GuaranteeExpired)).IsFalse();
        await Assert.That(second.Peer.Pushed.All(value => value == receipt.OperationId)).IsTrue();
        await Assert.That(second.Domain.EffectsFor(receipt.OperationId)).IsEqualTo(1);
    }

    /// <summary>Asserts one lost acknowledgement is recovered by resending the same operation with a single server effect.</summary>
    /// <param name="transport">The transport selector.</param>
    /// <param name="guarantee">The delivery guarantee.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertLostAcknowledgementResendsOnceAsync(int transport, DeliveryGuarantee guarantee)
    {
        await using var stack = await DeliveryStack.StartAsync(transport, CreateOptions(ExactlyOnceExpiryBehavior.StopAndReport), LongRetention);
        stack.DropPushResponses(true);

        var receipt = await stack.ClientStream.PublishAsync(new(1), CreatePublishOptions(guarantee), CancellationToken.None);
        await PumpUntilAsync(stack.Clock, () => new(stack.DroppedResponses > 0), $"{TransportName(transport)} {guarantee} lost acknowledgement");
        stack.DropPushResponses(false);
        await PumpUntilAsync(
            stack.Clock,
            async () => (await stack.GetStatusAsync(receipt.OperationId))?.State == SyncOperationState.Synchronized,
            $"{TransportName(transport)} {guarantee} synchronization");

        await stack.Context.SyncEngine.AwaitSynchronizedAsync(receipt.OperationId, GuardTimeout);
        await Assert.That(stack.DroppedResponses).IsEqualTo(1);
        await Assert.That(stack.Peer.Pushed.Count).IsGreaterThanOrEqualTo(ResendPushes);
        await Assert.That(stack.Peer.Pushed.All(value => value == receipt.OperationId)).IsTrue();
        await Assert.That(stack.Domain.EffectsFor(receipt.OperationId)).IsEqualTo(1);
        await Assert.That(stack.HasFault(SyncReasonCodes.GuaranteeExpired)).IsFalse();
        await Assert.That(stack.HasFault(SyncReasonCodes.GuaranteeDowngraded)).IsFalse();
    }
}
