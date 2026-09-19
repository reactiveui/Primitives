// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;
using System.Reflection;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="LoopbackTransportAdapter"/>.</summary>
public sealed partial class LoopbackTransportAdapterTests
{
    /// <summary>The trusted client identifier.</summary>
    private const string TrustedClientId = "device-1";

    /// <summary>The trusted tenant hint.</summary>
    private const string TrustedTenant = "trusted-tenant";

    /// <summary>The untrusted tenant hint.</summary>
    private const string SpoofedTenant = "spoofed-tenant";

    /// <summary>The remote cursor after a receive batch.</summary>
    private const string NextCursor = "cursor-2";

    /// <summary>The remote cursor before a resumed receive batch.</summary>
    private const string PreviousCursor = "cursor-1";

    /// <summary>The remote cursor after two sequential receive batches.</summary>
    private const string ThirdCursor = "cursor-3";

    /// <summary>All known remote transport features.</summary>
    private const RemoteTransportCapabilities AllFeatures = RemoteTransportCapabilities.BatchPush
        | RemoteTransportCapabilities.CursorResume
        | RemoteTransportCapabilities.ReceiveAcknowledgements
        | RemoteTransportCapabilities.ServerIdempotency
        | RemoteTransportCapabilities.AtomicApplyAndAcknowledge
        | RemoteTransportCapabilities.StreamingReceive;

    /// <summary>The oversized push scenario.</summary>
    private const string OversizedScenario = "oversized";

    /// <summary>The mismatched result scenario.</summary>
    private const string MismatchScenario = "mismatch";

    /// <summary>The null result scenario.</summary>
    private const string NullScenario = "null";

    /// <summary>The count overflow scenario.</summary>
    private const string CountScenario = "count";

    /// <summary>The receive byte overflow scenario.</summary>
    private const string BytesScenario = "bytes";

    /// <summary>The expected apply call count after a caller retry.</summary>
    private const int ExplicitRetryApplyCalls = 2;

    /// <summary>The expected count for two sequential batches.</summary>
    private const int ExpectedSequentialBatchCount = 2;

    /// <summary>The request slots needed for one idle prepared push and one active prepared send.</summary>
    private const int DualPreparedRequestSlots = 2;

    /// <summary>The peer operation count limit.</summary>
    private const int PeerMaximumOperations = 8;

    /// <summary>The default batch byte limit.</summary>
    private const int DefaultBatchBytes = 4096;

    /// <summary>The small batch byte limit.</summary>
    private const int SmallBatchBytes = 64;

    /// <summary>The bounded string limit used by validation tests.</summary>
    private const int BoundedStringBytes = 64;

    /// <summary>The oversized string length.</summary>
    private const int OversizedStringLength = 256;

    /// <summary>The payload contract identifier.</summary>
    private const string ContractId = "reading";

    /// <summary>The payload content type.</summary>
    private const string PayloadContentType = "application/json";

    /// <summary>The server idempotency retention in days.</summary>
    private const int ServerRetentionDays = 7;

    /// <summary>The client inbox retention requirement in days.</summary>
    private const int ClientRetentionDays = 1;

    /// <summary>The first payload byte.</summary>
    private const byte FirstPayloadByte = 1;

    /// <summary>The second payload byte.</summary>
    private const byte SecondPayloadByte = 2;

    /// <summary>The third payload byte.</summary>
    private const byte ThirdPayloadByte = 3;

    /// <summary>The maximum wait for deterministic gates.</summary>
    private static readonly TimeSpan GateTimeout = TimeSpan.FromSeconds(5);

    /// <summary>The standard operation payload used by local batches.</summary>
    private static readonly byte[] OperationPayload = [FirstPayloadByte, SecondPayloadByte, ThirdPayloadByte];

    /// <summary>The standard remote event payload.</summary>
    private static readonly byte[] RemotePayload = [FirstPayloadByte];

    /// <summary>The fixed event timestamp.</summary>
    private static readonly DateTimeOffset CommittedUtc = new(2026, 9, 12, 12, 0, 0, TimeSpan.Zero);

    /// <summary>The stream used by the loopback tests.</summary>
    private static readonly StreamId Stream = new("sensor/temperature");

    /// <summary>Verifies push, receive, and acknowledgement calls preserve caller objects and use the trusted host identity.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ForwardsPushReceiveAndAcknowledgementWithTrustedIdentity()
    {
        var first = CreateRemoteEvent();
        var second = CreateRemoteEvent();
        var zeroEventCompletion = new RemoteOperationCompletion(new(TrustedClientId, OperationId.New()), []);
        var receive = CreateReceiveBatch(first, second) with { CompletedOperations = [.. CreateCompletions(first, second), zeroEventCompletion] };
        var batch = CreateBatch();
        var result = CreateResult(batch);
        var trusted = new ServerAuthenticatedClient(TrustedTenant, TrustedClientId);
        var hub = new RecordingHub { ApplyHandler = (_, _, _) => ValueTask.FromResult(new ServerSyncResult(result, [])), SubscribeHandler = (_, _, _) => YieldBatches(receive) };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub, trusted));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(new(TrustedClientId, SpoofedTenant)), CancellationToken.None);

        var push = await session.PushAsync(batch, CancellationToken.None);
        var received = await CollectAsync(session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None));
        var acknowledgement = new ReceiveAcknowledgement(SubscriptionId.New(), Stream, NextCursor);
        await session.AcknowledgeAsync(acknowledgement, CancellationToken.None);

        await Assert.That(push).IsSameReferenceAs(result);
        await Assert.That(received).Count().IsEqualTo(1);
        await Assert.That(received[0]).IsSameReferenceAs(receive);
        await Assert.That(received[0].Events[0]).IsSameReferenceAs(first);
        await Assert.That(received[0].Events[1]).IsSameReferenceAs(second);
        await Assert.That(received[0].CompletedOperations[2]).IsSameReferenceAs(zeroEventCompletion);
        await Assert.That(hub.ApplyBatch).IsSameReferenceAs(batch);
        await Assert.That(hub.ApplyClient).IsSameReferenceAs(trusted);
        await Assert.That(hub.SubscribeClient).IsSameReferenceAs(trusted);
        await Assert.That(hub.Acknowledgement).IsSameReferenceAs(acknowledgement);
        await Assert.That(hub.AcknowledgeClient).IsSameReferenceAs(trusted);
    }

    /// <summary>Verifies connection rejects untrusted client identities and unsupported protocol requirements before hub use.</summary>
    /// <param name="scenario">The rejected connection scenario.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("client")]
    [Arguments("protocol")]
    [Arguments("guarantee")]
    public async Task ConnectRejectsInvalidClientProtocolOrGuaranteeBeforeHubUse(string scenario)
    {
        var hub = new RecordingHub();
        var options = scenario == "guarantee"
            ? CreateOptions(hub) with
            {
                PeerCapabilities = CreateCapabilities(features: AllFeatures & ~RemoteTransportCapabilities.AtomicApplyAndAcknowledge),
            }
            : CreateOptions(hub);
        await using var adapter = new LoopbackTransportAdapter(options);
        var request = scenario switch
        {
            "client" => CreateConnectRequest(new("other-client")),
            "protocol" => new TransportConnectRequest(new(new(2, 0), new(2, 1)), new(TrustedClientId), [DeliveryGuarantee.AtLeastOnce]),
            _ => CreateConnectRequest(new(TrustedClientId), [DeliveryGuarantee.ExactlyOnce]),
        };

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => adapter.ConnectAsync(request, CancellationToken.None).AsTask());
        await Assert.That(hub.ApplyCalls).IsEqualTo(0);
        await Assert.That(hub.SubscribeCalls).IsEqualTo(0);
        await Assert.That(hub.AcknowledgeCalls).IsEqualTo(0);
    }

    /// <summary>Verifies loopback options reject nonpositive logical batch byte limits.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConstructorRejectsNonPositiveMaximumLogicalBatchBytes()
    {
        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            static () =>
            {
                _ = new LoopbackTransportAdapter(CreateOptions(new RecordingHub()) with { MaximumLogicalBatchBytes = 0 });
                return Task.CompletedTask;
            });

        await Assert.That(exception?.Message).Contains("Maximum logical batch bytes");
    }

    /// <summary>Verifies push validates bounds before hub use and validates hub results without retrying.</summary>
    /// <param name="scenario">The rejected push scenario.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(OversizedScenario)]
    [Arguments(MismatchScenario)]
    [Arguments(NullScenario)]
    public async Task PushRejectsOversizedInputAndMalformedHubResponses(string scenario)
    {
        var batch = CreateBatch(metadata: scenario == OversizedScenario ? CreateLargeMetadata() : null);
        var hub = new RecordingHub
        {
            ApplyHandler = scenario switch
            {
                MismatchScenario => static (_, _, _) => ValueTask.FromResult(new ServerSyncResult(new(Guid.NewGuid(), [], null, null), [])),
                NullScenario => static (_, _, _) => default,
                _ => (_, _, _) => ValueTask.FromResult(new ServerSyncResult(CreateResult(batch), [])),
            },
        };
        var options = CreateOptions(hub) with { MaximumLogicalBatchBytes = scenario == OversizedScenario ? SmallBatchBytes : DefaultBatchBytes };
        await using var adapter = new LoopbackTransportAdapter(options);
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        var action = () => session.PushAsync(batch, CancellationToken.None).AsTask();

        if (scenario == MismatchScenario)
        {
            _ = await Assert.ThrowsExactlyAsync<SyncBatchValidationException>(action);
            await Assert.That(hub.ApplyCalls).IsEqualTo(1);
            return;
        }

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(action);
        await Assert.That(hub.ApplyCalls).IsEqualTo(scenario == OversizedScenario ? 0 : 1);
    }

    /// <summary>Verifies ambiguous push failure is not retried and caller retry preserves operation identifiers.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PushDoesNotRetryAndExplicitCallerRetryPreservesOperationIds()
    {
        var batch = CreateBatch();
        var operationId = batch.Operations[0].OperationId;
        var hub = new RecordingHub { DropNextApplyResponse = true };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => session.PushAsync(batch, CancellationToken.None).AsTask());
        var result = await session.PushAsync(batch, CancellationToken.None);

        await Assert.That(result.Operations[0].OperationId).IsEqualTo(operationId);
        await Assert.That(hub.ApplyCalls).IsEqualTo(ExplicitRetryApplyCalls);
        await Assert.That(hub.UniqueServerEffects).IsEqualTo(1);
        await Assert.That(hub.ApplyBatches[0]).IsSameReferenceAs(batch);
        await Assert.That(hub.ApplyBatches[1]).IsSameReferenceAs(batch);
    }

    /// <summary>Verifies receive validation rejects malformed and over-limit batches before exposure.</summary>
    /// <param name="scenario">The malformed receive scenario.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(CountScenario)]
    [Arguments(BytesScenario)]
    public async Task SubscribeRejectsMalformedReceiveBatchesBeforeExposure(string scenario)
    {
        var batch = scenario == CountScenario
            ? new RemoteEventBatch(Guid.NewGuid(), Stream, null, NextCursor, [CreateRemoteEvent(), CreateRemoteEvent()])
            : new RemoteEventBatch(Guid.NewGuid(), Stream, null, NextCursor, [])
            { CompletedOperations = [new(new(new string('c', OversizedStringLength), OperationId.New()), [])] };
        var hub = new RecordingHub { SubscribeHandler = (_, _, _) => YieldBatches(batch) };
        var options = CreateOptions(hub) with
        {
            MaximumReceiveEvents = 1,
            PeerCapabilities = CreateCapabilities(maximumBytes: scenario == BytesScenario ? SmallBatchBytes : DefaultBatchBytes),
        };
        await using var adapter = new LoopbackTransportAdapter(options);
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => CollectAsync(session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None)).AsTask());
    }

    /// <summary>Verifies receive batches must remain on the requested stream and cursor chain.</summary>
    /// <param name="scenario">The invalid subscription sequence scenario.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("stream")]
    [Arguments("first-cursor")]
    [Arguments("next-cursor")]
    public async Task SubscribeRejectsForeignOrSkippedReceiveBatchBeforeExposure(string scenario)
    {
        var first = new RemoteEventBatch(Guid.NewGuid(), Stream, PreviousCursor, NextCursor, []);
        var invalid = scenario switch
        {
            "stream" => new RemoteEventBatch(Guid.NewGuid(), new("sensor/humidity"), PreviousCursor, NextCursor, []),
            "first-cursor" => new RemoteEventBatch(Guid.NewGuid(), Stream, "cursor-0", NextCursor, []),
            _ => new RemoteEventBatch(Guid.NewGuid(), Stream, "cursor-4", "cursor-5", []),
        };
        var request = new RemoteSubscribeRequest(Stream, SubscriptionId.New(), PreviousCursor, StartPosition.Latest);
        var hub = new RecordingHub { SubscribeHandler = (_, _, _) => scenario == "next-cursor" ? YieldBatches(first, invalid) : YieldBatches(invalid) };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var enumerator = session.SubscribeAsync(request, CancellationToken.None).GetAsyncEnumerator();

        if (scenario == "next-cursor")
        {
            await Assert.That(await enumerator.MoveNextAsync()).IsTrue();
            await Assert.That(enumerator.Current).IsSameReferenceAs(first);
        }

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => enumerator.MoveNextAsync().AsTask());
        await Assert.That(await enumerator.MoveNextAsync()).IsFalse();
    }

    /// <summary>Verifies sequential cursor batches and zero-event completion groups preserve hub order.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribePreservesSequentialZeroEventCompletionBatches()
    {
        var completion = new RemoteOperationCompletion(new(TrustedClientId, OperationId.New()), []);
        var first = new RemoteEventBatch(Guid.NewGuid(), Stream, PreviousCursor, NextCursor, []) { CompletedOperations = [completion] };
        var second = new RemoteEventBatch(Guid.NewGuid(), Stream, NextCursor, ThirdCursor, []);
        var request = new RemoteSubscribeRequest(Stream, SubscriptionId.New(), PreviousCursor, StartPosition.Latest);
        var hub = new RecordingHub { SubscribeHandler = (_, _, _) => YieldBatches(first, second) };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        var batches = await CollectAsync(session.SubscribeAsync(request, CancellationToken.None));

        await Assert.That(batches).Count().IsEqualTo(ExpectedSequentialBatchCount);
        await Assert.That(batches[0]).IsSameReferenceAs(first);
        await Assert.That(batches[0].CompletedOperations[0]).IsSameReferenceAs(completion);
        await Assert.That(batches[1]).IsSameReferenceAs(second);
    }

    /// <summary>Verifies active request capacity rejects excess requests without retaining waiters.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActiveRequestCapacityRejectsExcessImmediately()
    {
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var batch = CreateBatch();
        var hub = new RecordingHub
        {
            ApplyHandler = async (_, _, cancellationToken) =>
            {
                _ = entered.TrySetResult();
                await release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                return new(CreateResult(batch), []);
            },
        };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub) with { MaximumConcurrentRequests = 1 });
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var first = session.PushAsync(batch, CancellationToken.None).AsTask();

        try
        {
            await entered.Task.WaitAsync(GateTimeout).ConfigureAwait(false);
            _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => session.PushAsync(CreateBatch(), CancellationToken.None).AsTask());
            await session.AcknowledgeAsync(new(SubscriptionId.New(), Stream, NextCursor), CancellationToken.None);
            await Assert.That(first.IsCompleted).IsFalse();
        }
        finally
        {
            _ = release.TrySetResult();
            await first.ConfigureAwait(false);
        }

        await Assert.That(hub.ApplyCalls).IsEqualTo(1);
        await Assert.That(hub.AcknowledgeCalls).IsEqualTo(1);
    }

    /// <summary>Verifies push admission occurs before bounded batch validation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PushCapacityRejectsBeforeMalformedBatchValidation()
    {
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var batch = CreateBatch();
        var hub = new RecordingHub
        {
            ApplyHandler = async (_, _, cancellationToken) =>
            {
                _ = entered.TrySetResult();
                await release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                return new(CreateResult(batch), []);
            },
        };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub) with { MaximumConcurrentRequests = 1 });
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var first = session.PushAsync(batch, CancellationToken.None).AsTask();

        try
        {
            await entered.Task.WaitAsync(GateTimeout).ConfigureAwait(false);
            var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
                () => session.PushAsync(new(Guid.Empty, []), CancellationToken.None).AsTask());
            await Assert.That(exception?.Message).Contains("active operation limit");
        }
        finally
        {
            _ = release.TrySetResult();
            await first.ConfigureAwait(false);
        }

        await Assert.That(hub.ApplyCalls).IsEqualTo(1);
    }

    /// <summary>Verifies active subscription capacity and enumerator disposal are bounded.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ActiveSubscriptionCapacityAndEnumeratorDisposalAreBounded()
    {
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource disposed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var hub = new RecordingHub { SubscribeHandler = (_, _, cancellationToken) => WaitForRelease(entered, release, disposed, cancellationToken) };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub) with { MaximumConcurrentSubscriptions = 1 });
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var enumerator = session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None).GetAsyncEnumerator();
        var firstMove = enumerator.MoveNextAsync().AsTask();
        await entered.Task.WaitAsync(GateTimeout).ConfigureAwait(false);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None).GetAsyncEnumerator().MoveNextAsync().AsTask());
        await enumerator.DisposeAsync();
        await disposed.Task.WaitAsync(GateTimeout).ConfigureAwait(false);
        _ = release.TrySetResult();
        await Assert.That(firstMove.IsCompleted).IsTrue();
        await Assert.That(hub.SubscribeCalls).IsEqualTo(1);
    }

    /// <summary>Verifies session disposal can close a paused subscription consumer.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeClosesSubscriptionPausedAfterSuccessfulMove()
    {
        TaskCompletionSource disposed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var receive = CreateReceiveBatch(CreateRemoteEvent());
        var hub = new RecordingHub { SubscribeHandler = (_, _, cancellationToken) => YieldThenWait(receive, disposed, cancellationToken) };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var enumerator = session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None).GetAsyncEnumerator();

        await Assert.That(await enumerator.MoveNextAsync()).IsTrue();
        var dispose = session.DisposeAsync().AsTask();

        await disposed.Task.WaitAsync(GateTimeout).ConfigureAwait(false);
        await dispose.ConfigureAwait(false);
    }

    /// <summary>Verifies disposal cancellation callbacks start after admission gates are released.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeCancellationCallbackCanReenterAdmissionWithoutDeadlock()
    {
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<Task> callbackCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var acknowledgement = new ReceiveAcknowledgement(SubscriptionId.New(), Stream, NextCursor);
        IRemoteTransportSession? capturedSession = null;
        var hub = new RecordingHub
        {
            ApplyHandler = async (_, _, cancellationToken) =>
            {
                var context = new ReentrantAcknowledgeContext(capturedSession, acknowledgement, callbackCompleted);
                await using var registration = cancellationToken.UnsafeRegister(CompleteReentrantAcknowledge, context);
                _ = entered.TrySetResult();
                await Task.Delay(TimeSpan.FromMinutes(1), cancellationToken).ConfigureAwait(false);
                return new(CreateResult(CreateBatch()), []);
            },
        };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub) with { MaximumConcurrentAcknowledgements = 1 });
        capturedSession = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var push = capturedSession.PushAsync(CreateBatch(), CancellationToken.None).AsTask();

        await entered.Task.WaitAsync(GateTimeout).ConfigureAwait(false);
        await capturedSession.DisposeAsync();

        var acknowledgementTask = await callbackCompleted.Task.WaitAsync(GateTimeout).ConfigureAwait(false);
        await Assert.That(acknowledgementTask).ThrowsExactly<ObjectDisposedException>();
        await AssertCancelsAsync(push);
    }

    /// <summary>Verifies a cancellation callback fault does not keep the adapter session slot.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeCancellationCallbackFaultStillReleasesSession()
    {
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var batch = CreateBatch();
        var hub = new RecordingHub
        {
            ApplyHandler = async (_, _, cancellationToken) =>
            {
                await using var registration = cancellationToken.UnsafeRegister(
                    static _ => throw new InvalidOperationException("callback failed"),
                    null);
                _ = entered.TrySetResult();
                await Task.Delay(TimeSpan.FromMinutes(1), cancellationToken).ConfigureAwait(false);
                return new(CreateResult(batch), []);
            },
        };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var push = session.PushAsync(batch, CancellationToken.None).AsTask();

        await entered.Task.WaitAsync(GateTimeout).ConfigureAwait(false);
        var exception = await Assert.ThrowsAsync<Exception>(() => session.DisposeAsync().AsTask());
        await Assert.That(exception).IsNotNull();
        await AssertCancelsAsync(push);
        await using var next = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        await Assert.That(next.NegotiatedCapabilities).IsEqualTo(CreateCapabilities());
    }

    /// <summary>Verifies subscription admission links the enumeration cancellation token.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscribeUsesEnumerationCancellationToken()
    {
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var hub = new RecordingHub { SubscribeHandler = (_, _, cancellationToken) => GatedSequence(entered, release, cancellationToken) };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        using var cancellation = new CancellationTokenSource();
        var enumerator = session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None).GetAsyncEnumerator(cancellation.Token);
        var move = enumerator.MoveNextAsync().AsTask();

        await entered.Task.WaitAsync(GateTimeout).ConfigureAwait(false);
#if NET8_0_OR_GREATER
        await cancellation.CancelAsync().ConfigureAwait(false);
#else
        cancellation.Cancel();
#endif
        await AssertCancelsAsync(move);
        await enumerator.DisposeAsync();
    }

    /// <summary>Verifies session disposal cancels in-flight work, drains it, and then permits reconnect.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeCancelsDrainsAndPermitsReconnect()
    {
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var batch = CreateBatch();
        var hub = new RecordingHub
        {
            ApplyHandler = async (_, _, cancellationToken) =>
            {
                _ = entered.TrySetResult();
                await Task.Delay(TimeSpan.FromMinutes(1), cancellationToken).ConfigureAwait(false);
                return new(CreateResult(batch), []);
            },
        };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub) with { MaximumConcurrentRequests = 1 });
        var firstSession = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var push = firstSession.PushAsync(batch, CancellationToken.None).AsTask();
        await entered.Task.WaitAsync(GateTimeout).ConfigureAwait(false);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None).AsTask());
        await firstSession.DisposeAsync();
        await AssertCancelsAsync(push);
        await firstSession.DisposeAsync();
        await using var secondSession = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        await Assert.That(secondSession.NegotiatedCapabilities).IsEqualTo(CreateCapabilities());
    }

    /// <summary>Verifies adapter properties and disposed admission behavior.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task AdapterPropertiesAndDisposedAdmissionAreStable()
    {
        var hub = new RecordingHub();
        var adapter = new LoopbackTransportAdapter(CreateOptions(hub));

        await Assert.That(adapter.Capabilities).IsEqualTo(AllFeatures);
        await adapter.DisposeAsync();
        await adapter.DisposeAsync();

        _ = await Assert.ThrowsExactlyAsync<ObjectDisposedException>(() => adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None).AsTask());
    }

    /// <summary>Verifies subscription enumerator defensive state paths are bounded.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SubscriptionEnumeratorRejectsInvalidStateAndOverlappingMove()
    {
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var hub = new RecordingHub { SubscribeHandler = (_, _, cancellationToken) => GatedSequence(entered, release, cancellationToken) };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        var enumerator = session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None).GetAsyncEnumerator();

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () =>
            {
                _ = enumerator.Current;
                return Task.CompletedTask;
            });
        var firstMove = enumerator.MoveNextAsync().AsTask();
        await entered.Task.WaitAsync(GateTimeout).ConfigureAwait(false);
        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => enumerator.MoveNextAsync().AsTask());
        _ = release.TrySetResult();
        await Assert.That(await firstMove.ConfigureAwait(false)).IsTrue();
        await enumerator.DisposeAsync();
        await Assert.That(await enumerator.MoveNextAsync()).IsFalse();
    }

    /// <summary>Verifies null public inputs are rejected.</summary>
    /// <param name="scenario">The null input scenario.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("options")]
    [Arguments("connect")]
    [Arguments("push")]
    [Arguments("subscribe")]
    [Arguments("ack")]
    public async Task PublicInputNullsAreRejected(string scenario)
    {
        var hub = new RecordingHub();
        if (scenario == "options")
        {
            var constructor = typeof(LoopbackTransportAdapter).GetConstructors().Single();
            var exception = await Assert.That(() => constructor.Invoke([null])).ThrowsExactly<TargetInvocationException>();
            await Assert.That(exception?.InnerException).IsTypeOf<ArgumentNullException>();
            return;
        }

        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hub));
        if (scenario == "connect")
        {
            Delegate connect = (Func<TransportConnectRequest, Task>)(request => adapter.ConnectAsync(request, CancellationToken.None).AsTask());
            var exception = Assert.ThrowsExactly<TargetInvocationException>(() => connect.DynamicInvoke([null]));
            await Assert.That(exception.InnerException).IsTypeOf<ArgumentNullException>();
            return;
        }

        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);
        if (scenario == "subscribe")
        {
            Delegate subscribe = (Func<RemoteSubscribeRequest, Task>)(request => CollectAsync(session.SubscribeAsync(request, CancellationToken.None)).AsTask());
            var exception = Assert.ThrowsExactly<TargetInvocationException>(() => subscribe.DynamicInvoke([null]));
            await Assert.That(exception.InnerException).IsTypeOf<ArgumentNullException>();
            return;
        }

        var action = scenario == "push"
            ? InvokeWithNull<SyncBatch>(batch => session.PushAsync(batch, CancellationToken.None).AsTask())
            : InvokeWithNull<ReceiveAcknowledgement>(acknowledgement => session.AcknowledgeAsync(acknowledgement, CancellationToken.None).AsTask());

        _ = await Assert.ThrowsExactlyAsync<ArgumentNullException>(action);
    }

    /// <summary>Creates loopback adapter options.</summary>
    /// <param name="hub">The server hub.</param>
    /// <param name="client">The trusted client identity.</param>
    /// <returns>The options.</returns>
    private static LoopbackTransportAdapterOptions CreateOptions(IServerStreamHub hub, ServerAuthenticatedClient? client = null) =>
        new() { Hub = hub, AuthenticatedClient = client ?? new(TrustedTenant, TrustedClientId), PeerCapabilities = CreateCapabilities() };

    /// <summary>Creates a valid capability offer.</summary>
    /// <param name="features">The feature flags.</param>
    /// <param name="maximumBytes">The batch byte limit.</param>
    /// <returns>The negotiated capabilities.</returns>
    private static NegotiatedCapabilities CreateCapabilities(
        RemoteTransportCapabilities features = AllFeatures,
        long maximumBytes = DefaultBatchBytes) =>
        new(new(1, 0), features, PeerMaximumOperations, maximumBytes, TimeSpan.FromDays(ServerRetentionDays), TimeSpan.FromDays(ClientRetentionDays));

    /// <summary>Creates a connect request.</summary>
    /// <param name="client">The client claimed by the request.</param>
    /// <param name="guarantees">The required guarantees.</param>
    /// <returns>The connect request.</returns>
    private static TransportConnectRequest CreateConnectRequest(
        ClientIdentity? client = null,
        IReadOnlyCollection<DeliveryGuarantee>? guarantees = null) =>
        new(new(new(1, 0), new(1, 0)), client ?? new(TrustedClientId), guarantees ?? [DeliveryGuarantee.AtLeastOnce]);

    /// <summary>Creates a subscription request.</summary>
    /// <returns>The request.</returns>
    private static RemoteSubscribeRequest CreateSubscribeRequest() =>
        new(Stream, SubscriptionId.New(), null, StartPosition.Latest);

    /// <summary>Creates a synchronization batch.</summary>
    /// <param name="metadata">Optional operation metadata.</param>
    /// <returns>The batch.</returns>
    private static SyncBatch CreateBatch(IReadOnlyDictionary<string, string>? metadata = null)
    {
        var operation = new SyncOperation
        {
            OperationId = OperationId.New(),
            StreamId = Stream,
            ClientSequence = 1,
            TimestampUtc = CommittedUtc,
            Type = SyncOperationType.Append,
            Payload = new(ContractId, 1, PayloadContentType, OperationPayload, "hash"),
            Metadata = metadata ?? new ReadOnlyDictionary<string, string>(new Dictionary<string, string>(StringComparer.Ordinal)),
        };

        return new(Guid.NewGuid(), [operation]);
    }

    /// <summary>Creates a synchronization operation.</summary>
    /// <param name="operationId">The optional operation identifier.</param>
    /// <param name="streamId">The optional stream identifier.</param>
    /// <param name="sequence">The client sequence.</param>
    /// <param name="type">The operation type.</param>
    /// <param name="baseVersion">The optional base version.</param>
    /// <param name="payload">The optional payload envelope.</param>
    /// <param name="metadata">The optional metadata.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateOperation(
        OperationId? operationId = null,
        StreamId? streamId = null,
        long sequence = 1,
        SyncOperationType type = SyncOperationType.Append,
        string? baseVersion = null,
        PayloadEnvelope? payload = null,
        IReadOnlyDictionary<string, string>? metadata = null) =>
        new()
        {
            OperationId = operationId ?? OperationId.New(),
            StreamId = streamId ?? Stream,
            ClientSequence = sequence,
            TimestampUtc = CommittedUtc,
            BaseVersion = baseVersion,
            Type = type,
            Payload = payload ?? CreatePayload(),
            Metadata = metadata ?? new ReadOnlyDictionary<string, string>(new Dictionary<string, string>(StringComparer.Ordinal)),
        };

    /// <summary>Creates a payload envelope.</summary>
    /// <returns>The payload.</returns>
    private static PayloadEnvelope CreatePayload() =>
        new(ContractId, 1, PayloadContentType, OperationPayload, "hash");

    /// <summary>Creates a successful remote result for a batch.</summary>
    /// <param name="batch">The batch.</param>
    /// <returns>The result.</returns>
    private static RemoteSyncResult CreateResult(SyncBatch batch) =>
        new(batch.BatchId, [new(batch.Operations[0].OperationId, OperationResultKind.Accepted, null, "v1")], NextCursor, null);

    /// <summary>Creates operation metadata that exceeds small byte limits.</summary>
    /// <returns>The metadata dictionary.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ReadOnlyDictionary<string, string> CreateLargeMetadata() =>
        new(new Dictionary<string, string>(StringComparer.Ordinal) { ["large"] = new('x', OversizedStringLength) });

    /// <summary>Creates a remote event.</summary>
    /// <returns>The event.</returns>
    private static RemoteEvent CreateRemoteEvent()
    {
        var operationId = OperationId.New();
        var origin = new RemoteEventOrigin(TrustedClientId, operationId);
        var payload = new PayloadEnvelope(ContractId, 1, PayloadContentType, RemotePayload, "hash");
        return new(Guid.NewGuid(), Stream, NextCursor, CommittedUtc, operationId, payload, new Dictionary<string, string>(StringComparer.Ordinal)) { Origin = origin };
    }

    /// <summary>Creates a valid receive batch for remote events.</summary>
    /// <param name="events">The remote events.</param>
    /// <returns>The receive batch.</returns>
    private static RemoteEventBatch CreateReceiveBatch(params RemoteEvent[] events) =>
        new(Guid.NewGuid(), Stream, null, NextCursor, events) { CompletedOperations = CreateCompletions(events) };

    /// <summary>Creates completion declarations for remote events.</summary>
    /// <param name="events">The remote events.</param>
    /// <returns>The completion declarations.</returns>
    /// <exception cref="InvalidOperationException">A fixture event has no origin.</exception>
    private static List<RemoteOperationCompletion> CreateCompletions(params RemoteEvent[] events)
    {
        List<RemoteOperationCompletion> completions = [];
        for (var index = 0; index < events.Length; index++)
        {
            var remoteEvent = events[index];
            if (remoteEvent.Origin is null)
            {
                throw new InvalidOperationException("The receive test event has no origin.");
            }

            completions.Add(new(remoteEvent.Origin, [remoteEvent.EventId]));
        }

        return completions;
    }

    /// <summary>Creates a subscribe request with a null position through public constructor reflection.</summary>
    /// <returns>The malformed request.</returns>
    /// <exception cref="InvalidOperationException">The reflected fixture could not be created.</exception>
    private static RemoteSubscribeRequest CreateSubscribeRequestWithNullPosition()
    {
        var constructor = typeof(RemoteSubscribeRequest).GetConstructors().Single();
        var result = constructor.Invoke([Stream, SubscriptionId.New(), null, null]);
        if (result is RemoteSubscribeRequest request)
        {
            return request;
        }

        throw new InvalidOperationException("The reflected subscribe request fixture was not created.");
    }

    /// <summary>Creates a sync operation whose payload parameter was supplied through delegate dispatch.</summary>
    /// <returns>The malformed operation.</returns>
    /// <exception cref="InvalidOperationException">The delegate fixture could not be created.</exception>
    private static SyncOperation CreateOperationWithNullPayload()
    {
        Delegate factory = (Func<PayloadEnvelope, SyncOperation>)CreateOperationFromPayload;
        var result = factory.DynamicInvoke([null]);
        if (result is SyncOperation operation)
        {
            return operation;
        }

        throw new InvalidOperationException("The reflected operation fixture was not created.");
    }

    /// <summary>Creates a remote event whose payload parameter was supplied through delegate dispatch.</summary>
    /// <returns>The malformed remote event.</returns>
    /// <exception cref="InvalidOperationException">The delegate fixture could not be created.</exception>
    private static RemoteEvent CreateRemoteEventWithNullPayload()
    {
        Delegate factory = (Func<PayloadEnvelope, RemoteEvent>)CreateRemoteEventFromPayload;
        var result = factory.DynamicInvoke([null]);
        if (result is RemoteEvent remoteEvent)
        {
            return remoteEvent;
        }

        throw new InvalidOperationException("The reflected remote event fixture was not created.");
    }

    /// <summary>Collects an async sequence into a list.</summary>
    /// <param name="source">The source.</param>
    /// <returns>The collected batches.</returns>
    private static async ValueTask<IReadOnlyList<RemoteEventBatch>> CollectAsync(IAsyncEnumerable<RemoteEventBatch> source)
    {
        List<RemoteEventBatch> batches = [];
        await foreach (var batch in source.ConfigureAwait(false))
        {
            batches.Add(batch);
        }

        return batches;
    }

    /// <summary>Yields fixed receive batches.</summary>
    /// <param name="batches">The batches.</param>
    /// <returns>The async sequence.</returns>
    private static async IAsyncEnumerable<RemoteEventBatch> YieldBatches(params RemoteEventBatch[] batches)
    {
        await Task.Yield();
        for (var index = 0; index < batches.Length; index++)
        {
            yield return batches[index];
        }
    }

    /// <summary>Creates a gated subscription sequence.</summary>
    /// <param name="entered">Signals entry.</param>
    /// <param name="release">Releases the sequence.</param>
    /// <param name="disposed">Signals iterator disposal.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The async sequence.</returns>
    private static async IAsyncEnumerable<RemoteEventBatch> WaitForRelease(
        TaskCompletionSource entered,
        TaskCompletionSource release,
        TaskCompletionSource disposed,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        try
        {
            _ = entered.TrySetResult();
            await release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            yield return CreateReceiveBatch(CreateRemoteEvent());
        }
        finally
        {
            _ = disposed.TrySetResult();
        }
    }

    /// <summary>Yields one batch and then waits until cancellation or disposal.</summary>
    /// <param name="batch">The first batch.</param>
    /// <param name="disposed">Signals enumerator disposal.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The async sequence.</returns>
    private static async IAsyncEnumerable<RemoteEventBatch> YieldThenWait(
        RemoteEventBatch batch,
        TaskCompletionSource disposed,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        try
        {
            yield return batch;
            await Task.Delay(TimeSpan.FromMinutes(1), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = disposed.TrySetResult();
        }
    }

    /// <summary>Yields a batch after a deterministic gate is released.</summary>
    /// <param name="entered">Signals entry.</param>
    /// <param name="release">Releases the sequence.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The async sequence.</returns>
    private static async IAsyncEnumerable<RemoteEventBatch> GatedSequence(
        TaskCompletionSource entered,
        TaskCompletionSource release,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        _ = entered.TrySetResult();
        await release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        yield return CreateReceiveBatch(CreateRemoteEvent());
    }

    /// <summary>Creates a sync operation from the supplied payload.</summary>
    /// <param name="payload">The payload supplied by public delegate dispatch.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreateOperationFromPayload(PayloadEnvelope payload) =>
        new()
        {
            OperationId = OperationId.New(),
            StreamId = Stream,
            ClientSequence = 1,
            TimestampUtc = CommittedUtc,
            Type = SyncOperationType.Append,
            Payload = payload,
            Metadata = new ReadOnlyDictionary<string, string>(new Dictionary<string, string>(StringComparer.Ordinal)),
        };

    /// <summary>Creates a remote event from the supplied payload.</summary>
    /// <param name="payload">The payload supplied by public delegate dispatch.</param>
    /// <returns>The remote event.</returns>
    private static RemoteEvent CreateRemoteEventFromPayload(PayloadEnvelope payload) =>
        new(
            Guid.NewGuid(),
            Stream,
            NextCursor,
            CommittedUtc,
            null,
            payload,
            new Dictionary<string, string>(StringComparer.Ordinal));

    /// <summary>Asserts that an operation completed by cancellation.</summary>
    /// <param name="operation">The operation task.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertCancelsAsync(Task operation)
    {
        var canceled = false;
        try
        {
            await operation.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            canceled = true;
        }

        await Assert.That(canceled).IsTrue();
    }

    /// <summary>Runs acknowledgement admission from a cancellation callback.</summary>
    /// <param name="state">The callback state.</param>
    private static void CompleteReentrantAcknowledge(object? state)
    {
        if (state is not ReentrantAcknowledgeContext context)
        {
            return;
        }

        try
        {
            var acknowledgement = context.Session is null
                ? Task.CompletedTask
                : context.Session.AcknowledgeAsync(context.Acknowledgement, CancellationToken.None).AsTask();
            _ = context.Completed.TrySetResult(acknowledgement);
        }
        catch (InvalidOperationException exception)
        {
            _ = context.Completed.TrySetResult(Task.FromException(exception));
        }
    }

    /// <summary>Invokes a public method with a null argument through reflection-compatible delegate dispatch.</summary>
    /// <typeparam name="T">The null argument type.</typeparam>
    /// <param name="call">The public call.</param>
    /// <returns>The invocation task.</returns>
    private static Func<Task> InvokeWithNull<T>(Func<T, Task> call) =>
        () =>
        {
            Delegate target = call;
            var invocation = target.DynamicInvoke([null]);
            return invocation is Task task ? task : Task.CompletedTask;
        };
}
