// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>
/// Audits <see cref="LoopbackTransportAdapter"/> for hidden retries. The sync engine owns retry policy, so every engine
/// request must reach the hub exactly once under every failure class, and the hub's failure and retry hint must reach
/// the engine unchanged.
/// </summary>
public sealed partial class LoopbackTransportAdapterTests
{
    /// <summary>A transient typed transport failure with a retry hint.</summary>
    private const string TransientRetryAfterHubFailure = "transient+retry-after";

    /// <summary>An invalid-operation hub failure.</summary>
    private const string InvalidOperationHubFailure = "invalid-operation";

    /// <summary>An I/O hub failure.</summary>
    private const string StreamHubFailure = "io";

    /// <summary>A timeout hub failure.</summary>
    private const string TimeoutHubFailure = "timeout";

    /// <summary>A hub cancellation that the caller did not request.</summary>
    private const string CanceledHubFailure = "canceled";

    /// <summary>The retry hint, in seconds, carried by retry-hint failures.</summary>
    private const int LoopbackRetryAfterSeconds = 90;

    /// <summary>Provides every hub failure class for the single-attempt audit.</summary>
    /// <returns>The failure class names.</returns>
    public static IEnumerable<string> LoopbackSingleAttemptFailures()
    {
        yield return TransientRetryAfterHubFailure;
        yield return InvalidOperationHubFailure;
        yield return StreamHubFailure;
        yield return TimeoutHubFailure;
        yield return CanceledHubFailure;
    }

    /// <summary>Verifies push, acknowledge, subscribe and snapshot each reach the hub once and surface the hub failure unchanged.</summary>
    /// <param name="failure">The failure class.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [MethodDataSource(nameof(LoopbackSingleAttemptFailures))]
    public async Task EveryOperationReachesHubExactlyOncePerFailureClass(string failure)
    {
        var hub = new FailingHub(failure);
        var options = CreateOptions(hub) with
        {
            PeerCapabilities = CreateCapabilities(AllFeatures | RemoteTransportCapabilities.SnapshotRecovery),
            SnapshotRecoveryHub = hub,
        };
        await using var adapter = new LoopbackTransportAdapter(options);
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        var push = await CaptureLoopbackFailureAsync(async () => _ = await session.PushAsync(CreateBatch(), CancellationToken.None));
        var acknowledge = await CaptureLoopbackFailureAsync(async () => await session.AcknowledgeAsync(new(SubscriptionId.New(), Stream, NextCursor), CancellationToken.None));
        var subscribe = await CaptureLoopbackFailureAsync(async () => _ = await CollectAsync(session.SubscribeAsync(CreateSubscribeRequest(), CancellationToken.None)));
        var snapshot = await CaptureLoopbackFailureAsync(
            async () => _ = await ((IRemoteSnapshotRecoverySession)session).GetSnapshotAsync(CreateSnapshotRequest(), CancellationToken.None));

        await Assert.That(hub.Calls).IsEqualTo(FailingHub.OperationCount);
        await Assert.That(hub.ApplyCalls).IsEqualTo(1);
        await Assert.That(hub.AcknowledgeCalls).IsEqualTo(1);
        await Assert.That(hub.SubscribeCalls).IsEqualTo(1);
        await Assert.That(hub.SnapshotCalls).IsEqualTo(1);
        await AssertSurfacedAsync(hub, failure, push);
        await AssertSurfacedAsync(hub, failure, acknowledge);
        await AssertSurfacedAsync(hub, failure, subscribe);
        await AssertSurfacedAsync(hub, failure, snapshot);
    }

    /// <summary>Verifies a push result's retry hint reaches the engine unchanged after one hub call.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task PushAsyncSurfacesHubRetryAfterHintAfterOneCall()
    {
        var retryAfter = TimeSpan.FromSeconds(LoopbackRetryAfterSeconds);
        var hintedHub = new RecordingHub
        {
            ApplyHandler = (batch, _, _) =>
            {
                var accepted = CreateAcceptedResult(batch);
                return ValueTask.FromResult(new ServerSyncResult(new(accepted.BatchId, accepted.Operations, accepted.ServerCursor, retryAfter), []));
            },
        };
        await using var adapter = new LoopbackTransportAdapter(CreateOptions(hintedHub));
        await using var session = await adapter.ConnectAsync(CreateConnectRequest(), CancellationToken.None);

        var result = await session.PushAsync(CreateBatch(), CancellationToken.None);

        await Assert.That(result.RetryAfter).IsEqualTo(retryAfter);
        await Assert.That(hintedHub.ApplyCalls).IsEqualTo(1);
    }

    /// <summary>Runs an operation and captures the exception it surfaces; a hang surfaces as the wait's timeout.</summary>
    /// <param name="action">The operation.</param>
    /// <returns>The surfaced exception.</returns>
    /// <exception cref="InvalidOperationException">The operation completed successfully.</exception>
    private static async Task<Exception> CaptureLoopbackFailureAsync(Func<Task> action)
    {
        try
        {
            await action().WaitAsync(GateTimeout);
        }
        catch (Exception exception)
        {
            return exception;
        }

        throw new InvalidOperationException("Expected the loopback operation to fail.");
    }

    /// <summary>Asserts the hub failure reached the caller unchanged, including any retry hint.</summary>
    /// <param name="hub">The failing hub.</param>
    /// <param name="failure">The failure class.</param>
    /// <param name="surfaced">The surfaced exception.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    private static async Task AssertSurfacedAsync(FailingHub hub, string failure, Exception surfaced)
    {
        await Assert.That(hub.Thrown.Contains(surfaced)).IsTrue();
        if (failure == TransientRetryAfterHubFailure)
        {
            await Assert.That(((IRemoteTransportFailure)surfaced).RetryFailure.RetryAfter).IsEqualTo(TimeSpan.FromSeconds(LoopbackRetryAfterSeconds));
        }
    }

    /// <summary>A typed transport failure that carries a retry classification and hint.</summary>
    private sealed class LoopbackTransportFailureException : Exception, IRemoteTransportFailure
    {
        /// <summary>The default failure message.</summary>
        private const string FailureMessage = "loopback transport failure";

        /// <summary>Initializes a new instance of the <see cref="LoopbackTransportFailureException"/> class.</summary>
        public LoopbackTransportFailureException()
            : this(new(RetryFailureKind.Transient), FailureMessage, innerException: null)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="LoopbackTransportFailureException"/> class.</summary>
        /// <param name="message">The failure message.</param>
        public LoopbackTransportFailureException(string? message)
            : this(new(RetryFailureKind.Transient), message, innerException: null)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="LoopbackTransportFailureException"/> class.</summary>
        /// <param name="message">The failure message.</param>
        /// <param name="innerException">The inner exception.</param>
        public LoopbackTransportFailureException(string? message, Exception? innerException)
            : this(new(RetryFailureKind.Transient), message, innerException)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="LoopbackTransportFailureException"/> class.</summary>
        /// <param name="retryFailure">The retry classification.</param>
        public LoopbackTransportFailureException(RetryFailure retryFailure)
            : this(retryFailure, FailureMessage, innerException: null)
        {
        }

        /// <summary>Initializes a new instance of the <see cref="LoopbackTransportFailureException"/> class.</summary>
        /// <param name="retryFailure">The retry classification.</param>
        /// <param name="message">The failure message.</param>
        /// <param name="innerException">The inner exception.</param>
        private LoopbackTransportFailureException(RetryFailure retryFailure, string? message, Exception? innerException)
            : base(message, innerException) => RetryFailure = retryFailure;

        /// <inheritdoc/>
        public RetryFailure RetryFailure { get; }
    }

    /// <summary>A hub that fails every call with one failure class and counts calls per operation.</summary>
    /// <param name="failure">The failure class.</param>
    private sealed class FailingHub(string failure) : IServerStreamHub, IServerSnapshotRecoveryHub
    {
        /// <summary>The number of audited operation kinds.</summary>
        public const int OperationCount = 4;

        /// <summary>Gets every exception the hub threw.</summary>
        public List<Exception> Thrown { get; } = [];

        /// <summary>Gets the total hub calls.</summary>
        public int Calls => ApplyCalls + AcknowledgeCalls + SubscribeCalls + SnapshotCalls;

        /// <summary>Gets the apply call count.</summary>
        public int ApplyCalls { get; private set; }

        /// <summary>Gets the acknowledge call count.</summary>
        public int AcknowledgeCalls { get; private set; }

        /// <summary>Gets the subscribe call count.</summary>
        public int SubscribeCalls { get; private set; }

        /// <summary>Gets the snapshot call count.</summary>
        public int SnapshotCalls { get; private set; }

        /// <inheritdoc/>
        public ValueTask<ServerSyncResult> ApplyOperationsAsync(SyncBatch batch, ServerAuthenticatedClient client, CancellationToken cancellationToken)
        {
            ApplyCalls++;
            return ValueTask.FromException<ServerSyncResult>(CreateFailure());
        }

        /// <inheritdoc/>
        public ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement, ServerAuthenticatedClient client, CancellationToken cancellationToken)
        {
            AcknowledgeCalls++;
            return ValueTask.FromException(CreateFailure());
        }

        /// <inheritdoc/>
        public IAsyncEnumerable<RemoteEventBatch> SubscribeStreamAsync(RemoteSubscribeRequest request, ServerAuthenticatedClient client, CancellationToken cancellationToken)
        {
            SubscribeCalls++;
            return ThrowOnFirstMoveAsync(CreateFailure(), cancellationToken);
        }

        /// <inheritdoc/>
        public ValueTask<RemoteSnapshotRecoveryResult> GetSnapshotAsync(
            RemoteSnapshotRecoveryRequest request,
            ServerAuthenticatedClient client,
            CancellationToken cancellationToken)
        {
            SnapshotCalls++;
            return ValueTask.FromException<RemoteSnapshotRecoveryResult>(CreateFailure());
        }

        /// <summary>Returns a sequence that throws on its first move.</summary>
        /// <param name="exception">The exception to throw.</param>
        /// <param name="cancellationToken">The enumeration cancellation token.</param>
        /// <returns>The failing sequence.</returns>
        private static async IAsyncEnumerable<RemoteEventBatch> ThrowOnFirstMoveAsync(Exception exception, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            cancellationToken.ThrowIfCancellationRequested();
            if (exception is not null)
            {
                throw exception;
            }

            yield break;
        }

        /// <summary>Creates and records one failure.</summary>
        /// <returns>The failure.</returns>
        /// <exception cref="ArgumentOutOfRangeException">The failure class is unknown.</exception>
        private Exception CreateFailure()
        {
            Exception exception = failure switch
            {
                TransientRetryAfterHubFailure => new LoopbackTransportFailureException(new RetryFailure(RetryFailureKind.Transient, TimeSpan.FromSeconds(LoopbackRetryAfterSeconds), null)),
                InvalidOperationHubFailure => new InvalidOperationException("hub failed"),
                StreamHubFailure => new IOException("hub stream failed"),
                TimeoutHubFailure => new TimeoutException("hub timed out"),
                CanceledHubFailure => new OperationCanceledException("hub canceled"),
                _ => throw new ArgumentOutOfRangeException(nameof(failure), failure, "Unknown failure class."),
            };
            Thrown.Add(exception);
            return exception;
        }
    }
}
