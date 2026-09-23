// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Additional context lifecycle and validation tests for <see cref="OccasionallyConnectedBuilder"/>.</content>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>The recover count expected after a mid-sweep stream registration.</summary>
    private const int ExpectedRecoverCallsAfterLateRegistration = 2;

    /// <summary>The injected session disposal failure message.</summary>
    private const string SessionDisposeFailureMessage = "session dispose failed";

    /// <summary>The timeout message used when a test-controlled recovery gate is not released.</summary>
    private const string RecoveryReleaseTimeoutMessage = "The test did not release stream recovery.";

    /// <summary>The alternate stream identity used by context lifecycle tests.</summary>
    private static readonly StreamId AlternateBuilderStream = new("builder/alternate");

    /// <summary>Verifies disposed contexts reject new stream registrations.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task DisposedContextRejectsStreamRegistration()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var context = CreateReadyBuilder(store, transport).Build();
        await context.DisposeAsync();

        await Assert.That(() => context.GetOrCreateStream(CreateDefinition()))
            .ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies malformed context composition options fail before any lifecycle work can start.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ContextOptionsValidationRejectsMissingEngineAndRegistryCapacity()
    {
        await using var missingEngineStore = new RecordingStoreAdapter();
        await using var missingEngineTransport = new RecordingTransportAdapter();
        var missingEngineOptions = CreateContextOptions(missingEngineStore, missingEngineTransport) with { Engine = null! };

        await Assert.That(() => new OccasionallyConnectedContext(missingEngineOptions))
            .ThrowsExactly<InvalidOperationException>();

        await using var capacityStore = new RecordingStoreAdapter();
        await using var capacityTransport = new RecordingTransportAdapter();
        var invalidCapacityOptions = CreateContextOptions(capacityStore, capacityTransport) with { RegistryCapacity = 0 };

        await Assert.That(() => new OccasionallyConnectedContext(invalidCapacityOptions))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies context start retries its stream sweep when registration changes during startup.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StartSweepIncludesStreamRegisteredDuringBlockedRecovery()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        using ManualResetEventSlim recoverEntered = new();
        using ManualResetEventSlim releaseRecover = new();
        var recoverCalls = 0;
        store.BeforeRecover = () =>
        {
            if (Interlocked.Increment(ref recoverCalls) == 1)
            {
                recoverEntered.Set();
                if (!releaseRecover.Wait(GuardTimeout))
                {
                    throw new TimeoutException(RecoveryReleaseTimeoutMessage);
                }
            }
        };
        await using var context = CreateReadyBuilder(store, transport).Build();
        _ = context.GetOrCreateStream(CreateDefinition());
        var startTask = StartContextOnDedicatedThreadForBuilder(context);
        try
        {
            await Assert.That(recoverEntered.Wait(GuardTimeout)).IsTrue();
            _ = context.GetOrCreateStream(CreateDefinition(AlternateBuilderStream));
            releaseRecover.Set();
            await startTask.WaitAsync(GuardTimeout);

            await Assert.That(recoverCalls).IsEqualTo(ExpectedRecoverCallsAfterLateRegistration);
        }
        finally
        {
            releaseRecover.Set();
            await startTask.WaitAsync(GuardTimeout);
        }
    }

    /// <summary>Verifies stop cancels startup while a stream sweep is waiting for recovery to finish.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StopDuringStreamSweepCancelsStartupBeforeContextCommit()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        using ManualResetEventSlim recoverEntered = new();
        using ManualResetEventSlim releaseRecover = new();
        var recoverTokenCanBeCanceled = true;
        store.BeforeRecoverWithToken = cancellationToken =>
        {
            recoverEntered.Set();
            if (!releaseRecover.Wait(GuardTimeout, CancellationToken.None))
            {
                throw new TimeoutException(RecoveryReleaseTimeoutMessage);
            }

            recoverTokenCanBeCanceled = cancellationToken.CanBeCanceled;
        };
        var context = CreateReadyBuilder(store, transport).Build();
        _ = context.GetOrCreateStream(CreateDefinition());
        var startTask = StartContextOnDedicatedThreadForBuilder(context);
        Task? stopTask = null;
        try
        {
            await Assert.That(recoverEntered.Wait(GuardTimeout)).IsTrue();
            stopTask = context.StopAsync(CancellationToken.None).AsTask();
            await Assert.That(stopTask.IsCompleted).IsFalse();
            releaseRecover.Set();

            await Assert.That(() => startTask.WaitAsync(GuardTimeout)).Throws<OperationCanceledException>();
            await stopTask.WaitAsync(GuardTimeout);
            await Assert.That(recoverTokenCanBeCanceled).IsFalse();
            await context.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await context.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        }
        finally
        {
            releaseRecover.Set();
            try
            {
                await ObserveExpectedStartupCancellationAsync(startTask);
                if (stopTask is not null)
                {
                    await stopTask.WaitAsync(GuardTimeout);
                }
            }
            finally
            {
                await DisposeExpectedFailureAsync(context);
            }
        }
    }

    /// <summary>Verifies stop waits for a tracked late stream-start failure and surfaces it to the stop caller.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StopWaitsForTrackedStreamStartFailure()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        using ManualResetEventSlim recoverEntered = new();
        using ManualResetEventSlim releaseRecover = new();
        var failure = new InvalidOperationException("late recover failed");
        store.BeforeRecover = () =>
        {
            recoverEntered.Set();
            if (!releaseRecover.Wait(GuardTimeout, CancellationToken.None))
            {
                throw new TimeoutException(RecoveryReleaseTimeoutMessage);
            }

            throw failure;
        };
        var context = CreateReadyBuilder(store, transport).Build();
        await context.StartAsync(CancellationToken.None);
        _ = context.GetOrCreateStream(CreateDefinition(AlternateBuilderStream));
        try
        {
            await Assert.That(recoverEntered.Wait(GuardTimeout)).IsTrue();
            var stopTask = context.StopAsync(CancellationToken.None).AsTask();
            releaseRecover.Set();

            var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => stopTask.WaitAsync(GuardTimeout));
            await Assert.That(exception).IsSameReferenceAs(failure);
        }
        finally
        {
            releaseRecover.Set();
            await DisposeExpectedFailureAsync(context);
        }
    }

    /// <summary>Verifies dependency operation cancellation during late stream start is reported by stop.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StopWaitsForTrackedLateStreamOperationCancellationFailure()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        using ManualResetEventSlim recoverEntered = new();
        using ManualResetEventSlim releaseRecover = new();
        var failure = new OperationCanceledException("late recover canceled by store", CancellationToken.None);
        store.BeforeRecover = () =>
        {
            recoverEntered.Set();
            if (!releaseRecover.Wait(GuardTimeout, CancellationToken.None))
            {
                throw new TimeoutException(RecoveryReleaseTimeoutMessage);
            }

            throw failure;
        };
        var context = CreateReadyBuilder(store, transport).Build();
        await context.StartAsync(CancellationToken.None);
        var stream = context.GetOrCreateStream(CreateDefinition(AlternateBuilderStream));
        Task? stopTask = null;
        try
        {
            var faults = new FaultObserver();
            using var faultSubscription = stream.Faults.Subscribe(faults);
            await Assert.That(recoverEntered.Wait(GuardTimeout)).IsTrue();
            stopTask = context.StopAsync(CancellationToken.None).AsTask();
            releaseRecover.Set();

            var exception = await Assert.ThrowsAsync<OperationCanceledException>(
                () => stopTask.WaitAsync(GuardTimeout));
            await Assert.That(exception?.CancellationToken.CanBeCanceled).IsFalse();
            await WaitForConditionAsync(() => faults.Values.Count != 0);
            await Assert.That(faults.Values[0].Code).IsEqualTo("OC.Stream.Lifecycle");
            await Assert.That(faults.Values[0].Exception).IsNotSameReferenceAs(failure);
        }
        finally
        {
            releaseRecover.Set();
            try
            {
                await ObserveExpectedOperationCancellationAsync(stopTask, failure);
            }
            finally
            {
                await DisposeExpectedOperationCancellationAsync(context, failure);
            }
        }
    }

    /// <summary>Verifies a stream startup failure still attempts to stop a started engine and preserves the startup failure.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StreamStartupFailureStopsEngineEvenWhenEngineStopFails()
    {
        await using var store = new RecordingStoreAdapter();
        var stopFailure = new InvalidOperationException(SessionDisposeFailureMessage);
        await using var transport = new RecordingTransportAdapter { SessionDisposeException = stopFailure };
        var failure = new InvalidOperationException("recover failed");
        store.BeforeRecover = () => throw failure;
        var context = CreateReadyBuilder(store, transport).Build();
        _ = context.GetOrCreateStream(CreateDefinition());
        try
        {
            var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
                () => context.StartAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout));

            await Assert.That(exception).IsSameReferenceAs(failure);
            await Assert.That(transport.LastSession?.DisposeCalls).IsEqualTo(1);
        }
        finally
        {
            await DisposeExpectedFailureAsync(context);
        }
    }

    /// <summary>Verifies accepted context stop reports engine stop failures through the shared stop task.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StopReportsEngineStopFailure()
    {
        await using var store = new RecordingStoreAdapter();
        var failure = new InvalidOperationException(SessionDisposeFailureMessage);
        await using var transport = new RecordingTransportAdapter { SessionDisposeException = failure };
        var context = CreateReadyBuilder(store, transport).Build();
        await context.StartAsync(CancellationToken.None);
        try
        {
            var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
                () => context.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout));

            await Assert.That(exception).IsSameReferenceAs(failure);
        }
        finally
        {
            await DisposeExpectedFailureAsync(context);
        }
    }

    /// <summary>Verifies remote event application uses the context input deserializer and advances the durable cursor.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task RemoteApplyUsesContextInputDeserializer()
    {
        const int RemoteDelta = 7;
        const string NextCursor = "remote-cursor-1";
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateReadyBuilder(store, transport).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        TaskCompletionSource<RemoteMessage<CounterInput>> observed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var subscription = stream.Remote.Subscribe(new RemoteMessageObserver(observed));
        await context.StartAsync(CancellationToken.None);
        var payload = new PayloadEnvelope(
            InputContract,
            1,
            "text/plain",
            Encoding.UTF8.GetBytes(RemoteDelta.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            $"hash-{RemoteDelta}");
        var remoteEvent = new RemoteEvent(Guid.NewGuid(), Stream, NextCursor, DateTimeOffset.UnixEpoch, null, payload, new Dictionary<string, string>());
        var batch = new RemoteEventBatch(Guid.NewGuid(), Stream, null, NextCursor, [remoteEvent]);

        var result = await ((IOccasionallyConnectedStreamParticipant)stream)
            .ApplyRemoteBatchAsync(batch, CancellationToken.None)
            .AsTask()
            .WaitAsync(GuardTimeout);

        var message = await observed.Task.WaitAsync(GuardTimeout);

        await Assert.That(result.Receipt.NextCursor).IsEqualTo(NextCursor);
        await Assert.That(result.Receipt.AppliedCount).IsEqualTo(1);
        await Assert.That(message.Value.Delta).IsEqualTo(RemoteDelta);
    }

    /// <summary>Starts a context on a dedicated long-running thread for builder lifecycle tests.</summary>
    /// <param name="context">The context to start.</param>
    /// <returns>The start task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task StartContextOnDedicatedThreadForBuilder(OccasionallyConnectedContext context) =>
        Task.Factory.StartNew(
                static state => ((OccasionallyConnectedContext)state!).StartAsync(CancellationToken.None).AsTask(),
                context,
                CancellationToken.None,
                TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
                TaskScheduler.Default)
            .Unwrap();

    /// <summary>Observes an expected operation cancellation from a tracked lifecycle task.</summary>
    /// <param name="task">The task that may hold the expected cancellation.</param>
    /// <param name="expected">The expected operation cancellation.</param>
    /// <returns>The observation task.</returns>
    private static async Task ObserveExpectedOperationCancellationAsync(Task? task, OperationCanceledException expected)
    {
        if (task is null)
        {
            return;
        }

        try
        {
            await task.WaitAsync(GuardTimeout);
        }
        catch (OperationCanceledException exception) when (IsExpectedDependencyOperationCancellation(exception, expected))
        {
        }
    }

    /// <summary>Disposes a context expected to retain a specific operation cancellation failure.</summary>
    /// <param name="context">The context to dispose.</param>
    /// <param name="expected">The expected operation cancellation.</param>
    /// <returns>The disposal task.</returns>
    private static async Task DisposeExpectedOperationCancellationAsync(
        OccasionallyConnectedContext context,
        OperationCanceledException expected)
    {
        try
        {
            await context.DisposeAsync().AsTask().WaitAsync(GuardTimeout);
        }
        catch (OperationCanceledException exception) when (IsExpectedDependencyOperationCancellation(exception, expected))
        {
        }
    }

    /// <summary>Gets whether the observed cancellation matches the expected dependency cancellation shape.</summary>
    /// <param name="exception">The observed operation cancellation.</param>
    /// <param name="expected">The expected dependency operation cancellation.</param>
    /// <returns><see langword="true"/> when both cancellations are independent from a cancelable token.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsExpectedDependencyOperationCancellation(
        OperationCanceledException exception,
        OperationCanceledException expected) =>
        !exception.CancellationToken.CanBeCanceled && !expected.CancellationToken.CanBeCanceled;

    /// <summary>Creates validated context options for internal context composition validation tests.</summary>
    /// <param name="store">The store dependency.</param>
    /// <param name="transport">The transport dependency.</param>
    /// <returns>The context options.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OccasionallyConnectedContextOptions CreateContextOptions(
        RecordingStoreAdapter store,
        RecordingTransportAdapter transport)
    {
        ClientIdentity client = new(ClientId);
        var engine = new SyncEngine(new SyncEngineOptions
        {
            Store = store,
            Transport = transport,
            StoreOwnership = SyncEngineDependencyOwnership.Borrowed,
            TransportOwnership = SyncEngineDependencyOwnership.Borrowed,
            StoreInitialization = new(StoreIdentity, 1, false) { ClientId = ClientId },
            Client = client,
        });
        return new()
        {
            Engine = engine,
            Store = store,
            Serializer = new TextPayloadSerializer(),
            TimeProvider = TimeProvider.System,
            OperationIdSource = GuidOperationIdSource.Instance,
            NotificationScheduler = ThreadPoolObserverNotificationScheduler.Instance,
            Options = OccasionallyConnectedOptions.Default,
            Client = client,
            RegistryCapacity = 1,
        };
    }

    /// <summary>Captures one remote message notification.</summary>
    /// <param name="completion">The completion source that receives the message.</param>
    private sealed class RemoteMessageObserver(TaskCompletionSource<RemoteMessage<CounterInput>> completion) : IObserver<RemoteMessage<CounterInput>>
    {
        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => completion.TrySetException(error);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(RemoteMessage<CounterInput> value) => completion.TrySetResult(value);
    }
}
