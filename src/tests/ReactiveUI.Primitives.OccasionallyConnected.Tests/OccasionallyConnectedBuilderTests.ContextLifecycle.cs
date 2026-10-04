// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Context lifecycle failure tests for <see cref="OccasionallyConnectedBuilder"/>.</content>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>Verifies asynchronous recovery yields to the caller and preserves the callback failure.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AsynchronousRecoveryCallbackYieldsAndPreservesFailure()
    {
        await using var store = new RecordingStoreAdapter();
        TaskCompletionSource releaseRecover = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var failure = new InvalidOperationException("asynchronous recover failed");
        store.BeforeRecoverAsync = async cancellationToken =>
        {
            await releaseRecover.Task.WaitAsync(GuardTimeout, CancellationToken.None).ConfigureAwait(false);
            throw failure;
        };
        var recovery = store.RecoverStreamAsync(Stream, SubscriptionId.New(), CancellationToken.None).AsTask();
        try
        {
            await Assert.That(recovery.IsCompleted).IsFalse();
        }
        finally
        {
            _ = releaseRecover.TrySetResult();
            var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => recovery.WaitAsync(GuardTimeout));
            await Assert.That(exception).IsSameReferenceAs(failure);
        }
    }

    /// <summary>Verifies a late stream auto-start failure is promptly observable through stream faults.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task LateStreamAutoStartFailurePublishesStreamFaultBeforeStopOrDispose()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        TaskCompletionSource recoverEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseRecover = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var failure = new InvalidOperationException("recover failed");
        store.BeforeRecoverAsync = async cancellationToken =>
        {
            _ = recoverEntered.TrySetResult();
            try
            {
                await releaseRecover.Task.WaitAsync(GuardTimeout, CancellationToken.None).ConfigureAwait(false);
            }
            catch (TimeoutException exception)
            {
                throw new TimeoutException("The test did not release stream recovery.", exception);
            }

            throw failure;
        };
        var context = CreateReadyBuilder(store, transport).Build();
        await context.StartAsync(CancellationToken.None);

        var stream = context.GetOrCreateStream(CreateDefinition());
        try
        {
            await recoverEntered.Task.WaitAsync(GuardTimeout);
            var faults = new FaultObserver();
            using var faultSubscription = stream.Faults.Subscribe(faults);
            _ = releaseRecover.TrySetResult();
            await WaitForConditionAsync(() => faults.Values.Count != 0);

            await Assert.That(faults.Values).Count().IsEqualTo(1);
            await Assert.That(faults.Values[0].Code).IsEqualTo("OC.Stream.Lifecycle");
            await Assert.That(faults.Values[0].Exception).IsNotSameReferenceAs(failure);
        }
        finally
        {
            _ = releaseRecover.TrySetResult();
            await DisposeExpectedFailureAsync(context);
        }
    }

    /// <summary>Verifies context startup failure after engine start stops the shared engine session.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StreamFailureDuringContextStartStopsStartedEngineSession()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var failure = new InvalidOperationException("recover failed");
        store.BeforeRecover = () => throw failure;
        var context = CreateReadyBuilder(store, transport).Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        var faults = new FaultObserver();
        using var faultSubscription = stream.Faults.Subscribe(faults);

        await Assert.That(() => context.StartAsync(CancellationToken.None).AsTask()).ThrowsExactly<InvalidOperationException>();

        var session = transport.LastSession;
        await Assert.That(session).IsNotNull();
        await Assert.That(session!.DisposeCalls).IsEqualTo(1);
        await WaitForConditionAsync(() => faults.Values.Count != 0);
        await Assert.That(faults.Values).Count().IsEqualTo(1);
        await Assert.That(faults.Values[0].Code).IsEqualTo("OC.Stream.Lifecycle");
        await DisposeExpectedFailureAsync(context);
    }

    /// <summary>Observes a startup task that is expected to cancel during stop cleanup.</summary>
    /// <param name="startTask">The startup task.</param>
    /// <returns>The observation task.</returns>
    private static async Task ObserveExpectedStartupCancellationAsync(Task startTask)
    {
        try
        {
            await startTask.WaitAsync(GuardTimeout);
        }
        catch (OperationCanceledException)
        {
        }
    }

    /// <summary>Disposes a context that is expected to retain a startup failure.</summary>
    /// <param name="context">The context to dispose.</param>
    /// <returns>The disposal task.</returns>
    private static async Task DisposeExpectedFailureAsync(OccasionallyConnectedContext context)
    {
        try
        {
            await context.DisposeAsync();
        }
        catch (InvalidOperationException exception)
        {
            _ = exception;
        }
    }

    /// <summary>Waits for a condition to become true.</summary>
    /// <param name="condition">The condition.</param>
    /// <returns>The wait task.</returns>
    /// <exception cref="TimeoutException">The condition did not become true.</exception>
    private static async Task WaitForConditionAsync(Func<bool> condition)
    {
        var deadline = TimeProvider.System.GetUtcNow() + GuardTimeout;
        while (!condition())
        {
            if (TimeProvider.System.GetUtcNow() >= deadline)
            {
                throw new TimeoutException("The expected condition was not reached.");
            }

            await Task.Yield();
        }
    }

    /// <summary>Records fault notifications.</summary>
    private sealed class FaultObserver : IObserver<OccasionallyConnectedFault>
    {
        /// <summary>Gets observed faults.</summary>
        public List<OccasionallyConnectedFault> Values { get; } = [];

        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => ArgumentNullException.ThrowIfNull(error);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(OccasionallyConnectedFault value) => Values.Add(value);
    }
}
