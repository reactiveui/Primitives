// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Extensions.Time.Testing;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedExtensions"/>.</summary>
public sealed class OccasionallyConnectedExtensionsTests
{
    /// <summary>The number of convenience overloads exercised together.</summary>
    private const int ConvenienceOverloadCount = 2;

    /// <summary>The timeout used for synchronization waits.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(20);

    /// <summary>Verifies convenience overloads recover an existing result without waiting for another notification.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenStatusIsAlreadySynchronized_ThenConvenienceWaitsRecoverIt()
    {
        var operationId = OperationId.New();
        var persisted = new SyncOperationStatus(operationId, new("orders"), SyncOperationState.Synchronized, 1, DateTimeOffset.UnixEpoch, null);
        await using var engine = new Engine { Persisted = persisted };

        await engine.AwaitSynchronizedAsync(operationId, WaitTimeout);
        await engine.AwaitSynchronizedAsync(operationId, WaitTimeout, new FakeTimeProvider());

        await Assert.That(engine.LookupCount).IsEqualTo(ConvenienceOverloadCount);
        await Assert.That(engine.LookupId).IsEqualTo(operationId);
        await Assert.That(engine.States.HasObservers).IsFalse();
    }

    /// <summary>Verifies both explicit cancellation overloads stop the wait before lookup.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenCallerAlreadyCanceled_ThenBothOverloadsPreserveCancellation()
    {
        await using var engine = new Engine();
        var operationId = OperationId.New();
        var cancellation = new CancellationToken(canceled: true);
        var systemWait = engine.AwaitSynchronizedAsync(operationId, WaitTimeout, cancellation).AsTask();
        var customWait = engine.AwaitSynchronizedAsync(operationId, WaitTimeout, new FakeTimeProvider(), cancellation).AsTask();

        await Assert.That(() => systemWait).ThrowsExactly<OperationCanceledException>();
        await Assert.That(() => customWait).ThrowsExactly<OperationCanceledException>();
        await Assert.That(engine.LookupCount).IsEqualTo(0);
        await Assert.That(engine.States.HasObservers).IsFalse();
    }

    /// <summary>Verifies the supplied clock controls the complete public waiting operation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WhenInjectedClockReachesTimeout_ThenPublicWaitReleasesSubscription()
    {
        await using var engine = new Engine();
        var clock = new FakeTimeProvider();
        var wait = engine.AwaitSynchronizedAsync(OperationId.New(), WaitTimeout, clock, CancellationToken.None).AsTask();
        clock.Advance(WaitTimeout);

        await Assert.That(() => wait).ThrowsExactly<TimeoutException>();
        await Assert.That(engine.LookupCount).IsEqualTo(1);
        await Assert.That(engine.States.HasObservers).IsFalse();
    }

    /// <summary>Provides persisted and live status without synchronization side effects.</summary>
    private sealed class Engine : ISyncEngine
    {
        /// <summary>Gets the live status signal.</summary>
        public Signal<SyncOperationStatus> States { get; } = new();

        /// <summary>Gets the persisted result.</summary>
        public SyncOperationStatus? Persisted { get; init; }

        /// <summary>Gets the number of lookups.</summary>
        public int LookupCount { get; private set; }

        /// <summary>Gets the requested operation identifier.</summary>
        public OperationId LookupId { get; private set; }

        /// <inheritdoc/>
        public IObservable<SyncState> SyncStates => throw new NotSupportedException();

        /// <inheritdoc/>
        public IObservable<SyncOperationStatus> OperationStates => States;

        /// <inheritdoc/>
        public IObservable<OccasionallyConnectedFault> Faults => throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            LookupCount++;
            LookupId = operationId;
            return new(Persisted);
        }

        /// <inheritdoc/>
        public ValueTask<PublishReceipt> EnqueueOperationAsync(SyncOperation operation, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask StartAsync(CancellationToken cancellationToken) => throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask StopAsync(CancellationToken cancellationToken) => throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask TriggerSyncAsync(CancellationToken cancellationToken) => throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            States.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
