// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Controls finite global diagnostic scheduler ownership.</content>
public sealed partial class SyncEngineTests
{
    /// <summary>The second recovered queue revision and operation count.</summary>
    private const int SecondDiagnosticRevision = 2;

    /// <summary>The test's retained bytes per recovered operation.</summary>
    private const int DiagnosticBytesPerOperation = 512;

    /// <summary>Proves a queued schedule owns one slot during handoff and leaves the other configured slot usable.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GlobalDiagnosticQueuedScheduleHandoffCountsOneSlot()
    {
        using var scheduler = new BlockingDiagnosticSchedule(executeBeforeBlock: false);
        await using var engine = CreateEngine(maxDiagnosticSubscriptions: 2, notificationScheduler: scheduler);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        await SeedGlobalDiagnosticStateAsync(engine);
        scheduler.BlockNext();

        var first = engine.SyncStates.Subscribe(new RecordingObserver<SyncState>());
        try
        {
            await scheduler.Entered.WaitAsync(GuardTimeout);
            first.Dispose();
            var second = new RecordingObserver<SyncState>();
            using var secondSubscription = engine.SyncStates.Subscribe(second);
            await WaitForConditionAsync(() => second.Values.Exists(static state => state.PendingOperations == 1));
        }
        finally
        {
            scheduler.Release();
            first.Dispose();
        }
    }

    /// <summary>Proves a work item that finished inline still owns its blocked scheduling call.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GlobalDiagnosticExecutedWorkRetainsBlockedScheduleSlot()
    {
        using var scheduler = new BlockingDiagnosticSchedule(executeBeforeBlock: true);
        await using var engine = CreateEngine(maxDiagnosticSubscriptions: 1, notificationScheduler: scheduler);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        await SeedGlobalDiagnosticStateAsync(engine);
        scheduler.BlockNext();

        var first = engine.SyncStates.Subscribe(new RecordingObserver<SyncState>());
        try
        {
            await scheduler.Entered.WaitAsync(GuardTimeout);
            first.Dispose();
            await Assert.That(() => engine.SyncStates.Subscribe(new RecordingObserver<SyncState>()))
                .ThrowsExactly<InvalidOperationException>();
        }
        finally
        {
            scheduler.Release();
            first.Dispose();
        }

        await WaitForConditionAsync(() => TrySubscribeAfterScheduleRelease(engine));
    }

    /// <summary>Verifies a full scheduling budget defers the next state drain until its old lease finishes.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GlobalDiagnosticFullScheduleBudgetDeliversDeferredLatestState()
    {
        using var scheduler = new BlockingDiagnosticSchedule(executeBeforeBlock: true);
        await using var engine = CreateEngine(maxDiagnosticSubscriptions: 1, notificationScheduler: scheduler);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        await SeedGlobalDiagnosticStateAsync(engine);
        scheduler.BlockNext();
        var states = new RecordingObserver<SyncState>();
        using var subscription = engine.SyncStates.Subscribe(states);

        try
        {
            await scheduler.Entered.WaitAsync(GuardTimeout);
            var priorCalls = scheduler.ScheduleCalls;
            engine.RecordRecoveredQueueAggregate(Stream, new(SecondDiagnosticRevision, SecondDiagnosticRevision * DiagnosticBytesPerOperation, SecondDiagnosticRevision));
            await WaitForConditionAsync(() => scheduler.ScheduleCalls > priorCalls);
            await Assert.That(states.Values.Count).IsEqualTo(1);
        }
        finally
        {
            scheduler.Release();
        }

        await WaitForConditionAsync(() => states.Values.Exists(static state => state.PendingOperations == SecondDiagnosticRevision));
    }

    /// <summary>Verifies a rejected replay drain releases its reservation for a later public subscriber.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">The replacement subscriber cannot be admitted after the rejected drain.</exception>
    [Test]
    public async Task GlobalDiagnosticRejectedReplayReleasesScheduleSlot()
    {
        using var scheduler = new BlockingDiagnosticSchedule(executeBeforeBlock: false);
        await using var engine = CreateEngine(maxDiagnosticSubscriptions: 1, notificationScheduler: scheduler);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        await SeedGlobalDiagnosticStateAsync(engine);
        scheduler.RejectNext();

        using var rejected = engine.SyncStates.Subscribe(new RecordingObserver<SyncState>());
        await scheduler.Rejected.WaitAsync(GuardTimeout);
        var recovered = new RecordingObserver<SyncState>();
        IDisposable? recoveredSubscription = null;
        await WaitForConditionAsync(() => TrySubscribeAfterScheduleRelease(engine, recovered, out recoveredSubscription));
        if (recoveredSubscription is not { } accepted)
        {
            throw new InvalidOperationException("A replacement diagnostic subscription was not admitted.");
        }

        using (accepted)
        {
            await WaitForConditionAsync(() => recovered.Values.Exists(static state => state.PendingOperations == 1));
        }
    }

    /// <summary>Creates a replayable aggregate state through the public observer and participant paths.</summary>
    /// <param name="engine">The engine whose state is seeded.</param>
    /// <returns>The assertion task.</returns>
    private static async Task SeedGlobalDiagnosticStateAsync(SyncEngine engine)
    {
        var initial = new RecordingObserver<SyncState>();
        using var subscription = engine.SyncStates.Subscribe(initial);
        engine.RecordRecoveredQueueAggregate(Stream, new(1, DiagnosticBytesPerOperation, 1));
        await WaitForConditionAsync(() => initial.Values.Exists(static state => state.PendingOperations == 1));
    }

    /// <summary>Checks that the single scheduler slot eventually returns after its blocked call exits.</summary>
    /// <param name="engine">The engine whose subscription is probed.</param>
    /// <returns>Whether the slot was available.</returns>
    private static bool TrySubscribeAfterScheduleRelease(SyncEngine engine)
    {
        try
        {
            using var subscription = engine.SyncStates.Subscribe(new RecordingObserver<SyncState>());
            return true;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }

    /// <summary>Tries to retain a replacement subscription after the rejected scheduler call finishes.</summary>
    /// <param name="engine">The engine whose subscription is probed.</param>
    /// <param name="observer">The observer to subscribe.</param>
    /// <param name="subscription">The admitted subscription, if any.</param>
    /// <returns>Whether the subscription was admitted.</returns>
    private static bool TrySubscribeAfterScheduleRelease(SyncEngine engine, IObserver<SyncState> observer, out IDisposable? subscription)
    {
        try
        {
            subscription = engine.SyncStates.Subscribe(observer);
            return true;
        }
        catch (InvalidOperationException)
        {
            subscription = null;
            return false;
        }
    }

    /// <summary>Blocks one configured Schedule call before or after executing its work item.</summary>
    /// <param name="executeBeforeBlock">Whether the blocked call executes before waiting.</param>
    private sealed class BlockingDiagnosticSchedule(bool executeBeforeBlock) : IObserverNotificationScheduler, IDisposable
    {
        /// <summary>Releases the blocked scheduling call.</summary>
        private readonly ManualResetEventSlim _release = new();

        /// <summary>Signals that the selected scheduling call entered its blocked phase.</summary>
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Signals that a configured scheduler call was rejected.</summary>
        private readonly TaskCompletionSource _rejected = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Marks the next call for blocking.</summary>
        private int _blockNext;

        /// <summary>Marks the next call for rejection.</summary>
        private int _rejectNext;

        /// <summary>Counts configured scheduling calls.</summary>
        private int _scheduleCalls;

        /// <summary>Gets the blocked scheduling call's entry task.</summary>
        internal Task Entered => _entered.Task;

        /// <summary>Gets the scheduler rejection task.</summary>
        internal Task Rejected => _rejected.Task;

        /// <summary>Gets the configured scheduling call count.</summary>
        internal int ScheduleCalls => Volatile.Read(ref _scheduleCalls);

        /// <inheritdoc />
        public void Schedule(IWorkItem item)
        {
            _ = Interlocked.Increment(ref _scheduleCalls);
            if (Interlocked.Exchange(ref _rejectNext, 0) != 0)
            {
                _ = _rejected.TrySetResult();
                throw new InvalidOperationException("The configured diagnostic scheduler rejected one drain.");
            }

            if (Interlocked.Exchange(ref _blockNext, 0) == 0)
            {
                item.Execute();
                return;
            }

            if (executeBeforeBlock)
            {
                item.Execute();
            }

            _ = _entered.TrySetResult();
            _release.Wait();
            if (!executeBeforeBlock)
            {
                item.Execute();
            }
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _release.Dispose();

        /// <summary>Selects the next scheduling call for the controlled block.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void BlockNext() => Volatile.Write(ref _blockNext, 1);

        /// <summary>Selects the next scheduling call for rejection.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void RejectNext() => Volatile.Write(ref _rejectNext, 1);

        /// <summary>Releases the blocked scheduling call.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Release() => _release.Set();
    }
}
