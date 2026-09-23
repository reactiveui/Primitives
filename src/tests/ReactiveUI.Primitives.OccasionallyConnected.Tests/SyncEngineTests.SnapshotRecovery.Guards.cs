// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Snapshot recovery guard-path tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>The follow-up value published after malformed commit poison.</summary>
    private const int SnapshotRecoveryPoisonFollowUpValue = 2;

    /// <summary>The value published to create a legitimate fresh-capture fence change.</summary>
    private const int SnapshotRecoveryConcurrentPublishValue = 2;

    /// <summary>Verifies terminal remote recovery status faults before local commit.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task SnapshotRecoveryRejectsTerminalRemoteStatusBeforeCommit() =>
        AssertRecoveryFaultAfterRemoteAsync(
            new() { Status = RemoteSnapshotRecoveryStatus.UnsupportedProjection },
            typeof(InvalidOperationException).FullName,
            static store => store.LastSnapshotRecoveryMutation is null);

    /// <summary>Verifies changed fresh capture fences fault before durable commit or acknowledgement.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public async Task SnapshotRecoveryRejectsChangedFreshCaptureFenceBeforeCommit()
    {
        var result = await CreateRecoveredSnapshotResultAsync().ConfigureAwait(false);
        await AssertRecoveryFaultAfterRemoteAsync(
                result,
                typeof(SnapshotRecoveryRetryableConcurrentChangeException).FullName,
                static store => store.LastSnapshotRecoveryMutation is null,
                PublishDuringBlockedRecoveryAsync)
            .ConfigureAwait(false);
    }

    /// <summary>Verifies malformed custom-store recovery receipts poison before acknowledgement.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public async Task SnapshotRecoveryPoisonsMalformedStoreCommitBeforeAcknowledgement()
    {
        var result = await CreateRecoveredSnapshotResultAsync().ConfigureAwait(false);
        await AssertRecoveryFaultAfterRemoteAsync(
                result,
                typeof(InvalidOperationException).FullName,
                static store => store.LastSnapshotRecoveryCommit is not null,
                static store => store.SnapshotRecoveryCommitTransform = CreateMalformedRecoveryCommit,
                AssertMalformedStoreCommitPoisonedStreamAsync)
            .ConfigureAwait(false);
    }

    /// <summary>Verifies mismatched recovery receipt pending counts poison before acknowledgement.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public async Task SnapshotRecoveryPoisonsMismatchedPreservedCountBeforeAcknowledgement()
    {
        var result = await CreateRecoveredSnapshotResultAsync().ConfigureAwait(false);
        await AssertRecoveryFaultAfterRemoteAsync(
                result,
                typeof(InvalidOperationException).FullName,
                static store => store.LastSnapshotRecoveryCommit is not null,
                static store => store.SnapshotRecoveryCommitTransform = CreateMismatchedPreservedCountCommit,
                AssertMalformedStoreCommitPoisonedStreamAsync)
            .ConfigureAwait(false);
    }

    /// <summary>Verifies mismatched recovery receipt payloads poison before acknowledgement.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public async Task SnapshotRecoveryPoisonsMismatchedPayloadBeforeAcknowledgement()
    {
        var result = await CreateRecoveredSnapshotResultAsync().ConfigureAwait(false);
        await AssertRecoveryFaultAfterRemoteAsync(
                result,
                typeof(InvalidOperationException).FullName,
                static store => store.LastSnapshotRecoveryCommit is not null,
                static store => store.SnapshotRecoveryCommitTransform = CreateMismatchedPayloadCommit,
                AssertMalformedStoreCommitPoisonedStreamAsync)
            .ConfigureAwait(false);
    }

    /// <summary>Verifies stores that advertise atomic recovery must expose the capture facet.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public async Task SnapshotRecoveryRequiresCaptureFacetWhenStoreAdvertisesAtomicRecovery()
    {
        var result = await CreateRecoveredSnapshotResultAsync().ConfigureAwait(false);
        await AssertRecoveryFaultWithStoreFacetAsync(
                static store => new DelegatingSnapshotRecoveryStore(store),
                result,
                0)
            .ConfigureAwait(false);
    }

    /// <summary>Verifies stores that advertise atomic recovery must expose the recovery commit facet.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public async Task SnapshotRecoveryRequiresCommitFacetWhenStoreAdvertisesAtomicRecovery()
    {
        var result = await CreateRecoveredSnapshotResultAsync().ConfigureAwait(false);
        await AssertRecoveryFaultWithStoreFacetAsync(
                static store => new CaptureOnlySnapshotRecoveryStore(store),
                result,
                ExpectedSingleOperation)
            .ConfigureAwait(false);
    }

    /// <summary>Runs one snapshot recovery and asserts a receive fault after remote response.</summary>
    /// <param name="result">The remote recovery response.</param>
    /// <param name="expectedDiagnostic">The expected sanitized diagnostic message.</param>
    /// <param name="assertStore">The store-state assertion.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertRecoveryFaultAfterRemoteAsync(
        RemoteSnapshotRecoveryResult result,
        string? expectedDiagnostic,
        Func<InstrumentedSnapshotRecoveryStore, bool> assertStore)
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync().ConfigureAwait(false);
        var fixture = new SnapshotRecoveryStoreFixture(clock, store, store, CreateSnapshotRecoveryFaultOptions());
        var expectation = CreateSnapshotRecoveryFaultExpectation(expectedDiagnostic, assertStore, ExpectedSingleOperation);
        await AssertRecoveryFaultAfterRemoteAsync(result, expectation, fixture, CreateSnapshotRecoveryFaultCallbacks())
            .ConfigureAwait(false);
    }

    /// <summary>Runs one configured snapshot recovery and asserts a receive fault after remote response.</summary>
    /// <param name="result">The remote recovery response.</param>
    /// <param name="expectedDiagnostic">The expected sanitized diagnostic message.</param>
    /// <param name="assertStore">The store-state assertion.</param>
    /// <param name="afterRecoveryEntered">The callback after recovery reaches remote and before release.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertRecoveryFaultAfterRemoteAsync(
        RemoteSnapshotRecoveryResult result,
        string? expectedDiagnostic,
        Func<InstrumentedSnapshotRecoveryStore, bool> assertStore,
        Func<OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput>, Task> afterRecoveryEntered)
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync().ConfigureAwait(false);
        var fixture = new SnapshotRecoveryStoreFixture(clock, store, store, CreateSnapshotRecoveryImmediateStopOptions());
        var expectation = CreateSnapshotRecoveryFaultExpectation(expectedDiagnostic, assertStore, ExpectedSingleOperation);
        var callbacks = CreateSnapshotRecoveryFaultCallbacks(afterRecoveryEntered, static _ => Task.CompletedTask);
        await AssertRecoveryFaultAfterRemoteAsync(result, expectation, fixture, callbacks).ConfigureAwait(false);
    }

    /// <summary>Runs one configured snapshot recovery and asserts a stream failure after the receive fault.</summary>
    /// <param name="result">The remote recovery response.</param>
    /// <param name="expectedDiagnostic">The expected sanitized diagnostic message.</param>
    /// <param name="assertStore">The store-state assertion.</param>
    /// <param name="configureStore">The store configuration.</param>
    /// <param name="assertStream">The stream-state assertion.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertRecoveryFaultAfterRemoteAsync(
        RemoteSnapshotRecoveryResult result,
        string? expectedDiagnostic,
        Func<InstrumentedSnapshotRecoveryStore, bool> assertStore,
        Action<InstrumentedSnapshotRecoveryStore> configureStore,
        Func<OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput>, Task> assertStream)
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var store = CreateSnapshotRecoveryStore(clock);
        await store.InitializeForEngineAsync().ConfigureAwait(false);
        configureStore(store);
        var fixture = new SnapshotRecoveryStoreFixture(clock, store, store, CreateSnapshotRecoveryFaultOptions());
        var expectation = CreateSnapshotRecoveryFaultExpectation(expectedDiagnostic, assertStore, ExpectedSingleOperation);
        var callbacks = CreateSnapshotRecoveryFaultCallbacks(static _ => Task.CompletedTask, assertStream);
        await AssertRecoveryFaultAfterRemoteAsync(result, expectation, fixture, callbacks).ConfigureAwait(false);
    }

    /// <summary>Runs one prepared snapshot recovery and asserts a receive fault.</summary>
    /// <param name="result">The remote recovery response.</param>
    /// <param name="expectation">The expected fault details.</param>
    /// <param name="fixture">The store fixture.</param>
    /// <param name="callbacks">The fault-run callbacks.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertRecoveryFaultAfterRemoteAsync(
        RemoteSnapshotRecoveryResult result,
        SnapshotRecoveryFaultExpectation expectation,
        SnapshotRecoveryStoreFixture fixture,
        SnapshotRecoveryFaultCallbacks callbacks)
    {
        var gates = new SnapshotRecoveryFaultGates(
            new(TaskCreationOptions.RunContinuationsAsynchronously),
            new(TaskCreationOptions.RunContinuationsAsynchronously));
        var session = CreateSnapshotRecoverySingleGapSession(gates.ReleaseGap, gates.ReleaseRecovery);
        session.SnapshotRecoveryResult = result;
        await using var engine = CreateSnapshotRecoveryEngine(fixture.EngineStore, session, fixture.Clock, fixture.Options);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await using var stream = CreateSnapshotRecoveryStream(fixture.EngineStore, engine, fixture.Clock, Stream, Subscription);
        await stream.StartAsync(CancellationToken.None).ConfigureAwait(false);
        var run = new SnapshotRecoveryFaultRun(engine, session, fixture.ObservedStore, faults, gates, stream, callbacks);
        await AssertRecoveryFaultAfterRemoteAsync(run, expectation).ConfigureAwait(false);
        await callbacks.AssertStream(stream).ConfigureAwait(false);
    }

    /// <summary>Asserts a receive fault after releasing remote snapshot recovery.</summary>
    /// <param name="run">The recovery run state.</param>
    /// <param name="expectation">The fault expectation.</param>
    /// <returns>The assertion task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task AssertRecoveryFaultAfterRemoteAsync(
        SnapshotRecoveryFaultRun run,
        SnapshotRecoveryFaultExpectation expectation) =>
        AssertRecoveryFaultCoreAsync(run, expectation);

    /// <summary>Runs recovery with a custom store facet and asserts the public receive fault.</summary>
    /// <param name="createStore">Creates the store exposed to the engine.</param>
    /// <param name="result">The remote recovery response.</param>
    /// <param name="expectedRequests">The expected remote recovery request count.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertRecoveryFaultWithStoreFacetAsync(
        Func<InstrumentedSnapshotRecoveryStore, ILocalStoreAdapter> createStore,
        RemoteSnapshotRecoveryResult result,
        int expectedRequests)
    {
        var clock = new ManualTimerTimeProvider(DateTimeOffset.UnixEpoch);
        await using var observedStore = CreateSnapshotRecoveryStore(clock);
        await observedStore.InitializeForEngineAsync().ConfigureAwait(false);
        var fixture = new SnapshotRecoveryStoreFixture(
            clock,
            observedStore,
            createStore(observedStore),
            CreateSnapshotRecoveryFaultOptions());
        var expectation = CreateSnapshotRecoveryFaultExpectation(
            typeof(InvalidOperationException).FullName,
            static store => store.LastSnapshotRecoveryMutation is null,
            expectedRequests);
        await AssertRecoveryFaultAfterRemoteAsync(result, expectation, fixture, CreateSnapshotRecoveryFaultCallbacks())
            .ConfigureAwait(false);
    }

    /// <summary>Asserts a receive fault after releasing remote snapshot recovery.</summary>
    /// <param name="run">The recovery run state.</param>
    /// <param name="expectation">The fault expectation.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertRecoveryFaultCoreAsync(
        SnapshotRecoveryFaultRun run,
        SnapshotRecoveryFaultExpectation expectation)
    {
        try
        {
            await run.Engine.StartAsync(CancellationToken.None).ConfigureAwait(false);
            await run.Session.SubscriptionGapReady.Task.WaitAsync(GuardTimeout).ConfigureAwait(false);
            run.Gates.ReleaseGap.SetResult();
            await WaitForRecoveryRequestOrFaultAsync(run).ConfigureAwait(false);
            if (run.Session.SnapshotRecoveryEntered.Task.IsCompleted)
            {
                await run.Callbacks.AfterRecoveryEntered(run.Stream).ConfigureAwait(false);
            }

            _ = run.Gates.ReleaseRecovery.TrySetResult();
            await WaitForConditionAsync(() =>
                    HasReceivePumpFault<Exception>(run.Faults, Stream) || run.Session.Acknowledgements.Count != 0)
                .ConfigureAwait(false);
            await Assert.That(GetReceivePumpFaultDiagnosticMessage(run.Faults, Stream)).IsEqualTo(expectation.Diagnostic);
            await Assert.That(run.Session.Acknowledgements).IsEmpty();
            await Assert.That(run.Session.SnapshotRecoveryRequestCount).IsEqualTo(expectation.ExpectedRequests);
            await Assert.That(expectation.AssertStore(run.Store)).IsTrue();
        }
        finally
        {
            _ = run.Gates.ReleaseGap.TrySetResult();
            _ = run.Gates.ReleaseRecovery.TrySetResult();
        }
    }

    /// <summary>Waits until recovery reaches remote or faults before remote.</summary>
    /// <param name="run">The recovery run state.</param>
    /// <returns>The wait task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task WaitForRecoveryRequestOrFaultAsync(SnapshotRecoveryFaultRun run) =>
        WaitForConditionAsync(() =>
            run.Session.SnapshotRecoveryEntered.Task.IsCompleted || HasReceivePumpFault<Exception>(run.Faults, Stream));

    /// <summary>Publishes a real local edit while remote recovery is blocked.</summary>
    /// <param name="stream">The stream under test.</param>
    /// <returns>The publish task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task PublishDuringBlockedRecoveryAsync(
        OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput> stream) =>
        stream
            .PublishAsync(
                new(SnapshotRecoveryConcurrentPublishValue),
                CreateVolatilePublishOptions(Stream),
                CancellationToken.None)
            .AsTask();

    /// <summary>Asserts malformed recovery poisoned the stream against further local publish.</summary>
    /// <param name="stream">The stream under test.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertMalformedStoreCommitPoisonedStreamAsync(
        OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput> stream)
    {
        Func<Task> publish = () => stream
            .PublishAsync(new(SnapshotRecoveryPoisonFollowUpValue), CreateVolatilePublishOptions(Stream), CancellationToken.None)
            .AsTask();
        await Assert.That(publish).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Creates snapshot recovery fault expectations.</summary>
    /// <param name="diagnostic">The expected sanitized diagnostic.</param>
    /// <param name="assertStore">The store-state assertion.</param>
    /// <param name="expectedRequests">The expected remote request count.</param>
    /// <returns>The expectation.</returns>
    private static SnapshotRecoveryFaultExpectation CreateSnapshotRecoveryFaultExpectation(
        string? diagnostic,
        Func<InstrumentedSnapshotRecoveryStore, bool> assertStore,
        int expectedRequests) =>
        new(diagnostic, assertStore, expectedRequests);

    /// <summary>Creates options for snapshot recovery fault guard tests.</summary>
    /// <returns>The options.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OccasionallyConnectedOptions CreateSnapshotRecoveryFaultOptions() =>
        CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes);

    /// <summary>Creates options that surface retryable recovery failures immediately.</summary>
    /// <returns>The options.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OccasionallyConnectedOptions CreateSnapshotRecoveryImmediateStopOptions() =>
        CreateSnapshotRecoveryFaultOptions() with
        {
            Retry = OccasionallyConnectedOptions.Default.Retry with { MaximumRetryAttempts = 0 },
        };

    /// <summary>Creates no-op fault-run callbacks.</summary>
    /// <returns>The callbacks.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SnapshotRecoveryFaultCallbacks CreateSnapshotRecoveryFaultCallbacks() =>
        CreateSnapshotRecoveryFaultCallbacks(static _ => Task.CompletedTask, static _ => Task.CompletedTask);

    /// <summary>Creates fault-run callbacks.</summary>
    /// <param name="afterRecoveryEntered">The callback after recovery reaches remote and before release.</param>
    /// <param name="assertStream">The stream-state assertion.</param>
    /// <returns>The callbacks.</returns>
    private static SnapshotRecoveryFaultCallbacks CreateSnapshotRecoveryFaultCallbacks(
        Func<OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput>, Task> afterRecoveryEntered,
        Func<OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput>, Task> assertStream) =>
        new(afterRecoveryEntered, assertStream);

    /// <summary>Creates a malformed durable recovery commit receipt.</summary>
    /// <param name="commit">The valid commit receipt.</param>
    /// <returns>The malformed commit receipt.</returns>
    private static LocalSnapshotRecoveryResult CreateMalformedRecoveryCommit(LocalSnapshotRecoveryResult commit) =>
        commit with { Snapshot = commit.Snapshot with { ServerCursor = "malformed-cursor" } };

    /// <summary>Creates a malformed durable recovery commit receipt with mismatched pending accounting.</summary>
    /// <param name="commit">The valid commit receipt.</param>
    /// <returns>The malformed commit receipt.</returns>
    private static LocalSnapshotRecoveryResult CreateMismatchedPreservedCountCommit(
        LocalSnapshotRecoveryResult commit) =>
        commit with { PreservedPendingOperationCount = commit.PreservedPendingOperationCount + 1 };

    /// <summary>Creates a malformed durable recovery commit receipt with mismatched payload content.</summary>
    /// <param name="commit">The valid commit receipt.</param>
    /// <returns>The malformed commit receipt.</returns>
    private static LocalSnapshotRecoveryResult CreateMismatchedPayloadCommit(
        LocalSnapshotRecoveryResult commit) =>
        commit with { Snapshot = commit.Snapshot with { State = CreateDifferentPayload(commit.Snapshot.State) } };

    /// <summary>Stores snapshot recovery fault gates.</summary>
    /// <param name="ReleaseGap">The retained-history gap release gate.</param>
    /// <param name="ReleaseRecovery">The recovery response release gate.</param>
    private readonly record struct SnapshotRecoveryFaultGates(
        TaskCompletionSource ReleaseGap,
        TaskCompletionSource ReleaseRecovery);

    /// <summary>Stores snapshot recovery fault expectations.</summary>
    /// <param name="Diagnostic">The expected sanitized diagnostic.</param>
    /// <param name="AssertStore">The store-state assertion.</param>
    /// <param name="ExpectedRequests">The expected remote recovery request count.</param>
    private readonly record struct SnapshotRecoveryFaultExpectation(
        string? Diagnostic,
        Func<InstrumentedSnapshotRecoveryStore, bool> AssertStore,
        int ExpectedRequests);

    /// <summary>Stores snapshot recovery store fixture state.</summary>
    /// <param name="Clock">The shared test clock.</param>
    /// <param name="ObservedStore">The instrumented store.</param>
    /// <param name="EngineStore">The store supplied to the engine and stream.</param>
    /// <param name="Options">The engine options.</param>
    private readonly record struct SnapshotRecoveryStoreFixture(
        TimeProvider Clock,
        InstrumentedSnapshotRecoveryStore ObservedStore,
        ILocalStoreAdapter EngineStore,
        OccasionallyConnectedOptions Options);

    /// <summary>Stores snapshot recovery fault callbacks.</summary>
    /// <param name="AfterRecoveryEntered">The callback after recovery reaches remote and before release.</param>
    /// <param name="AssertStream">The stream-state assertion.</param>
    private sealed record SnapshotRecoveryFaultCallbacks(
        Func<OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput>, Task> AfterRecoveryEntered,
        Func<OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput>, Task> AssertStream);

    /// <summary>Stores one snapshot recovery fault run.</summary>
    /// <param name="Engine">The engine under test.</param>
    /// <param name="Session">The remote session.</param>
    /// <param name="Store">The instrumented store.</param>
    /// <param name="Faults">The observed engine faults.</param>
    /// <param name="Gates">The recovery gates.</param>
    /// <param name="Stream">The stream under test.</param>
    /// <param name="Callbacks">The recovery callbacks.</param>
    private sealed record SnapshotRecoveryFaultRun(
        SyncEngine Engine,
        ReceiveSession Session,
        InstrumentedSnapshotRecoveryStore Store,
        RecordingObserver<OccasionallyConnectedFault> Faults,
        SnapshotRecoveryFaultGates Gates,
        OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput> Stream,
        SnapshotRecoveryFaultCallbacks Callbacks);
}
