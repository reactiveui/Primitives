// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Snapshot recovery factories for <see cref="SyncEngine"/> tests.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Creates a snapshot recovery session that emits a single retained-history gap.</summary>
    /// <param name="releaseGap">The retained-history gap release gate.</param>
    /// <param name="releaseRecovery">The remote recovery release gate.</param>
    /// <returns>The configured session.</returns>
    private static ReceiveSession CreateSnapshotRecoverySingleGapSession(
        TaskCompletionSource releaseGap,
        TaskCompletionSource releaseRecovery) =>
        new()
        {
            SubscriptionGap = CreateSnapshotRecoveryGap(),
            ReleaseSubscriptionGap = releaseGap,
            ReleaseSnapshotRecovery = releaseRecovery,
            SubscriptionGapEmissionLimit = ExpectedSingleOperation,
            NegotiatedCapabilities = CreateBatchPushCapabilities(ExpectedSingleOperation, PreparedUploadBytes) with
            {
                Features = SnapshotRecoveryRemoteFeatures,
            },
        };

    /// <summary>Creates an engine configured for snapshot recovery orchestration tests.</summary>
    /// <param name="store">The instrumented local store.</param>
    /// <param name="session">The transport session.</param>
    /// <param name="clock">The shared test clock.</param>
    /// <returns>The engine.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SyncEngine CreateSnapshotRecoveryEngine(
        InstrumentedSnapshotRecoveryStore store,
        ReceiveSession session,
        TimeProvider clock) =>
        CreateSnapshotRecoveryEngine(
            store,
            session,
            clock,
            CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes));

    /// <summary>Creates an engine configured for snapshot recovery orchestration tests.</summary>
    /// <param name="store">The local store.</param>
    /// <param name="session">The transport session.</param>
    /// <param name="clock">The shared test clock.</param>
    /// <returns>The engine.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SyncEngine CreateSnapshotRecoveryEngine(
        ILocalStoreAdapter store,
        ReceiveSession session,
        TimeProvider clock) =>
        CreateSnapshotRecoveryEngine(
            store,
            session,
            clock,
            CreateDiagnosticsBatchOptions(ExpectedSingleOperation, PreparedUploadBytes));

    /// <summary>Creates an engine configured for snapshot recovery orchestration tests.</summary>
    /// <param name="store">The instrumented local store.</param>
    /// <param name="session">The transport session.</param>
    /// <param name="clock">The shared test clock.</param>
    /// <param name="options">The engine options.</param>
    /// <returns>The engine.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SyncEngine CreateSnapshotRecoveryEngine(
        InstrumentedSnapshotRecoveryStore store,
        ReceiveSession session,
        TimeProvider clock,
        OccasionallyConnectedOptions options) =>
        CreateSnapshotRecoveryEngine((ILocalStoreAdapter)store, session, clock, options);

    /// <summary>Creates an engine configured for snapshot recovery orchestration tests.</summary>
    /// <param name="store">The local store.</param>
    /// <param name="session">The transport session.</param>
    /// <param name="clock">The shared test clock.</param>
    /// <param name="options">The engine options.</param>
    /// <returns>The engine.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SyncEngine CreateSnapshotRecoveryEngine(
        ILocalStoreAdapter store,
        ReceiveSession session,
        TimeProvider clock,
        OccasionallyConnectedOptions options) =>
        CreateSnapshotRecoveryEngine(store, (IRemoteTransportSession)session, clock, options);

    /// <summary>Creates an engine configured for snapshot recovery orchestration tests.</summary>
    /// <param name="store">The local store.</param>
    /// <param name="session">The transport session.</param>
    /// <param name="clock">The shared test clock.</param>
    /// <param name="options">The engine options.</param>
    /// <returns>The engine.</returns>
    private static SyncEngine CreateSnapshotRecoveryEngine(
        ILocalStoreAdapter store,
        IRemoteTransportSession session,
        TimeProvider clock,
        OccasionallyConnectedOptions options) =>
        new(new()
        {
            Store = store,
            Transport = new RecordingTransport { SessionOverride = session, Capabilities = SnapshotRecoveryRemoteFeatures },
            StoreOwnership = SyncEngineDependencyOwnership.Borrowed,
            TransportOwnership = SyncEngineDependencyOwnership.Owned,
            Options = options,
            StoreInitialization = new("sync-engine-tests", RequiredSchemaVersion: 1, RequireAuthenticatedEncryptionAtRest: false) { ClientId = EngineClientId },
            Client = new(EngineClientId),
            TimeProvider = clock,
            MaxRegisteredStreams = SyncEngineOptions.DefaultMaxRegisteredStreams,
            MaxDiagnosticSubscriptions = SyncEngineOptions.DefaultMaxDiagnosticSubscriptions,
        });

    /// <summary>Creates an instrumented real in-memory store.</summary>
    /// <param name="clock">The shared test clock.</param>
    /// <returns>The instrumented store.</returns>
    private static InstrumentedSnapshotRecoveryStore CreateSnapshotRecoveryStore(TimeProvider clock) =>
        new(new InMemoryLocalStoreAdapter(clock, ReceiveReplayRecordCount, ReceiveReplayStoreBytes, new()));

    /// <summary>Creates and installs a snapshot recovery commit gate for one stream.</summary>
    /// <param name="store">The instrumented store.</param>
    /// <param name="streamId">The stream whose commit should be gated.</param>
    /// <returns>The installed commit gate.</returns>
    private static (TaskCompletionSource Entered, TaskCompletionSource Release) CreateSnapshotRecoveryCommitGate(
        InstrumentedSnapshotRecoveryStore store,
        StreamId streamId)
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        store.SnapshotRecoveryCommitGateStream = streamId;
        store.SnapshotRecoveryCommitEntered = entered;
        store.ReleaseSnapshotRecoveryCommit = release;
        return (entered, release);
    }

    /// <summary>Creates a real counter stream for snapshot recovery orchestration tests.</summary>
    /// <param name="store">The local store.</param>
    /// <param name="coordinator">The engine coordinator.</param>
    /// <param name="projection">The projection under observation.</param>
    /// <param name="timeProvider">The shared test clock.</param>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="subscriptionId">The subscription identity.</param>
    /// <param name="receiveEnabled">Whether the stream has a remote receive subscription.</param>
    /// <returns>The constructed stream.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput> CreateSnapshotRecoveryCounterStream(
        ILocalStoreAdapter store,
        IOccasionallyConnectedStreamCoordinator coordinator,
        ReceiveCounterProjection projection,
        TimeProvider timeProvider,
        StreamId streamId,
        SubscriptionId subscriptionId,
        bool receiveEnabled) =>
        CreateSnapshotRecoveryCounterStreamWithProjection(
            store,
            coordinator,
            projection,
            timeProvider,
            new(streamId, subscriptionId, receiveEnabled, ExpectedCapacityCommitAttempts));

    /// <summary>Creates a counter stream with a projection that exposes replay order.</summary>
    /// <param name="store">The local store.</param>
    /// <param name="coordinator">The engine coordinator.</param>
    /// <param name="projection">The local projection.</param>
    /// <param name="timeProvider">The shared test clock.</param>
    /// <param name="shape">The stream identity, subscription, receive mode, and work capacity.</param>
    /// <param name="notificationOptions">Optional observer notification limits.</param>
    /// <returns>The constructed stream.</returns>
    private static OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput> CreateSnapshotRecoveryCounterStreamWithProjection(
        ILocalStoreAdapter store,
        IOccasionallyConnectedStreamCoordinator coordinator,
        ILocalProjection<ReceiveCounterState, ReceiveCounterInput> projection,
        TimeProvider timeProvider,
        SnapshotRecoveryStreamShape shape,
        ObserverNotificationSubscriptionOptions? notificationOptions = null)
    {
        var serializer = new ReceiveCounterSerializer();
        return new(new()
        {
            Definition = new()
            {
                StreamId = shape.StreamId,
                SubscriptionId = shape.SubscriptionId,
                Projection = projection,
                InputContractId = "counter-input",
                StateContractId = SnapshotRecoveryCounterStateContractId,
                Subscription = CreateSnapshotRecoverySubscription(shape.StreamId, shape.SubscriptionId, shape.ReceiveEnabled),
            },
            Store = store,
            Serializer = serializer,
            TimeProvider = timeProvider,
            OperationIdSource = ReceiveOperationIdSource.Instance,
            Coordinator = coordinator,
            InputProducer = new ReceiveInputProducer(),
            LocalStateSnapshotFactory = static (payload, _) => new(ReceiveCounterSerializer.CreateState(payload)),
            RemoteInputSnapshotFactory = static (payload, _) => new(ReceiveCounterSerializer.CreateInput(payload)),
            NotificationScheduler = InlineObserverScheduler.Instance,
            NotificationOptions = notificationOptions ?? new(ReceiveReplayNotificationCapacity, ReceiveReplayNotificationBytes, ObserverNotificationOverflowMode.CoalesceLatest),
            WorkCapacity = shape.WorkCapacity,
            LocalAdmissionRetainedBytes = PreparedUploadBytes,
            ClientId = EngineClientId,
        });
    }

    /// <summary>Creates a receive-enabled stream for snapshot recovery orchestration tests.</summary>
    /// <param name="store">The local store.</param>
    /// <param name="coordinator">The engine coordinator.</param>
    /// <param name="timeProvider">The shared test clock.</param>
    /// <param name="stream">The stream identity.</param>
    /// <param name="subscription">The subscription identity.</param>
    /// <returns>The constructed stream.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OccasionallyConnectedStream<ReceiveCounterState, ReceiveCounterInput> CreateSnapshotRecoveryStream(
        ILocalStoreAdapter store,
        IOccasionallyConnectedStreamCoordinator coordinator,
        TimeProvider timeProvider,
        StreamId stream,
        SubscriptionId subscription) =>
        CreateSnapshotRecoveryCounterStream(store, coordinator, new(), timeProvider, stream, subscription, receiveEnabled: true);

    /// <summary>Creates the optional receive subscription for a counter stream.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="subscriptionId">The subscription identity.</param>
    /// <param name="receiveEnabled">Whether receiving is enabled.</param>
    /// <returns>The receive subscription, if enabled.</returns>
    private static RemoteSubscriptionOptions? CreateSnapshotRecoverySubscription(
        StreamId streamId,
        SubscriptionId subscriptionId,
        bool receiveEnabled) =>
        receiveEnabled
            ? new() { StreamId = streamId, SubscriptionId = subscriptionId, DeliveryGuarantee = DeliveryGuarantee.ExactlyOnce }
            : null;

    /// <summary>Creates volatile publish options for a stream.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <returns>The publish options.</returns>
    private static RemotePublishOptions CreateVolatilePublishOptions(StreamId streamId) =>
        new() { StreamId = streamId, Durable = false };

    /// <summary>Creates volatile publish options for a stream and base version.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <param name="baseVersion">The operation base version.</param>
    /// <returns>The publish options.</returns>
    private static RemotePublishOptions CreateVolatilePublishOptions(StreamId streamId, string baseVersion) =>
        new() { StreamId = streamId, Durable = false, BaseVersion = baseVersion };

    /// <summary>Groups the snapshot recovery stream settings used by the test factory.</summary>
    /// <param name="StreamId">The stream identity.</param>
    /// <param name="SubscriptionId">The subscription identity.</param>
    /// <param name="ReceiveEnabled">Whether the stream receives remote events.</param>
    /// <param name="WorkCapacity">The local work capacity.</param>
    private readonly record struct SnapshotRecoveryStreamShape(
        StreamId StreamId,
        SubscriptionId SubscriptionId,
        bool ReceiveEnabled,
        int WorkCapacity);
}
