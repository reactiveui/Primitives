// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedBuilder"/>.</summary>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>The client identifier used by builder tests.</summary>
    private const string ClientId = "client-a";

    /// <summary>The default store identity used by builder tests.</summary>
    private const string StoreIdentity = "builder-tests";

    /// <summary>The store identity parameter name.</summary>
    private const string StoreIdentityParameterName = "storeIdentity";

    /// <summary>The input contract used by builder tests.</summary>
    private const string InputContract = "counter-input";

    /// <summary>The state contract used by builder tests.</summary>
    private const string StateContract = "counter-state";

    /// <summary>The declared retained bytes for one typed input.</summary>
    private const long TypedInputBytes = 128;

    /// <summary>The deterministic retry random value used by tests.</summary>
    private const double RetryRandomValue = 0.25D;

    /// <summary>The stream identity used by builder tests.</summary>
    private static readonly StreamId Stream = new("builder/main");

    /// <summary>The guard timeout used by fixture synchronization.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies all mandatory construction dependencies are required.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task BuildRequiresClientStoreTransportAndSerializer()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var serializer = new TextPayloadSerializer();

        await Assert.That(static () => new OccasionallyConnectedBuilder().Build())
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => CreateBuilder().UseBorrowedStore(store).UseBorrowedTransport(transport).UseSerializer(serializer).Build())
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => CreateBuilder().UseClient(new(ClientId)).UseBorrowedTransport(transport).UseSerializer(serializer).Build())
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => CreateBuilder().UseClient(new(ClientId)).UseBorrowedStore(store).UseSerializer(serializer).Build())
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => CreateBuilder().UseClient(new(ClientId)).UseBorrowedStore(store).UseBorrowedTransport(transport).Build())
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies store identities must be stable non-empty partition names.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UseStoreIdentityRejectsNullEmptyAndWhitespace()
    {
        var nullException = await Assert.That(static () => CreateBuilder().UseStoreIdentity(NullReference<string>()))
            .ThrowsExactly<ArgumentNullException>();
        var emptyException = await Assert.That(static () => CreateBuilder().UseStoreIdentity(string.Empty))
            .ThrowsExactly<ArgumentException>();
        var whitespaceException = await Assert.That(static () => CreateBuilder().UseStoreIdentity(" \t "))
            .ThrowsExactly<ArgumentException>();

        await Assert.That(nullException?.ParamName).IsEqualTo(StoreIdentityParameterName);
        await Assert.That(emptyException?.ParamName).IsEqualTo(StoreIdentityParameterName);
        await Assert.That(whitespaceException?.ParamName).IsEqualTo(StoreIdentityParameterName);
    }

    /// <summary>Verifies the default store initialization binds client and encryption options.</summary>
    /// <returns>A task representing the assertions.</returns>
    /// <exception cref="InvalidOperationException">The builder creates invalid context state.</exception>
    [Test]
    public async Task BuildCreatesDefaultStoreInitializationFromClientAndSecurityOptions()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateBuilder()
            .UseClient(new(ClientId))
            .UseBorrowedStore(store)
            .UseBorrowedTransport(transport)
            .UseSerializer(new TextPayloadSerializer())
            .UseStoreIdentity(StoreIdentity)
            .Build();

        await context.StartAsync(CancellationToken.None);

        var initialization = store.Initialization
            ?? throw new InvalidOperationException("The store was not initialized.");
        await Assert.That(initialization.StoreIdentity).IsEqualTo(StoreIdentity);
        await Assert.That(initialization.RequiredSchemaVersion).IsEqualTo(1);
        await Assert.That(initialization.ClientId).IsEqualTo(ClientId);
        await Assert.That(initialization.RequireAuthenticatedEncryptionAtRest).IsFalse();
    }

    /// <summary>Verifies explicit store initialization cannot bind a different client partition.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ExplicitStoreInitializationClientIdMustMatchUseClient()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var builder = CreateReadyBuilder(store, transport)
            .UseStoreInitialization(new(StoreIdentity, 1, false) { ClientId = "client-b" });

        await Assert.That(builder.Build).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies explicit store initialization cannot weaken required encryption.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ExplicitStoreInitializationCannotWeakenRequiredEncryption()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var options = OccasionallyConnectedOptions.Default with
        {
            Security = OccasionallyConnectedOptions.Default.Security with
            {
                RequireAuthenticatedEncryptionAtRest = true,
            },
        };
        var builder = CreateReadyBuilder(store, transport)
            .UseOptions(options)
            .UseStoreInitialization(new(StoreIdentity, 1, false));

        await Assert.That(builder.Build).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a failed validation does not transfer ownership or poison a later successful build.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task BuildCanBeRetriedAfterValidationFailureWithoutDoubleDisposal()
    {
        var store = new RecordingStoreAdapter();
        var transport = new RecordingTransportAdapter();
        var invalid = OccasionallyConnectedOptions.Default with { MaxConcurrentStreams = 0 };
        var builder = CreateBuilder()
            .UseClient(new(ClientId))
            .UseStore(store)
            .UseTransport(transport)
            .UseSerializer(new TextPayloadSerializer())
            .UseStoreIdentity(StoreIdentity)
            .UseOptions(invalid);

        await Assert.That(builder.Build).ThrowsExactly<ArgumentOutOfRangeException>();

        await using var context = builder.UseOptions(OccasionallyConnectedOptions.Default).Build();
        await context.DisposeAsync();

        await Assert.That(store.DisposeCalls).IsEqualTo(1);
        await Assert.That(transport.DisposeCalls).IsEqualTo(1);
    }

    /// <summary>Verifies owned dependencies cannot be transferred twice after a successful build.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task BuildCannotBeCalledTwiceAfterSuccessfulOwnershipTransfer()
    {
        var store = new RecordingStoreAdapter();
        var transport = new RecordingTransportAdapter();
        var builder = CreateBuilder()
            .UseClient(new(ClientId))
            .UseStore(store)
            .UseTransport(transport)
            .UseSerializer(new TextPayloadSerializer())
            .UseStoreIdentity(StoreIdentity);

        await using var context = builder.Build();

        await Assert.That(builder.Build).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies borrowed dependencies remain caller-owned after context disposal.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UseBorrowedStoreAndTransportRemainAliveAfterContextDispose()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var context = CreateReadyBuilder(store, transport).Build();
        await context.DisposeAsync();

        await Assert.That(store.DisposeCalls).IsEqualTo(0);
        await Assert.That(transport.DisposeCalls).IsEqualTo(0);
    }

    /// <summary>Verifies owned dependencies are disposed through the context-owned engine.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UseStoreAndTransportAreDisposedAfterContextDispose()
    {
        var store = new RecordingStoreAdapter();
        var transport = new RecordingTransportAdapter();
        var context = CreateBuilder()
            .UseClient(new(ClientId))
            .UseStore(store)
            .UseTransport(transport)
            .UseSerializer(new TextPayloadSerializer())
            .UseStoreIdentity(StoreIdentity)
            .Build();
        await context.DisposeAsync();

        await Assert.That(store.DisposeCalls).IsEqualTo(1);
        await Assert.That(transport.DisposeCalls).IsEqualTo(1);
    }

    /// <summary>Verifies AutoStart starts shared work after Build without blocking Build.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AutoStartBeginsSharedStartWithoutBlockingBuild()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter
        {
            ConnectEntered = new(TaskCreationOptions.RunContinuationsAsynchronously),
            ReleaseConnect = new(TaskCreationOptions.RunContinuationsAsynchronously),
        };
        var options = OccasionallyConnectedOptions.Default with { AutoStart = true };

        OccasionallyConnectedContext? context = null;
        try
        {
            context = CreateReadyBuilder(store, transport).UseOptions(options).Build();
            await transport.ConnectEntered.Task.WaitAsync(GuardTimeout);

            await Assert.That(context.StartupTask.IsCompleted).IsFalse();
            _ = transport.ReleaseConnect.TrySetResult();
            await context.StartupTask.WaitAsync(GuardTimeout);
        }
        finally
        {
            _ = transport.ReleaseConnect.TrySetResult();
            if (context is not null)
            {
                await context.DisposeAsync();
            }
        }
    }

    /// <summary>Verifies AutoStart does not let synchronous store initialization block Build.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AutoStartBuildReturnsWhenStoreInitializationBlocksSynchronously()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        using ManualResetEventSlim initializeEntered = new();
        using ManualResetEventSlim releaseInitialize = new();
        store.BeforeInitialize = () =>
        {
            initializeEntered.Set();
            if (!releaseInitialize.Wait(GuardTimeout))
            {
                throw new TimeoutException("The test did not release blocked store initialization.");
            }
        };

        var buildTask = StartAutoStartBuildOnDedicatedThread(store, transport);
        try
        {
            var context = await buildTask.WaitAsync(GuardTimeout);
            await Assert.That(initializeEntered.Wait(GuardTimeout)).IsTrue();
            await Assert.That(context.StartupTask.IsCompleted).IsFalse();

            releaseInitialize.Set();
            await context.StartupTask.WaitAsync(GuardTimeout);
        }
        finally
        {
            releaseInitialize.Set();
            await DisposeBuildResultAsync(buildTask);
        }
    }

    /// <summary>Verifies AutoStart does not let synchronous transport connection block Build.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AutoStartBuildReturnsWhenTransportConnectBlocksSynchronously()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        using ManualResetEventSlim connectEntered = new();
        using ManualResetEventSlim releaseConnect = new();
        transport.BeforeConnect = () =>
        {
            connectEntered.Set();
            if (!releaseConnect.Wait(GuardTimeout))
            {
                throw new TimeoutException("The test did not release blocked transport connection.");
            }
        };

        var buildTask = StartAutoStartBuildOnDedicatedThread(store, transport);
        try
        {
            var context = await buildTask.WaitAsync(GuardTimeout);
            await Assert.That(connectEntered.Wait(GuardTimeout)).IsTrue();
            await Assert.That(context.StartupTask.IsCompleted).IsFalse();

            releaseConnect.Set();
            await context.StartupTask.WaitAsync(GuardTimeout);
        }
        finally
        {
            releaseConnect.Set();
            await DisposeBuildResultAsync(buildTask);
        }
    }

    /// <summary>Verifies AutoStart failures remain directly observable after Build returns.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AutoStartFailureRemainsObservableThroughStartupTaskAfterBuildReturns()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter { ConnectException = new InvalidOperationException("connect failed") };
        var options = OccasionallyConnectedOptions.Default with { AutoStart = true };

        await using var context = CreateReadyBuilder(store, transport).UseOptions(options).Build();

        await Assert.That(async () => await context.StartupTask).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies Stop cancels a blocked AutoStart connection without requiring the transport gate to release.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StopCancelsBlockedAutoStartConnect()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter
        {
            ConnectEntered = new(TaskCreationOptions.RunContinuationsAsynchronously),
            ReleaseConnect = new(TaskCreationOptions.RunContinuationsAsynchronously),
        };
        await using var context = CreateReadyBuilder(store, transport)
            .UseOptions(OccasionallyConnectedOptions.Default with { AutoStart = true })
            .Build();

        await transport.ConnectEntered.Task.WaitAsync(GuardTimeout);

        await context.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
    }

    /// <summary>Verifies Dispose cancels a blocked AutoStart connection without requiring the transport gate to release.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task DisposeCancelsBlockedAutoStartConnect()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter
        {
            ConnectEntered = new(TaskCreationOptions.RunContinuationsAsynchronously),
            ReleaseConnect = new(TaskCreationOptions.RunContinuationsAsynchronously),
        };
        var context = CreateReadyBuilder(store, transport)
            .UseOptions(OccasionallyConnectedOptions.Default with { AutoStart = true })
            .Build();

        await transport.ConnectEntered.Task.WaitAsync(GuardTimeout);

        await context.DisposeAsync().AsTask().WaitAsync(GuardTimeout);
    }

    /// <summary>Verifies a stream registered while AutoStart is blocked is included in the eventual start sweep.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AutoStartStartsStreamRegisteredWhileConnectIsBlocked()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter
        {
            ConnectEntered = new(TaskCreationOptions.RunContinuationsAsynchronously),
            ReleaseConnect = new(TaskCreationOptions.RunContinuationsAsynchronously),
        };
        await using var context = CreateReadyBuilder(store, transport)
            .UseOptions(OccasionallyConnectedOptions.Default with { AutoStart = true })
            .Build();
        await transport.ConnectEntered.Task.WaitAsync(GuardTimeout);
        var stream = context.GetOrCreateStream(CreateDefinition());

        _ = transport.ReleaseConnect.TrySetResult();
        await context.StartupTask.WaitAsync(GuardTimeout);

        await Assert.That(stream.SubscriptionId.Value).IsNotEqualTo(Guid.Empty);
    }

    /// <summary>Verifies GetOrCreate after Stop does not infer running state from the historical startup task.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetOrCreateStreamAfterStopDoesNotStartFromHistoricalStartupTask()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateReadyBuilder(store, transport).Build();
        await context.StartAsync(CancellationToken.None);
        await context.StopAsync(CancellationToken.None);

        var stream = context.GetOrCreateStream(CreateDefinition());

        await Assert.That(() => stream.SubscriptionId).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies GetOrCreate while running returns without synchronously blocking on stream startup recovery.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetOrCreateStreamWhileRunningDoesNotBlockOnSynchronousRecovery()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        using ManualResetEventSlim recoverEntered = new();
        using ManualResetEventSlim releaseRecover = new();
        store.BeforeRecover = () =>
        {
            recoverEntered.Set();
            if (!releaseRecover.Wait(GuardTimeout))
            {
                throw new TimeoutException("The test did not release blocked stream recovery.");
            }
        };
        await using var context = CreateReadyBuilder(store, transport).Build();
        await context.StartAsync(CancellationToken.None);

        try
        {
            var stream = context.GetOrCreateStream(CreateDefinition());

            await Assert.That(stream).IsNotNull();
            await Assert.That(recoverEntered.Wait(GuardTimeout)).IsTrue();
            var stopTask = context.StopAsync(CancellationToken.None).AsTask();
            await Assert.That(stopTask.IsCompleted).IsFalse();
            releaseRecover.Set();
            await stopTask.WaitAsync(GuardTimeout);
        }
        finally
        {
            releaseRecover.Set();
        }
    }

    /// <summary>Verifies observer notification scheduling can be delegated to a public sequencer.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UseSequencerAdaptsPublicSequencerForObserverNotifications()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var sequencer = new RecordingSequencer();
        await using var context = CreateReadyBuilder(store, transport)
            .UseSequencer(sequencer)
            .Build();
        var stream = context.GetOrCreateStream(CreateDefinition());
        using var subscription = stream.Local.Subscribe(new RecordingObserver<CounterState>());

        _ = await stream.PublishAsync(new(1), CreateVolatilePublishOptions(), CancellationToken.None);

        await Assert.That(sequencer.ScheduleCalls).IsGreaterThan(0);
    }

    /// <summary>Verifies local commits use the caller-supplied operation identifier source.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UseOperationIdSourceSuppliesLocalCommitIdentifiers()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var operationId = new OperationId(Guid.Parse("3cc99e1e-08b5-4ccb-a19e-03dfac1c737e"));
        await using var context = CreateReadyBuilder(store, transport)
            .UseOperationIdSource(new FixedOperationIdSource(operationId))
            .Build();
        var stream = context.GetOrCreateStream(CreateDefinition());

        _ = await stream.PublishAsync(new(1), CreateVolatilePublishOptions(), CancellationToken.None);

        await Assert.That(store.LastCommittedOperation?.OperationId).IsEqualTo(operationId);
    }

    /// <summary>Verifies the public builder accepts an explicit deterministic retry random source.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UseRetryRandomSourceAcceptsDeterministicSource()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var randomSource = new RecordingRetryRandomSource(RetryRandomValue);

        await using var context = CreateReadyBuilder(store, transport)
            .UseRetryRandomSource(randomSource)
            .Build();

        await Assert.That(context.SyncEngine).IsNotNull();
        await Assert.That(randomSource.LastValue).IsEqualTo(RetryRandomValue);
    }

    /// <summary>Creates an empty builder.</summary>
    /// <returns>The builder.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OccasionallyConnectedBuilder CreateBuilder() => new();

    /// <summary>Creates volatile publish options supported by the in-memory store.</summary>
    /// <returns>The publish options.</returns>
    private static RemotePublishOptions CreateVolatilePublishOptions() => new() { StreamId = Stream, Durable = false };

    /// <summary>Starts AutoStart Build on a dedicated long-running thread.</summary>
    /// <param name="store">The store dependency.</param>
    /// <param name="transport">The transport dependency.</param>
    /// <returns>The build task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<OccasionallyConnectedContext> StartAutoStartBuildOnDedicatedThread(
        RecordingStoreAdapter store,
        RecordingTransportAdapter transport) =>
        Task.Factory.StartNew(
            static state =>
            {
                if (state is not BuildTaskState buildState)
                {
                    throw new InvalidOperationException("The build state is unavailable.");
                }

                return CreateReadyBuilder(buildState.Store, buildState.Transport)
                    .UseOptions(OccasionallyConnectedOptions.Default with { AutoStart = true })
                    .Build();
            },
            new BuildTaskState(store, transport),
            CancellationToken.None,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);

    /// <summary>Observes a build task and disposes the returned context.</summary>
    /// <param name="buildTask">The build task.</param>
    /// <returns>A task representing cleanup.</returns>
    private static async ValueTask DisposeBuildResultAsync(Task<OccasionallyConnectedContext> buildTask)
    {
        var context = await buildTask.WaitAsync(GuardTimeout).ConfigureAwait(false);
        await context.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Creates a ready builder with borrowed dependencies.</summary>
    /// <param name="store">The store dependency.</param>
    /// <param name="transport">The transport dependency.</param>
    /// <returns>The configured builder.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OccasionallyConnectedBuilder CreateReadyBuilder(
        RecordingStoreAdapter store,
        RecordingTransportAdapter transport) =>
        CreateBuilder()
            .UseClient(new(ClientId))
            .UseBorrowedStore(store)
            .UseBorrowedTransport(transport)
            .UseSerializer(new TextPayloadSerializer())
            .UseStoreIdentity(StoreIdentity);

    /// <summary>Creates the default stream definition.</summary>
    /// <returns>The stream definition.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static StreamDefinition<CounterState, CounterInput> CreateDefinition() => CreateDefinition(Stream);

    /// <summary>Creates a stream definition for a specific stream identity.</summary>
    /// <param name="streamId">The stream identity.</param>
    /// <returns>The stream definition.</returns>
    private static StreamDefinition<CounterState, CounterInput> CreateDefinition(StreamId streamId) => new()
    {
        StreamId = streamId,
        Projection = new CounterProjection(),
        InputContractId = InputContract,
        StateContractId = StateContract,
        TypedInput = new() { MaximumRetainedInputBytes = TypedInputBytes },
    };

    /// <summary>Creates a typed null reference for runtime-null contract regression tests.</summary>
    /// <typeparam name="T">The reference type.</typeparam>
    /// <returns>A null reference typed as <typeparamref name="T"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T NullReference<T>()
        where T : class
    {
        object? value = null;
        return Unsafe.As<object?, T>(ref value);
    }
}
