// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedContext"/>.</summary>
public sealed class OccasionallyConnectedContextTests
{
    /// <summary>The client identifier used by context tests.</summary>
    private const string ClientId = "client-a";

    /// <summary>The store identity used by context tests.</summary>
    private const string StoreIdentity = "context-tests";

    /// <summary>The input contract used by context tests.</summary>
    private const string InputContract = "counter-input";

    /// <summary>The state contract used by context tests.</summary>
    private const string StateContract = "counter-state";

    /// <summary>The alternate input contract used by context tests.</summary>
    private const string AlternateInputContract = "counter-input-v2";

    /// <summary>The declared retained bytes for one typed input.</summary>
    private const long TypedInputBytes = 128;

    /// <summary>The timeout message used when tests fail to release a connect callback.</summary>
    private const string ConnectReleaseTimeoutMessage = "The test did not release connect.";

    /// <summary>The alternate schema version used by compatibility tests.</summary>
    private const int AlternateSchemaVersion = 2;

    /// <summary>The nested buffer capacity used by compatibility tests.</summary>
    private const int NestedBufferCapacity = 8;

    /// <summary>The alternate nested buffer capacity used by compatibility tests.</summary>
    private const int AlternateNestedBufferCapacity = 9;

    /// <summary>The alternate priority used by compatibility tests.</summary>
    private const int AlternatePriority = 2;

    /// <summary>The connect count expected after stop drains and start runs again.</summary>
    private const int RestartConnectCount = 2;

    /// <summary>The guard timeout seconds used by fixture synchronization.</summary>
    private const int GuardTimeoutSeconds = 5;

    /// <summary>The stream identity used by context tests.</summary>
    private static readonly StreamId Stream = new("context/main");

    /// <summary>The alternate stream identity used by context tests.</summary>
    private static readonly StreamId AlternateStream = new("context/other");

    /// <summary>The guard timeout used by fixture synchronization.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(GuardTimeoutSeconds);

    /// <summary>Verifies compatible definitions return the same stream facade instance.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetOrCreateStreamReturnsSameInstanceForCompatibleDefinition()
    {
        await using var context = CreateContext();
        var definition = CreateDefinition();

        var first = context.GetOrCreateStream(definition);
        var second = context.GetOrCreateStream(definition with { });

        await Assert.That(second).IsSameReferenceAs(first);
    }

    /// <summary>Verifies the public runtime requires the retained typed-input bound before stream registration.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetOrCreateStreamRejectsMissingTypedInputBeforeRegistration()
    {
        await using var context = CreateContext();
        var definition = CreateDefinitionWithoutTypedInput();

        await Assert.That(() => context.GetOrCreateStream(definition))
            .ThrowsExactly<InvalidOperationException>();
        _ = context.GetOrCreateStream(CreateDefinition());
    }

    /// <summary>Verifies state type compatibility is part of stream registration identity.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetOrCreateStreamRejectsDifferentStateTypeForSameStream()
    {
        await using var context = CreateContext();
        _ = context.GetOrCreateStream(CreateDefinition());

        await Assert.That(() => context.GetOrCreateStream(CreateOtherStateDefinition()))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies input type compatibility is part of stream registration identity.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetOrCreateStreamRejectsDifferentInputTypeForSameStream()
    {
        await using var context = CreateContext();
        _ = context.GetOrCreateStream(CreateDefinition());

        await Assert.That(() => context.GetOrCreateStream(CreateOtherInputDefinition()))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies projection behavior is compared by reference.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetOrCreateStreamRejectsDifferentProjectionReference()
    {
        await using var context = CreateContext();
        _ = context.GetOrCreateStream(CreateDefinition());
        var candidate = CreateDefinition() with { Projection = new CounterProjection() };

        await Assert.That(() => context.GetOrCreateStream(candidate))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies observer input capture behavior is compared by reference.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetOrCreateStreamRejectsDifferentInputCaptureReference()
    {
        await using var context = CreateContext();
        var capture = new InputCapture();
        _ = context.GetOrCreateStream(CreateDefinition() with { Input = new(), InputCapture = capture });
        var candidate = CreateDefinition() with { Input = new(), InputCapture = new InputCapture() };

        await Assert.That(() => context.GetOrCreateStream(candidate))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies wire contracts and recovery versions are part of stream compatibility.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetOrCreateStreamRejectsDifferentContractsVersionsOrSnapshotFormat()
    {
        await using var context = CreateContext();
        _ = context.GetOrCreateStream(CreateDefinition());

        await Assert.That(() => context.GetOrCreateStream(CreateDefinition() with { InputContractId = AlternateInputContract }))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => context.GetOrCreateStream(CreateDefinition() with { InputSchemaVersion = AlternateSchemaVersion }))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => context.GetOrCreateStream(CreateDefinition() with { SnapshotFormatVersion = AlternateSchemaVersion }))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies nested stream options and typed input options are part of compatibility.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetOrCreateStreamRejectsDifferentNestedSubscriptionPublishInputOrTypedOptions()
    {
        await using var context = CreateContext();
        _ = context.GetOrCreateStream(CreateDefinition() with
        {
            Subscription = new() { StreamId = Stream, BufferCapacity = NestedBufferCapacity },
            Publish = new() { StreamId = Stream, Priority = 1 },
            Input = new() { BufferCapacity = NestedBufferCapacity },
            InputCapture = new InputCapture(),
            TypedInput = new() { BufferCapacity = NestedBufferCapacity, MaximumRetainedInputBytes = TypedInputBytes },
        });

        await Assert.That(() => context.GetOrCreateStream(
                CreateDefinition() with
                {
                    Subscription = new() { StreamId = Stream, BufferCapacity = AlternateNestedBufferCapacity },
                }))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => context.GetOrCreateStream(
                CreateDefinition() with
                {
                    Publish = new() { StreamId = Stream, Priority = AlternatePriority },
                }))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => context.GetOrCreateStream(
                CreateDefinition() with
                {
                    Input = new() { BufferCapacity = AlternateNestedBufferCapacity },
                    InputCapture = new InputCapture(),
                }))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => context.GetOrCreateStream(CreateDefinition() with
        {
            TypedInput = new() { BufferCapacity = NestedBufferCapacity, MaximumRetainedInputBytes = TypedInputBytes + 1 },
        }))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies effective subscription identity mismatches are rejected after nested identity normalization.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetOrCreateStreamRejectsDifferentEffectiveSubscriptionId()
    {
        await using var context = CreateContext();
        var subscriptionId = SubscriptionId.New();
        _ = context.GetOrCreateStream(CreateDefinition() with
        {
            SubscriptionId = subscriptionId,
            Subscription = new() { StreamId = Stream, SubscriptionId = subscriptionId },
        });

        await Assert.That(() => context.GetOrCreateStream(CreateDefinition() with
        {
            SubscriptionId = SubscriptionId.New(),
            Subscription = new() { StreamId = Stream },
        }))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies nested subscription options are compared after effective identity normalization.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetOrCreateStreamRejectsDifferentNestedSubscriptionOptionsWhenEffectiveSubscriptionIdMatches()
    {
        await using var context = CreateContext();
        var subscriptionId = SubscriptionId.New();
        _ = context.GetOrCreateStream(CreateDefinition() with
        {
            SubscriptionId = subscriptionId,
            Subscription = new() { StreamId = Stream, BufferCapacity = NestedBufferCapacity },
        });

        await Assert.That(() => context.GetOrCreateStream(CreateDefinition() with
        {
            Subscription = new() { StreamId = Stream, SubscriptionId = subscriptionId, BufferCapacity = AlternateNestedBufferCapacity },
        }))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies nested subscription options still participate when the effective identity is absent.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetOrCreateStreamRejectsNestedSubscriptionOptionsWhenEffectiveSubscriptionIdIsAbsent()
    {
        await using var context = CreateContext();
        _ = context.GetOrCreateStream(CreateDefinition());

        await Assert.That(() => context.GetOrCreateStream(CreateDefinition() with
        {
            Subscription = new() { StreamId = Stream, BufferCapacity = NestedBufferCapacity },
        }))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies absent and explicit subscription identities are incompatible in either registration order.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetOrCreateStreamRejectsNullAndExplicitSubscriptionIdentityMismatch()
    {
        var subscriptionId = SubscriptionId.New();

        await using var nullFirstContext = CreateContext();
        _ = nullFirstContext.GetOrCreateStream(CreateDefinition());
        await Assert.That(() => nullFirstContext.GetOrCreateStream(CreateDefinition() with
        {
            SubscriptionId = subscriptionId,
            Subscription = new() { StreamId = Stream, SubscriptionId = subscriptionId },
        }))
            .ThrowsExactly<InvalidOperationException>();

        await using var explicitFirstContext = CreateContext();
        _ = explicitFirstContext.GetOrCreateStream(CreateDefinition() with
        {
            SubscriptionId = subscriptionId,
            Subscription = new() { StreamId = Stream, SubscriptionId = subscriptionId },
        });
        await Assert.That(() => explicitFirstContext.GetOrCreateStream(CreateDefinition()))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies nested publish and input options are compared after behavior identity matches.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetOrCreateStreamRejectsDifferentNestedPublishOrInputOptionsAfterBehaviorMatches()
    {
        await using var publishContext = CreateContext();
        _ = publishContext.GetOrCreateStream(CreateDefinition() with { Publish = new() { StreamId = Stream, Priority = 1 } });

        await Assert.That(() => publishContext.GetOrCreateStream(
                CreateDefinition() with
                {
                    Publish = new() { StreamId = Stream, Priority = AlternatePriority },
                }))
            .ThrowsExactly<InvalidOperationException>();

        await using var inputContext = CreateContext();
        var capture = new InputCapture();
        _ = inputContext.GetOrCreateStream(CreateDefinition() with
        {
            Input = new() { BufferCapacity = NestedBufferCapacity },
            InputCapture = capture,
        });

        await Assert.That(() => inputContext.GetOrCreateStream(CreateDefinition() with
        {
            Input = new() { BufferCapacity = AlternateNestedBufferCapacity },
            InputCapture = capture,
        }))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies definition-level and nested subscription identities coalesce before compatibility comparison.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetOrCreateStreamAcceptsDefinitionAndNestedMatchingSubscriptionId()
    {
        await using var context = CreateContext();
        var subscriptionId = SubscriptionId.New();
        var first = context.GetOrCreateStream(CreateDefinition() with
        {
            SubscriptionId = subscriptionId,
            Subscription = new() { StreamId = Stream },
        });
        var second = context.GetOrCreateStream(CreateDefinition() with
        {
            Subscription = new() { StreamId = Stream, SubscriptionId = subscriptionId },
        });

        await Assert.That(second).IsSameReferenceAs(first);
    }

    /// <summary>Verifies incompatible definitions are rejected without replacing the existing registration.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task GetOrCreateStreamRejectsIncompatibleDefinitionBeforeRegistration()
    {
        await using var context = CreateContext();
        var definition = CreateDefinition();
        var first = context.GetOrCreateStream(definition);

        await Assert.That(() => context.GetOrCreateStream(definition with { InputContractId = AlternateInputContract }))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(context.GetOrCreateStream(definition)).IsSameReferenceAs(first);
    }

    /// <summary>Verifies registry capacity is enforced before constructing another stream facade.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task RegistryCapacityIsEnforcedBeforeStreamConstruction()
    {
        await using var context = CreateBuilder().WithRegistryCapacity(1).Build();
        _ = context.GetOrCreateStream(CreateDefinition());

        await Assert.That(() => context.GetOrCreateStream(CreateDefinition(AlternateStream)))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies disposal is serialized and idempotent.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task DisposeStopsEngineDisposesStreamFacadesAndIsIdempotent()
    {
        var transport = new RecordingTransportAdapter();
        var builder = CreateBuilder(transport);
        var context = builder.Build();
        _ = context.GetOrCreateStream(CreateDefinition());

        await context.DisposeAsync();
        await context.DisposeAsync();

        await Assert.That(transport.DisposeCalls).IsEqualTo(1);
    }

    /// <summary>Verifies stopping an idle context does not poison the next start intent.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StoppedContextStopThenStartSuccessfully()
    {
        var transport = new RecordingTransportAdapter();
        await using var context = CreateBuilder(transport).Build();

        await context.StopAsync(CancellationToken.None);
        await context.StartAsync(CancellationToken.None);

        await Assert.That(transport.ConnectCalls).IsEqualTo(1);
    }

    /// <summary>Verifies start waits for an accepted stop whose cancellation callbacks are still draining.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StartAsyncDuringStopDrainWaitsForAcceptedStopCompletion()
    {
        var transport = new RecordingTransportAdapter();
        using ManualResetEventSlim connectEntered = new();
        using ManualResetEventSlim releaseConnect = new();
        using ManualResetEventSlim callbackEntered = new();
        using ManualResetEventSlim releaseCallback = new();
        transport.OnConnect = token =>
        {
            _ = token.UnsafeRegister(
                RunCancellationCallback,
                new CancellationCallbackGate(callbackEntered, releaseCallback, GuardTimeout));
            connectEntered.Set();
            if (!releaseConnect.Wait(GuardTimeout, CancellationToken.None))
            {
                throw new TimeoutException(ConnectReleaseTimeoutMessage);
            }

            token.ThrowIfCancellationRequested();
        };
        await using var context = CreateBuilder(transport).Build();
        var blockedStart = StartContextOnDedicatedThread(context);
        try
        {
            await Assert.That(connectEntered.Wait(GuardTimeout)).IsTrue();
            var stopTask = context.StopAsync(CancellationToken.None).AsTask();
            await Assert.That(callbackEntered.Wait(GuardTimeout)).IsTrue();
            var startTask = context.StartAsync(CancellationToken.None).AsTask();
            await Assert.That(startTask.IsCompleted).IsFalse();

            releaseCallback.Set();
            releaseConnect.Set();
            await stopTask.WaitAsync(GuardTimeout);
            await Assert.That(async () => await blockedStart.WaitAsync(GuardTimeout)).Throws<OperationCanceledException>();
            await startTask.WaitAsync(GuardTimeout);

            await Assert.That(transport.ConnectCalls).IsEqualTo(RestartConnectCount);
        }
        finally
        {
            releaseCallback.Set();
            releaseConnect.Set();
        }
    }

    /// <summary>Verifies cancellation callbacks cannot hold the context gate during stop admission.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StopCancellationCallbacksDoNotHoldContextGate()
    {
        var transport = new RecordingTransportAdapter();
        using ManualResetEventSlim connectEntered = new();
        using ManualResetEventSlim releaseConnect = new();
        using ManualResetEventSlim callbackEntered = new();
        using ManualResetEventSlim releaseCallback = new();
        using var callerCancellation = new CancellationTokenSource();
        transport.OnConnect = token =>
        {
            _ = token.UnsafeRegister(
                RunCancellationCallback,
                new CancellationCallbackGate(callbackEntered, releaseCallback, GuardTimeout));
            connectEntered.Set();
            if (!releaseConnect.Wait(GuardTimeout, CancellationToken.None))
            {
                throw new TimeoutException(ConnectReleaseTimeoutMessage);
            }

            token.ThrowIfCancellationRequested();
        };
        await using var context = CreateBuilder(transport)
            .UseOptions(OccasionallyConnectedOptions.Default with { AutoStart = true })
            .Build();
        try
        {
            await Assert.That(connectEntered.Wait(GuardTimeout)).IsTrue();
            var stopTask = context.StopAsync(callerCancellation.Token).AsTask();
            await Assert.That(callbackEntered.Wait(GuardTimeout)).IsTrue();
            await callerCancellation.CancelAsync();
            await Assert.That(async () => await stopTask).Throws<OperationCanceledException>();

            var registrationTask = Task.Factory.StartNew(
                CreateStreamFromContextState,
                context,
                CancellationToken.None,
                TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
                TaskScheduler.Default);
            await Assert.That(await registrationTask.WaitAsync(GuardTimeout)).IsNotNull();
        }
        finally
        {
            releaseCallback.Set();
            releaseConnect.Set();
        }

        await context.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        await Assert.That(async () => await context.StartupTask).Throws<OperationCanceledException>();
    }

    /// <summary>Verifies cancellation callback failures are surfaced through the accepted stop task.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StopReportsStartupCancellationCallbackFailure()
    {
        var fixture = new StartupCancellationFailureFixture();
        try
        {
            fixture.StartContext();
            await Assert.That(fixture.ConnectEntered.Wait(GuardTimeout)).IsTrue();
            fixture.StopContext();
            await Assert.That(fixture.CallbackEntered.Wait(GuardTimeout)).IsTrue();
            fixture.ReleaseConnect();

            await fixture.AssertStopReportsCallbackFailureAsync();
            await fixture.AssertStartCanceledAsync();
            await Assert.That(fixture.ConnectObservedCancellation).IsTrue();
            await Assert.That(fixture.Transport.ConnectCalls).IsEqualTo(1);
        }
        finally
        {
            await fixture.DisposeAsync();
        }
    }

    /// <summary>Creates a context with owned dependencies.</summary>
    /// <returns>The context.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OccasionallyConnectedContext CreateContext() => CreateBuilder().Build();

    /// <summary>Creates a ready builder.</summary>
    /// <param name="transport">The optional transport dependency.</param>
    /// <returns>The builder.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OccasionallyConnectedBuilder CreateBuilder(RecordingTransportAdapter? transport = null) =>
        new OccasionallyConnectedBuilder()
            .UseClient(new(ClientId))
            .UseStore(new InMemoryLocalStoreAdapter())
            .UseTransport(transport ?? new RecordingTransportAdapter())
            .UseSerializer(new TextPayloadSerializer())
            .UseStoreIdentity(StoreIdentity);

    /// <summary>Runs a cancellation callback from explicit callback state.</summary>
    /// <param name="state">The callback state.</param>
    /// <exception cref="InvalidOperationException">The callback state is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void RunCancellationCallback(object? state)
    {
        if (state is not CancellationCallbackGate gate)
        {
            throw new InvalidOperationException("Cancellation callback state is invalid.");
        }

        gate.Run();
    }

    /// <summary>Runs a throwing cancellation callback from explicit callback state.</summary>
    /// <param name="state">The callback state.</param>
    /// <exception cref="InvalidOperationException">The callback state is invalid.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void RunThrowingCancellationCallback(object? state)
    {
        if (state is not ThrowingCancellationCallbackGate gate)
        {
            throw new InvalidOperationException("Cancellation callback state is invalid.");
        }

        gate.Run();
    }

    /// <summary>Creates the default stream from explicit task state.</summary>
    /// <param name="state">The task state.</param>
    /// <returns>The created stream.</returns>
    /// <exception cref="InvalidOperationException">The task state is invalid.</exception>
    private static IOccasionallyConnectedStream<CounterState, CounterInput> CreateStreamFromContextState(object? state)
    {
        if (state is not OccasionallyConnectedContext context)
        {
            throw new InvalidOperationException("Context task state is invalid.");
        }

        return context.GetOrCreateStream(CreateDefinition());
    }

    /// <summary>Starts a context from explicit task state.</summary>
    /// <param name="state">The task state.</param>
    /// <returns>The context start task.</returns>
    /// <exception cref="InvalidOperationException">The task state is invalid.</exception>
    private static Task StartContextFromState(object? state)
    {
        if (state is not OccasionallyConnectedContext context)
        {
            throw new InvalidOperationException("Context task state is invalid.");
        }

        return context.StartAsync(CancellationToken.None).AsTask();
    }

    /// <summary>Starts a context on a dedicated long-running thread.</summary>
    /// <param name="context">The context to start.</param>
    /// <returns>The start task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task StartContextOnDedicatedThread(OccasionallyConnectedContext context) =>
        Task.Factory.StartNew(
                StartContextFromState,
                context,
                CancellationToken.None,
                TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
                TaskScheduler.Default)
            .Unwrap();

    /// <summary>Creates a counter stream definition.</summary>
    /// <param name="streamId">The optional stream identity.</param>
    /// <returns>The stream definition.</returns>
    private static StreamDefinition<CounterState, CounterInput> CreateDefinition(StreamId? streamId = null) => new()
    {
        StreamId = streamId ?? Stream,
        Projection = CounterProjection.Instance,
        InputContractId = InputContract,
        StateContractId = StateContract,
        TypedInput = new() { MaximumRetainedInputBytes = TypedInputBytes },
    };

    /// <summary>Creates a definition whose typed input declaration is absent.</summary>
    /// <returns>The stream definition.</returns>
    private static StreamDefinition<CounterState, CounterInput> CreateDefinitionWithoutTypedInput() =>
        CreateDefinition() with { TypedInput = null };

    /// <summary>Creates a definition with an alternate state type.</summary>
    /// <returns>The stream definition.</returns>
    private static StreamDefinition<OtherState, CounterInput> CreateOtherStateDefinition() => new()
    {
        StreamId = Stream,
        Projection = new OtherStateProjection(),
        InputContractId = InputContract,
        StateContractId = StateContract,
        TypedInput = new() { MaximumRetainedInputBytes = TypedInputBytes },
    };

    /// <summary>Creates a definition with an alternate input type.</summary>
    /// <returns>The stream definition.</returns>
    private static StreamDefinition<CounterState, OtherInput> CreateOtherInputDefinition() => new()
    {
        StreamId = Stream,
        Projection = new OtherInputProjection(),
        InputContractId = InputContract,
        StateContractId = StateContract,
        TypedInput = new() { MaximumRetainedInputBytes = TypedInputBytes },
    };

    /// <summary>Records remote transport disposal.</summary>
    private sealed class RecordingTransportAdapter : IRemoteTransportAdapter
    {
        /// <summary>Gets or sets the connect callback.</summary>
        public Action<CancellationToken> OnConnect { get; set; } = static _ => { };

        /// <summary>Gets the number of connect calls.</summary>
        public int ConnectCalls { get; private set; }

        /// <summary>Gets the number of disposal calls.</summary>
        public int DisposeCalls { get; private set; }

        /// <inheritdoc />
        public RemoteTransportCapabilities Capabilities => RemoteTransportCapabilities.BatchPush;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IRemoteTransportSession> ConnectAsync(TransportConnectRequest request, CancellationToken cancellationToken)
        {
            ConnectCalls++;
            OnConnect(cancellationToken);
            return new(new RecordingTransportSession());
        }

        /// <inheritdoc />
        public ValueTask DisposeAsync()
        {
            DisposeCalls++;
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>Blocks inside a cancellation callback until the test releases it.</summary>
    /// <param name="entered">The signal set when the callback starts.</param>
    /// <param name="release">The signal that releases the callback.</param>
    /// <param name="timeout">The bounded wait timeout.</param>
    private sealed class CancellationCallbackGate(ManualResetEventSlim entered, ManualResetEventSlim release, TimeSpan timeout)
    {
        /// <summary>Runs the blocking callback.</summary>
        /// <exception cref="TimeoutException">The test did not release the callback.</exception>
        public void Run()
        {
            entered.Set();
            if (!release.Wait(timeout))
            {
                throw new TimeoutException("The test did not release the cancellation callback.");
            }
        }
    }

    /// <summary>Owns the blocked startup cancellation failure fixture.</summary>
    private sealed class StartupCancellationFailureFixture : IAsyncDisposable
    {
        /// <summary>The context under test.</summary>
        private readonly OccasionallyConnectedContext _context;

        /// <summary>The named callback failure thrown by the cancellation callback.</summary>
        private readonly InvalidOperationException _callbackFailure = new("startup cancellation callback failed");

        /// <summary>The gate that releases the blocked connect callback.</summary>
        private readonly ManualResetEventSlim _releaseConnect = new();

        /// <summary>The accepted stop task, when stop has been requested.</summary>
        private Task? _stopTask;

        /// <summary>The context startup task.</summary>
        private Task _startTask = Task.CompletedTask;

        /// <summary>Initializes a new instance of the <see cref="StartupCancellationFailureFixture" /> class.</summary>
        public StartupCancellationFailureFixture()
        {
            _context = CreateBuilder(Transport).Build();
            Transport.OnConnect = Connect;
        }

        /// <summary>Gets the connect entry signal.</summary>
        public ManualResetEventSlim ConnectEntered { get; } = new();

        /// <summary>Gets the callback entry signal.</summary>
        public ManualResetEventSlim CallbackEntered { get; } = new();

        /// <summary>Gets the transport adapter.</summary>
        public RecordingTransportAdapter Transport { get; } = new();

        /// <summary>Gets a value indicating whether connect observed cancellation.</summary>
        public bool ConnectObservedCancellation { get; private set; }

        /// <summary>Starts the context on a dedicated thread.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void StartContext() => _startTask = StartContextOnDedicatedThread(_context);

        /// <summary>Requests context stop.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void StopContext() => _stopTask = _context.StopAsync(CancellationToken.None).AsTask();

        /// <summary>Releases the blocked connect callback.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ReleaseConnect() => _releaseConnect.Set();

        /// <summary>Asserts the accepted stop reports the original callback failure.</summary>
        /// <returns>A task representing the assertions.</returns>
        /// <exception cref="InvalidOperationException">Stop was not requested or the stop failure was not captured.</exception>
        public async Task AssertStopReportsCallbackFailureAsync()
        {
            var stopTask = _stopTask ?? throw new InvalidOperationException("Stop was not requested.");
            var stopException = await Assert.ThrowsExactlyAsync<AggregateException>(() => stopTask.WaitAsync(GuardTimeout))
                ?? throw new InvalidOperationException("Stop failure was not captured.");

            await Assert.That(stopException.InnerExceptions).Count().IsEqualTo(1);
            await Assert.That(stopException.InnerExceptions[0]).IsSameReferenceAs(_callbackFailure);
        }

        /// <summary>Asserts startup observes cancellation.</summary>
        /// <returns>A task representing the assertions.</returns>
        /// <exception cref="InvalidOperationException">Start cancellation was not captured.</exception>
        public async Task AssertStartCanceledAsync()
        {
            var startException = await Assert.ThrowsExactlyAsync<OperationCanceledException>(() => _startTask.WaitAsync(GuardTimeout))
                ?? throw new InvalidOperationException("Start cancellation was not captured.");

            await Assert.That(startException.CancellationToken.IsCancellationRequested).IsTrue();
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => new(CleanupAsync());

        /// <summary>Cleans up the fixture while always attempting context disposal.</summary>
        /// <returns>A task representing the cleanup.</returns>
        private async Task CleanupAsync()
        {
            ReleaseConnect();
            try
            {
                if (_stopTask is not null)
                {
                    await AssertStopReportsCallbackFailureAsync();
                }

                await AssertStartCanceledAsync();
            }
            finally
            {
                try
                {
                    await _context.DisposeAsync().AsTask().WaitAsync(GuardTimeout);
                }
                catch (AggregateException exception) when (exception.InnerExceptions.Count == 1)
                {
                    await Assert.That(exception.InnerExceptions[0]).IsSameReferenceAs(_callbackFailure);
                }
                finally
                {
                    ConnectEntered.Dispose();
                    CallbackEntered.Dispose();
                    _releaseConnect.Dispose();
                }
            }

            await Assert.That(Transport.DisposeCalls).IsEqualTo(1);
        }

        /// <summary>Blocks connection startup and registers the throwing cancellation callback.</summary>
        /// <param name="token">The startup cancellation token supplied by the engine.</param>
        /// <exception cref="TimeoutException">The test did not release the connect callback.</exception>
        private void Connect(CancellationToken token)
        {
            _ = token.UnsafeRegister(
                RunThrowingCancellationCallback,
                new ThrowingCancellationCallbackGate(CallbackEntered, _callbackFailure));
            ConnectEntered.Set();
            if (!_releaseConnect.Wait(GuardTimeout, CancellationToken.None))
            {
                throw new TimeoutException(ConnectReleaseTimeoutMessage);
            }

            ConnectObservedCancellation = token.IsCancellationRequested;
            token.ThrowIfCancellationRequested();
        }
    }

    /// <summary>Signals and throws a named cancellation callback failure.</summary>
    /// <param name="entered">The callback entry signal.</param>
    /// <param name="failure">The original failure to throw.</param>
    private sealed class ThrowingCancellationCallbackGate(ManualResetEventSlim entered, Exception failure)
    {
        /// <summary>Signals callback entry and throws the original failure.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Run()
        {
            entered.Set();
            throw failure;
        }
    }

    /// <summary>Provides an inert remote transport session.</summary>
    private sealed class RecordingTransportSession : IRemoteTransportSession
    {
        /// <summary>The negotiated maximum batch operation count used by tests.</summary>
        private const int MaxBatchOperations = 100;

        /// <summary>The negotiated maximum batch payload size used by tests.</summary>
        private const int MaxBatchPayloadBytes = 1_048_576;

        /// <inheritdoc />
        public NegotiatedCapabilities NegotiatedCapabilities { get; } = new(
            new(1, 0),
            RemoteTransportCapabilities.BatchPush,
            MaxBatchOperations,
            MaxBatchPayloadBytes,
            null,
            null);

        /// <inheritdoc />
        public ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken) =>
            new(new RemoteSyncResult(batch.BatchId, [], null, null));

        /// <inheritdoc />
        public async IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(
            RemoteSubscribeRequest request,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask.ConfigureAwait(false);
            yield break;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask AcknowledgeAsync(ReceiveAcknowledgement acknowledgement, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>Serializes counter values as invariant text payloads.</summary>
    private sealed class TextPayloadSerializer : IPayloadSerializer
    {
        /// <inheritdoc />
        public string ContentType => "text/plain";

        /// <inheritdoc />
        public ValueTask<PayloadEnvelope> SerializeAsync<T>(
            string contractId,
            int schemaVersion,
            T value,
            CancellationToken cancellationToken)
        {
            var text = value switch
            {
                CounterInput input => input.Delta.ToString(System.Globalization.CultureInfo.InvariantCulture),
                CounterState state => state.Sum.ToString(System.Globalization.CultureInfo.InvariantCulture),
                _ => "0",
            };
            var bytes = Encoding.UTF8.GetBytes(text);
            return ValueTask.FromResult(new PayloadEnvelope(contractId, schemaVersion, ContentType, bytes, $"hash-{text}"));
        }

        /// <inheritdoc />
        public ValueTask<object> DeserializeAsync(PayloadEnvelope envelope, Type targetType, CancellationToken cancellationToken)
        {
            var text = Encoding.UTF8.GetString(envelope.Payload.Span);
            var value = int.Parse(text, System.Globalization.CultureInfo.InvariantCulture);
            if (targetType == typeof(CounterInput))
            {
                return new(new CounterInput(value));
            }

            if (targetType == typeof(CounterState))
            {
                return new(new CounterState(value));
            }

            throw new InvalidOperationException("Unexpected target type.");
        }
    }

    /// <summary>Captures observer input as a test payload.</summary>
    private sealed class InputCapture : IOccasionallyConnectedInputCapture<CounterInput>
    {
        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetRetainedByteCount(CounterInput value) => TypedInputBytes;

        /// <inheritdoc />
        public PayloadEnvelope Capture(CounterInput value)
        {
            var text = value.Delta.ToString(System.Globalization.CultureInfo.InvariantCulture);
            return new(InputContract, 1, "text/plain", Encoding.UTF8.GetBytes(text), $"hash-{text}");
        }
    }

    /// <summary>Projects counter state.</summary>
    private sealed class CounterProjection : ILocalProjection<CounterState, CounterInput>
    {
        /// <summary>Gets the singleton projection used by compatible definitions.</summary>
        public static CounterProjection Instance { get; } = new();

        /// <inheritdoc />
        public CounterState InitialState { get; } = new(0);

        /// <inheritdoc />
        public CounterState ApplyLocal(CounterState state, CounterInput input, SyncOperation operation) =>
            new(state.Sum + input.Delta);

        /// <inheritdoc />
        public CounterState ApplyRemote(CounterState state, CounterInput input, RemoteEvent remoteEvent) =>
            new(state.Sum + input.Delta);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CounterState Reconcile(CounterState state, ConflictResolutionResult result) => state;
    }

    /// <summary>Projects alternate state.</summary>
    private sealed class OtherStateProjection : ILocalProjection<OtherState, CounterInput>
    {
        /// <inheritdoc />
        public OtherState InitialState { get; } = new(0);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public OtherState ApplyLocal(OtherState state, CounterInput input, SyncOperation operation) => state;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public OtherState ApplyRemote(OtherState state, CounterInput input, RemoteEvent remoteEvent) => state;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public OtherState Reconcile(OtherState state, ConflictResolutionResult result) => state;
    }

    /// <summary>Projects alternate input.</summary>
    private sealed class OtherInputProjection : ILocalProjection<CounterState, OtherInput>
    {
        /// <inheritdoc />
        public CounterState InitialState { get; } = new(0);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CounterState ApplyLocal(CounterState state, OtherInput input, SyncOperation operation) => state;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CounterState ApplyRemote(CounterState state, OtherInput input, RemoteEvent remoteEvent) => state;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CounterState Reconcile(CounterState state, ConflictResolutionResult result) => state;
    }

    /// <summary>Test input value.</summary>
    /// <param name="Delta">The delta.</param>
    private sealed record CounterInput(int Delta);

    /// <summary>Test state value.</summary>
    /// <param name="Sum">The sum.</param>
    private sealed record CounterState(int Sum);

    /// <summary>Alternate input value.</summary>
    /// <param name="Delta">The delta.</param>
    private sealed record OtherInput(int Delta);

    /// <summary>Alternate state value.</summary>
    /// <param name="Sum">The sum.</param>
    private sealed record OtherState(int Sum);
}
