// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Time.Testing;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Configuration and public API tests for <see cref="OccasionallyConnectedBuilder"/>.</content>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>The sample counter delta used by JSON builder configuration tests.</summary>
    private const int JsonCounterDelta = 7;

    /// <summary>The counter delta that exceeds the tiny JSON payload limit.</summary>
    private const int OversizedJsonCounterDelta = 10_000;

    /// <summary>The tiny JSON payload byte limit used by builder configuration tests.</summary>
    private const int TinyJsonPayloadBytes = 4;

    /// <summary>The counter delta used by clock propagation tests.</summary>
    private const int ClockCounterDelta = 1;

    /// <summary>The timestamp supplied by the configured fake clock.</summary>
    private static readonly DateTimeOffset ConfiguredTimestamp = new(2026, 9, 18, 12, 34, 56, TimeSpan.Zero);

    /// <summary>Verifies JSON serializer configuration accepts allowlisted stream contracts without an artificial payload limit.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UseJsonSerializerPublishesRegisteredPayloadsWithoutPayloadLimit()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var registry = CreateCounterSchemaRegistry();
        await using var context = CreateReadyBuilder(store, transport)
            .UseJsonSerializer(registry)
            .Build();
        var stream = context.GetOrCreateStream(CreateDefinition());

        var receipt = await stream.PublishAsync(new(JsonCounterDelta), CreateVolatilePublishOptions(), CancellationToken.None);
        var operation = store.LastCommittedOperation;

        await Assert.That(operation).IsNotNull();
        await Assert.That(receipt.OperationId.Value).IsEqualTo(operation!.OperationId.Value);
        await Assert.That(operation.Payload.ContractId).IsEqualTo(InputContract);
    }

    /// <summary>Verifies the JSON serializer payload limit configured through the builder is enforced on typed input publication.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UseJsonSerializerPayloadLimitRejectsOversizedInput()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var registry = CreateCounterSchemaRegistry();
        await using var context = CreateReadyBuilder(store, transport)
            .UseJsonSerializer(registry, TinyJsonPayloadBytes)
            .Build();
        var stream = context.GetOrCreateStream(CreateDefinition());

        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(
            () => stream.PublishAsync(new(OversizedJsonCounterDelta), CreateVolatilePublishOptions(), CancellationToken.None).AsTask());

        await Assert.That(exception?.Reason).IsEqualTo(PayloadSchemaFailureReason.PayloadTooLarge);
    }

    /// <summary>Verifies context registration rejects definitions outside the configured JSON schema allowlist.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UseJsonSerializerRejectsDefinitionsOutsideRegistryAllowlist()
    {
        await using var missingInputStore = new RecordingStoreAdapter();
        await using var missingInputTransport = new RecordingTransportAdapter();
        await using var missingInputContext = CreateReadyBuilder(missingInputStore, missingInputTransport)
            .UseJsonSerializer(new SchemaRegistry().Register(StateContract, 1, BuilderJsonContext.Default.CounterState))
            .Build();

        var inputException = await Assert.That(() => missingInputContext.GetOrCreateStream(CreateDefinition()))
            .ThrowsExactly<PayloadSchemaException>();

        await using var missingStateStore = new RecordingStoreAdapter();
        await using var missingStateTransport = new RecordingTransportAdapter();
        await using var missingStateContext = CreateReadyBuilder(missingStateStore, missingStateTransport)
            .UseJsonSerializer(new SchemaRegistry().Register(InputContract, 1, BuilderJsonContext.Default.CounterInput))
            .Build();

        var stateException = await Assert.That(() => missingStateContext.GetOrCreateStream(CreateDefinition()))
            .ThrowsExactly<PayloadSchemaException>();

        await Assert.That(inputException?.Reason).IsEqualTo(PayloadSchemaFailureReason.TypeNotAllowed);
        await Assert.That(stateException?.Reason).IsEqualTo(PayloadSchemaFailureReason.TypeNotAllowed);
    }

    /// <summary>Verifies the configured clock supplies timestamps for local commits.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UseTimeProviderSuppliesLocalOperationTimestamp()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var clock = new FakeTimeProvider(ConfiguredTimestamp);
        await using var context = CreateReadyBuilder(store, transport)
            .UseTimeProvider(clock)
            .Build();
        var stream = context.GetOrCreateStream(CreateDefinition());

        _ = await stream.PublishAsync(new(ClockCounterDelta), CreateVolatilePublishOptions(), CancellationToken.None);
        var operation = store.LastCommittedOperation;

        await Assert.That(operation).IsNotNull();
        await Assert.That(operation!.TimestampUtc).IsEqualTo(ConfiguredTimestamp);
    }

    /// <summary>Verifies default store initialization requires an explicit store identity.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task BuildRejectsMissingStoreIdentityForDefaultInitialization()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var builder = CreateBuilder()
            .UseClient(new(ClientId))
            .UseBorrowedStore(store)
            .UseBorrowedTransport(transport)
            .UseSerializer(new TextPayloadSerializer());

        await Assert.That(builder.Build).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies explicit local store initialization is structurally validated by the builder.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ExplicitStoreInitializationRejectsMissingIdentityAndSchemaVersion()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var missingIdentity = CreateReadyBuilder(store, transport)
            .UseStoreInitialization(new(" ", 1, false));

        await Assert.That(missingIdentity.Build).ThrowsExactly<InvalidOperationException>();

        var invalidSchema = missingIdentity.UseStoreInitialization(new(StoreIdentity, 0, false));

        await Assert.That(invalidSchema.Build).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies no-token public context extension overloads start and stop a composed context.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ContextInterfaceExtensionsStartAndStopWithoutExplicitToken()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        await using var context = CreateReadyBuilder(store, transport).Build();
        using var subscription = ((IOccasionallyConnectedContext)context).SyncStates.Subscribe(new RecordingObserver<SyncState>());

        await ((IOccasionallyConnectedContext)context).StartAsync();
        await ((IOccasionallyConnectedContext)context).StopAsync();

        await Assert.That(transport.ConnectCalls).IsEqualTo(1);
    }

    /// <summary>Verifies option transforms compose in order and reach the built context.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ConfigureOptionsComposesTransformsIntoBuiltContext()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();
        var observedAutoStart = false;
        await using var context = CreateReadyBuilder(store, transport)
            .ConfigureOptions(static options => options with { AutoStart = true })
            .ConfigureOptions(options =>
            {
                observedAutoStart = options.AutoStart;
                return options;
            })
            .Build();

        await context.StartupTask.WaitAsync(GuardTimeout);

        await Assert.That(observedAutoStart).IsTrue();
        await Assert.That(transport.ConnectCalls).IsEqualTo(1);
    }

    /// <summary>Verifies option transforms reject missing delegates, null results, and use after build.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ConfigureOptionsRejectsInvalidTransformsAndConsumedBuilder()
    {
        await using var store = new RecordingStoreAdapter();
        await using var transport = new RecordingTransportAdapter();

        await Assert.That(static () => CreateBuilder().ConfigureOptions(NullReference<Func<OccasionallyConnectedOptions, OccasionallyConnectedOptions>>()))
            .ThrowsExactly<ArgumentNullException>();
        await Assert.That(static () => CreateBuilder().ConfigureOptions(static _ => NullReference<OccasionallyConnectedOptions>()))
            .ThrowsExactly<InvalidOperationException>();

        var builder = CreateReadyBuilder(store, transport);
        await using var context = builder.Build();

        await Assert.That(() => builder.ConfigureOptions(static options => options))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Creates a schema registry for the counter stream payload types.</summary>
    /// <returns>The registry.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SchemaRegistry CreateCounterSchemaRegistry() => new SchemaRegistry()
        .Register(InputContract, 1, BuilderJsonContext.Default.CounterInput)
        .Register(StateContract, 1, BuilderJsonContext.Default.CounterState);

    /// <summary>Provides source-generated JSON metadata for builder counter payload tests.</summary>
    [JsonSerializable(typeof(CounterInput))]
    [JsonSerializable(typeof(CounterState))]
    private sealed partial class BuilderJsonContext : JsonSerializerContext;
}
