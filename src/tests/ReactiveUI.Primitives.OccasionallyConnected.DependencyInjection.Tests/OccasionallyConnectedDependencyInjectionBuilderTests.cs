// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection;

namespace ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedDependencyInjectionBuilder"/>.</summary>
public sealed class OccasionallyConnectedDependencyInjectionBuilderTests
{
    /// <summary>The delta published by the generated JSON metadata test.</summary>
    private const int PersistedCounterIncrement = 3;

    /// <summary>The expected number of factory calls after retry.</summary>
    private const int MalformedFactoryRetryCount = 2;

    /// <summary>The maximum stream name length used by the rejection test.</summary>
    private const int ShortStreamNameLimit = 4;

    /// <summary>The counter stream name used by tests.</summary>
    private const string CounterName = "counter";

    /// <summary>The stream name whose length matches the configured short limit.</summary>
    private const string NameAtShortLimit = "main";

    /// <summary>The first alternate stream name used by limit tests.</summary>
    private const string CounterAName = "counter-a";

    /// <summary>The second alternate stream name used by limit tests.</summary>
    private const string CounterBName = "counter-b";

    /// <summary>The delta that exceeds a generated JSON byte limit.</summary>
    private const int OverLimitCounterIncrement = 8;

    /// <summary>Verifies the store identity guard preserves parameter names for null input.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UseStoreIdentityRejectsNullIdentityWithParameterName()
    {
        var services = CreateRequiredServices();

        var exception = Assert.ThrowsExactly<ArgumentNullException>(() => services.AddOccasionallyConnected(static builder =>
            ConfigureRequired(builder).UseStoreIdentity(NullReference<string>())));

        await Assert.That(exception.ParamName).IsEqualTo("storeIdentity");
    }

    /// <summary>Verifies the store identity guard preserves parameter names for blank input.</summary>
    /// <param name="storeIdentity">The invalid store identity.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments("")]
    [Arguments(" \t")]
    public async Task UseStoreIdentityRejectsBlankIdentityWithParameterName(string storeIdentity)
    {
        var services = CreateRequiredServices();

        var exception = Assert.ThrowsExactly<ArgumentException>(() => services.AddOccasionallyConnected(builder =>
            ConfigureRequired(builder).UseStoreIdentity(storeIdentity)));
        await Assert.That(exception.ParamName).IsEqualTo("storeIdentity");
    }

    /// <summary>Verifies stream names are bounded before registration.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AddStreamRejectsNameLongerThanConfiguredBoundBeforeRegistration()
    {
        var services = CreateRequiredServices();

        await Assert.That(() => services.AddOccasionallyConnected(static builder =>
            ConfigureRequired(builder)
                .WithMaximumStreamNameLength(ShortStreamNameLimit)
                .AddStream(
                    "counter-main",
                    DependencyInjectionTestDoubles.CreateDefinition)))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies stream registration rejects a null name with the expected parameter name.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AddStreamRejectsNullNameWithParameterName()
    {
        var services = CreateRequiredServices();

        var exception = Assert.ThrowsExactly<ArgumentNullException>(() => services.AddOccasionallyConnected(static builder =>
            ConfigureRequired(builder).AddStream(NullReference<string>(), DependencyInjectionTestDoubles.CreateDefinition)));

        await Assert.That(exception.ParamName).IsEqualTo("name");
    }

    /// <summary>Verifies stream registration rejects blank names with the expected parameter name.</summary>
    /// <param name="name">The invalid stream name.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments("")]
    [Arguments(" \t")]
    public async Task AddStreamRejectsBlankNameWithParameterName(string name)
    {
        var services = CreateRequiredServices();

        var exception = Assert.ThrowsExactly<ArgumentException>(() => services.AddOccasionallyConnected(builder =>
            ConfigureRequired(builder).AddStream(name, DependencyInjectionTestDoubles.CreateDefinition)));
        await Assert.That(exception.ParamName).IsEqualTo("name");
    }

    /// <summary>Verifies the named stream registry has a finite registration count.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AddStreamRejectsRegistrationsBeyondConfiguredCountBeforeRegistration()
    {
        var services = CreateRequiredServices();

        await Assert.That(() => services.AddOccasionallyConnected(static builder =>
            ConfigureRequired(builder)
                .WithMaximumNamedStreams(1)
                .AddStream(
                    CounterAName,
                    DependencyInjectionTestDoubles.CreateDefinition)
                .AddStream(
                    CounterBName,
                    DependencyInjectionTestDoubles.CreateDefinition)))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies JSON contract registration rejects a null contract identifier with the expected parameter name.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AddJsonContractRejectsNullContractIdWithParameterName()
    {
        var services = CreateRequiredServices();

        var exception = Assert.ThrowsExactly<ArgumentNullException>(() => services.AddOccasionallyConnected(static builder =>
            ConfigureRequired(builder).AddJsonContract(
                NullReference<string>(),
                1,
                DependencyInjectionJsonContext.Default.CounterInput)));

        await Assert.That(exception.ParamName).IsEqualTo("contractId");
    }

    /// <summary>Verifies JSON contract registration rejects blank contract identifiers with the expected parameter name.</summary>
    /// <param name="contractId">The invalid contract identifier.</param>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    [Arguments("")]
    [Arguments(" \t")]
    public async Task AddJsonContractRejectsBlankContractIdWithParameterName(string contractId)
    {
        var services = CreateRequiredServices();

        var exception = Assert.ThrowsExactly<ArgumentException>(() => services.AddOccasionallyConnected(builder =>
            ConfigureRequired(builder).AddJsonContract(
                contractId,
                1,
                DependencyInjectionJsonContext.Default.CounterInput)));
        await Assert.That(exception.ParamName).IsEqualTo("contractId");
    }

    /// <summary>Verifies duplicate name and stream type pairs are rejected deterministically.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AddStreamRejectsDuplicateNameAndTypeBeforeProviderBuild()
    {
        var services = CreateRequiredServices();

        await Assert.That(() => services.AddOccasionallyConnected(static builder =>
            ConfigureRequired(builder)
                .AddStream(CounterName, DependencyInjectionTestDoubles.CreateDefinition)
                .AddStream(CounterName, DependencyInjectionTestDoubles.CreateDefinition)))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies duplicate names with different stream types are rejected before resolution.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AddStreamRejectsDuplicateNameWithDifferentTypeBeforeProviderBuild()
    {
        var services = CreateRequiredServices();

        await Assert.That(() => services.AddOccasionallyConnected(static builder =>
            ConfigureRequired(builder)
                .AddStream(
                    CounterName,
                    DependencyInjectionTestDoubles.CreateDefinition)
                .AddStream(CounterName, DependencyInjectionTestDoubles.CreateOtherDefinition)))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies JSON contract registration flows into the schema registry used by the context.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AddJsonContractBuildsSerializerRegistryFromGeneratedMetadata()
    {
        await using var provider = CreateProvider(RegisterCounterStream);
        var streams = provider.GetRequiredService<IOccasionallyConnectedStreamRegistry>();
        var store = provider.GetRequiredService<DependencyInjectionTestDoubles.RecordingStoreAdapter>();

        var stream = streams.GetRequiredStream(CreateCounterStreamKey());
        var observer = new DependencyInjectionTestDoubles.RecordingObserver<DependencyInjectionTestDoubles.CounterState>();
        using var subscription = stream.Local.Subscribe(observer);

        var receipt = await stream.PublishAsync(
            new(PersistedCounterIncrement),
            null,
            CancellationToken.None);
        var snapshot = await store.RecoverLastSnapshotAsync();
        var persisted = DependencyInjectionTestDoubles.DeserializeCounterState(snapshot);

        await Assert.That(receipt.ClientSequence).IsEqualTo(1);
        var observed = await observer.WaitForValueAsync(
            static value => value.Sum == PersistedCounterIncrement,
            DependencyInjectionTestDoubles.GuardTimeout);

        await Assert.That(observed.Sum).IsEqualTo(PersistedCounterIncrement);
        await Assert.That(persisted.Sum).IsEqualTo(PersistedCounterIncrement);
    }

    /// <summary>Verifies malformed stream definitions fail through the public context and are not cached.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AddStreamFactoryResultIsValidatedByContextBeforeCaching()
    {
        var calls = 0;
        await using var provider = CreateProvider(builder => builder.AddStream(
            CounterName,
            services => Interlocked.Increment(ref calls) == 1
                ? DependencyInjectionTestDoubles.CreateDefinition(services) with { TypedInput = null }
                : DependencyInjectionTestDoubles.CreateDefinition(services)));
        var streams = provider.GetRequiredService<IOccasionallyConnectedStreamRegistry>();

        await Assert.That(() => streams.GetRequiredStream(CreateCounterStreamKey()))
            .ThrowsExactly<InvalidOperationException>();

        var stream = streams.GetRequiredStream(CreateCounterStreamKey());

        await Assert.That(stream).IsNotNull();
        await Assert.That(calls).IsEqualTo(MalformedFactoryRetryCount);
    }

    /// <summary>Verifies stream factories returning null are rejected through the public registry.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AddStreamFactoryRejectsNullResultThroughPublicRegistry()
    {
        await using var provider = CreateProvider(static builder => builder.AddStream(
            CounterName,
            NullCounterDefinition));
        var streams = provider.GetRequiredService<IOccasionallyConnectedStreamRegistry>();

        await Assert.That(() => streams.GetRequiredStream(CreateCounterStreamKey()))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a direct null runtime options snapshot is rejected.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UseOptionsRejectsNullRuntimeOptions()
    {
        var services = CreateRequiredServices();

        await Assert.That(() => services.AddOccasionallyConnected(static builder => ConfigureRequired(builder)
            .UseOptions(NullReference<OccasionallyConnectedOptions>())))
            .ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies runtime options supplied through UseOptions are applied at context resolution.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UseOptionsAppliesRuntimeOptionsWhenContextIsResolved()
    {
        var services = CreateRequiredServices();
        _ = services.AddOccasionallyConnected(static builder => ConfigureRequired(builder)
            .UseOptions(OccasionallyConnectedOptions.Default with { MaxConcurrentStreams = 0 }));
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        await Assert.That(provider.GetRequiredService<OccasionallyConnectedContext>)
            .ThrowsExactly<OptionsValidationException>();
    }

    /// <summary>Verifies a null runtime option transform result is rejected.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ConfigureOptionsRejectsNullTransformResult()
    {
        var services = CreateRequiredServices();

        await Assert.That(() => services.AddOccasionallyConnected(static builder => ConfigureRequired(builder)
            .ConfigureOptions(static _ => NullReference<OccasionallyConnectedOptions>())))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies lowering the stream count limit after registrations validates existing streams.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task WithMaximumNamedStreamsRejectsExistingRegistrationCountAboveLimit()
    {
        var services = CreateRequiredServices();

        await Assert.That(() => services.AddOccasionallyConnected(static builder => ConfigureRequired(builder)
            .AddStream(CounterAName, DependencyInjectionTestDoubles.CreateDefinition)
            .AddStream(CounterBName, DependencyInjectionTestDoubles.CreateDefinition)
            .WithMaximumNamedStreams(1)))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies lowering the stream name length after registration validates existing streams.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task WithMaximumStreamNameLengthRejectsExistingNameAboveLimit()
    {
        var services = CreateRequiredServices();

        await Assert.That(() => services.AddOccasionallyConnected(static builder => ConfigureRequired(builder)
            .AddStream(CounterName, DependencyInjectionTestDoubles.CreateDefinition)
            .WithMaximumStreamNameLength(ShortStreamNameLimit)))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies names at the configured stream name limit are accepted.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task WithMaximumStreamNameLengthAcceptsExistingNameAtLimit()
    {
        var services = CreateRequiredServices();
        _ = services.AddOccasionallyConnected(static builder => ConfigureRequired(builder)
            .AddStream(NameAtShortLimit, DependencyInjectionTestDoubles.CreateDefinition)
            .WithMaximumStreamNameLength(ShortStreamNameLimit));
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var registry = provider.GetRequiredService<IOccasionallyConnectedStreamRegistry>();

        var stream = registry.GetRequiredStream(CreateCounterStreamKey(NameAtShortLimit));

        await Assert.That(stream).IsNotNull();
    }

    /// <summary>Verifies missing client selection is rejected when other required configuration is complete.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AddOccasionallyConnectedRejectsMissingClientSelection()
    {
        var services = CreateRequiredServices();

        await Assert.That(() => services.AddOccasionallyConnected(static builder => _ = builder
            .UseStoreIdentity(DependencyInjectionTestDoubles.StoreIdentity)
            .UseStoreInitialization(DependencyInjectionTestDoubles.CreateStoreInitialization())
            .UseStore(typeof(DependencyInjectionTestDoubles.RecordingStoreAdapter))
            .UseTransport(typeof(DependencyInjectionTestDoubles.RecordingTransportAdapter))
            .UseJsonSerializer()
            .AddJsonContract(
                DependencyInjectionTestDoubles.InputContract,
                1,
                DependencyInjectionJsonContext.Default.CounterInput)
            .AddJsonContract(
                DependencyInjectionTestDoubles.StateContract,
                1,
                DependencyInjectionJsonContext.Default.CounterState)))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies missing store selection is rejected when the configuration is built.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AddOccasionallyConnectedRejectsMissingStoreSelection()
    {
        var services = CreateRequiredServices();

        await Assert.That(() => services.AddOccasionallyConnected(static builder => _ = builder
            .UseClient(new(DependencyInjectionTestDoubles.ClientId))
            .UseTransport(typeof(DependencyInjectionTestDoubles.RecordingTransportAdapter))
            .UseJsonSerializer()))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies missing transport selection is rejected when the configuration is built.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AddOccasionallyConnectedRejectsMissingTransportSelection()
    {
        var services = CreateRequiredServices();

        await Assert.That(() => services.AddOccasionallyConnected(static builder => _ = builder
            .UseClient(new(DependencyInjectionTestDoubles.ClientId))
            .UseStore(typeof(DependencyInjectionTestDoubles.RecordingStoreAdapter))
            .UseJsonSerializer()))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies missing serializer selection is rejected when JSON metadata is not selected.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task AddOccasionallyConnectedRejectsMissingSerializerSelection()
    {
        var services = CreateRequiredServices();

        await Assert.That(() => services.AddOccasionallyConnected(static builder => _ = builder
            .UseClient(new(DependencyInjectionTestDoubles.ClientId))
            .UseStore(typeof(DependencyInjectionTestDoubles.RecordingStoreAdapter))
            .UseTransport(typeof(DependencyInjectionTestDoubles.RecordingTransportAdapter))))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies selected store service types must implement the local store contract.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UseStoreRejectsTypeOutsideStoreContract()
    {
        var services = CreateRequiredServices();

        await Assert.That(() => services.AddOccasionallyConnected(static builder => ConfigureRequired(builder)
            .UseStore(typeof(DependencyInjectionTestDoubles.RecordingTransportAdapter))))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies selected transport service types must implement the remote transport contract.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UseTransportRejectsTypeOutsideTransportContract()
    {
        var services = CreateRequiredServices();

        await Assert.That(() => services.AddOccasionallyConnected(static builder => ConfigureRequired(builder)
            .UseTransport(typeof(DependencyInjectionTestDoubles.RecordingStoreAdapter))))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies selected serializer service types must implement the payload serializer contract.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UseSerializerRejectsTypeOutsideSerializerContract()
    {
        var services = CreateRequiredServices();
        _ = services.AddSingleton(static _ => DependencyInjectionTestDoubles.CreateJsonPayloadSerializer());

        await Assert.That(() => services.AddOccasionallyConnected(static builder => ConfigureRequired(builder)
            .UseSerializer(typeof(DependencyInjectionTestDoubles.RecordingStoreAdapter))))
            .ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies selecting an explicit serializer publishes through the selected serializer.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UseSerializerAfterJsonSerializerPublishesWithExplicitSerializer()
    {
        var services = CreateRequiredServices();
        _ = services.AddSingleton(static _ => DependencyInjectionTestDoubles.CreateJsonPayloadSerializer());
        _ = services.AddOccasionallyConnected(static builder => ConfigureRequired(builder)
            .UseJsonSerializer()
            .UseSerializer(typeof(JsonPayloadSerializer))
            .AddStream(CounterName, DependencyInjectionTestDoubles.CreateDefinition));
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        var persisted = await PublishAndRecoverCounterAsync(provider, PersistedCounterIncrement);

        await Assert.That(persisted.Sum).IsEqualTo(PersistedCounterIncrement);
    }

    /// <summary>Verifies the last successful explicit serializer selection wins after JSON mode.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UseSerializerAfterJsonAfterSerializerPublishesWithFinalExplicitSerializer()
    {
        var services = CreateRequiredServices();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.PrimaryRecordingPayloadSerializer>();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.SecondaryRecordingPayloadSerializer>();
        _ = services.AddOccasionallyConnected(static builder => ConfigureRequired(builder)
            .UseSerializer(typeof(DependencyInjectionTestDoubles.PrimaryRecordingPayloadSerializer))
            .UseJsonSerializer()
            .UseSerializer(typeof(DependencyInjectionTestDoubles.SecondaryRecordingPayloadSerializer))
            .AddStream(CounterName, DependencyInjectionTestDoubles.CreateDefinition));
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var first = provider.GetRequiredService<DependencyInjectionTestDoubles.PrimaryRecordingPayloadSerializer>();
        var second = provider.GetRequiredService<DependencyInjectionTestDoubles.SecondaryRecordingPayloadSerializer>();

        var persisted = await PublishAndRecoverCounterAsync(provider, PersistedCounterIncrement);

        await Assert.That(persisted.Sum).IsEqualTo(PersistedCounterIncrement);
        await Assert.That(first.SerializeCalls).IsEqualTo(0);
        await Assert.That(second.SerializeCalls > 0).IsTrue();
    }

    /// <summary>Verifies a failed serializer selection leaves the previous valid explicit selection intact.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task FailedUseSerializerCallRetainsPreviousExplicitSerializerSelection()
    {
        var services = CreateRequiredServices();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.PrimaryRecordingPayloadSerializer>();
        _ = services.AddOccasionallyConnected(static builder =>
        {
            _ = ConfigureRequired(builder)
                .UseSerializer(typeof(DependencyInjectionTestDoubles.PrimaryRecordingPayloadSerializer));
            _ = Assert.ThrowsExactly<InvalidOperationException>(
                () => builder.UseSerializer(typeof(JsonPayloadSerializer)));
            _ = builder.AddStream(CounterName, DependencyInjectionTestDoubles.CreateDefinition);
        });
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var serializer = provider.GetRequiredService<DependencyInjectionTestDoubles.PrimaryRecordingPayloadSerializer>();

        var persisted = await PublishAndRecoverCounterAsync(provider, PersistedCounterIncrement);

        await Assert.That(persisted.Sum).IsEqualTo(PersistedCounterIncrement);
        await Assert.That(serializer.SerializeCalls > 0).IsTrue();
    }

    /// <summary>Verifies a failed store selection leaves the previous valid singleton store selection intact.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task FailedUseStoreCallRetainsPreviousStoreSelection()
    {
        var services = CreateRequiredServices();
        _ = services.AddTransient<InMemoryLocalStoreAdapter>();
        _ = services.AddOccasionallyConnected(static builder =>
        {
            _ = ConfigureRequired(builder);
            _ = Assert.ThrowsExactly<InvalidOperationException>(
                () => builder.UseStore(typeof(InMemoryLocalStoreAdapter)));
            _ = builder.AddStream(CounterName, DependencyInjectionTestDoubles.CreateDefinition);
        });
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var store = provider.GetRequiredService<DependencyInjectionTestDoubles.RecordingStoreAdapter>();

        var persisted = await PublishAndRecoverCounterAsync(provider, PersistedCounterIncrement);

        await Assert.That(persisted.Sum).IsEqualTo(PersistedCounterIncrement);
        await Assert.That(store.LastCommittedOperation).IsNotNull();
    }

    /// <summary>Verifies a failed transport selection leaves the previous valid singleton transport selection intact.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task FailedUseTransportCallRetainsPreviousTransportSelection()
    {
        var services = CreateRequiredServices();
        _ = services.AddTransient<LoopbackTransportAdapter>();
        _ = services.AddOccasionallyConnected(static builder =>
        {
            _ = ConfigureRequired(builder);
            _ = Assert.ThrowsExactly<InvalidOperationException>(
                () => builder.UseTransport(typeof(LoopbackTransportAdapter)));
            _ = builder.AddStream(CounterName, DependencyInjectionTestDoubles.CreateDefinition);
        });
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var transport = provider.GetRequiredService<DependencyInjectionTestDoubles.RecordingTransportAdapter>();

        var persisted = await PublishAndRecoverCounterAsync(provider, PersistedCounterIncrement);

        await Assert.That(persisted.Sum).IsEqualTo(PersistedCounterIncrement);
        await Assert.That(transport.DisposeCalls).IsEqualTo(0);
    }

    /// <summary>Verifies generated JSON serialization publishes payloads at the explicit byte limit.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UseJsonSerializerWithMaximumPayloadBytesPublishesPayloadAtLimit()
    {
        var payloadBytes = SerializedInputLength(PersistedCounterIncrement);
        var services = CreateRequiredServices();
        _ = services.AddOccasionallyConnected(builder => ConfigureRequired(builder)
            .UseJsonSerializer(payloadBytes)
            .AddStream(CounterName, DependencyInjectionTestDoubles.CreateDefinition));
        await using var provider = services.BuildServiceProvider(validateScopes: true);

        var persisted = await PublishAndRecoverCounterAsync(provider, PersistedCounterIncrement);

        await Assert.That(persisted.Sum).IsEqualTo(PersistedCounterIncrement);
    }

    /// <summary>Verifies generated JSON serialization rejects payloads above the explicit byte limit.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UseJsonSerializerWithMaximumPayloadBytesRejectsOverLimitPayloadWithoutStoreMutation()
    {
        var payloadBytes = SerializedInputLength(OverLimitCounterIncrement);
        var services = CreateRequiredServices();
        _ = services.AddOccasionallyConnected(builder => ConfigureRequired(builder)
            .UseJsonSerializer(payloadBytes - 1)
            .AddStream(CounterName, DependencyInjectionTestDoubles.CreateDefinition));
        await using var provider = services.BuildServiceProvider(validateScopes: true);
        var registry = provider.GetRequiredService<IOccasionallyConnectedStreamRegistry>();
        var store = provider.GetRequiredService<DependencyInjectionTestDoubles.RecordingStoreAdapter>();
        var stream = registry.GetRequiredStream(CreateCounterStreamKey());

        await Assert.That(async () => await stream.PublishAsync(
                new(OverLimitCounterIncrement),
                null,
                CancellationToken.None).AsTask().WaitAsync(DependencyInjectionTestDoubles.GuardTimeout))
            .ThrowsExactly<PayloadSchemaException>();
        await Assert.That(store.LastCommittedOperation).IsNull();
        await Assert.That(store.LastSnapshotMutation).IsNull();
    }

    /// <summary>Registers the counter stream used by the generated JSON metadata test.</summary>
    /// <param name="builder">The DI builder.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void RegisterCounterStream(OccasionallyConnectedDependencyInjectionBuilder builder) =>
        _ = builder.AddStream(CounterName, DependencyInjectionTestDoubles.CreateDefinition);

    /// <summary>Creates the strongly typed key for the counter stream.</summary>
    /// <returns>The stream key.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OccasionallyConnectedStreamKey<DependencyInjectionTestDoubles.CounterState, DependencyInjectionTestDoubles.CounterInput>
        CreateCounterStreamKey() =>
        CreateCounterStreamKey(CounterName);

    /// <summary>Creates the strongly typed key for the named counter stream.</summary>
    /// <param name="name">The stream name.</param>
    /// <returns>The stream key.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OccasionallyConnectedStreamKey<DependencyInjectionTestDoubles.CounterState, DependencyInjectionTestDoubles.CounterInput>
        CreateCounterStreamKey(string name) =>
        new(name);

    /// <summary>Creates a null counter stream definition for malformed external factory tests.</summary>
    /// <param name="services">The service provider supplied by the registry.</param>
    /// <returns>A null counter stream definition.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static StreamDefinition<DependencyInjectionTestDoubles.CounterState, DependencyInjectionTestDoubles.CounterInput>
        NullCounterDefinition(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        return NullReference<StreamDefinition<DependencyInjectionTestDoubles.CounterState, DependencyInjectionTestDoubles.CounterInput>>();
    }

    /// <summary>Publishes a counter input and recovers the persisted counter snapshot.</summary>
    /// <param name="provider">The service provider.</param>
    /// <param name="delta">The counter delta.</param>
    /// <returns>The recovered counter state.</returns>
    private static async Task<DependencyInjectionTestDoubles.CounterState> PublishAndRecoverCounterAsync(
        IServiceProvider provider,
        int delta)
    {
        var registry = provider.GetRequiredService<IOccasionallyConnectedStreamRegistry>();
        var store = provider.GetRequiredService<DependencyInjectionTestDoubles.RecordingStoreAdapter>();
        var stream = registry.GetRequiredStream(CreateCounterStreamKey());

        _ = await stream.PublishAsync(new(delta), null, CancellationToken.None)
            .AsTask()
            .WaitAsync(DependencyInjectionTestDoubles.GuardTimeout);
        var snapshot = await store.RecoverLastSnapshotAsync();
        return DependencyInjectionTestDoubles.DeserializeCounterState(snapshot);
    }

    /// <summary>Computes the generated JSON payload length for a counter input.</summary>
    /// <param name="delta">The counter delta.</param>
    /// <returns>The serialized payload byte length.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int SerializedInputLength(int delta) =>
        JsonSerializer.SerializeToUtf8Bytes(
            new(delta),
            DependencyInjectionJsonContext.Default.CounterInput).Length;

    /// <summary>Creates a service provider with required DI services.</summary>
    /// <param name="configure">The additional builder configuration.</param>
    /// <returns>The service provider.</returns>
    private static ServiceProvider CreateProvider(Action<OccasionallyConnectedDependencyInjectionBuilder> configure)
    {
        var services = CreateRequiredServices();
        _ = services.AddOccasionallyConnected(builder =>
        {
            _ = ConfigureRequired(builder);
            configure(builder);
        });
        return services.BuildServiceProvider(validateScopes: true);
    }

    /// <summary>Creates a null reference for a non-nullable malformed-input fixture.</summary>
    /// <typeparam name="T">The non-nullable reference type.</typeparam>
    /// <returns>A null reference typed as <typeparamref name="T" />.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T NullReference<T>()
        where T : class
    {
        object? value = null;
        return Unsafe.As<object?, T>(ref value);
    }

    /// <summary>Creates required test services.</summary>
    /// <returns>The service collection.</returns>
    private static ServiceCollection CreateRequiredServices()
    {
        ServiceCollection services = new();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.RecordingStoreAdapter>();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.RecordingTransportAdapter>();
        _ = services.AddSingleton<DependencyInjectionTestDoubles.CounterProjection>();
        return services;
    }

    /// <summary>Applies the required builder configuration used by tests.</summary>
    /// <param name="builder">The DI builder.</param>
    /// <returns>The same builder.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static OccasionallyConnectedDependencyInjectionBuilder ConfigureRequired(
        OccasionallyConnectedDependencyInjectionBuilder builder) =>
        builder
            .UseClient(new(DependencyInjectionTestDoubles.ClientId))
            .UseStoreIdentity(DependencyInjectionTestDoubles.StoreIdentity)
            .UseStoreInitialization(DependencyInjectionTestDoubles.CreateStoreInitialization())
            .UseStore(typeof(DependencyInjectionTestDoubles.RecordingStoreAdapter))
            .UseTransport(typeof(DependencyInjectionTestDoubles.RecordingTransportAdapter))
            .UseJsonSerializer()
            .AddJsonContract(
                DependencyInjectionTestDoubles.InputContract,
                1,
                DependencyInjectionJsonContext.Default.CounterInput)
            .AddJsonContract(
                DependencyInjectionTestDoubles.StateContract,
                1,
                DependencyInjectionJsonContext.Default.CounterState);
}
