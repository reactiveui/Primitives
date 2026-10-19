// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json.Serialization;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="SchemaRegistry"/>.</summary>
public sealed partial class SchemaRegistryTests
{
    /// <summary>The contract identifier used by schema tests.</summary>
    private const string Contract = "sample.contract";

    /// <summary>The first schema version.</summary>
    private const int SchemaV1 = 1;

    /// <summary>The second schema version.</summary>
    private const int SchemaV2 = 2;

    /// <summary>The third schema version.</summary>
    private const int SchemaV3 = 3;

    /// <summary>Verifies untrusted version ranges cannot reserve memory proportional to the version gap.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExtremeVersionGapRejectsMissingChainWithoutUnboundedAllocation()
    {
        var registry = new SchemaRegistry();
        var exception = CapturePayloadSchemaException(() => registry.GetUpcastChain(Contract, SchemaV1, int.MaxValue));
        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.MissingUpcaster);
    }

    /// <summary>Verifies metadata cannot change after its safety checks have passed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RegisteredMetadataIsFrozen()
    {
        var metadata = RegistryJsonContext.Default.SampleV1;
        _ = new SchemaRegistry().Register(Contract, SchemaV1, metadata);
        await Assert.That(metadata.IsReadOnly).IsTrue();
        await Assert.That(() => metadata.PolymorphismOptions = new()).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies registered schemas are allowlisted by contract, version, and type.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task IsRegisteredReturnsTrueOnlyForExactAllowlistEntry()
    {
        var registry = new SchemaRegistry().Register(Contract, SchemaV1, RegistryJsonContext.Default.SampleV1);

        await Assert.That(registry.IsRegistered(Contract, SchemaV1, typeof(SampleV1))).IsTrue();
        await Assert.That(registry.IsRegistered(Contract, SchemaV2, typeof(SampleV1))).IsFalse();
        await Assert.That(registry.IsRegistered(Contract, SchemaV1, typeof(SampleV2))).IsFalse();
    }

    /// <summary>Verifies registration rejects empty contract identifiers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RegisterRejectsEmptyContractId()
    {
        var registry = new SchemaRegistry();

        var exception = CapturePayloadSchemaException(() => registry.Register(string.Empty, SchemaV1, RegistryJsonContext.Default.SampleV1));

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.UnknownContract);
    }

    /// <summary>Verifies registration rejects nonpositive schema versions.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RegisterRejectsInvalidSchemaVersion()
    {
        var registry = new SchemaRegistry();

        var exception = CapturePayloadSchemaException(() => registry.Register(Contract, 0, RegistryJsonContext.Default.SampleV1));

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.InvalidSchemaVersion);
    }

    /// <summary>Verifies duplicate schema registrations are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RegisterRejectsDuplicateSchema()
    {
        var registry = new SchemaRegistry().Register(Contract, SchemaV1, RegistryJsonContext.Default.SampleV1);

        var exception = CapturePayloadSchemaException(() => registry.Register(Contract, SchemaV1, RegistryJsonContext.Default.SampleV1));

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.InvalidSchemaVersion);
    }

    /// <summary>Verifies older schema registrations do not replace the target version.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RegisterKeepsHighestTargetVersion()
    {
        var registry = new SchemaRegistry()
            .Register(Contract, SchemaV2, RegistryJsonContext.Default.SampleV1)
            .Register(Contract, SchemaV1, RegistryJsonContext.Default.SampleV1);

        await Assert.That(registry.IsRegistered(Contract, SchemaV1, typeof(SampleV1))).IsTrue();
    }

    /// <summary>Verifies upcast chains are strictly contiguous.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetUpcastChainReturnsContiguousChain()
    {
        var registry = new SchemaRegistry()
            .Register(Contract, SchemaV1, RegistryJsonContext.Default.SampleV1)
            .Register(Contract, SchemaV2, RegistryJsonContext.Default.SampleV2)
            .Register(Contract, SchemaV3, RegistryJsonContext.Default.SampleV3)
            .RegisterUpcaster(new Upcaster(SchemaV1, SchemaV2))
            .RegisterUpcaster(new Upcaster(SchemaV2, SchemaV3));

        var chain = registry.GetUpcastChain(Contract, SchemaV1, SchemaV3);

        await Assert.That(chain).Count().IsEqualTo(SchemaV2);
        await Assert.That(chain[0].FromVersion).IsEqualTo(SchemaV1);
        await Assert.That(chain[1].FromVersion).IsEqualTo(SchemaV2);
    }

    /// <summary>Verifies upcaster metadata is captured when registered.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RegisterUpcasterCapturesMutableMetadata()
    {
        MutableUpcaster upcaster = new() { ContractId = Contract, FromVersion = SchemaV1, ToVersion = SchemaV2 };
        var registry = new SchemaRegistry().RegisterUpcaster(upcaster);
        upcaster.ContractId = "changed.contract";
        upcaster.FromVersion = SchemaV2;
        upcaster.ToVersion = SchemaV3;

        var chain = registry.GetUpcastChain(Contract, SchemaV1, SchemaV2);

        await Assert.That(chain).Count().IsEqualTo(SchemaV1);
        await Assert.That(chain[0].ContractId).IsEqualTo(Contract);
        await Assert.That(chain[0].FromVersion).IsEqualTo(SchemaV1);
        await Assert.That(chain[0].ToVersion).IsEqualTo(SchemaV2);
    }

    /// <summary>Verifies null upcaster registration is rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RegisterUpcasterRejectsNullUpcaster()
    {
        var registry = new SchemaRegistry();

        var registerUpcaster = typeof(SchemaRegistry).GetMethod(nameof(SchemaRegistry.RegisterUpcaster));
        ArgumentNullException.ThrowIfNull(registerUpcaster);
        var exception = Assert.ThrowsExactly<ArgumentNullException>(() => registerUpcaster.Invoke(registry, BindingFlags.DoNotWrapExceptions, null, [null], null));

        await Assert.That(exception.ParamName).IsEqualTo("upcaster");
    }

    /// <summary>Verifies empty upcaster contract identifiers are rejected with a stable reason.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RegisterUpcasterRejectsEmptyContractId()
    {
        var registry = new SchemaRegistry();

        var exception = CapturePayloadSchemaException(() => registry.RegisterUpcaster(new Upcaster(SchemaV1, SchemaV2, string.Empty)));

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.UnknownContract);
    }

    /// <summary>Verifies nonpositive upcaster versions are rejected with a stable reason.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RegisterUpcasterRejectsNonpositiveVersions()
    {
        var registry = new SchemaRegistry();

        var exception = CapturePayloadSchemaException(() => registry.RegisterUpcaster(new Upcaster(0, SchemaV1)));

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.InvalidSchemaVersion);
    }

    /// <summary>Verifies maximum source versions cannot overflow into a contiguous step.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RegisterUpcasterRejectsMaximumSourceVersion()
    {
        var registry = new SchemaRegistry();

        var exception = CapturePayloadSchemaException(() => registry.RegisterUpcaster(new Upcaster(int.MaxValue, int.MaxValue)));

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.InvalidSchemaVersion);
    }

    /// <summary>Verifies noncontiguous upcaster registrations are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RegisterUpcasterRejectsNoncontiguousVersionStep()
    {
        var registry = new SchemaRegistry();

        var exception = CapturePayloadSchemaException(() => registry.RegisterUpcaster(new Upcaster(SchemaV1, SchemaV3)));

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.MissingUpcaster);
    }

    /// <summary>Verifies reference-preserving JSON metadata is rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RegisterRejectsReferencePreservingJsonMetadata()
    {
        var registry = new SchemaRegistry();

        var exception = CapturePayloadSchemaException(() => registry.Register(Contract, SchemaV1, ReferencePayloadJsonContext.Default.SampleV1));

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.TypeNotAllowed);
    }

    /// <summary>Verifies polymorphic JSON metadata is rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RegisterRejectsPolymorphicJsonMetadata()
    {
        var registry = new SchemaRegistry();

        var exception = CapturePayloadSchemaException(() => registry.Register(Contract, SchemaV1, RegistryJsonContext.Default.PolymorphicSample));

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.TypeNotAllowed);
    }

    /// <summary>Verifies missing upcasters fail with a stable reason.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetUpcastChainRejectsMissingContiguousStep()
    {
        var registry = new SchemaRegistry()
            .Register(Contract, SchemaV1, RegistryJsonContext.Default.SampleV1)
            .Register(Contract, SchemaV3, RegistryJsonContext.Default.SampleV3);

        var exception = CapturePayloadSchemaException(() => registry.GetUpcastChain(Contract, SchemaV1, SchemaV3));

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.MissingUpcaster);
    }

    /// <summary>Verifies ambiguous upcasters fail with a stable reason.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetUpcastChainRejectsAmbiguousStep()
    {
        var registry = new SchemaRegistry()
            .Register(Contract, SchemaV1, RegistryJsonContext.Default.SampleV1)
            .Register(Contract, SchemaV2, RegistryJsonContext.Default.SampleV2)
            .RegisterUpcaster(new Upcaster(SchemaV1, SchemaV2))
            .RegisterUpcaster(new Upcaster(SchemaV1, SchemaV2));

        var exception = CapturePayloadSchemaException(() => registry.GetUpcastChain(Contract, SchemaV1, SchemaV2));

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.AmbiguousUpcaster);
    }

    /// <summary>Verifies downcasts are not supported.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetUpcastChainRejectsDowncast()
    {
        var registry = new SchemaRegistry();

        var exception = CapturePayloadSchemaException(() => registry.GetUpcastChain(Contract, SchemaV2, SchemaV1));

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.DowncastNotSupported);
    }

    /// <summary>Verifies upcast chain resolution accepts equal versions without conversion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetUpcastChainReturnsEmptyListForEqualVersions()
    {
        var registry = new SchemaRegistry();

        var chain = registry.GetUpcastChain(Contract, SchemaV1, SchemaV1);

        await Assert.That(chain).Count().IsEqualTo(0);
    }

    /// <summary>Verifies upcast chain resolution rejects empty contracts.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetUpcastChainRejectsEmptyContractId()
    {
        var registry = new SchemaRegistry();

        var exception = CapturePayloadSchemaException(() => registry.GetUpcastChain(string.Empty, SchemaV1, SchemaV2));

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.UnknownContract);
    }

    /// <summary>Verifies upcast chain resolution rejects invalid versions.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetUpcastChainRejectsInvalidVersions()
    {
        var registry = new SchemaRegistry();

        var exception = CapturePayloadSchemaException(() => registry.GetUpcastChain(Contract, 0, SchemaV2));

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.InvalidSchemaVersion);
    }

    /// <summary>Verifies schema metadata lookup rejects unknown contracts.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetJsonTypeInfoRejectsUnknownContract()
    {
        var registry = new SchemaRegistry();

        var exception = CapturePayloadSchemaException(() => registry.GetJsonTypeInfo(Contract, SchemaV1, typeof(SampleV1)));

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.UnknownContract);
    }

    /// <summary>Verifies schema metadata lookup rejects invalid versions for known contracts.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetJsonTypeInfoRejectsInvalidSchemaVersion()
    {
        var registry = new SchemaRegistry().Register(Contract, SchemaV1, RegistryJsonContext.Default.SampleV1);

        var exception = CapturePayloadSchemaException(() => registry.GetJsonTypeInfo(Contract, 0, typeof(SampleV1)));

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.InvalidSchemaVersion);
    }

    /// <summary>Verifies schema metadata lookup rejects unregistered type mappings.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetJsonTypeInfoRejectsUnregisteredType()
    {
        var registry = new SchemaRegistry().Register(Contract, SchemaV1, RegistryJsonContext.Default.SampleV1);

        var exception = CapturePayloadSchemaException(() => registry.GetJsonTypeInfo(Contract, SchemaV1, typeof(SampleV2)));

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.TypeNotAllowed);
    }

    /// <summary>Verifies target schema lookup rejects unknown contracts.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task GetTargetSchemaVersionRejectsUnknownContract()
    {
        var registry = new SchemaRegistry();

        var exception = CapturePayloadSchemaException(() => registry.GetTargetSchemaVersion(Contract, typeof(SampleV1)));

        await Assert.That(exception.Reason).IsEqualTo(PayloadSchemaFailureReason.UnknownContract);
    }

    /// <summary>Captures a schema exception from a synchronous action.</summary>
    /// <param name="action">The action expected to fail.</param>
    /// <returns>The captured schema exception.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="action"/> did not throw a schema exception.</exception>
    private static PayloadSchemaException CapturePayloadSchemaException(Action action)
    {
        try
        {
            action();
        }
        catch (PayloadSchemaException exception)
        {
            return exception;
        }

        throw new InvalidOperationException("The action did not throw a payload schema exception.");
    }

    /// <summary>Defines source-generated JSON metadata for schema test types.</summary>
    [JsonSerializable(typeof(SampleV1))]
    [JsonSerializable(typeof(SampleV2))]
    [JsonSerializable(typeof(SampleV3))]
    [JsonSerializable(typeof(PolymorphicSample))]
    private sealed partial class RegistryJsonContext : JsonSerializerContext;

    /// <summary>Defines reference-preserving source-generated JSON metadata for schema test types.</summary>
    [JsonSourceGenerationOptions(ReferenceHandler = JsonKnownReferenceHandler.Preserve)]
    [JsonSerializable(typeof(SampleV1))]
    private sealed partial class ReferencePayloadJsonContext : JsonSerializerContext;

    /// <summary>Represents mutable upcaster metadata.</summary>
    private sealed class MutableUpcaster : IPayloadUpcaster
    {
        /// <inheritdoc/>
        public string ContractId { get; set; } = Contract;

        /// <inheritdoc/>
        public int FromVersion { get; set; }

        /// <inheritdoc/>
        public int ToVersion { get; set; }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PayloadEnvelope> UpcastAsync(PayloadEnvelope source, CancellationToken cancellationToken) =>
            ValueTask.FromResult(source with
            {
                SchemaVersion = ToVersion,
            });
    }

    /// <summary>Records an upcast step.</summary>
    /// <param name="FromVersion">The source version.</param>
    /// <param name="ToVersion">The target version.</param>
    /// <param name="RegisteredContractId">The registered contract identifier.</param>
    private sealed record Upcaster(int FromVersion, int ToVersion, string RegisteredContractId) : IPayloadUpcaster
    {
        /// <summary>Initializes a new instance of the <see cref="Upcaster"/> class.</summary>
        /// <param name="fromVersion">The source version.</param>
        /// <param name="toVersion">The target version.</param>
        public Upcaster(int fromVersion, int toVersion)
            : this(fromVersion, toVersion, Contract)
        {
        }

        /// <inheritdoc/>
        public string ContractId => RegisteredContractId;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PayloadEnvelope> UpcastAsync(PayloadEnvelope source, CancellationToken cancellationToken) =>
            ValueTask.FromResult(source with
            {
                SchemaVersion = ToVersion,
            });
    }

    /// <summary>Represents a polymorphic payload base type.</summary>
    [JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")]
    [JsonDerivedType(typeof(PolymorphicSampleChild), "child")]
    private abstract record PolymorphicSample
    {
        /// <summary>Gets the discriminator value used by the test type.</summary>
        public abstract string Discriminator { get; }
    }

    /// <summary>Represents a polymorphic payload child type.</summary>
    /// <param name="Value">The sample value.</param>
    private sealed record PolymorphicSampleChild(string Value) : PolymorphicSample
    {
        /// <inheritdoc/>
        public override string Discriminator => "child";
    }

    /// <summary>Represents schema version one.</summary>
    /// <param name="Value">The sample value.</param>
    private sealed record SampleV1(string Value);

    /// <summary>Represents schema version two.</summary>
    /// <param name="Value">The sample value.</param>
    private sealed record SampleV2(string Value);

    /// <summary>Represents schema version three.</summary>
    /// <param name="Value">The sample value.</param>
    private sealed record SampleV3(string Value);
}
