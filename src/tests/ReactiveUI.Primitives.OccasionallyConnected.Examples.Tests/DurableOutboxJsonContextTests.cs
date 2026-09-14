// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using OccasionallyConnected.DurableOutbox;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Examples.Tests;

/// <summary>Tests for the sample source-generated JSON metadata context.</summary>
public sealed class DurableOutboxJsonContextTests
{
    /// <summary>The serialized reading value used by context tests.</summary>
    private const double ReadingValue = 42.5;

    /// <summary>The serialized total reading value used by context tests.</summary>
    private const double TotalReading = 84.5;

    /// <summary>The serialized device id used by context tests.</summary>
    private const string DeviceId = "device-json";

    /// <summary>The serialized unit used by context tests.</summary>
    private const string Unit = "C";

    /// <summary>The serialized snapshot reading count used by context tests.</summary>
    private const int SnapshotReadingCount = 2;

    /// <summary>The serialized single-reading snapshot count used by context tests.</summary>
    private const int SingleReadingCount = 1;

    /// <summary>The generated property count for the reading contract.</summary>
    private const int ReadingPropertyCount = 4;

    /// <summary>The generated property count for the snapshot contract.</summary>
    private const int SnapshotPropertyCount = 5;

    /// <summary>The payload contract for reading operations.</summary>
    private const string ReadingContract = "example.temperature-reading";

    /// <summary>The payload contract for snapshots.</summary>
    private const string SnapshotContract = "example.temperature-snapshot";

    /// <summary>An unsupported payload contract used by fail-closed serializer tests.</summary>
    private const string UnknownContract = "example.unknown";

    /// <summary>The payload schema version used by the sample contracts.</summary>
    private const int PayloadSchemaVersion = 1;

    /// <summary>The JSON content type emitted by the app serializer.</summary>
    private const string JsonContentType = "application/json";

    /// <summary>Indented JSON options used by the custom context test.</summary>
    private static readonly JsonSerializerOptions IndentedOptions = new() { WriteIndented = true };

    /// <summary>Resolver options used by unknown-type metadata lookup.</summary>
    private static readonly JsonSerializerOptions ResolverOptions = new();

    /// <summary>Converter options used to verify runtime custom converter metadata.</summary>
    private static readonly JsonSerializerOptions ConverterOptions = new();

    /// <summary>Invalid converter options used to verify generated metadata failures.</summary>
    private static readonly JsonSerializerOptions InvalidConverterOptions = new();

    /// <summary>Wrong converter options used to verify generated metadata validation.</summary>
    private static readonly JsonSerializerOptions WrongConverterOptions = new();

    /// <summary>Malformed JSON that hashes correctly but cannot be deserialized as a sample contract.</summary>
    private static readonly byte[] MalformedJsonPayload = "{"u8.ToArray();

    /// <summary>Initializes static members of the <see cref="DurableOutboxJsonContextTests"/> class.</summary>
    static DurableOutboxJsonContextTests()
    {
        ConverterOptions.Converters.Add(new ConstantDoubleConverter());
        ConverterOptions.Converters.Add(new ConstantInt32Converter());
        ConverterOptions.Converters.Add(new ConstantStringConverter());
        ConverterOptions.Converters.Add(new ConstantDateTimeOffsetConverter());
        InvalidConverterOptions.Converters.Add(new InvalidDoubleConverterFactory());
        WrongConverterOptions.Converters.Add(new WrongDoubleConverterFactory());
    }

    /// <summary>Verifies the generated context round-trips the durable reading contract.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenReadingUsesGeneratedContext_ThenItRoundTrips()
    {
        DateTimeOffset observedAt = new(2026, 1, 2, 3, 4, 5, TimeSpan.Zero);
        TemperatureReading reading = new(DeviceId, ReadingValue, "C", observedAt);

        var json = JsonSerializer.Serialize(reading, DurableOutboxJsonContext.Default.TemperatureReading);
        var roundTrip = JsonSerializer.Deserialize(json, DurableOutboxJsonContext.Default.TemperatureReading);

        if (roundTrip is null)
        {
            await Assert.That(roundTrip).IsNotNull();
            return;
        }

        await Assert.That(roundTrip.DeviceId).IsEqualTo(DeviceId);
        await Assert.That(roundTrip.Value).IsEqualTo(ReadingValue);
        await Assert.That(roundTrip.ObservedAtUtc).IsEqualTo(observedAt);
    }

    /// <summary>Verifies the generated context round-trips the durable snapshot contract.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSnapshotUsesGeneratedContext_ThenItRoundTrips()
    {
        DateTimeOffset observedAt = new(2026, 1, 2, 3, 4, 5, TimeSpan.Zero);
        TemperatureSnapshot snapshot = new(SnapshotReadingCount, DeviceId, ReadingValue, observedAt, TotalReading);

        var json = JsonSerializer.Serialize(snapshot, DurableOutboxJsonContext.Default.TemperatureSnapshot);
        var roundTrip = JsonSerializer.Deserialize(json, DurableOutboxJsonContext.Default.TemperatureSnapshot);

        if (roundTrip is null)
        {
            await Assert.That(roundTrip).IsNotNull();
            return;
        }

        await Assert.That(roundTrip.ReadingCount).IsEqualTo(SnapshotReadingCount);
        await Assert.That(roundTrip.LastDeviceId).IsEqualTo(DeviceId);
        await Assert.That(roundTrip.TotalReading).IsEqualTo(TotalReading);
    }

    /// <summary>Verifies custom serializer options are honored by generated metadata.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenContextUsesCustomOptions_ThenOptionsAffectSerialization()
    {
        DurableOutboxJsonContext context = new(IndentedOptions);
        DateTimeOffset observedAt = new(2026, 1, 2, 3, 4, 5, TimeSpan.Zero);
        TemperatureSnapshot snapshot = new(SingleReadingCount, DeviceId, ReadingValue, observedAt, ReadingValue);

        var json = JsonSerializer.Serialize(snapshot, context.TemperatureSnapshot);

        await Assert.That(json).Contains(Environment.NewLine);
    }

    /// <summary>Verifies the generated resolver only serves the sample allowlisted contracts.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenUnknownTypeIsRequested_ThenGeneratedResolverReturnsNoMetadata()
    {
        IJsonTypeInfoResolver resolver = DurableOutboxJsonContext.Default;

        var info = resolver.GetTypeInfo(typeof(Guid), ResolverOptions);

        await Assert.That(info).IsNull();
    }

    /// <summary>Verifies the generated context can be constructed with its own default options.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenContextUsesDefaultConstructor_ThenGeneratedOptionsResolveMetadata()
    {
        DurableOutboxJsonContext context = new();

        await Assert.That(context.GetTypeInfo(typeof(TemperatureSnapshot))).IsNotNull();
    }

    /// <summary>Verifies generated metadata exposes the record contracts used by the app serializer.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRecordMetadataIsInspected_ThenContractsExposeAppProperties()
    {
        var observedAt = DateTimeOffset.UnixEpoch;
        TemperatureReading reading = new(DeviceId, ReadingValue, Unit, observedAt);
        TemperatureSnapshot snapshot = new(SnapshotReadingCount, DeviceId, ReadingValue, observedAt, TotalReading);

        var readingJson = JsonSerializer.Serialize(reading, DurableOutboxJsonContext.Default.TemperatureReading);
        var snapshotJson = JsonSerializer.Serialize(snapshot, DurableOutboxJsonContext.Default.TemperatureSnapshot);

        await Assert.That(readingJson).Contains("\"DeviceId\":\"device-json\"");
        await Assert.That(readingJson).Contains("\"Value\":42.5");
        await Assert.That(readingJson).Contains("\"Unit\":\"C\"");
        await Assert.That(readingJson).Contains("\"ObservedAtUtc\":\"1970-01-01T00:00:00+00:00\"");
        await Assert.That(snapshotJson).Contains("\"ReadingCount\":2");
        await Assert.That(snapshotJson).Contains("\"LastDeviceId\":\"device-json\"");
        await Assert.That(snapshotJson).Contains("\"LastReading\":42.5");
        await Assert.That(snapshotJson).Contains("\"TotalReading\":84.5");
        await Assert.That(DurableOutboxJsonContext.Default.TemperatureReading.Properties.Count).IsEqualTo(ReadingPropertyCount);
        await Assert.That(DurableOutboxJsonContext.Default.TemperatureSnapshot.Properties.Count).IsEqualTo(SnapshotPropertyCount);
        await Assert.That(DurableOutboxJsonContext.Default.GetTypeInfo(typeof(TemperatureReading))).IsNotNull();
    }

    /// <summary>Verifies generated metadata honors runtime converters registered through serializer options.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRuntimeConvertersAreRegistered_ThenGeneratedMetadataUsesThem()
    {
        DurableOutboxJsonContext context = new(ConverterOptions);

        var doubleJson = JsonSerializer.Serialize(ReadingValue, context.Double);
        var intJson = JsonSerializer.Serialize(SnapshotReadingCount, context.Int32);
        var stringJson = JsonSerializer.Serialize(DeviceId, context.String);
        var dateJson = JsonSerializer.Serialize(DateTimeOffset.UnixEpoch, context.DateTimeOffset);

        await Assert.That(doubleJson).IsEqualTo("\"double-converter\"");
        await Assert.That(intJson).IsEqualTo("\"int-converter\"");
        await Assert.That(stringJson).IsEqualTo("\"string-converter\"");
        await Assert.That(dateJson).IsEqualTo("\"date-converter\"");
    }

    /// <summary>Verifies invalid runtime converter factories fail closed through generated metadata.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRuntimeConverterFactoryReturnsInvalidConverter_ThenGeneratedMetadataFailsClosed()
    {
        DurableOutboxJsonContext context = new(InvalidConverterOptions);

        await Assert.That(() => context.Double)
            .ThrowsExactly<InvalidOperationException>()
            .WithMessageContaining("cannot return null or a JsonConverterFactory instance");
    }

    /// <summary>Verifies incompatible runtime converter factories fail closed through generated metadata.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenRuntimeConverterFactoryReturnsWrongConverter_ThenGeneratedMetadataFailsClosed()
    {
        DurableOutboxJsonContext context = new(WrongConverterOptions);

        await Assert.That(() => JsonSerializer.Serialize(ReadingValue, context.Double))
            .ThrowsExactly<InvalidCastException>()
            .WithMessageContaining("Unable to cast object of type");
    }

    /// <summary>Verifies the app serializer rejects contracts outside the generated allowlist.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSerializerReceivesUnknownContract_ThenItFailsClosed()
    {
        var serializer = CreateAppSerializer();
        TemperatureReading reading = new(DeviceId, ReadingValue, Unit, DateTimeOffset.UnixEpoch);

        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(
            () => serializer.SerializeAsync(UnknownContract, PayloadSchemaVersion, reading).AsTask());

        await Assert.That(exception?.Reason).IsEqualTo(PayloadSchemaFailureReason.UnknownContract);
    }

    /// <summary>Verifies the app serializer rejects malformed JSON even when the durable payload hash is valid.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task WhenSerializerReceivesMalformedReadingPayload_ThenItFailsClosed()
    {
        var serializer = CreateAppSerializer();
        var payload = MalformedJsonPayload;
        PayloadEnvelope envelope = new(
            ReadingContract,
            PayloadSchemaVersion,
            JsonContentType,
            payload,
            JsonPayloadSerializer.ComputePayloadHash(payload));

        var exception = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(
            () => serializer.DeserializeAsync(envelope, typeof(TemperatureReading)).AsTask());

        await Assert.That(exception?.Reason).IsEqualTo(PayloadSchemaFailureReason.DeserializationFailed);
    }

    /// <summary>Creates the app serializer with the sample generated contracts.</summary>
    /// <returns>The serializer.</returns>
    private static JsonPayloadSerializer CreateAppSerializer()
    {
        var schemaRegistry = new SchemaRegistry()
            .Register(ReadingContract, PayloadSchemaVersion, DurableOutboxJsonContext.Default.TemperatureReading)
            .Register(SnapshotContract, PayloadSchemaVersion, DurableOutboxJsonContext.Default.TemperatureSnapshot);
        return new(schemaRegistry);
    }

    /// <summary>Writes a marker for double converter metadata.</summary>
    private sealed class ConstantDoubleConverter : JsonConverter<double>
    {
        /// <inheritdoc/>
        public override double Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
            ReadingValue;

        /// <inheritdoc/>
        public override void Write(Utf8JsonWriter writer, double value, JsonSerializerOptions options) =>
            writer.WriteStringValue("double-converter");
    }

    /// <summary>Writes a marker for integer converter metadata.</summary>
    private sealed class ConstantInt32Converter : JsonConverter<int>
    {
        /// <inheritdoc/>
        public override int Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
            SnapshotReadingCount;

        /// <inheritdoc/>
        public override void Write(Utf8JsonWriter writer, int value, JsonSerializerOptions options) =>
            writer.WriteStringValue("int-converter");
    }

    /// <summary>Writes a marker for string converter metadata.</summary>
    private sealed class ConstantStringConverter : JsonConverter<string>
    {
        /// <inheritdoc/>
        public override string Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
            DeviceId;

        /// <inheritdoc/>
        public override void Write(Utf8JsonWriter writer, string value, JsonSerializerOptions options) =>
            writer.WriteStringValue("string-converter");
    }

    /// <summary>Writes a marker for date-time offset converter metadata.</summary>
    private sealed class ConstantDateTimeOffsetConverter : JsonConverter<DateTimeOffset>
    {
        /// <inheritdoc/>
        public override DateTimeOffset Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
            DateTimeOffset.UnixEpoch;

        /// <inheritdoc/>
        public override void Write(Utf8JsonWriter writer, DateTimeOffset value, JsonSerializerOptions options) =>
            writer.WriteStringValue("date-converter");
    }

    /// <summary>Returns an invalid converter to verify generated metadata fails closed.</summary>
    private sealed class InvalidDoubleConverterFactory : JsonConverterFactory
    {
        /// <inheritdoc/>
        public override bool CanConvert(Type typeToConvert) => typeToConvert == typeof(double);

        /// <inheritdoc/>
        public override JsonConverter? CreateConverter(Type typeToConvert, JsonSerializerOptions options) => null;
    }

    /// <summary>Returns the wrong converter type to verify generated metadata validates runtime factories.</summary>
    private sealed class WrongDoubleConverterFactory : JsonConverterFactory
    {
        /// <inheritdoc/>
        public override bool CanConvert(Type typeToConvert) => typeToConvert == typeof(double);

        /// <inheritdoc/>
        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options) =>
            new ConstantStringConverter();
    }
}
