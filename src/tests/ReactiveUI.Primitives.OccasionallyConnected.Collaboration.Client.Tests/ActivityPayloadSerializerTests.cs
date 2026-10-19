// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client.Tests;

/// <summary>Tests the custom activity wire contract used by the collaboration client.</summary>
public sealed class ActivityPayloadSerializerTests
{
    /// <summary>The valid status shared by wire contract tests.</summary>
    private const string ReadyStatus = "ready";

    /// <summary>A value outside the supported activity payload types.</summary>
    private const int UnsupportedPayload = 42;

    /// <summary>Verifies an explicit null patch survives serialization and parsing.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CapturePreservesExplicitTitleClearAndOmittedDetails()
    {
        var update = new ActivityUpdate { Status = "active", TitleSpecified = true, Title = null };

        var envelope = ActivityPayloadSerializer.Instance.Capture(update);
        var restored = ActivityPayloadSerializer.ReadUpdate(envelope);

        await Assert.That(restored.Status).IsEqualTo("active");
        await Assert.That(restored.TitleSpecified).IsTrue();
        await Assert.That(restored.Title).IsNull();
        await Assert.That(restored.DetailsSpecified).IsFalse();
        await Assert.That(ActivityPayloadSerializer.Instance.GetRetainedByteCount(update))
            .IsEqualTo(ActivityPayloadSerializer.MaximumRetainedInputBytes);
    }

    /// <summary>Verifies canonical server metadata survives a full view round trip.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CanonicalViewRoundTripPreservesAcceptedMetadata()
    {
        var expected = ActivityView.Empty() with
        {
            Status = ReadyStatus,
            Title = "Shared title",
            Details = "Shared details",
            AcceptedClientId = "client-b",
            AcceptedOperationId = Guid.NewGuid().ToString("N"),
            AcceptedVersion = "activity-v4",
        };

        var envelope = await ActivityPayloadSerializer.Instance.SerializeAsync(
                ActivityContracts.ContractId,
                ActivityContracts.SchemaVersion,
                expected,
                CancellationToken.None)
            .ConfigureAwait(false);
        var restored = (ActivityView)await ActivityPayloadSerializer.Instance.DeserializeAsync(
                envelope,
                typeof(ActivityView),
                CancellationToken.None)
            .ConfigureAwait(false);

        await Assert.That(restored).IsEqualTo(expected);
        await Assert.That(ActivityPayloadSerializer.TryReadView(envelope, out var parsed)).IsTrue();
        await Assert.That(parsed).IsEqualTo(expected);
    }

    /// <summary>Verifies a changed body cannot be accepted under its former payload hash.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReadUpdateRejectsTamperedPayloadHash()
    {
        var envelope = ActivityPayloadSerializer.Instance.Capture(new ActivityUpdate { Status = ReadyStatus });
        var tampered = envelope with { Payload = "{\"status\":\"changed\"}"u8.ToArray() };

        _ = await Assert.ThrowsAsync<ArgumentException>(() =>
        {
            _ = ActivityPayloadSerializer.ReadUpdate(tampered);
            return Task.CompletedTask;
        });
        await Assert.That(ActivityPayloadSerializer.TryReadView(tampered, out _)).IsFalse();
    }

    /// <summary>Verifies well-hashed but malformed activity JSON is rejected.</summary>
    /// <param name="json">The malformed activity payload.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("[]")]
    [Arguments("{\"status\":\"ready\",\"status\":\"changed\"}")]
    [Arguments("{\"status\":\"ready\",\"unknown\":\"value\"}")]
    [Arguments("{\"status\":null}")]
    public async Task ReadUpdateRejectsMalformedActivityObjects(string json)
    {
        var envelope = CreateEnvelope(json);

        _ = await Assert.ThrowsAsync<ArgumentException>(() =>
        {
            _ = ActivityPayloadSerializer.ReadUpdate(envelope);
            return Task.CompletedTask;
        });
        await Assert.That(ActivityPayloadSerializer.TryReadView(envelope, out _)).IsFalse();
    }

    /// <summary>Verifies activity contract metadata is checked before parsing.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ReadUpdateRejectsChangedContractSchemaAndContentType()
    {
        var envelope = ActivityPayloadSerializer.Instance.Capture(new ActivityUpdate { Status = ReadyStatus });
        var wrongContract = envelope with { ContractId = "other.contract" };
        var wrongSchema = envelope with { SchemaVersion = ActivityContracts.SchemaVersion + 1 };
        var wrongContent = envelope with { ContentType = "application/json" };

        _ = await Assert.ThrowsAsync<InvalidOperationException>(() => ReadAsync(wrongContract));
        _ = await Assert.ThrowsAsync<InvalidOperationException>(() => ReadAsync(wrongSchema));
        _ = await Assert.ThrowsAsync<ArgumentException>(() => ReadAsync(wrongContent));
    }

    /// <summary>Verifies the serializer rejects payload types outside the activity contract.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SerializerRejectsUnsupportedTypes()
    {
        var envelope = ActivityPayloadSerializer.Instance.Capture(new ActivityUpdate { Status = ReadyStatus });

        _ = await Assert.ThrowsAsync<InvalidOperationException>(static () =>
            ActivityPayloadSerializer.Instance.SerializeAsync(
                ActivityContracts.ContractId,
                ActivityContracts.SchemaVersion,
                UnsupportedPayload,
                CancellationToken.None).AsTask());
        _ = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            ActivityPayloadSerializer.Instance.DeserializeAsync(
                envelope,
                typeof(string),
                CancellationToken.None).AsTask());
        await Assert.That(ActivityPayloadSerializer.Instance.ContentType).IsEqualTo(ActivityContracts.ContentType);
    }

    /// <summary>Verifies an invalid canonical view is rejected before it reaches the wire.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SerializeRejectsInvalidCanonicalMetadata()
    {
        var valid = ActivityView.Empty() with
        {
            Status = ReadyStatus,
            AcceptedClientId = "client-a",
            AcceptedOperationId = Guid.NewGuid().ToString("N"),
            AcceptedVersion = "v1",
        };
        var badOperation = valid with { AcceptedOperationId = "not-a-guid" };
        var badTimestamp = valid with { ServerAcceptedUtc = "not-a-timestamp" };

        _ = await Assert.ThrowsAsync<ArgumentException>(() => SerializeViewAsync(badOperation));
        _ = await Assert.ThrowsAsync<ArgumentException>(() => SerializeViewAsync(badTimestamp));
    }

    /// <summary>Verifies the byte and text limits reject oversized activity data.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task PayloadLimitsRejectOversizedData()
    {
        var longText = new string('x', ActivityPayloadSerializer.MaximumTextLength + 1);
        var longStatus = new ActivityUpdate { Status = longText };
        var longTitle = new ActivityUpdate { Status = ReadyStatus, TitleSpecified = true, Title = longText };
        var validEnvelope = ActivityPayloadSerializer.Instance.Capture(new ActivityUpdate { Status = ReadyStatus });
        var oversizedEnvelope = validEnvelope with { Payload = new byte[ActivityPayloadSerializer.MaximumPayloadBytes + 1] };

        _ = await Assert.ThrowsAsync<ArgumentException>(() => CaptureAsync(longStatus));
        _ = await Assert.ThrowsAsync<ArgumentException>(() => CaptureAsync(longTitle));
        _ = await Assert.ThrowsAsync<ArgumentException>(() => ReadAsync(oversizedEnvelope));
    }

    /// <summary>Verifies JSON escaping cannot make a valid text length exceed the byte contract.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CaptureRejectsEscapedTextAboveWireByteLimit()
    {
        var escapedText = new string('\0', ActivityPayloadSerializer.MaximumTextLength);
        ActivityUpdate update = new() { Status = escapedText, Title = escapedText, TitleSpecified = true, Details = escapedText, DetailsSpecified = true };

        _ = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => CaptureAsync(update));
    }

    /// <summary>Verifies the nonthrowing view reader catches unsupported contract metadata.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TryReadViewRejectsWrongContractWithoutThrowing()
    {
        var envelope = ActivityPayloadSerializer.Instance.Capture(new ActivityUpdate { Status = ReadyStatus });
        var wrongContract = envelope with { ContractId = "other.contract" };

        await Assert.That(ActivityPayloadSerializer.TryReadView(wrongContract, out _)).IsFalse();
    }

    /// <summary>Verifies incorrect JSON field types and empty statuses are rejected.</summary>
    /// <param name="json">The invalid JSON body.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments("{}")]
    [Arguments("{\"status\":\" \"}")]
    [Arguments("{\"status\":42}")]
    [Arguments("{\"status\":\"ready\",\"title\":42}")]
    [Arguments("{\"status\":\"ready\",\"details\":false}")]
    public async Task ReadUpdateRejectsInvalidFieldValues(string json) =>
        _ = await Assert.ThrowsAsync<ArgumentException>(() => ReadAsync(CreateEnvelope(json)));

    /// <summary>Serializes a canonical view through the interface path.</summary>
    /// <param name="view">The view to serialize.</param>
    /// <returns>The serialization task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<PayloadEnvelope> SerializeViewAsync(ActivityView view) =>
        ActivityPayloadSerializer.Instance.SerializeAsync(
            ActivityContracts.ContractId,
            ActivityContracts.SchemaVersion,
            view,
            CancellationToken.None).AsTask();

    /// <summary>Captures one invalid update while preserving the exception as an asynchronous test delegate.</summary>
    /// <param name="update">The update to capture.</param>
    /// <returns>The capture task.</returns>
    private static Task CaptureAsync(ActivityUpdate update)
    {
        _ = ActivityPayloadSerializer.Instance.Capture(update);
        return Task.CompletedTask;
    }

    /// <summary>Reads an envelope through the public serializer surface.</summary>
    /// <param name="envelope">The envelope to parse.</param>
    /// <returns>The read task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task<object> ReadAsync(PayloadEnvelope envelope) =>
        ActivityPayloadSerializer.Instance.DeserializeAsync(envelope, typeof(ActivityUpdate), CancellationToken.None).AsTask();

    /// <summary>Creates a correctly hashed envelope around test JSON.</summary>
    /// <param name="json">The activity JSON.</param>
    /// <returns>The envelope.</returns>
    private static PayloadEnvelope CreateEnvelope(string json)
    {
        var bytes = Encoding.UTF8.GetBytes(json);
        var hash = SHA256.HashData(bytes);
        return new(
            ActivityContracts.ContractId,
            ActivityContracts.SchemaVersion,
            ActivityContracts.ContentType,
            bytes,
            $"sha256-{Convert.ToBase64String(hash)}");
    }
}
