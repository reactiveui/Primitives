// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpRemoteTransportCapabilities"/>.</summary>
public sealed class HttpRemoteTransportCapabilitiesTests
{
    /// <summary>The supported adapter capabilities.</summary>
    private const RemoteTransportCapabilities AdapterCapabilities =
        RemoteTransportCapabilities.BatchPush
        | RemoteTransportCapabilities.CursorResume
        | RemoteTransportCapabilities.ReceiveAcknowledgements
        | RemoteTransportCapabilities.ServerIdempotency
        | RemoteTransportCapabilities.AtomicApplyAndAcknowledge;

    /// <summary>The valid batch operation count.</summary>
    private const int MaximumBatchOperations = 10;

    /// <summary>The valid batch byte count.</summary>
    private const long MaximumBatchBytes = 1024;

    /// <summary>Verifies valid guarantees are accepted when their feature requirements are present.</summary>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ValidateNegotiationAcceptsSupportedGuarantees() =>
        HttpRemoteTransportCapabilities.ValidateNegotiation(
            CreateRequest([DeliveryGuarantee.AtMostOnce, DeliveryGuarantee.AtLeastOnce, DeliveryGuarantee.ExactlyOnce]),
            CreateCapabilities(AdapterCapabilities),
            AdapterCapabilities);

    /// <summary>Verifies incompatible negotiated protocol versions are rejected.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ValidateNegotiationRejectsProtocolVersionOutsideRequestedRange()
    {
        var exception = await CaptureHttpExceptionAsync(static () => HttpRemoteTransportCapabilities.ValidateNegotiation(
            CreateRequest([DeliveryGuarantee.AtMostOnce]),
            CreateCapabilities(RemoteTransportCapabilities.None) with { ProtocolVersion = new(2, 0) },
            AdapterCapabilities));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.SchemaIncompatible);
    }

    /// <summary>Verifies unsupported feature flags are rejected.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ValidateNegotiationRejectsUnsupportedFeatureFlags()
    {
        var exception = await CaptureHttpExceptionAsync(static () => HttpRemoteTransportCapabilities.ValidateNegotiation(
            CreateRequest([DeliveryGuarantee.AtMostOnce]),
            CreateCapabilities(AdapterCapabilities | RemoteTransportCapabilities.StreamingReceive),
            AdapterCapabilities));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies invalid maximum counts are rejected.</summary>
    /// <param name="maximumBatchOperations">The maximum batch operations.</param>
    /// <param name="maximumBatchBytes">The maximum batch bytes.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(0, MaximumBatchBytes)]
    [Arguments(MaximumBatchOperations, 0L)]
    public async Task ValidateNegotiationRejectsInvalidMaximums(int maximumBatchOperations, long maximumBatchBytes)
    {
        var exception = await CaptureHttpExceptionAsync(() => HttpRemoteTransportCapabilities.ValidateNegotiation(
            CreateRequest([DeliveryGuarantee.AtMostOnce]),
            CreateCapabilities(AdapterCapabilities) with { MaximumBatchOperations = maximumBatchOperations, MaximumBatchBytes = maximumBatchBytes },
            AdapterCapabilities));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies guarantee-specific feature requirements are enforced.</summary>
    /// <param name="guarantee">The requested guarantee.</param>
    /// <param name="features">The negotiated feature flags.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(DeliveryGuarantee.AtLeastOnce, RemoteTransportCapabilities.BatchPush)]
    [Arguments(DeliveryGuarantee.ExactlyOnce, RemoteTransportCapabilities.BatchPush | RemoteTransportCapabilities.ServerIdempotency)]
    [Arguments(
        DeliveryGuarantee.ExactlyOnce,
        RemoteTransportCapabilities.BatchPush | RemoteTransportCapabilities.ServerIdempotency | RemoteTransportCapabilities.ReceiveAcknowledgements)]
    public async Task ValidateNegotiationRejectsMissingGuaranteeFeatures(DeliveryGuarantee guarantee, RemoteTransportCapabilities features)
    {
        var exception = await CaptureHttpExceptionAsync(() => HttpRemoteTransportCapabilities.ValidateNegotiation(
            CreateRequest([guarantee]),
            CreateCapabilities(features),
            AdapterCapabilities));

        await Assert.That(exception.Kind).IsEqualTo(HttpTransportFailureKind.SchemaIncompatible);
    }

    /// <summary>Creates a connect request.</summary>
    /// <param name="guarantees">The required guarantees.</param>
    /// <returns>The request.</returns>
    private static TransportConnectRequest CreateRequest(IReadOnlyCollection<DeliveryGuarantee> guarantees) =>
        new(new VersionRange(new Version(1, 0), new Version(1, 0)), new("client", "tenant"), guarantees);

    /// <summary>Creates negotiated capabilities.</summary>
    /// <param name="features">The feature flags.</param>
    /// <returns>The capabilities.</returns>
    private static NegotiatedCapabilities CreateCapabilities(RemoteTransportCapabilities features) =>
        new(new Version(1, 0), features, MaximumBatchOperations, MaximumBatchBytes, null, null);

    /// <summary>Captures a typed HTTP transport exception.</summary>
    /// <param name="action">The action.</param>
    /// <returns>The captured exception.</returns>
    /// <exception cref="InvalidOperationException">The action did not throw the expected exception.</exception>
    private static async Task<HttpRemoteTransportException> CaptureHttpExceptionAsync(Action action)
    {
        try
        {
            action();
        }
        catch (HttpRemoteTransportException exception)
        {
            return exception;
        }

        throw new InvalidOperationException("Expected an HTTP transport exception.");
    }
}
