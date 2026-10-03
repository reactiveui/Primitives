// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpRemoteTransportException"/>.</summary>
public sealed class HttpRemoteTransportExceptionTests
{
    /// <summary>The custom diagnostic message.</summary>
    private const string CustomMessage = "custom";

    /// <summary>Verifies legacy exception constructors preserve protocol violation defaults.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ConstructorsPreserveProtocolViolationDefaults()
    {
        var defaultException = new HttpRemoteTransportException();
        var messageException = new HttpRemoteTransportException(CustomMessage);
        var inner = new InvalidOperationException(CustomMessage);
        var nestedException = new HttpRemoteTransportException(CustomMessage, inner);

        await Assert.That(defaultException.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(messageException.Message).IsEqualTo(CustomMessage);
        await Assert.That(messageException.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
        await Assert.That(nestedException.InnerException).IsSameReferenceAs(inner);
        await Assert.That(nestedException.Kind).IsEqualTo(HttpTransportFailureKind.ProtocolViolation);
    }

    /// <summary>Verifies typed exception constructors produce stable diagnostics and retry metadata.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task TypedConstructorsPreserveStatusRetryAndInnerException()
    {
        var noStatus = new HttpRemoteTransportException(HttpTransportFailureKind.Configuration);
        var withStatus = new HttpRemoteTransportException(HttpTransportFailureKind.Authentication, HttpStatusCode.Unauthorized);
        var retryAfter = TimeSpan.FromSeconds(1);
        var withRetryAfter = new HttpRemoteTransportException(HttpTransportFailureKind.Transient, HttpStatusCode.ServiceUnavailable, retryAfter);
        var inner = new HttpRequestException(CustomMessage);
        var ambiguous = new HttpRemoteTransportException(HttpTransportFailureKind.AmbiguousTransportOutcome, null, null, inner);

        await Assert.That(noStatus.Message).Contains("status none");
        await Assert.That(withStatus.Message).Contains("status 401");
        await Assert.That(withRetryAfter.RetryAfter).IsEqualTo(retryAfter);
        await Assert.That(ambiguous.InnerException).IsSameReferenceAs(inner);
        await Assert.That(withRetryAfter.IsTransient).IsTrue();
        await Assert.That(ambiguous.IsTransient).IsTrue();
        await Assert.That(withStatus.IsTransient).IsFalse();
    }

    /// <summary>Verifies HTTP failure kinds expose their Core retry classifications.</summary>
    /// <param name="kind">The HTTP failure kind.</param>
    /// <param name="expectedKind">The expected Core retry failure kind.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(HttpTransportFailureKind.ProtocolViolation, RetryFailureKind.ValidationRejected)]
    [Arguments(HttpTransportFailureKind.Authentication, RetryFailureKind.Authentication)]
    [Arguments(HttpTransportFailureKind.AuthorizationDenied, RetryFailureKind.AuthorizationDenied)]
    [Arguments(HttpTransportFailureKind.ValidationRejected, RetryFailureKind.ValidationRejected)]
    [Arguments(HttpTransportFailureKind.SchemaIncompatible, RetryFailureKind.SchemaIncompatible)]
    [Arguments(HttpTransportFailureKind.PayloadTooLarge, RetryFailureKind.PayloadTooLarge)]
    [Arguments(HttpTransportFailureKind.Transient, RetryFailureKind.Transient)]
    [Arguments(HttpTransportFailureKind.AmbiguousTransportOutcome, RetryFailureKind.AmbiguousTransportOutcome)]
    [Arguments(HttpTransportFailureKind.Configuration, RetryFailureKind.ValidationRejected)]
    [Arguments((HttpTransportFailureKind)999, RetryFailureKind.ValidationRejected)]
    public async Task TypedConstructorsExposeCoreRetryFailure(
        HttpTransportFailureKind kind,
        RetryFailureKind expectedKind)
    {
        var retryAfter = TimeSpan.FromSeconds(1);
        var exception = Identity<Exception>(new HttpRemoteTransportException(kind, HttpStatusCode.ServiceUnavailable, retryAfter));
        var transportFailure = exception as IRemoteTransportFailure;

        await Assert.That(transportFailure).IsNotNull();
        await Assert.That(transportFailure?.RetryFailure.Kind).IsEqualTo((RetryFailureKind?)expectedKind);
        await Assert.That(transportFailure?.RetryFailure.RetryAfter).IsEqualTo(retryAfter);
        await Assert.That(transportFailure?.RetryFailure.CredentialsVersion).IsNull();
    }

    /// <summary>Verifies legacy constructors expose the protocol violation retry classification.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task LegacyConstructorsExposeValidationRejectedRetryFailure()
    {
        var exception = Identity<Exception>(new HttpRemoteTransportException(CustomMessage));
        var transportFailure = exception as IRemoteTransportFailure;

        await Assert.That(transportFailure).IsNotNull();
        await Assert.That(transportFailure?.RetryFailure.Kind).IsEqualTo((RetryFailureKind?)RetryFailureKind.ValidationRejected);
        await Assert.That(transportFailure?.RetryFailure.RetryAfter).IsNull();
        await Assert.That(transportFailure?.RetryFailure.CredentialsVersion).IsNull();
    }

    /// <summary>Returns the supplied instance with its declared static type.</summary>
    /// <typeparam name="T">The declared static type.</typeparam>
    /// <param name="value">The instance to return.</param>
    /// <returns>The supplied instance.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static T Identity<T>(T value) => value;
}
