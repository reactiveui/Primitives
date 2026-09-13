// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;

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
}
