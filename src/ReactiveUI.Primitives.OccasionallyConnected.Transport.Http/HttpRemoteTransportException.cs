// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Represents a typed HTTP transport failure.</summary>
[System.Diagnostics.DebuggerDisplay("{Kind,nq} {StatusCode,nq}")]
public sealed class HttpRemoteTransportException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="HttpRemoteTransportException"/> class.</summary>
    public HttpRemoteTransportException()
        : this(HttpTransportFailureKind.ProtocolViolation)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="HttpRemoteTransportException"/> class.</summary>
    /// <param name="message">The diagnostic message.</param>
    public HttpRemoteTransportException(string message)
        : base(message)
    {
        Kind = HttpTransportFailureKind.ProtocolViolation;
    }

    /// <summary>Initializes a new instance of the <see cref="HttpRemoteTransportException"/> class.</summary>
    /// <param name="message">The diagnostic message.</param>
    /// <param name="innerException">The local exception that caused the failure.</param>
    public HttpRemoteTransportException(string message, Exception innerException)
        : base(message, innerException)
    {
        Kind = HttpTransportFailureKind.ProtocolViolation;
    }

    /// <summary>Initializes a new instance of the <see cref="HttpRemoteTransportException"/> class.</summary>
    /// <param name="kind">The failure kind.</param>
    public HttpRemoteTransportException(HttpTransportFailureKind kind)
        : this(kind, statusCode: null, retryAfter: null, innerException: null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="HttpRemoteTransportException"/> class.</summary>
    /// <param name="kind">The failure kind.</param>
    /// <param name="statusCode">The optional HTTP status code.</param>
    public HttpRemoteTransportException(HttpTransportFailureKind kind, HttpStatusCode? statusCode)
        : this(kind, statusCode, retryAfter: null, innerException: null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="HttpRemoteTransportException"/> class.</summary>
    /// <param name="kind">The failure kind.</param>
    /// <param name="statusCode">The optional HTTP status code.</param>
    /// <param name="retryAfter">The optional bounded retry hint.</param>
    public HttpRemoteTransportException(HttpTransportFailureKind kind, HttpStatusCode? statusCode, TimeSpan? retryAfter)
        : this(kind, statusCode, retryAfter, innerException: null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="HttpRemoteTransportException"/> class.</summary>
    /// <param name="kind">The failure kind.</param>
    /// <param name="statusCode">The optional HTTP status code.</param>
    /// <param name="retryAfter">The optional bounded retry hint.</param>
    /// <param name="innerException">The local exception that caused the failure.</param>
    public HttpRemoteTransportException(
        HttpTransportFailureKind kind,
        HttpStatusCode? statusCode,
        TimeSpan? retryAfter,
        Exception? innerException)
        : base(CreateMessage(kind, statusCode), innerException)
    {
        Kind = kind;
        StatusCode = statusCode;
        RetryAfter = retryAfter;
    }

    /// <summary>Gets the stable failure kind.</summary>
    public HttpTransportFailureKind Kind { get; }

    /// <summary>Gets the optional HTTP status code.</summary>
    public HttpStatusCode? StatusCode { get; }

    /// <summary>Gets the optional bounded retry hint.</summary>
    public TimeSpan? RetryAfter { get; }

    /// <summary>Gets whether this failure is normally transient.</summary>
    public bool IsTransient => Kind is HttpTransportFailureKind.Transient or HttpTransportFailureKind.AmbiguousTransportOutcome;

    /// <summary>Creates a stable diagnostic message.</summary>
    /// <param name="kind">The failure kind.</param>
    /// <param name="statusCode">The optional status code.</param>
    /// <returns>The stable diagnostic message.</returns>
    private static string CreateMessage(HttpTransportFailureKind kind, HttpStatusCode? statusCode)
    {
        var status = statusCode.HasValue ? ((int)statusCode.Value).ToString(System.Globalization.CultureInfo.InvariantCulture) : "none";
        return $"HTTP transport failure {kind} (status {status}).";
    }
}
