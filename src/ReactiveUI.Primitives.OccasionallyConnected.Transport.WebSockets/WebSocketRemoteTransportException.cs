// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.WebSockets;

/// <summary>Describes a protocol or transport failure reported by the WebSocket adapter.</summary>
[DebuggerDisplay("{Code}: {Message}")]
public sealed class WebSocketRemoteTransportException : Exception
{
    /// <summary>The fallback code for exceptions without a protocol-specific code.</summary>
    private const string DefaultErrorCode = "transport-error";

    /// <summary>Initializes a new instance of the <see cref="WebSocketRemoteTransportException"/> class with the default transport error code.</summary>
    public WebSocketRemoteTransportException()
        : this(DefaultErrorCode, "The WebSocket transport reported an unspecified error.")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="WebSocketRemoteTransportException"/> class with the default transport error code.</summary>
    /// <param name="message">The failure message.</param>
    public WebSocketRemoteTransportException(string message)
        : this(DefaultErrorCode, message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="WebSocketRemoteTransportException"/> class with the default transport error code and an inner exception.</summary>
    /// <param name="message">The failure message.</param>
    /// <param name="innerException">The exception that caused this failure.</param>
    public WebSocketRemoteTransportException(string message, Exception innerException)
        : this(DefaultErrorCode, message, innerException)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="WebSocketRemoteTransportException"/> class.</summary>
    /// <param name="code">The stable protocol failure code.</param>
    /// <param name="message">The failure message.</param>
    public WebSocketRemoteTransportException(string code, string message)
        : base(message) => Code = code;

    /// <summary>Initializes a new instance of the <see cref="WebSocketRemoteTransportException"/> class with a protocol code and inner exception.</summary>
    /// <param name="code">The stable protocol failure code.</param>
    /// <param name="message">The failure message.</param>
    /// <param name="innerException">The exception that caused this failure.</param>
    public WebSocketRemoteTransportException(string code, string message, Exception innerException)
        : base(message, innerException) => Code = code;

    /// <summary>Gets the stable protocol failure code.</summary>
    public string Code { get; }
}
