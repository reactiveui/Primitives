// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Represents a receive cursor that can no longer be proven from retained server history.</summary>
[System.Diagnostics.DebuggerDisplay("{ReasonCode,nq}")]
public sealed class ServerReceiveRetentionGapException : InvalidOperationException
{
    /// <summary>The stable reason code value for receive retention gaps.</summary>
    private const string ReceiveRetentionGapReasonCodeValue = "server-receive-retention-gap";

    /// <summary>Initializes a new instance of the <see cref="ServerReceiveRetentionGapException"/> class.</summary>
    public ServerReceiveRetentionGapException()
        : this("The requested receive cursor is outside retained server history.")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ServerReceiveRetentionGapException"/> class.</summary>
    /// <param name="message">The sanitized retention-gap message.</param>
    public ServerReceiveRetentionGapException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ServerReceiveRetentionGapException"/> class.</summary>
    /// <param name="message">The sanitized retention-gap message.</param>
    /// <param name="innerException">The inner exception.</param>
    public ServerReceiveRetentionGapException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Gets the stable reason code value for receive retention gaps.</summary>
    public static string ReceiveRetentionGapReasonCode => ReceiveRetentionGapReasonCodeValue;

    /// <summary>Gets the stable reason code for HTTP and transport mapping.</summary>
    public string ReasonCode => ReceiveRetentionGapReasonCode;
}
