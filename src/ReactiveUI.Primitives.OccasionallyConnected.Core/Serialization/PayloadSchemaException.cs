// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Represents a stable schema failure suitable for quarantine decisions.</summary>
[DebuggerDisplay("{Reason,nq}: {Message,nq}")]
public sealed class PayloadSchemaException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="PayloadSchemaException"/> class.</summary>
    public PayloadSchemaException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PayloadSchemaException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    public PayloadSchemaException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PayloadSchemaException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    /// <param name="innerException">The exception that caused the failure.</param>
    public PayloadSchemaException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PayloadSchemaException"/> class.</summary>
    /// <param name="reason">The stable failure reason.</param>
    /// <param name="message">The failure message.</param>
    public PayloadSchemaException(PayloadSchemaFailureReason reason, string message)
        : base(message)
    {
        Reason = reason;
    }

    /// <summary>Initializes a new instance of the <see cref="PayloadSchemaException"/> class.</summary>
    /// <param name="reason">The stable failure reason.</param>
    /// <param name="message">The failure message.</param>
    /// <param name="innerException">The exception that caused the failure.</param>
    public PayloadSchemaException(PayloadSchemaFailureReason reason, string message, Exception innerException)
        : base(message, innerException)
    {
        Reason = reason;
    }

    /// <summary>Gets the stable reason for the schema failure.</summary>
    public PayloadSchemaFailureReason Reason { get; }
}
