// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Represents a remote subscription cursor gap that requires snapshot recovery.</summary>
[DebuggerDisplay("{StreamId,nq} {SubscriptionId,nq}")]
public sealed class RemoteSubscriptionRetentionGapException : InvalidOperationException
{
    /// <summary>Initializes a new instance of the <see cref="RemoteSubscriptionRetentionGapException"/> class.</summary>
    public RemoteSubscriptionRetentionGapException()
        : base("The remote subscription cursor is outside retained history and requires snapshot recovery.")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="RemoteSubscriptionRetentionGapException"/> class.</summary>
    /// <param name="message">The message that describes the error.</param>
    public RemoteSubscriptionRetentionGapException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="RemoteSubscriptionRetentionGapException"/> class.</summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
    public RemoteSubscriptionRetentionGapException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="RemoteSubscriptionRetentionGapException"/> class.</summary>
    /// <param name="streamId">The stream whose retained history has a gap.</param>
    /// <param name="subscriptionId">The subscription whose cursor cannot be replayed.</param>
    /// <param name="expiredCursor">The expired cursor, or null when the gap is before the first resumable cursor.</param>
    /// <param name="reasonCode">An optional bounded stable reason code.</param>
    /// <exception cref="ArgumentException"><paramref name="expiredCursor"/> is empty.</exception>
    public RemoteSubscriptionRetentionGapException(
        StreamId streamId,
        SubscriptionId subscriptionId,
        string? expiredCursor,
        string? reasonCode)
        : base("The remote subscription cursor is outside retained history and requires snapshot recovery.")
    {
        if (expiredCursor is not null && expiredCursor.Length == 0)
        {
            throw new ArgumentException("Expired cursor must be null or non-empty.", nameof(expiredCursor));
        }

        StreamId = streamId;
        SubscriptionId = subscriptionId;
        ExpiredCursor = expiredCursor;
        ReasonCode = reasonCode;
    }

    /// <summary>Gets the stream whose retained history has a gap.</summary>
    public StreamId StreamId { get; }

    /// <summary>Gets the subscription whose cursor cannot be replayed.</summary>
    public SubscriptionId SubscriptionId { get; }

    /// <summary>Gets the expired cursor, or null when the gap is before the first resumable cursor.</summary>
    public string? ExpiredCursor { get; }

    /// <summary>Gets the optional bounded stable reason code.</summary>
    public string? ReasonCode { get; }
}
