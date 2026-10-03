// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Reports that an operation reached a durable state from which it cannot synchronize.</summary>
[DebuggerDisplay("{Message,nq}")]
public sealed class SyncOperationFailedException : InvalidOperationException
{
    /// <summary>Initializes a new instance of the <see cref="SyncOperationFailedException"/> class.</summary>
    public SyncOperationFailedException()
        : this("The operation cannot synchronize.")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SyncOperationFailedException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    public SyncOperationFailedException(string message)
        : base(message ?? throw new ArgumentNullException(nameof(message)))
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SyncOperationFailedException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    /// <param name="innerException">The exception that caused the failure.</param>
    public SyncOperationFailedException(string message, Exception innerException)
        : base(message ?? throw new ArgumentNullException(nameof(message)), innerException)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SyncOperationFailedException"/> class.</summary>
    /// <param name="status">The durable status that ended synchronization.</param>
    /// <exception cref="ArgumentNullException"><paramref name="status"/> is <see langword="null"/>.</exception>
    public SyncOperationFailedException(SyncOperationStatus status)
        : base(CreateMessage(status))
    {
        Status = status;
    }

    /// <summary>Gets the durable status that ended synchronization, when known.</summary>
    public SyncOperationStatus? Status { get; }

    /// <summary>Gets the durable state that ended synchronization, when known.</summary>
    public SyncOperationState? State => Status?.State;

    /// <summary>Gets the stable reason code that ended synchronization, when known.</summary>
    public string? ReasonCode => Status?.ReasonCode;

    /// <summary>Creates the stable failure message for a durable status.</summary>
    /// <param name="status">The durable status.</param>
    /// <returns>The failure message.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="status"/> is <see langword="null"/>.</exception>
    private static string CreateMessage(SyncOperationStatus status) =>
        status is null
            ? throw new ArgumentNullException(nameof(status))
            : $"The operation cannot synchronize from state {status.State}.";
}
