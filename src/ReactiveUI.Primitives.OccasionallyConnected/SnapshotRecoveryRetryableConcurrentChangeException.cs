// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.IO;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Represents a retryable snapshot recovery conflict that requires a fresh bounded capture.</summary>
internal sealed class SnapshotRecoveryRetryableConcurrentChangeException : IOException
{
    /// <summary>The default retryable concurrent change message.</summary>
    private const string DefaultMessage = "Snapshot recovery observed a concurrent change and must be retried with a fresh capture.";

    /// <summary>Initializes a new instance of the <see cref="SnapshotRecoveryRetryableConcurrentChangeException"/> class.</summary>
    public SnapshotRecoveryRetryableConcurrentChangeException()
        : base(DefaultMessage)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SnapshotRecoveryRetryableConcurrentChangeException"/> class.</summary>
    /// <param name="message">The exception message.</param>
    public SnapshotRecoveryRetryableConcurrentChangeException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SnapshotRecoveryRetryableConcurrentChangeException"/> class.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    public SnapshotRecoveryRetryableConcurrentChangeException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
