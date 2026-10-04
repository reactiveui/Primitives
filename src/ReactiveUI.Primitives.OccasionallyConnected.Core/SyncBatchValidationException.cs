// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Represents a synchronization batch validation failure.</summary>
[DebuggerDisplay("{Error,nq}")]
public sealed class SyncBatchValidationException : ArgumentException
{
    /// <summary>Initializes a new instance of the <see cref="SyncBatchValidationException"/> class.</summary>
    public SyncBatchValidationException()
        : this(SyncBatchValidationError.MalformedBatch, "The synchronization batch result is invalid.")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SyncBatchValidationException"/> class.</summary>
    /// <param name="message">The validation message.</param>
    public SyncBatchValidationException(string message)
        : this(SyncBatchValidationError.MalformedBatch, message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SyncBatchValidationException"/> class.</summary>
    /// <param name="message">The validation message.</param>
    /// <param name="innerException">The exception that caused this validation failure.</param>
    public SyncBatchValidationException(string message, Exception innerException)
        : base(message, innerException)
    {
        Error = SyncBatchValidationError.MalformedBatch;
    }

    /// <summary>Initializes a new instance of the <see cref="SyncBatchValidationException"/> class.</summary>
    /// <param name="error">The validation error.</param>
    /// <param name="message">The validation message.</param>
    public SyncBatchValidationException(SyncBatchValidationError error, string message)
        : base(message)
    {
        Error = error;
    }

    /// <summary>Gets the validation error.</summary>
    public SyncBatchValidationError Error { get; }
}
