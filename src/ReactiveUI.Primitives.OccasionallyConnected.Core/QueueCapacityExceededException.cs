// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Represents a queue-capacity failure raised before persistence or other effects are applied.</summary>
[DebuggerDisplay("{Message,nq}; CanFitWhenEmpty={CanFitWhenEmpty}")]
public sealed class QueueCapacityExceededException : InvalidOperationException
{
    /// <summary>Initializes a new instance of the <see cref="QueueCapacityExceededException"/> class.</summary>
    public QueueCapacityExceededException()
        : this(string.Empty, canFitWhenEmpty: false)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="QueueCapacityExceededException"/> class.</summary>
    /// <param name="message">The capacity failure message.</param>
    public QueueCapacityExceededException(string message)
        : this(message, canFitWhenEmpty: false)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="QueueCapacityExceededException"/> class.</summary>
    /// <param name="message">The capacity failure message.</param>
    /// <param name="canFitWhenEmpty">
    /// <see langword="true"/> when the item may fit after the queue is drained; otherwise, <see langword="false"/>
    /// when the item cannot fit in an empty queue with its configured capacity.
    /// </param>
    public QueueCapacityExceededException(string message, bool canFitWhenEmpty)
        : base(message ?? throw new ArgumentNullException(nameof(message)))
    {
        CanFitWhenEmpty = canFitWhenEmpty;
    }

    /// <summary>Initializes a new instance of the <see cref="QueueCapacityExceededException"/> class.</summary>
    /// <param name="message">The capacity failure message.</param>
    /// <param name="innerException">The exception that caused the capacity failure.</param>
    public QueueCapacityExceededException(string message, Exception innerException)
        : base(message ?? throw new ArgumentNullException(nameof(message)), innerException)
    {
    }

    /// <summary>Gets a value indicating whether the item may fit after the queue is drained.</summary>
    /// <remarks>
    /// A true value permits waiting for capacity but does not guarantee a later admission. Constructors without an
    /// explicit capacity hint default to false, so callers must not automatically wait for space after those failures.
    /// </remarks>
    public bool CanFitWhenEmpty { get; }
}
