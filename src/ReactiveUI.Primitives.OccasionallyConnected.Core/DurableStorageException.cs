// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Represents a durable store write that the storage medium refused, such as a full disk.</summary>
/// <remarks>
/// A store raises this exception after it rolls back the refused transaction, so no partial state is visible. The
/// failure is not transient: the same write fails again until space is freed or the I/O fault is fixed. The engine
/// reports it as a <see cref="FaultCategory.Storage"/> fault that is not transient.
/// </remarks>
[DebuggerDisplay("{Message,nq}; Failure={Failure}")]
public sealed class DurableStorageException : IOException
{
    /// <summary>Initializes a new instance of the <see cref="DurableStorageException"/> class.</summary>
    public DurableStorageException()
        : base("The durable store could not write to its storage medium.")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="DurableStorageException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    public DurableStorageException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="DurableStorageException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    /// <param name="innerException">The storage exception that reported the failure.</param>
    public DurableStorageException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="DurableStorageException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    /// <param name="failure">The kind of storage failure.</param>
    /// <param name="innerException">The storage exception that reported the failure.</param>
    public DurableStorageException(string message, DurableStorageFailure failure, Exception innerException)
        : base(message, innerException) => Failure = failure;

    /// <summary>Gets the kind of storage failure.</summary>
    public DurableStorageFailure Failure { get; }

    /// <summary>Determines whether an exception or one of its inner exceptions reports a durable storage failure.</summary>
    /// <param name="exception">The exception to inspect.</param>
    /// <returns><see langword="true"/> when the exception chain contains a <see cref="DurableStorageException"/>.</returns>
    public static bool IsInChain(Exception? exception)
    {
        const int MaximumDepth = 32;
        for (var depth = 0; exception is not null && depth < MaximumDepth; depth++)
        {
            if (exception is DurableStorageException)
            {
                return true;
            }

            if (exception is AggregateException aggregate)
            {
                foreach (var inner in aggregate.InnerExceptions)
                {
                    if (IsInChain(inner))
                    {
                        return true;
                    }
                }

                return false;
            }

            exception = exception.InnerException;
        }

        return false;
    }
}
