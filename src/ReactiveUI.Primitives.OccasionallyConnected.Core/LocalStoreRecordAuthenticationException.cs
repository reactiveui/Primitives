// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Represents a persisted local record that failed decryption or authentication.</summary>
/// <remarks>
/// A store raises this exception after it quarantines the affected stream. The record is never applied or uploaded.
/// The engine reports the failure as a <see cref="FaultCategory.Security"/> fault.
/// </remarks>
[DebuggerDisplay("{Message,nq}")]
public sealed class LocalStoreRecordAuthenticationException : InvalidOperationException
{
    /// <summary>Initializes a new instance of the <see cref="LocalStoreRecordAuthenticationException"/> class.</summary>
    public LocalStoreRecordAuthenticationException()
        : base("A persisted local store record failed authentication.")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="LocalStoreRecordAuthenticationException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    public LocalStoreRecordAuthenticationException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="LocalStoreRecordAuthenticationException"/> class.</summary>
    /// <param name="message">The failure message.</param>
    /// <param name="innerException">The exception that identified the failure.</param>
    public LocalStoreRecordAuthenticationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Determines whether an exception or one of its inner exceptions reports a record authentication failure.</summary>
    /// <param name="exception">The exception to inspect.</param>
    /// <returns><see langword="true"/> when the exception chain contains a record authentication failure.</returns>
    public static bool IsInChain(Exception? exception)
    {
        const int MaximumDepth = 32;
        for (var depth = 0; exception is not null && depth < MaximumDepth; depth++)
        {
            if (exception is LocalStoreRecordAuthenticationException)
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
