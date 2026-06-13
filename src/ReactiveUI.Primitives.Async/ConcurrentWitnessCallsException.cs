// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if NETFRAMEWORK
using System.Runtime.Serialization;
#endif

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// The exception that is thrown when multiple concurrent calls are made to observer methods that do not support
/// concurrent execution.
/// </summary>
/// <remarks>This exception indicates that a call to OnNextAsync, OnErrorResumeAsync, or OnCompletedAsync was
/// attempted while a previous call to one of these methods is still pending. ObserverAsync{T} does not allow concurrent
/// invocations of these methods; callers should ensure that each call completes before initiating another.</remarks>
[Serializable]
[System.Diagnostics.CodeAnalysis.SuppressMessage(
    "Major Code Smell",
    "S3925:ISerializable should be implemented correctly",
    Justification = "The serialization constructor is present for .NET Framework targets where binary serialization is supported.")]
public class ConcurrentWitnessCallsException : Exception
{
    /// <summary>The default error message describing the concurrent observer call violation.</summary>
    private const string DefaultMessage =
        $"Concurrent calls of {nameof(WitnessAsync<>)}.OnNextAsync, {nameof(WitnessAsync<>)}.OnErrorResumeAsync," +
        $" {nameof(WitnessAsync<>)}.OnCompletedAsync are not allowed. There is already a call pending";

    /// <summary>Initializes a new instance of the <see cref="ConcurrentWitnessCallsException"/> class.</summary>
    public ConcurrentWitnessCallsException()
        : base(DefaultMessage)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ConcurrentWitnessCallsException"/> class with a specified error message.</summary>
    /// <param name="message">The message that describes the error.</param>
    public ConcurrentWitnessCallsException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ConcurrentWitnessCallsException"/> class with a specified error message
    /// and a reference to the inner exception that is the cause of this exception.
    /// </summary>
    /// <param name="message">The error message that explains the reason for the exception.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
    public ConcurrentWitnessCallsException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

#if NETFRAMEWORK
    /// <summary>Initializes a new instance of the <see cref="ConcurrentWitnessCallsException"/> class with serialized data.</summary>
    /// <param name="info">The <see cref="SerializationInfo"/> that holds the serialized object data about the exception being thrown.</param>
    /// <param name="context">The <see cref="StreamingContext"/> that contains contextual information about the source or destination.</param>
    protected ConcurrentWitnessCallsException(SerializationInfo info, StreamingContext context)
        : base(info, context)
    {
    }
#endif
}
