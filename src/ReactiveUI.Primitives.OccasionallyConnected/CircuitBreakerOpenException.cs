// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Represents a connection attempt rejected by an open endpoint circuit breaker.</summary>
internal sealed class CircuitBreakerOpenException : TimeoutException, IRemoteTransportFailure
{
    /// <summary>The message used when the breaker rejects a connection attempt.</summary>
    private const string OpenMessage = "The transport circuit breaker is open.";

    /// <summary>Initializes a new instance of the <see cref="CircuitBreakerOpenException"/> class.</summary>
    internal CircuitBreakerOpenException()
        : this(OpenMessage)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="CircuitBreakerOpenException"/> class.</summary>
    /// <param name="message">The exception message.</param>
    internal CircuitBreakerOpenException(string message)
        : base(message) =>
        RetryFailure = RetryFailure.Transient();

    /// <summary>Initializes a new instance of the <see cref="CircuitBreakerOpenException"/> class.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    internal CircuitBreakerOpenException(string message, Exception innerException)
        : base(message, innerException) =>
        RetryFailure = RetryFailure.Transient();

    /// <summary>Initializes a new instance of the <see cref="CircuitBreakerOpenException"/> class.</summary>
    /// <param name="retryAfter">The remaining open duration.</param>
    internal CircuitBreakerOpenException(TimeSpan retryAfter)
        : base(OpenMessage) =>
        RetryFailure = RetryFailure.Transient(retryAfter);

    /// <inheritdoc/>
    public RetryFailure RetryFailure { get; }
}
