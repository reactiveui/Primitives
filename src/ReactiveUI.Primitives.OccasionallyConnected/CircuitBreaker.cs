// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Coordinates deterministic admission for one remote endpoint.</summary>
[DebuggerDisplay("{_endpoint,nq}; {_state,nq}")]
public sealed class CircuitBreaker
{
    /// <summary>Synchronizes state transitions and snapshots.</summary>
    private readonly Lock _gate = new();

    /// <summary>Stores the endpoint identifier.</summary>
    private readonly string _endpoint;

    /// <summary>Stores the failure threshold.</summary>
    private readonly int _failureThreshold;

    /// <summary>Stores the configured open duration.</summary>
    private readonly TimeSpan _openDuration;

    /// <summary>Stores the clock used for deterministic state transitions.</summary>
    private readonly TimeProvider _timeProvider;

    /// <summary>Stores the latest consecutive transient failure count.</summary>
    private int _consecutiveTransientFailures;

    /// <summary>Stores the current admission state.</summary>
    private CircuitBreakerState _state;

    /// <summary>Stores the recovery deadline, which is only used while the breaker is not closed.</summary>
    private DateTimeOffset _retryAfterUtc;

    /// <summary>Initializes a new instance of the <see cref="CircuitBreaker"/> class with default options.</summary>
    /// <param name="endpoint">The non-empty endpoint identifier.</param>
    public CircuitBreaker(string endpoint)
        : this(endpoint, new(), TimeProvider.System)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="CircuitBreaker"/> class.</summary>
    /// <param name="endpoint">The non-empty endpoint identifier.</param>
    /// <param name="options">The breaker configuration.</param>
    /// <param name="timeProvider">The time source.</param>
    /// <exception cref="ArgumentException">An argument is <see langword="null"/>, empty, or whitespace.</exception>
    public CircuitBreaker(string endpoint, CircuitBreakerOptions options, TimeProvider timeProvider)
    {
        ArgumentExceptionHelper.ThrowIfNull(endpoint);
        ArgumentExceptionHelper.ThrowIfNull(options);
        ArgumentExceptionHelper.ThrowIfNull(timeProvider);
        if (endpoint.AsSpan().Trim().IsEmpty)
        {
            throw new ArgumentException("Endpoint must not be empty or whitespace.", nameof(endpoint));
        }

        options.Validate();
        _endpoint = endpoint;
        _failureThreshold = options.FailureThreshold;
        _openDuration = options.OpenDuration;
        _timeProvider = timeProvider;
    }

    /// <summary>Gets an immutable snapshot of the breaker state.</summary>
    public CircuitBreakerSnapshot Snapshot
    {
        get
        {
            lock (_gate)
            {
                return new(_endpoint, _state, _consecutiveTransientFailures, _state == CircuitBreakerState.Closed ? null : _retryAfterUtc);
            }
        }
    }

    /// <summary>Attempts to admit a connection or handshake operation.</summary>
    /// <returns><see langword="true"/> when this operation may proceed; otherwise, <see langword="false"/>.</returns>
    public bool TryAcquire()
    {
        lock (_gate)
        {
            if (_state == CircuitBreakerState.Closed)
            {
                return true;
            }

            if (_state == CircuitBreakerState.HalfOpen || _timeProvider.GetUtcNow() < _retryAfterUtc)
            {
                return false;
            }

            _state = CircuitBreakerState.HalfOpen;
            return true;
        }
    }

    /// <summary>Records a transient transport or handshake failure.</summary>
    public void RecordTransientFailure()
    {
        lock (_gate)
        {
            if (_state == CircuitBreakerState.Open)
            {
                return;
            }

            if (_state == CircuitBreakerState.Closed)
            {
                _consecutiveTransientFailures++;
            }

            if (_consecutiveTransientFailures < _failureThreshold)
            {
                return;
            }

            _consecutiveTransientFailures = _failureThreshold;
            _state = CircuitBreakerState.Open;
            _retryAfterUtc = CalculateRetryAfter(_timeProvider.GetUtcNow());
        }
    }

    /// <summary>Records a successful connection or handshake and resets failure state.</summary>
    public void RecordSuccess()
    {
        lock (_gate)
        {
            _consecutiveTransientFailures = 0;
            _state = CircuitBreakerState.Closed;
            _retryAfterUtc = default;
        }
    }

    /// <summary>Releases an acquired half-open probe that was cancelled or abandoned.</summary>
    public void AbandonProbe()
    {
        lock (_gate)
        {
            if (_state != CircuitBreakerState.HalfOpen)
            {
                return;
            }

            _state = CircuitBreakerState.Open;
            _retryAfterUtc = CalculateRetryAfter(_timeProvider.GetUtcNow());
        }
    }

    /// <summary>Calculates a retry deadline without overflowing the representable UTC range.</summary>
    /// <param name="now">The current UTC time.</param>
    /// <returns>The bounded retry deadline.</returns>
    private DateTimeOffset CalculateRetryAfter(DateTimeOffset now) =>
        _openDuration > DateTimeOffset.MaxValue - now ? DateTimeOffset.MaxValue : now.Add(_openDuration);
}
