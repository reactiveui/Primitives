// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Offline startup, background reconnection, and circuit breaking for <see cref="SyncEngine"/>.</summary>
internal sealed partial class SyncEngine
{
    /// <summary>The stable reason code reported while the remote endpoint is unreachable.</summary>
    internal const string TransportUnavailableReasonCode = "OC.Transport.Unavailable";

    /// <summary>The stable reason code reported while the endpoint circuit breaker rejects connections.</summary>
    internal const string CircuitOpenReasonCode = "OC.Transport.CircuitOpen";

    /// <summary>The stable fault code reported when background reconnection fails permanently.</summary>
    internal const string ConnectFaultCode = "OC.Engine.Connect";

    /// <summary>The sanitized fault message reported when background reconnection fails permanently.</summary>
    private const string ConnectFaultMessage = "The synchronization engine could not connect to the remote endpoint.";

    /// <summary>The endpoint name used by the shared transport circuit breaker.</summary>
    private const string CircuitBreakerEndpoint = "transport";

    /// <summary>Stores the cancellation owned by the active background connect loop.</summary>
    private CancellationTokenSource? _connectCancellation;

    /// <summary>Stores the active background connect loop.</summary>
    private Task? _connectTask;

    /// <summary>Wakes the background connect loop before its retry delay elapses.</summary>
    private TaskCompletionSource<bool>? _connectWake;

    /// <summary>Stores the retry delay reported with the current lifecycle state.</summary>
    private TimeSpan? _diagnosticRetryAfter;

    /// <summary>Stores the stable reason code reported with the current lifecycle state.</summary>
    private string? _diagnosticReasonCode;

    /// <summary>Determines whether a connection failure leaves the engine usable offline.</summary>
    /// <param name="exception">The observed connection failure.</param>
    /// <returns><see langword="true"/> when the failure is transient.</returns>
    private static bool IsTransientConnectFailure(Exception exception) =>
        ClassifyRetryFailure(exception).Kind == RetryFailureKind.Transient;

    /// <summary>Gets the stable reason code for a transient connection failure.</summary>
    /// <param name="failure">The connection failure.</param>
    /// <returns>The reason code.</returns>
    private static string GetConnectReasonCode(Exception failure) =>
        failure is CircuitBreakerOpenException ? CircuitOpenReasonCode : TransportUnavailableReasonCode;

    /// <summary>Admits a connection attempt through the endpoint circuit breaker.</summary>
    /// <exception cref="CircuitBreakerOpenException">The circuit breaker rejects the attempt.</exception>
    private void AcquireCircuitBreaker()
    {
        var breaker = _circuitBreaker;
        if (breaker.TryAcquire())
        {
            return;
        }

        var nowUtc = _options.TimeProvider.GetUtcNow();
        var retryAfter = breaker.Snapshot.RetryAfterUtc is { } retryAfterUtc && retryAfterUtc > nowUtc
            ? retryAfterUtc - nowUtc
            : TimeSpan.Zero;
        throw new CircuitBreakerOpenException(retryAfter);
    }

    /// <summary>Connects the transport and records the handshake outcome in the endpoint circuit breaker.</summary>
    /// <param name="request">The connection request.</param>
    /// <param name="cancellationToken">The connection cancellation token.</param>
    /// <returns>The connected session.</returns>
    private async ValueTask<IRemoteTransportSession> ConnectThroughCircuitBreakerAsync(
        TransportConnectRequest request,
        CancellationToken cancellationToken)
    {
        var breaker = _circuitBreaker;
        try
        {
            var session = await _options.Transport.ConnectAsync(request, cancellationToken).ConfigureAwait(false);
            breaker.RecordSuccess();
            return session;
        }
        catch (Exception exception) when (!cancellationToken.IsCancellationRequested && IsTransientConnectFailure(exception))
        {
            breaker.RecordTransientFailure();
            throw;
        }
        catch
        {
            breaker.AbandonProbe();
            throw;
        }
    }

    /// <summary>Completes startup offline and starts background reconnection after a transient failure.</summary>
    /// <param name="failure">The transient connection failure.</param>
    /// <param name="cancellation">The startup cancellation owner.</param>
    /// <exception cref="OperationCanceledException">An accepted stop superseded startup.</exception>
    private void BeginOfflineConnect(Exception failure, StartupCancellationOwner cancellation)
    {
        var retryPolicy = new RetryPolicy(_options.Options.Retry, _options.TimeProvider, _options.RetryRandomSource);
        var retry = GetConnectRetry(failure, retryPolicy, RetryState.Start(_options.TimeProvider.GetUtcNow()));
        RecordRetry();
        PublishLifecycleState(SyncLifecycleStatus.Offline, networkAvailable: false, retry.Delay, GetConnectReasonCode(failure));
        lock (_gate)
        {
            if (_disposed || cancellation.StopRequested || _admissionState == EngineAdmissionState.Stopping)
            {
                ThrowIfDisposedLocked();
                throw new OperationCanceledException("Startup was superseded by an accepted stop.", cancellation.Token);
            }

            var loopCancellation = new CancellationTokenSource();
            _connectCancellation = loopCancellation;
            _connectWake = CreateCompletion();
            _connectTask = Task.Run(() => RunConnectLoopAsync(loopCancellation, retryPolicy, retry.State, retry.Delay), CancellationToken.None);
        }
    }

    /// <summary>Computes the next reconnection delay, starting a new retry episode when the policy is exhausted.</summary>
    /// <param name="failure">The transient connection failure.</param>
    /// <param name="retryPolicy">The retry policy.</param>
    /// <param name="state">The current retry state.</param>
    /// <returns>The retry delay and next state.</returns>
    private (TimeSpan Delay, RetryState State) GetConnectRetry(Exception failure, RetryPolicy retryPolicy, RetryState state)
    {
        var classified = ClassifyRetryFailure(failure);
        var decision = retryPolicy.GetDecision(classified, state);
        if (decision.Kind == RetryDecisionKind.Retry && decision.Delay is { } delay)
        {
            return (delay, decision.NextState);
        }

        var maximumDelay = _options.Options.Retry.MaximumDelay;
        var restartDelay = classified.RetryAfter is { } retryAfter && retryAfter > maximumDelay ? retryAfter : maximumDelay;
        return (restartDelay, RetryState.Start(_options.TimeProvider.GetUtcNow()));
    }

    /// <summary>Retries transport connection until the engine is online, stopped, or permanently faulted.</summary>
    /// <param name="loopCancellation">The cancellation owned by this loop.</param>
    /// <param name="retryPolicy">The retry policy.</param>
    /// <param name="retryState">The retry state after the initial failure.</param>
    /// <param name="delay">The delay before the next attempt.</param>
    /// <returns>The loop task.</returns>
    private async Task RunConnectLoopAsync(
        CancellationTokenSource loopCancellation,
        RetryPolicy retryPolicy,
        RetryState retryState,
        TimeSpan delay)
    {
        var token = loopCancellation.Token;
        try
        {
            while (true)
            {
                await DelayConnectRetryAsync(delay, token).ConfigureAwait(false);
                PublishLifecycleState(SyncLifecycleStatus.Connecting, networkAvailable: false);
                IRemoteTransportSession session;
                try
                {
                    var requiredGuarantees = await GetRequiredTransportGuaranteesAsync(token).ConfigureAwait(false);
                    session = await ConnectValidatedSessionAsync(requiredGuarantees, token).ConfigureAwait(false);
                }
                catch (Exception exception) when (!token.IsCancellationRequested && IsTransientConnectFailure(exception))
                {
                    (delay, retryState) = GetConnectRetry(exception, retryPolicy, retryState);
                    RecordRetry();
                    PublishLifecycleState(SyncLifecycleStatus.Offline, networkAvailable: false, delay, GetConnectReasonCode(exception));
                    continue;
                }
                catch (Exception exception) when (!token.IsCancellationRequested)
                {
                    EndConnectWakes(loopCancellation);
                    PublishFault(ConnectFaultCode, ConnectFaultMessage, streamId: null, exception);
                    PublishLifecycleState(SyncLifecycleStatus.Faulted, networkAvailable: false, retryAfter: null, ConnectFaultCode);
                    return;
                }

                if (!TryPublishConnectedSession(session, loopCancellation, out var uploadCancellation))
                {
                    await DisposeRejectedReceiveSessionAsync(session).ConfigureAwait(false);
                    return;
                }

                StartSessionPumps(uploadCancellation);
                return;
            }
        }
        catch (Exception) when (token.IsCancellationRequested)
        {
            // Stop cancelled this loop; failures raised while the transport is torn down are expected.
        }
    }

    /// <summary>Stops accepting synchronization wakes for a loop that has ended permanently.</summary>
    /// <param name="loopCancellation">The cancellation owned by the ending loop.</param>
    private void EndConnectWakes(CancellationTokenSource loopCancellation)
    {
        lock (_gate)
        {
            if (_connectCancellation == loopCancellation)
            {
                _connectWake = null;
            }
        }
    }

    /// <summary>Publishes a session connected by the background loop when the loop is still current.</summary>
    /// <param name="session">The connected session.</param>
    /// <param name="loopCancellation">The cancellation owned by the loop.</param>
    /// <param name="uploadCancellation">The created upload cancellation source.</param>
    /// <returns>Whether the session became active.</returns>
    private bool TryPublishConnectedSession(
        IRemoteTransportSession session,
        CancellationTokenSource loopCancellation,
        out CancellationTokenSource? uploadCancellation)
    {
        uploadCancellation = null;
        lock (_gate)
        {
            if (_disposed
                || _connectCancellation != loopCancellation
                || loopCancellation.IsCancellationRequested
                || _admissionState != EngineAdmissionState.Starting)
            {
                return false;
            }

            _connectWake = null;
            uploadCancellation = PublishSessionLocked(session);
            return true;
        }
    }

    /// <summary>Waits for the next connection attempt, returning early when synchronization is triggered.</summary>
    /// <param name="delay">The bounded retry delay.</param>
    /// <param name="cancellationToken">The loop cancellation token.</param>
    /// <returns>The delay task.</returns>
    private async ValueTask DelayConnectRetryAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        Task wake;
        lock (_gate)
        {
            wake = _connectWake?.Task ?? Task.CompletedTask;
        }

        if (wake.IsCompleted || delay <= TimeSpan.Zero)
        {
            ResetConnectWake();
            return;
        }

        var completion = CreateCompletion();
        await using (var timer = _options.TimeProvider.CreateTimer(
                         static state =>
                         {
                             ArgumentExceptionHelper.ThrowIfNull(state);
                             _ = ((TaskCompletionSource<bool>)state).TrySetResult(true);
                         },
                         completion,
                         delay,
                         Timeout.InfiniteTimeSpan))
        {
            await using var registration = UnsafeRegisterDelayCancellation(completion, cancellationToken);
            _ = await Task.WhenAny(completion.Task, wake).ConfigureAwait(false);
        }

        cancellationToken.ThrowIfCancellationRequested();
        ResetConnectWake();
    }

    /// <summary>Replaces a consumed connection wake signal.</summary>
    private void ResetConnectWake()
    {
        lock (_gate)
        {
            if (_connectWake is { Task.IsCompleted: true } && _admissionState == EngineAdmissionState.Starting)
            {
                _connectWake = CreateCompletion();
            }
        }
    }

    /// <summary>Wakes the background connect loop while the engine lock is held.</summary>
    /// <returns><see langword="true"/> when a background connect loop is active.</returns>
    private bool TryWakeConnectLoopLocked()
    {
        if (_connectCancellation is null || _connectWake is null || _admissionState != EngineAdmissionState.Starting)
        {
            return false;
        }

        _ = _connectWake.TrySetResult(true);
        return true;
    }

    /// <summary>Cancels and joins the background connect loop.</summary>
    /// <returns>The loop failure, if any.</returns>
    private async Task StopConnectLoopAsync()
    {
        CancellationTokenSource? cancellation;
        Task? task;
        lock (_gate)
        {
            cancellation = _connectCancellation;
            task = _connectTask;
            _connectCancellation = null;
            _connectTask = null;
            _connectWake = null;
        }

        if (cancellation is null)
        {
            return;
        }

        try
        {
            await cancellation.CancelAsync().ConfigureAwait(false);
            if (task is not null)
            {
                await task.ConfigureAwait(false);
            }
        }
        finally
        {
            cancellation.Dispose();
        }
    }

    /// <summary>Publishes a lifecycle state transition without retry diagnostics.</summary>
    /// <param name="status">The lifecycle status.</param>
    /// <param name="networkAvailable">Whether network transport is available.</param>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private void PublishLifecycleState(SyncLifecycleStatus status, bool networkAvailable) =>
        PublishLifecycleState(status, networkAvailable, retryAfter: null, reasonCode: null);

    /// <summary>Publishes a lifecycle state transition with retry diagnostics.</summary>
    /// <param name="status">The lifecycle status.</param>
    /// <param name="networkAvailable">Whether network transport is available.</param>
    /// <param name="retryAfter">The delay before the next connection attempt.</param>
    /// <param name="reasonCode">The stable reason code.</param>
    private void PublishLifecycleState(SyncLifecycleStatus status, bool networkAvailable, TimeSpan? retryAfter, string? reasonCode)
    {
        SyncState aggregate;
        (IOccasionallyConnectedStreamDiagnosticsSink Sink, SyncState State)[] streams;
        long revision;
        lock (_gate)
        {
            _diagnosticLifecycleStatus = status;
            _diagnosticNetworkAvailable = networkAvailable;
            _diagnosticRetryAfter = retryAfter;
            _diagnosticReasonCode = reasonCode;
            revision = ++_diagnosticRevision;
            aggregate = CreateAggregateSyncStateLocked();
            streams = CaptureStreamSyncStatesLocked();
        }

        PublishGlobalSyncState(aggregate, revision);
        for (var i = 0; i < streams.Length; i++)
        {
            PublishStreamDiagnostic(streams[i].Sink, streams[i].State, revision);
        }

        RecordConnectionStateChange();
    }
}
