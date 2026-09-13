// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Net.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Lifecycle, capacity, and deadline behavior tests for <see cref="HttpServerEndpoint"/>.</summary>
public sealed partial class HttpServerEndpointTests
{
    /// <summary>The shared timer arm failure message used by lifecycle tests.</summary>
    private const string TimerArmFailureMessage = "Timer arm failed.";

    /// <summary>Verifies disposal closes subsequent endpoint admission with a transient HTTP response.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncAfterDisposeReturnsServiceUnavailable()
    {
        await using var endpoint = new HttpServerEndpoint(CreateOptions(new RecordingHub()));
        await endpoint.DisposeAsync();
        using var request = new HttpRequestMessage(HttpMethod.Get, $"{SubscribeUri}{SubscribeQuery}");

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
    }

    /// <summary>Verifies disposed connect admission returns a transient response before any protocol effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncConnectAfterDisposeReturnsServiceUnavailable()
    {
        await using var endpoint = new HttpServerEndpoint(CreateOptions(new RecordingHub()));
        await endpoint.DisposeAsync();
        using var request = CreateProtocolRequest(HttpMethod.Post, ConnectUri, CreateCodec().SerializeConnectRequest(CreateConnectRequest(ClientId)));

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
    }

    /// <summary>Verifies disposed push admission returns a transient response before hub effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushAfterDisposeReturnsServiceUnavailableBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        await endpoint.DisposeAsync();
        using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, CreateCodec().SerializePushRequest(CreateBatch()));

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
        await Assert.That(hub.ApplyClient).IsNull();
    }

    /// <summary>Verifies disposed ACK admission returns a transient response before hub acknowledgement effects.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncAcknowledgeAfterDisposeReturnsServiceUnavailableBeforeHub()
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        await endpoint.DisposeAsync();
        using var request = CreateProtocolRequest(HttpMethod.Post, AcknowledgeUri, CreateCodec().SerializeAcknowledgement(CreateAcknowledgement()));

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
        await Assert.That(hub.AcknowledgeClient).IsNull();
    }

    /// <summary>Verifies shutdown after push reaches the hub is reported as an ambiguous retryable outcome.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncPushShutdownAfterHubEntryReturnsAmbiguousResponse()
    {
        var applyEntered = CreateSignal();
        var hub = new RecordingHub { ApplyHandler = (batch, client, cancellationToken) => BlockApplyUntilCancelledAsync(applyEntered, batch, client, cancellationToken) };
        var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        HttpResponseMessage? response = null;
        try
        {
            using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, CreateCodec().SerializePushRequest(CreateBatch()));
            var responseTask = endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None).AsTask();

            await applyEntered.Task.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
            var disposeTask = endpoint.DisposeAsync().AsTask();
            response = await AwaitResultAsync(responseTask).ConfigureAwait(false);

            await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.InternalServerError);
            await Assert.That(hub.ApplyClient).IsEqualTo(CreateAuthenticatedClient());
            await AssertCompletesAsync(disposeTask).ConfigureAwait(false);
        }
        finally
        {
            response?.Dispose();
            await AssertCompletesAsync(endpoint.DisposeAsync().AsTask()).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies shutdown during body reads maps to a transient response before protocol effects.</summary>
    /// <param name="target">The route target to exercise.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(ConnectBodyReadTarget)]
    [Arguments(PushBodyReadTarget)]
    [Arguments(AcknowledgeBodyReadTarget)]
    public async Task HandleAsyncShutdownDuringBodyReadReturnsServiceUnavailableBeforeEffects(int target)
    {
        var readStarted = CreateSignal();
        var hub = new RecordingHub();
        var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        HttpResponseMessage? response = null;
        try
        {
            using var content = CreateBlockingProtocolContent(readStarted);
            using var request = CreateBodyReadRequest(target, content);
            var responseTask = endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None).AsTask();

            await readStarted.Task.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
            var disposeTask = endpoint.DisposeAsync().AsTask();
            response = await AwaitResultAsync(responseTask).ConfigureAwait(false);

            await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
            await Assert.That(hub.ApplyClient).IsNull();
            await Assert.That(hub.AcknowledgeClient).IsNull();
            await AssertCompletesAsync(disposeTask).ConfigureAwait(false);
        }
        finally
        {
            response?.Dispose();
            await AssertCompletesAsync(endpoint.DisposeAsync().AsTask()).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies request-body disposal by the host maps to a transient response before effects.</summary>
    /// <param name="target">The route target to exercise.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(ConnectBodyReadTarget)]
    [Arguments(PushBodyReadTarget)]
    [Arguments(AcknowledgeBodyReadTarget)]
    public async Task HandleAsyncDisposedBodyReadReturnsServiceUnavailableBeforeEffects(int target)
    {
        var hub = new RecordingHub();
        await using var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        using var content = CreateDisposedReadProtocolContent();
        using var request = CreateBodyReadRequest(target, content);

        using var response = await endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None);

        await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
        await Assert.That(hub.ApplyClient).IsNull();
        await Assert.That(hub.AcknowledgeClient).IsNull();
    }

    /// <summary>Verifies disposal drains admitted work before sharing shutdown callback failures.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DisposeAsyncDrainsActiveRequestBeforeSurfacingShutdownCallbackFailure()
    {
        var applyEntered = CreateSignal();
        var cancellationObserved = CreateSignal();
        var releaseApply = CreateSignal();
        Task<HttpResponseMessage>? responseTask = null;
        HttpResponseMessage? response = null;
        var hub = new RecordingHub
        {
            ApplyHandler = async (batch, client, cancellationToken) =>
            {
                await using var registration =
                    cancellationToken.Register(static () => throw new InvalidOperationException("Shutdown cancellation callback failed."));
                _ = applyEntered.TrySetResult(null);
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
                    return CreateServerResult(batch, client);
                }
                catch (OperationCanceledException)
                {
                    _ = cancellationObserved.TrySetResult(null);
                    await releaseApply.Task.ConfigureAwait(false);
                    throw;
                }
            },
        };
        var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        try
        {
            using var request = CreateProtocolRequest(HttpMethod.Post, PushUri, CreateCodec().SerializePushRequest(CreateBatch()));
            responseTask = endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None).AsTask();

            await applyEntered.Task.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
            var disposeTask = endpoint.DisposeAsync().AsTask();
            await cancellationObserved.Task.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);

            await Assert.That(disposeTask.IsCompleted).IsFalse();
            _ = releaseApply.TrySetResult(null);
            response = await AwaitResultAsync(responseTask).ConfigureAwait(false);

            await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.InternalServerError);
            await Assert.That(hub.ApplyClient).IsEqualTo(CreateAuthenticatedClient());
            await Assert.That(async () => await disposeTask.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds))).ThrowsExactly<AggregateException>();
        }
        finally
        {
            _ = releaseApply.TrySetResult(null);
            if (responseTask is not null && responseTask.IsCompleted && response is null)
            {
                response = await AwaitResultAsync(responseTask).ConfigureAwait(false);
            }

            response?.Dispose();
        }
    }

    /// <summary>Verifies endpoint disposal may be repeated without blocking cleanup completion.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DisposeAsyncIsIdempotent()
    {
        var endpoint = new HttpServerEndpoint(CreateOptions(new RecordingHub()));

        await AssertCompletesAsync(Task.WhenAll(endpoint.DisposeAsync().AsTask(), endpoint.DisposeAsync().AsTask())).ConfigureAwait(false);
        await AssertCompletesAsync(endpoint.DisposeAsync().AsTask()).ConfigureAwait(false);
    }

    /// <summary>Verifies shutdown after subscribe reaches the hub returns service unavailable after draining the poll.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeShutdownAfterHubEntryReturnsServiceUnavailable()
    {
        var subscribeEntered = CreateSignal();
        var hub = new RecordingHub { SubscribeHandler = (_, _, cancellationToken) => BlockUntilCancelledAsync(subscribeEntered, cancellationToken) };
        var endpoint = new HttpServerEndpoint(CreateOptions(hub));
        HttpResponseMessage? response = null;
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"{SubscribeUri}{SubscribeQuery}");
            var responseTask = endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None).AsTask();

            await subscribeEntered.Task.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
            var disposeTask = endpoint.DisposeAsync().AsTask();
            response = await AwaitResultAsync(responseTask).ConfigureAwait(false);

            await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
            await Assert.That(hub.SubscribeClient).IsEqualTo(CreateAuthenticatedClient());
            await AssertCompletesAsync(disposeTask).ConfigureAwait(false);
        }
        finally
        {
            response?.Dispose();
            await AssertCompletesAsync(endpoint.DisposeAsync().AsTask()).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies active polls share the common non-ACK capacity while ACKs remain independently available.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeConsumesCommonRequestCapacityAndAcknowledgeRemainsAvailable()
    {
        var pollEntered = CreateSignal();
        var releasePoll = CreateSignal();
        Task<HttpResponseMessage>? subscribeTask = null;
        HttpResponseMessage? subscribeResponse = null;
        var hub = new RecordingHub { SubscribeHandler = (_, _, cancellationToken) => BlockSubscriptionAsync(pollEntered, releasePoll.Task, cancellationToken) };
        var endpoint = new HttpServerEndpoint(CreateOptions(hub) with
        {
            MaximumConcurrentRequests = 1,
            MaximumConcurrentAcknowledgements = 1,
            MaximumConcurrentSubscriptions = 1,
        });
        try
        {
            using var subscribeRequest = new HttpRequestMessage(HttpMethod.Get, $"{SubscribeUri}{SubscribeQuery}");
            subscribeTask = endpoint.HandleAsync(subscribeRequest, CreateAuthenticatedClient(), CancellationToken.None).AsTask();

            await pollEntered.Task.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
            using var pushRequest = CreateProtocolRequest(HttpMethod.Post, PushUri, CreateCodec().SerializePushRequest(CreateBatch()));
            using var pushResponse = await AwaitResultAsync(endpoint
                .HandleAsync(pushRequest, CreateAuthenticatedClient(), CancellationToken.None)
                .AsTask()).ConfigureAwait(false);
            using var acknowledgeRequest = CreateProtocolRequest(HttpMethod.Post, AcknowledgeUri, CreateCodec().SerializeAcknowledgement(CreateAcknowledgement()));
            using var acknowledgeResponse = await AwaitResultAsync(endpoint
                .HandleAsync(acknowledgeRequest, CreateAuthenticatedClient(), CancellationToken.None)
                .AsTask()).ConfigureAwait(false);
            _ = releasePoll.TrySetResult(null);
            subscribeResponse = await AwaitResultAsync(subscribeTask).ConfigureAwait(false);

            await Assert.That(pushResponse.StatusCode).IsEqualTo(HttpStatusCode.TooManyRequests);
            await Assert.That(acknowledgeResponse.StatusCode).IsEqualTo(HttpStatusCode.NoContent);
            await Assert.That(subscribeResponse.StatusCode).IsEqualTo(HttpStatusCode.NoContent);
        }
        finally
        {
            _ = releasePoll.TrySetResult(null);
            try
            {
                if (subscribeTask is not null && !subscribeTask.IsCompleted)
                {
                    await AssertCompletesAsync(subscribeTask).ConfigureAwait(false);
                }

                if (subscribeTask is not null && subscribeResponse is null)
                {
                    subscribeResponse = await AwaitResultAsync(subscribeTask).ConfigureAwait(false);
                }
            }
            finally
            {
                subscribeResponse?.Dispose();
                await AssertCompletesAsync(endpoint.DisposeAsync().AsTask()).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Verifies a throwing cancellation registration cannot escape the endpoint-owned poll deadline callback.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeDeadlineCapturesThrowingCancellationRegistration()
    {
        var hubEntered = CreateSignal();
        var timeProvider = new ManualTimeProvider();
        Task<HttpResponseMessage>? responseTask = null;
        var hub = new RecordingHub { SubscribeHandler = (_, _, cancellationToken) => BlockWithThrowingCancellationRegistrationAsync(hubEntered, cancellationToken) };
        var endpoint = new HttpServerEndpoint(CreateOptions(hub) with { TimeProvider = timeProvider });
        HttpResponseMessage? response = null;
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"{SubscribeUri}{SubscribeQuery}");
            responseTask = endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None).AsTask();
            var timer = await timeProvider.WaitForTimerAsync().ConfigureAwait(false);

            await hubEntered.Task.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
            await timer.FireAsync().ConfigureAwait(false);
            response = await AwaitResultAsync(responseTask).ConfigureAwait(false);

            await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.InternalServerError);
            await Assert.That(timer.CallbackException).IsNull();
        }
        finally
        {
            var disposeTask = endpoint.DisposeAsync().AsTask();
            if (responseTask is not null && responseTask.IsCompleted && response is null)
            {
                response = await AwaitResultAsync(responseTask).ConfigureAwait(false);
            }

            response?.Dispose();
            await AssertCompletesAsync(disposeTask).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies completing a timed-out poll waits for an already-running deadline callback to finish.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeDeadlineDrainsInFlightTimerCallbackBeforeResponseCompletes()
    {
        var timeProvider = new ManualTimeProvider();
        var hub = new RecordingHub { SubscribeHandler = static (_, _, cancellationToken) => BlockUntilCancelledAsync(cancellationToken) };
        var endpoint = new HttpServerEndpoint(CreateOptions(hub) with { TimeProvider = timeProvider });
        ManualTimer? timer = null;
        Task<HttpResponseMessage>? responseTask = null;
        HttpResponseMessage? response = null;
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"{SubscribeUri}{SubscribeQuery}");
            responseTask = endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None).AsTask();
            timer = await timeProvider.WaitForTimerAsync().ConfigureAwait(false);

            await timer.FireAndHoldCallbackAsync().ConfigureAwait(false);
            await timer.DisposeAsyncStarted.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
            await Assert.That(responseTask.IsCompleted).IsFalse();
            _ = timer.ReleaseCallback();
            response = await AwaitResultAsync(responseTask).ConfigureAwait(false);

            await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.NoContent);
        }
        finally
        {
            _ = timer?.ReleaseCallback();
            try
            {
                if (responseTask is not null && !responseTask.IsCompleted)
                {
                    await AssertCompletesAsync(responseTask).ConfigureAwait(false);
                }

                if (responseTask is not null && response is null)
                {
                    response = await AwaitResultAsync(responseTask).ConfigureAwait(false);
                }
            }
            finally
            {
                response?.Dispose();
                await AssertCompletesAsync(endpoint.DisposeAsync().AsTask()).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Verifies a deadline timer is disposed when arming the poll deadline fails.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeDisposesDeadlineTimerWhenArmFails()
    {
        var timeProvider = new ArmFailureTimeProvider(returnsFalse: false);
        var endpoint = new HttpServerEndpoint(CreateOptions(new RecordingHub()) with { TimeProvider = timeProvider });
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"{SubscribeUri}{SubscribeQuery}");
            var responseTask = endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None).AsTask();
            var timer = await timeProvider.WaitForTimerAsync().ConfigureAwait(false);

            await Assert.That(async () => _ = await AwaitResultAsync(responseTask).ConfigureAwait(false)).ThrowsExactly<InvalidOperationException>();
            await timer.DisposeAsyncStarted.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
        }
        finally
        {
            await AssertCompletesAsync(endpoint.DisposeAsync().AsTask()).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies a successful subscribe response waits for asynchronous timer disposal to complete.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeDrainsIncompleteTimerDisposeBeforeSuccessfulResponse()
    {
        var timeProvider = new ControlledDisposeTimeProvider();
        var hub = new RecordingHub { SubscribeHandler = static (_, _, cancellationToken) => YieldBatches([CreateReceiveBatch()], cancellationToken) };
        var endpoint = new HttpServerEndpoint(CreateOptions(hub) with { TimeProvider = timeProvider });
        ControlledDisposeTimer? timer = null;
        Task<HttpResponseMessage>? responseTask = null;
        HttpResponseMessage? response = null;
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"{SubscribeUri}{SubscribeQuery}");
            responseTask = endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None).AsTask();
            timer = await timeProvider.WaitForTimerAsync().ConfigureAwait(false);

            await timer.DisposeAsyncStarted.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
            await Assert.That(responseTask.IsCompleted).IsFalse();
            _ = timer.ReleaseDispose();
            response = await AwaitResultAsync(responseTask).ConfigureAwait(false);

            await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
            await Assert.That(hub.SubscribeClient).IsEqualTo(CreateAuthenticatedClient());
        }
        finally
        {
            _ = timer?.ReleaseDispose();
            try
            {
                if (responseTask is not null && !responseTask.IsCompleted)
                {
                    await AssertCompletesAsync(responseTask).ConfigureAwait(false);
                }

                if (responseTask is not null && response is null)
                {
                    response = await AwaitResultAsync(responseTask).ConfigureAwait(false);
                }
            }
            finally
            {
                response?.Dispose();
                await AssertCompletesAsync(endpoint.DisposeAsync().AsTask()).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Verifies startup arm failure waits for asynchronous timer cleanup before preserving the original failure.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeDrainsIncompleteTimerDisposeBeforeStartupArmFailure()
    {
        var armFailure = new InvalidOperationException(TimerArmFailureMessage);
        var timeProvider = new ControlledDisposeTimeProvider(changeFailure: armFailure);
        var hub = new RecordingHub();
        var endpoint = new HttpServerEndpoint(CreateOptions(hub) with { TimeProvider = timeProvider });
        ControlledDisposeTimer? timer = null;
        Task<HttpResponseMessage>? responseTask = null;
        InvalidOperationException? exception = null;
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"{SubscribeUri}{SubscribeQuery}");
            responseTask = endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None).AsTask();
            timer = await timeProvider.WaitForTimerAsync().ConfigureAwait(false);

            await timer.DisposeAsyncStarted.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
            await Assert.That(responseTask.IsCompleted).IsFalse();
            _ = timer.ReleaseDispose();
            try
            {
                using var response = await AwaitResultAsync(responseTask).ConfigureAwait(false);
            }
            catch (InvalidOperationException thrown)
            {
                exception = thrown;
            }

            await Assert.That(exception).IsSameReferenceAs(armFailure);
            await Assert.That(hub.SubscribeClient).IsNull();
        }
        finally
        {
            _ = timer?.ReleaseDispose();
            await AssertCompletesAsync(endpoint.DisposeAsync().AsTask()).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies timer creation failure remains primary and cleans up the no-timer deadline owner.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribePropagatesCreateTimerFailureBeforeHub()
    {
        var createFailure = new InvalidOperationException("Timer creation failed.");
        var timeProvider = new CreateTimerFailureTimeProvider(createFailure);
        var hub = new RecordingHub();
        var endpoint = new HttpServerEndpoint(CreateOptions(hub) with { TimeProvider = timeProvider });
        InvalidOperationException? exception = null;
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"{SubscribeUri}{SubscribeQuery}");
            var responseTask = endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None).AsTask();

            try
            {
                using var response = await AwaitResultAsync(responseTask).ConfigureAwait(false);
            }
            catch (InvalidOperationException thrown)
            {
                exception = thrown;
            }

            await Assert.That(exception).IsSameReferenceAs(createFailure);
            await Assert.That(hub.SubscribeClient).IsNull();
        }
        finally
        {
            await AssertCompletesAsync(endpoint.DisposeAsync().AsTask()).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies a deadline timer is disposed when arming the poll deadline returns failure.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeDisposesDeadlineTimerWhenArmReturnsFalse()
    {
        var pollEntered = CreateSignal();
        var releasePoll = CreateSignal();
        var timeProvider = new ArmFailureTimeProvider(returnsFalse: true);
        var hub = new RecordingHub { SubscribeHandler = (_, _, cancellationToken) => BlockSubscriptionAsync(pollEntered, releasePoll.Task, cancellationToken) };
        var endpoint = new HttpServerEndpoint(CreateOptions(hub) with { TimeProvider = timeProvider });
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"{SubscribeUri}{SubscribeQuery}");
            var responseTask = endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None).AsTask();
            var timer = await timeProvider.WaitForTimerAsync().ConfigureAwait(false);

            await Assert.That(async () => _ = await AwaitResultAsync(responseTask).ConfigureAwait(false)).ThrowsExactly<InvalidOperationException>();
            await timer.DisposeAsyncStarted.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
            await Assert.That(pollEntered.Task.IsCompleted).IsFalse();
        }
        finally
        {
            _ = releasePoll.TrySetResult(null);
            await AssertCompletesAsync(endpoint.DisposeAsync().AsTask()).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies startup diagnostics preserve the primary arm failure without leaking cleanup failure text.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribePreservesPrimaryArmFailureDiagnosticsWhenCleanupFails()
    {
        var timeProvider = new ArmFailureTimeProvider(returnsFalse: false, disposeFailureMessage: SecretCleanupMessage);
        var endpoint = new HttpServerEndpoint(CreateOptions(new RecordingHub()) with { TimeProvider = timeProvider });
        InvalidOperationException? exception = null;
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"{SubscribeUri}{SubscribeQuery}");
            var responseTask = endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None).AsTask();
            var timer = await timeProvider.WaitForTimerAsync().ConfigureAwait(false);

            try
            {
                using var response = await AwaitResultAsync(responseTask).ConfigureAwait(false);
            }
            catch (InvalidOperationException thrown)
            {
                exception = thrown;
            }

            await timer.DisposeAsyncStarted.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
            await Assert.That(exception).IsNotNull();
            if (exception is null)
            {
                return;
            }

            await Assert.That(exception.Message).IsEqualTo(TimerArmFailureMessage);
            await Assert.That(exception.Data[StartupCleanupFailureReasonKey] as string).IsEqualTo(StartupCleanupFailureReasonCode);
            await Assert.That(exception.Data[StartupCleanupFailureTypeKey] as string).IsEqualTo(nameof(InvalidOperationException));
            await Assert.That(CreateDiagnosticMetadataText(exception).Contains(SecretCleanupMessage, StringComparison.Ordinal)).IsFalse();
        }
        finally
        {
            await AssertCompletesAsync(endpoint.DisposeAsync().AsTask()).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies timer disposal before subscribe effects is reported as transient endpoint unavailability.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribeMapsStartupObjectDisposedToServiceUnavailableBeforeHub()
    {
        var timeProvider = new ArmFailureTimeProvider(
            returnsFalse: false,
            changeFailure: new ObjectDisposedException("deadline-timer"));
        var hub = new RecordingHub();
        var endpoint = new HttpServerEndpoint(CreateOptions(hub) with { TimeProvider = timeProvider });
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"{SubscribeUri}{SubscribeQuery}");
            var responseTask = endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None).AsTask();
            var timer = await timeProvider.WaitForTimerAsync().ConfigureAwait(false);

            using var response = await AwaitResultAsync(responseTask).ConfigureAwait(false);

            await timer.DisposeAsyncStarted.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
            await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
            await Assert.That(hub.SubscribeClient).IsNull();
        }
        finally
        {
            await AssertCompletesAsync(endpoint.DisposeAsync().AsTask()).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies timer cancellation before subscribe effects remains caller-observable cancellation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribePropagatesStartupCancellationBeforeHub()
    {
        var armFailure = new OperationCanceledException("Timer arm canceled.");
        var timeProvider = new ArmFailureTimeProvider(returnsFalse: false, changeFailure: armFailure);
        var hub = new RecordingHub();
        var endpoint = new HttpServerEndpoint(CreateOptions(hub) with { TimeProvider = timeProvider });
        OperationCanceledException? exception = null;
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"{SubscribeUri}{SubscribeQuery}");
            var responseTask = endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None).AsTask();
            var timer = await timeProvider.WaitForTimerAsync().ConfigureAwait(false);

            try
            {
                using var response = await AwaitResultAsync(responseTask).ConfigureAwait(false);
            }
            catch (OperationCanceledException thrown)
            {
                exception = thrown;
            }

            await timer.DisposeAsyncStarted.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
            await Assert.That(exception).IsSameReferenceAs(armFailure);
            await Assert.That(hub.SubscribeClient).IsNull();
        }
        finally
        {
            await AssertCompletesAsync(endpoint.DisposeAsync().AsTask()).ConfigureAwait(false);
        }
    }

    /// <summary>Verifies inaccessible startup diagnostic metadata cannot replace the primary arming failure.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncSubscribePreservesPrimaryArmFailureWhenDiagnosticMetadataCannotBeAttached()
    {
        var armFailure = new InaccessibleDataException(TimerArmFailureMessage);
        var timeProvider = new ArmFailureTimeProvider(
            returnsFalse: false,
            disposeFailureMessage: SecretCleanupMessage,
            changeFailure: armFailure);
        var endpoint = new HttpServerEndpoint(CreateOptions(new RecordingHub()) with { TimeProvider = timeProvider });
        InaccessibleDataException? exception = null;
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"{SubscribeUri}{SubscribeQuery}");
            var responseTask = endpoint.HandleAsync(request, CreateAuthenticatedClient(), CancellationToken.None).AsTask();
            var timer = await timeProvider.WaitForTimerAsync().ConfigureAwait(false);

            try
            {
                using var response = await AwaitResultAsync(responseTask).ConfigureAwait(false);
            }
            catch (InaccessibleDataException thrown)
            {
                exception = thrown;
            }

            await timer.DisposeAsyncStarted.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
            await Assert.That(exception).IsSameReferenceAs(armFailure);
            await Assert.That(armFailure.ToString().Contains(SecretCleanupMessage, StringComparison.Ordinal)).IsFalse();
        }
        finally
        {
            await AssertCompletesAsync(endpoint.DisposeAsync().AsTask()).ConfigureAwait(false);
        }
    }
}
