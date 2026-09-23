// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Net.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests connect cleanup when the trusted clock fails during session registration.</summary>
public sealed partial class HttpServerEndpointTests
{
    /// <summary>Verifies failed registration cannot publish credentials or leave the replay owner running.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task HandleAsyncConnectRegistrationClockFailureReleasesOwnerAndAllowsFreshConnect()
    {
        var failure = new InvalidOperationException("Trusted registration clock failed.");
        var clock = new RegistrationFailureReplayClock(failure);
        var options = CreateReplayOptions(new RecordingHub()) with
        {
            ReplayProtection = new() { TimeProvider = clock },
        };
        await using var endpoint = new HttpServerEndpoint(options);
        var body = CreateCodec().SerializeConnectRequest(CreateConnectRequest(ClientId));
        using var first = CreateProtocolRequest(HttpMethod.Post, ConnectUri, body);
        using var duplicate = CreateProtocolRequest(HttpMethod.Post, ConnectUri, body);
        AddConnectReplayHeaders(first, body, ReplaySentAtUtc, "clock-message", "clock-nonce");
        AddConnectReplayHeaders(duplicate, body, ReplaySentAtUtc, "clock-message", "clock-nonce");
        InvalidOperationException? observed = null;
        try
        {
            using var response = await endpoint.HandleAsync(first, CreateAuthenticatedClient(), CancellationToken.None);
        }
        catch (InvalidOperationException exception)
        {
            observed = exception;
        }

        await Assert.That(observed).IsSameReferenceAs(failure);
        using var replay = await endpoint.HandleAsync(duplicate, CreateAuthenticatedClient(), CancellationToken.None);
        await Assert.That(replay.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);
        await Assert.That(GetResponseHeader(replay, ReplaySessionIdHeader)).IsNull();
        await Assert.That(GetResponseHeader(replay, ReplaySessionSecretHeader)).IsNull();
        var recovered = await ConnectReplaySessionAsync(endpoint);
        await Assert.That(recovered.SessionId).IsNotNull();
    }

    /// <summary>Fails the registration read after successful admission and credential preparation.</summary>
    /// <param name="failure">The injected registration failure.</param>
    private sealed class RegistrationFailureReplayClock(Exception failure) : TimeProvider
    {
        /// <summary>The admission, issuance, and registration read position.</summary>
        private const int RegistrationRead = 3;

        /// <summary>The number of clock observations.</summary>
        private int _reads;

        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow()
        {
            if (Interlocked.Increment(ref _reads) == RegistrationRead)
            {
                throw failure;
            }

            return ReplaySentAtUtc;
        }
    }
}
