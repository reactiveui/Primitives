// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Stale-credential and redaction tests for <see cref="SyncEngine"/> uploads.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>The renewed credential version reported by the stale-credential transport.</summary>
    private const string RenewedCredentialsVersion = "credentials-v2";

    /// <summary>A sentinel secret carried by transport failure text that must never reach a fault.</summary>
    private const string UploadSentinelSecret = "Bearer sentinel-token-5b1e tenant-sentinel payload-sentinel";

    /// <summary>Verifies a renewed-token authentication failure gets one immediate retry and then stops permanently.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptRetriesOnceAfterTokenRenewalThenFaultsAsAuthentication()
    {
        var operation = CreateOperation();
        var store = CreateUploadStore([operation]);
        store.RequeueReleasedLeases = true;
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes)
            { SendException = CreateTransportFailure(RetryFailureKind.Authentication, credentialsVersion: RenewedCredentialsVersion) };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(store, new() { SessionOverride = session });
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await engine.StartAsync(CancellationToken.None);

        engine.NotifyLocalCommitReady(Stream, operation);
        await WaitForConditionAsync(() => faults.Values.Count == ExpectedSingleOperation);

        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(store.RetryStates[operation.OperationId].AuthenticationState).IsEqualTo(RetryAuthenticationState.RenewalRetryUsed);
        await Assert.That(store.RetryStates[operation.OperationId].CredentialsVersion).IsEqualTo(RenewedCredentialsVersion);
        await Assert.That(faults.Values[0].Code).IsEqualTo(UploadAttemptFaultCode);
        await Assert.That(faults.Values[0].Category).IsEqualTo(FaultCategory.Authentication);
        await Assert.That(faults.Values[0].IsTransient).IsFalse();

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies an authentication failure without renewed credentials is permanent on the first attempt.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptWithStaleCredentialsFaultsWithoutRetry()
    {
        var operation = CreateOperation();
        var store = CreateUploadStore([operation]);
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes)
            { SendException = CreateTransportFailure(RetryFailureKind.Authentication) };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(store, new() { SessionOverride = session });
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await engine.StartAsync(CancellationToken.None);

        engine.NotifyLocalCommitReady(Stream, operation);
        await WaitForConditionAsync(() => faults.Values.Count == ExpectedSingleOperation);

        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.RetryStates.Count).IsEqualTo(0);
        await Assert.That(faults.Values[0].Category).IsEqualTo(FaultCategory.Authentication);
        await Assert.That(faults.Values[0].IsTransient).IsFalse();

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies an authorization denial is reported as a permanent authorization fault.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptAuthorizationDenialFaultsAsAuthorization()
    {
        var operation = CreateOperation();
        var store = CreateUploadStore([operation]);
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes)
            { SendException = CreateTransportFailure(RetryFailureKind.AuthorizationDenied) };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(store, new() { SessionOverride = session });
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await engine.StartAsync(CancellationToken.None);

        engine.NotifyLocalCommitReady(Stream, operation);
        await WaitForConditionAsync(() => faults.Values.Count == ExpectedSingleOperation);

        await Assert.That(faults.Values[0].Category).IsEqualTo(FaultCategory.Authorization);
        await Assert.That(faults.Values[0].IsTransient).IsFalse();

        await engine.StopAsync(CancellationToken.None);
    }

    /// <summary>Verifies upload faults carry a stable code and never the failure text, tokens, or payload.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task UploadAttemptFaultRedactsTransportFailureText()
    {
        var operation = CreateOperation();
        var store = CreateUploadStore([operation]);
        var session = new PreparedSession(ExpectedSingleOperation, PreparedUploadBytes)
            { SendException = new UnauthorizedAccessException(UploadSentinelSecret) };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(store, new() { SessionOverride = session });
        using var registration = engine.RegisterParticipant(CreateUploadParticipant(store));
        using var faultSubscription = engine.Faults.Subscribe(faults);
        await engine.StartAsync(CancellationToken.None);

        engine.NotifyLocalCommitReady(Stream, operation);
        await WaitForConditionAsync(() => faults.Values.Count == ExpectedSingleOperation);

        var fault = faults.Values[0];
        var diagnostics = $"{fault.Message}|{fault.Exception}|{fault.Exception?.Message}";
        await Assert.That(fault.Code).IsEqualTo(UploadAttemptFaultCode);
        await Assert.That(diagnostics).DoesNotContain("sentinel");
        await Assert.That(diagnostics).DoesNotContain("Bearer");

        await engine.StopAsync(CancellationToken.None);
    }
}
