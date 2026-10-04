// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reflection;

namespace ReactiveUI.Primitives.OccasionallyConnected.Core.Tests;

/// <summary>Tests failure classification and retry hints.</summary>
public sealed class RetryFailureTests
{
    /// <summary>Verifies a transient failure has no implicit server hint.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task TransientFailureHasNoImplicitHints()
    {
        var failure = RetryFailure.Transient();
        await Assert.That(failure.Kind).IsEqualTo(RetryFailureKind.Transient);
        await Assert.That(failure.RetryAfter).IsNull();
        await Assert.That(failure.CredentialsVersion).IsNull();
    }

    /// <summary>Verifies a server retry hint is preserved exactly.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ServerHintIsPreserved()
    {
        var failure = RetryFailure.Transient(TimeSpan.MaxValue);
        await Assert.That(failure.Kind).IsEqualTo(RetryFailureKind.Transient);
        await Assert.That(failure.RetryAfter).IsEqualTo(TimeSpan.MaxValue);
    }

    /// <summary>Verifies a remote session expiry marker has no credential or retry hint.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RemoteSessionExpiredHasNoImplicitHints()
    {
        var failure = new RetryFailure(RetryFailureKind.RemoteSessionExpired);
        await Assert.That(failure.Kind).IsEqualTo(RetryFailureKind.RemoteSessionExpired);
        await Assert.That(failure.RetryAfter).IsNull();
        await Assert.That(failure.CredentialsVersion).IsNull();
    }

    /// <summary>Verifies renewed credentials carry their opaque version.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RenewedCredentialsCarryTheirVersion()
    {
        const string version = "opaque-renewal-version";
        var failure = RetryFailure.AuthenticationTokenRenewed(version);
        await Assert.That(failure.Kind).IsEqualTo(RetryFailureKind.Authentication);
        await Assert.That(failure.CredentialsVersion).IsEqualTo(version);
        await Assert.That(failure.RetryAfter).IsNull();
    }

    /// <summary>Verifies missing renewal versions are rejected.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">The expected factory is unavailable.</exception>
    [Test]
    public async Task MissingRenewalVersionIsRejected()
    {
        var factory = typeof(RetryFailure).GetMethod(nameof(RetryFailure.AuthenticationTokenRenewed), [typeof(string)])
            ?? throw new InvalidOperationException("The renewal failure factory is unavailable.");
        var exception = await Assert.That(() => factory.Invoke(null, [null])).ThrowsExactly<TargetInvocationException>();

        await Assert.That(exception?.InnerException).IsTypeOf<ArgumentNullException>();
        await Assert.That(static () => RetryFailure.AuthenticationTokenRenewed(string.Empty)).ThrowsExactly<ArgumentException>();
    }
}
