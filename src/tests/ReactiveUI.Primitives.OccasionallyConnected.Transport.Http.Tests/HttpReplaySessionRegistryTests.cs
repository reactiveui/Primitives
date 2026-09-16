// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Tests <see cref="HttpReplaySessionRegistry"/>.</summary>
public sealed class HttpReplaySessionRegistryTests
{
    /// <summary>The authenticated tenant.</summary>
    private const string Tenant = "tenant";

    /// <summary>The authenticated client.</summary>
    private const string Client = "client";

    /// <summary>The alternate authenticated client.</summary>
    private const string AlternateClient = "client-b";

    /// <summary>The replay MAC value.</summary>
    private const string ReplayMac = "mac";

    /// <summary>The zeroed secret byte value.</summary>
    private const byte ZeroSecretByte = 0;

    /// <summary>The expected unpadded base64url length for 128-bit values.</summary>
    private const int Base64Url128BitLength = 22;

    /// <summary>The single session limit.</summary>
    private const int SingleSessionLimit = 1;

    /// <summary>The double session limit.</summary>
    private const int DoubleSessionLimit = 2;

    /// <summary>The triple session limit.</summary>
    private const int TripleSessionLimit = 3;

    /// <summary>The retained-byte cost of the deterministic registered session.</summary>
    private const int RegisteredSessionCharge = 29;

    /// <summary>The retained-byte cost after replacing the deterministic session with a shorter secret.</summary>
    private const int ShortRegisteredSessionCharge = 22;

    /// <summary>The deterministic session identifier.</summary>
    private const string RegisteredSessionId = "session-a";

    /// <summary>The alternate deterministic session identifier.</summary>
    private const string AlternateRegisteredSessionId = "session-b";

    /// <summary>The deterministic session secret.</summary>
    private const string RegisteredSessionSecret = "secret-a";

    /// <summary>The alternate deterministic session secret.</summary>
    private const string AlternateRegisteredSessionSecret = "secret-b";

    /// <summary>The larger deterministic session secret.</summary>
    private const string LargerRegisteredSessionSecret = "secret-b-larger";

    /// <summary>The shorter deterministic session secret.</summary>
    private const string ShortRegisteredSessionSecret = "s";

    /// <summary>The invalid session header value.</summary>
    private const string ControlSessionHeader = "session\nbad";

    /// <summary>The overflow parameter used by <see cref="HttpReplaySessionRegistry.AddChecked"/>.</summary>
    private const string ObservedUtcParameterName = "observedUtc";

    /// <summary>The baseline clock instant.</summary>
    private static readonly DateTimeOffset Start = new(2026, 9, 13, 12, 0, 0, TimeSpan.Zero);

    /// <summary>The MAC input bytes.</summary>
    private static readonly byte[] MacInput = [1, 2, 3];

    /// <summary>Verifies issued sessions use distinct bounded 128-bit id and secret values.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task IssueCreatesDistinctBoundedSessionAndSecretValues()
    {
        await using var registry = CreateRegistry();
        var principal = new HttpReplayPrincipal(Tenant, Client);

        var first = registry.Issue(principal, Start);
        var second = registry.Issue(principal, Start);

        await Assert.That(first.SessionId.Length).IsEqualTo(Base64Url128BitLength);
        await Assert.That(first.SessionSecret.Length).IsEqualTo(Base64Url128BitLength);
        await Assert.That(second.SessionId).IsNotEqualTo(first.SessionId);
        await Assert.That(second.SessionSecret).IsNotEqualTo(first.SessionSecret);
    }

    /// <summary>Verifies replay sessions are bound to the trusted authenticated principal.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task VerifyRejectsSessionOwnedByAnotherPrincipal()
    {
        await using var registry = CreateRegistry();
        var issued = registry.Issue(new(Tenant, Client), Start);

        await Assert.That(() => registry.Verify(new(Tenant, AlternateClient), issued.SessionId, MacInput, ReplayMac, Start)).ThrowsExactly<HttpRemoteTransportException>();
    }

    /// <summary>Verifies disposing an untransferred session secret owner clears retained secret bytes once.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SessionSecretOwnerDisposeClearsRetainedSecretBytesOnce()
    {
        var secretBytes = "secret-a"u8.ToArray();
        HttpReplaySessionSecretOwner owner = new(secretBytes);

        owner.Dispose();
        owner.Dispose();

        await AssertSecretBytesClearedAsync(secretBytes);
        await Assert.That(() => owner.Copy()).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies disposing a session secret candidate releases ownership and rejects transfer.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SessionSecretCandidateDisposeRejectsLaterTransfer()
    {
        var candidate = new HttpReplaySessionSecretCandidate();

        candidate.Dispose();
        candidate.Dispose();

        await Assert.That(() => candidate.Transfer()).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies transferring a session secret candidate keeps the secret owned until the transferred owner is disposed.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SessionSecretCandidateTransferKeepsSecretUntilTransferredOwnerDisposes()
    {
        var candidate = new HttpReplaySessionSecretCandidate();
        candidate.Retain(RegisteredSessionSecret);
        var owner = candidate.Transfer();

        candidate.Dispose();
        var copy = owner.Copy();

        await Assert.That(Encoding.UTF8.GetString(copy)).IsEqualTo(RegisteredSessionSecret);
        owner.Dispose();
        await Assert.That(() => candidate.Transfer()).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(() => owner.Copy()).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies a session secret owner cannot overwrite retained bytes without disposal.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SessionSecretOwnerRejectsSecondRetainUntilDisposed()
    {
        using HttpReplaySessionSecretOwner owner = new();

        owner.Retain(RegisteredSessionSecret);

        await Assert.That(() => owner.Retain(AlternateRegisteredSessionSecret)).ThrowsExactly<ObjectDisposedException>();
        await Assert.That(Encoding.UTF8.GetString(owner.Copy())).IsEqualTo(RegisteredSessionSecret);
    }

    /// <summary>Verifies a disposed session secret owner rejects later retention.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task SessionSecretOwnerRejectsRetainAfterDispose()
    {
        HttpReplaySessionSecretOwner owner = new();

        owner.Dispose();

        await Assert.That(() => owner.Retain(RegisteredSessionSecret)).ThrowsExactly<ObjectDisposedException>();
    }

    /// <summary>Verifies expired sessions clear secret state and release single-session capacity.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task VerifyExpiresSessionAndReusesSingleSessionCapacity()
    {
        var options = new HttpReplayProtectionOptions { MaximumReplaySessions = SingleSessionLimit };
        HttpReplayRetentionBudget budget = new(options.MaximumRetainedBytes);
        await using HttpReplaySessionRegistry registry = new(options, budget);
        var principal = new HttpReplayPrincipal(Tenant, Client);
        var issued = registry.Issue(principal, Start);
        HttpReplayEnvelopeHasher hasher = new(options);
        var replayMac = hasher.ComputeMac(Encoding.UTF8.GetBytes(issued.SessionSecret), MacInput);
        var expiredNow = issued.ExpiresAtUtc.AddTicks(SingleSessionLimit);

        await Assert.That(() => registry.Verify(principal, issued.SessionId, MacInput, replayMac, expiredNow)).ThrowsExactly<HttpRemoteTransportException>();

        var second = registry.Issue(principal, expiredNow);

        await Assert.That(second.SessionId).IsNotEqualTo(issued.SessionId);
        await Assert.That(registry.Count).IsEqualTo(SingleSessionLimit);
    }

    /// <summary>Verifies registering multiple sessions grows retained storage before ownership transfer.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RegisterIssuedGrowsEntryCapacityBeforeRetainingSecretAndBudget()
    {
        var options = new HttpReplayProtectionOptions { MaximumReplaySessions = DoubleSessionLimit, MaximumRetainedBytes = RegisteredSessionCharge + RegisteredSessionCharge };
        HttpReplayRetentionBudget budget = new(options.MaximumRetainedBytes);
        await using HttpReplaySessionRegistry registry = new(options, budget);
        var principal = new HttpReplayPrincipal(Tenant, Client);
        var first = new HttpReplayIssuedSession { SessionId = RegisteredSessionId, SessionSecret = RegisteredSessionSecret, ExpiresAtUtc = Start };
        var second = new HttpReplayIssuedSession { SessionId = AlternateRegisteredSessionId, SessionSecret = AlternateRegisteredSessionSecret, ExpiresAtUtc = Start };

        registry.RegisterIssued(principal, first, Start);
        registry.RegisterIssued(principal, second, Start);

        await Assert.That(registry.Count).IsEqualTo(DoubleSessionLimit);
        await Assert.That(budget.RetainedBytes).IsEqualTo(options.MaximumRetainedBytes);
    }

    /// <summary>Verifies retained session storage can grow to the configured maximum capacity.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RegisterIssuedGrowsEntryCapacityToMaximumReplaySessions()
    {
        var options = new HttpReplayProtectionOptions { MaximumReplaySessions = TripleSessionLimit, MaximumRetainedBytes = RegisteredSessionCharge * TripleSessionLimit };
        HttpReplayRetentionBudget budget = new(options.MaximumRetainedBytes);
        await using HttpReplaySessionRegistry registry = new(options, budget);
        var principal = new HttpReplayPrincipal(Tenant, Client);
        var first = new HttpReplayIssuedSession { SessionId = RegisteredSessionId, SessionSecret = RegisteredSessionSecret, ExpiresAtUtc = Start };
        var second = new HttpReplayIssuedSession { SessionId = AlternateRegisteredSessionId, SessionSecret = AlternateRegisteredSessionSecret, ExpiresAtUtc = Start };
        var third = new HttpReplayIssuedSession { SessionId = "session-c", SessionSecret = "secret-c", ExpiresAtUtc = Start };

        registry.RegisterIssued(principal, first, Start);
        registry.RegisterIssued(principal, second, Start);
        registry.RegisterIssued(principal, third, Start);

        await Assert.That(registry.Count).IsEqualTo(TripleSessionLimit);
        await Assert.That(budget.RetainedBytes).IsEqualTo(options.MaximumRetainedBytes);
    }

    /// <summary>Verifies expired sessions release retained bytes before a replacement reserves budget.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RegisterIssuedRemovesExpiredSessionsBeforeReservingBudget()
    {
        var options = new HttpReplayProtectionOptions { MaximumReplaySessions = SingleSessionLimit, MaximumRetainedBytes = RegisteredSessionCharge };
        HttpReplayRetentionBudget budget = new(options.MaximumRetainedBytes);
        await using HttpReplaySessionRegistry registry = new(options, budget);
        var principal = new HttpReplayPrincipal(Tenant, Client);
        var first = new HttpReplayIssuedSession { SessionId = RegisteredSessionId, SessionSecret = RegisteredSessionSecret, ExpiresAtUtc = Start };
        var secondObserved = Start.AddTicks(SingleSessionLimit);
        var second = new HttpReplayIssuedSession { SessionId = AlternateRegisteredSessionId, SessionSecret = AlternateRegisteredSessionSecret, ExpiresAtUtc = secondObserved };

        registry.RegisterIssued(principal, first, Start);
        registry.RegisterIssued(principal, second, secondObserved);

        await Assert.That(registry.Count).IsEqualTo(SingleSessionLimit);
    }

    /// <summary>Verifies replacing a duplicate session id releases the previous secret within the same retained-byte budget.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RegisterIssuedReplacesDuplicateSessionIdWithinSingleSessionBudget()
    {
        var options = new HttpReplayProtectionOptions { MaximumReplaySessions = SingleSessionLimit, MaximumRetainedBytes = RegisteredSessionCharge };
        HttpReplayRetentionBudget budget = new(options.MaximumRetainedBytes);
        await using HttpReplaySessionRegistry registry = new(options, budget);
        var principal = new HttpReplayPrincipal(Tenant, Client);
        var first = new HttpReplayIssuedSession { SessionId = RegisteredSessionId, SessionSecret = RegisteredSessionSecret, ExpiresAtUtc = Start };
        var replacement = new HttpReplayIssuedSession { SessionId = RegisteredSessionId, SessionSecret = AlternateRegisteredSessionSecret, ExpiresAtUtc = Start };

        registry.RegisterIssued(principal, first, Start);
        registry.RegisterIssued(principal, replacement, Start);
        var proof = registry.Verify(principal, replacement.SessionId, MacInput, CreateMac(options, replacement.SessionSecret), Start);

        await Assert.That(proof.SessionId).IsEqualTo(replacement.SessionId);
        await Assert.That(registry.Count).IsEqualTo(SingleSessionLimit);
        await Assert.That(budget.RetainedBytes).IsEqualTo(RegisteredSessionCharge);
    }

    /// <summary>Verifies replacing a duplicate session id with a smaller secret shrinks retained-byte accounting.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RegisterIssuedShrinksBudgetAfterSmallerReplacementSucceeds()
    {
        var options = new HttpReplayProtectionOptions { MaximumReplaySessions = SingleSessionLimit, MaximumRetainedBytes = RegisteredSessionCharge };
        HttpReplayRetentionBudget budget = new(options.MaximumRetainedBytes);
        await using HttpReplaySessionRegistry registry = new(options, budget);
        var principal = new HttpReplayPrincipal(Tenant, Client);
        var first = new HttpReplayIssuedSession { SessionId = RegisteredSessionId, SessionSecret = RegisteredSessionSecret, ExpiresAtUtc = Start };
        var replacement = new HttpReplayIssuedSession { SessionId = RegisteredSessionId, SessionSecret = ShortRegisteredSessionSecret, ExpiresAtUtc = Start };

        registry.RegisterIssued(principal, first, Start);
        registry.RegisterIssued(principal, replacement, Start);
        var proof = registry.Verify(principal, replacement.SessionId, MacInput, CreateMac(options, replacement.SessionSecret), Start);

        await Assert.That(proof.SessionId).IsEqualTo(replacement.SessionId);
        await Assert.That(registry.Count).IsEqualTo(SingleSessionLimit);
        await Assert.That(budget.RetainedBytes).IsEqualTo(ShortRegisteredSessionCharge);
        await Assert
            .That(() => registry.Verify(principal, first.SessionId, MacInput, CreateMac(options, first.SessionSecret), Start))
            .ThrowsExactly<HttpRemoteTransportException>();
    }

    /// <summary>Verifies a larger same-id replacement failure preserves the existing session and budget.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RegisterIssuedPreservesExistingSessionWhenLargerReplacementExceedsBudget()
    {
        var options = new HttpReplayProtectionOptions { MaximumReplaySessions = SingleSessionLimit, MaximumRetainedBytes = RegisteredSessionCharge };
        HttpReplayRetentionBudget budget = new(options.MaximumRetainedBytes);
        await using HttpReplaySessionRegistry registry = new(options, budget);
        var principal = new HttpReplayPrincipal(Tenant, Client);
        var first = new HttpReplayIssuedSession { SessionId = RegisteredSessionId, SessionSecret = RegisteredSessionSecret, ExpiresAtUtc = Start };
        var replacement = new HttpReplayIssuedSession { SessionId = RegisteredSessionId, SessionSecret = LargerRegisteredSessionSecret, ExpiresAtUtc = Start };

        registry.RegisterIssued(principal, first, Start);

        await Assert.That(() => registry.RegisterIssued(principal, replacement, Start)).ThrowsExactly<HttpRemoteTransportException>();

        var proof = registry.Verify(principal, first.SessionId, MacInput, CreateMac(options, first.SessionSecret), Start);

        await Assert.That(proof.SessionId).IsEqualTo(first.SessionId);
        await Assert.That(registry.Count).IsEqualTo(SingleSessionLimit);
        await Assert.That(budget.RetainedBytes).IsEqualTo(RegisteredSessionCharge);
        await Assert
            .That(() => registry.Verify(principal, replacement.SessionId, MacInput, CreateMac(options, replacement.SessionSecret), Start))
            .ThrowsExactly<HttpRemoteTransportException>();
    }

    /// <summary>Verifies failed larger replacement after staging preserves all retained sessions and budget.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RegisterIssuedPreservesExistingSessionsWhenReplacementGrowthCapacityIsConsumed()
    {
        var options = new HttpReplayProtectionOptions { MaximumReplaySessions = DoubleSessionLimit, MaximumRetainedBytes = ShortRegisteredSessionCharge + RegisteredSessionCharge };
        HttpReplayRetentionBudget budget = new(options.MaximumRetainedBytes);
        await using HttpReplaySessionRegistry registry = new(options, budget);
        var principal = new HttpReplayPrincipal(Tenant, Client);
        var first = new HttpReplayIssuedSession { SessionId = RegisteredSessionId, SessionSecret = ShortRegisteredSessionSecret, ExpiresAtUtc = Start };
        var second = new HttpReplayIssuedSession { SessionId = AlternateRegisteredSessionId, SessionSecret = AlternateRegisteredSessionSecret, ExpiresAtUtc = Start };
        var replacement = new HttpReplayIssuedSession { SessionId = RegisteredSessionId, SessionSecret = LargerRegisteredSessionSecret, ExpiresAtUtc = Start };

        registry.RegisterIssued(principal, first, Start);
        registry.RegisterIssued(principal, second, Start);

        await Assert.That(() => registry.RegisterIssued(principal, replacement, Start)).ThrowsExactly<HttpRemoteTransportException>();

        await Assert.That(registry.Verify(principal, first.SessionId, MacInput, CreateMac(options, first.SessionSecret), Start).SessionId).IsEqualTo(first.SessionId);
        await Assert.That(registry.Verify(principal, second.SessionId, MacInput, CreateMac(options, second.SessionSecret), Start).SessionId).IsEqualTo(second.SessionId);
        await Assert.That(registry.Count).IsEqualTo(DoubleSessionLimit);
        await Assert.That(budget.RetainedBytes).IsEqualTo(options.MaximumRetainedBytes);
    }

    /// <summary>Verifies a same-id collision from another principal cannot rebind the retained session.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RegisterIssuedRejectsSameSessionIdFromDifferentPrincipal()
    {
        var options = new HttpReplayProtectionOptions();
        HttpReplayRetentionBudget budget = new(options.MaximumRetainedBytes);
        await using HttpReplaySessionRegistry registry = new(options, budget);
        var principal = new HttpReplayPrincipal(Tenant, Client);
        var foreignPrincipal = new HttpReplayPrincipal(Tenant, AlternateClient);
        var first = new HttpReplayIssuedSession { SessionId = RegisteredSessionId, SessionSecret = RegisteredSessionSecret, ExpiresAtUtc = Start };
        var replacement = new HttpReplayIssuedSession { SessionId = RegisteredSessionId, SessionSecret = AlternateRegisteredSessionSecret, ExpiresAtUtc = Start };

        registry.RegisterIssued(principal, first, Start);

        await Assert.That(() => registry.RegisterIssued(foreignPrincipal, replacement, Start)).ThrowsExactly<HttpRemoteTransportException>();

        var proof = registry.Verify(principal, first.SessionId, MacInput, CreateMac(options, first.SessionSecret), Start);

        await Assert.That(proof.SessionId).IsEqualTo(first.SessionId);
        await Assert.That(registry.Count).IsEqualTo(SingleSessionLimit);
        await Assert
            .That(() => registry.Verify(foreignPrincipal, replacement.SessionId, MacInput, CreateMac(options, replacement.SessionSecret), Start))
            .ThrowsExactly<HttpRemoteTransportException>();
    }

    /// <summary>Verifies session ownership rejects a different tenant before MAC verification.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task VerifyRejectsSessionOwnedByAnotherTenant()
    {
        await using var registry = CreateRegistry();
        var issued = registry.Issue(new(Tenant, Client), Start);

        await Assert.That(() => registry.Verify(new("tenant-b", Client), issued.SessionId, MacInput, ReplayMac, Start)).ThrowsExactly<HttpRemoteTransportException>();
    }

    /// <summary>Verifies expiry releases a session identifier before another principal can bind it.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ExpiredSessionIdCanBeReboundOnlyAfterOriginalOwnerIsRejected()
    {
        var options = new HttpReplayProtectionOptions();
        HttpReplayRetentionBudget budget = new(options.MaximumRetainedBytes);
        await using HttpReplaySessionRegistry registry = new(options, budget);
        var principal = new HttpReplayPrincipal(Tenant, Client);
        var foreignPrincipal = new HttpReplayPrincipal(Tenant, AlternateClient);
        var expiredNow = Start.AddTicks(SingleSessionLimit);
        var first = new HttpReplayIssuedSession { SessionId = RegisteredSessionId, SessionSecret = RegisteredSessionSecret, ExpiresAtUtc = Start };
        var replacement = new HttpReplayIssuedSession { SessionId = RegisteredSessionId, SessionSecret = AlternateRegisteredSessionSecret, ExpiresAtUtc = expiredNow };

        registry.RegisterIssued(principal, first, Start);

        await Assert
            .That(() => registry.Verify(principal, first.SessionId, MacInput, CreateMac(options, first.SessionSecret), expiredNow))
            .ThrowsExactly<HttpRemoteTransportException>();

        registry.RegisterIssued(foreignPrincipal, replacement, expiredNow);
        var proof = registry.Verify(foreignPrincipal, replacement.SessionId, MacInput, CreateMac(options, replacement.SessionSecret), expiredNow);

        await Assert.That(proof.SessionId).IsEqualTo(replacement.SessionId);
        await Assert
            .That(() => registry.Verify(principal, first.SessionId, MacInput, CreateMac(options, replacement.SessionSecret), expiredNow))
            .ThrowsExactly<HttpRemoteTransportException>();
    }

    /// <summary>Verifies session MAC input is bounded before lookup or HMAC work.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task VerifyRejectsMacInputAboveCanonicalRequestLimit()
    {
        var options = new HttpReplayProtectionOptions { MaximumCanonicalRequestBytes = SingleSessionLimit };
        HttpReplayRetentionBudget budget = new(options.MaximumRetainedBytes);
        await using HttpReplaySessionRegistry registry = new(options, budget);

        await Assert
            .That(() => registry.Verify(new(Tenant, Client), RegisteredSessionId, MacInput, ReplayMac, Start))
            .ThrowsExactly<HttpRemoteTransportException>();
    }

    /// <summary>Verifies an owned session still rejects a non-matching replay MAC.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task VerifyRejectsWrongMacForOwnedSession()
    {
        var options = new HttpReplayProtectionOptions();
        HttpReplayRetentionBudget budget = new(options.MaximumRetainedBytes);
        await using HttpReplaySessionRegistry registry = new(options, budget);
        var principal = new HttpReplayPrincipal(Tenant, Client);
        var session = new HttpReplayIssuedSession { SessionId = RegisteredSessionId, SessionSecret = RegisteredSessionSecret, ExpiresAtUtc = Start };

        registry.RegisterIssued(principal, session, Start);

        await Assert
            .That(() => registry.Verify(principal, session.SessionId, MacInput, ReplayMac, Start))
            .ThrowsExactly<HttpRemoteTransportException>();
    }

    /// <summary>Verifies session registration rejects control characters before retaining any state.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RegisterIssuedRejectsControlCharacterHeadersWithoutRetainingBudget()
    {
        var options = new HttpReplayProtectionOptions { MaximumRetainedBytes = RegisteredSessionCharge };
        HttpReplayRetentionBudget budget = new(options.MaximumRetainedBytes);
        await using HttpReplaySessionRegistry registry = new(options, budget);
        var principal = new HttpReplayPrincipal(Tenant, Client);
        var session = new HttpReplayIssuedSession { SessionId = ControlSessionHeader, SessionSecret = RegisteredSessionSecret, ExpiresAtUtc = Start };

        await Assert.That(() => registry.RegisterIssued(principal, session, Start)).ThrowsExactly<HttpRemoteTransportException>();
        await Assert.That(registry.Count).IsEqualTo(0);
        await Assert.That(budget.RetainedBytes).IsEqualTo(0);
    }

    /// <summary>Verifies already-expired issued sessions are rejected before retaining capacity.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RegisterIssuedRejectsExpiredSessionWithoutRetainingBudget()
    {
        var options = new HttpReplayProtectionOptions { MaximumRetainedBytes = RegisteredSessionCharge };
        HttpReplayRetentionBudget budget = new(options.MaximumRetainedBytes);
        await using HttpReplaySessionRegistry registry = new(options, budget);
        var principal = new HttpReplayPrincipal(Tenant, Client);
        var session = new HttpReplayIssuedSession { SessionId = RegisteredSessionId, SessionSecret = RegisteredSessionSecret, ExpiresAtUtc = Start.AddTicks(-SingleSessionLimit) };

        await Assert.That(() => registry.RegisterIssued(principal, session, Start)).ThrowsExactly<HttpRemoteTransportException>();
        await Assert.That(registry.Count).IsEqualTo(0);
        await Assert.That(budget.RetainedBytes).IsEqualTo(0);
    }

    /// <summary>Verifies disposal clears retained sessions and prevents later issue or verify use.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DisposeAsyncClearsBudgetAndRejectsFurtherUse()
    {
        var options = new HttpReplayProtectionOptions { MaximumRetainedBytes = RegisteredSessionCharge };
        HttpReplayRetentionBudget budget = new(options.MaximumRetainedBytes);
        HttpReplaySessionRegistry registry = new(options, budget);
        var principal = new HttpReplayPrincipal(Tenant, Client);
        var session = new HttpReplayIssuedSession { SessionId = RegisteredSessionId, SessionSecret = RegisteredSessionSecret, ExpiresAtUtc = Start };

        registry.RegisterIssued(principal, session, Start);
        await registry.DisposeAsync();

        await Assert.That(registry.Count).IsEqualTo(0);
        await Assert.That(budget.RetainedBytes).IsEqualTo(0);
        await Assert.That(() => registry.Issue(principal, Start)).ThrowsExactly<HttpRemoteTransportException>();
        await Assert
            .That(() => registry.Verify(principal, session.SessionId, MacInput, CreateMac(options, session.SessionSecret), Start))
            .ThrowsExactly<HttpRemoteTransportException>();
    }

    /// <summary>Verifies registry disposal is idempotent after retained sessions are released.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DisposeAsyncCanBeCalledRepeatedlyAfterClearingSessions()
    {
        var options = new HttpReplayProtectionOptions { MaximumRetainedBytes = RegisteredSessionCharge };
        HttpReplayRetentionBudget budget = new(options.MaximumRetainedBytes);
        HttpReplaySessionRegistry registry = new(options, budget);
        var principal = new HttpReplayPrincipal(Tenant, Client);
        var session = new HttpReplayIssuedSession { SessionId = RegisteredSessionId, SessionSecret = RegisteredSessionSecret, ExpiresAtUtc = Start };

        registry.RegisterIssued(principal, session, Start);
        await registry.DisposeAsync();
        await registry.DisposeAsync();

        await Assert.That(registry.Count).IsEqualTo(0);
        await Assert.That(budget.RetainedBytes).IsEqualTo(0);
    }

    /// <summary>Verifies retention expiry arithmetic reports the overflowing argument name.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task AddCheckedThrowsConfiguredParameterNameOnOverflow()
    {
        var exception = await Assert
            .That(static () => HttpReplaySessionRegistry.AddChecked(DateTimeOffset.MaxValue, TimeSpan.FromTicks(SingleSessionLimit), ObservedUtcParameterName))
            .ThrowsExactly<ArgumentOutOfRangeException>();

        await Assert.That(exception!.ParamName).IsEqualTo(ObservedUtcParameterName);
    }

    /// <summary>Creates a replay session registry with shared retained-byte accounting.</summary>
    /// <returns>The replay session registry.</returns>
    private static HttpReplaySessionRegistry CreateRegistry()
    {
        var options = new HttpReplayProtectionOptions();
        HttpReplayRetentionBudget budget = new(options.MaximumRetainedBytes);
        return new(options, budget);
    }

    /// <summary>Verifies every byte in a secret buffer has been zeroed.</summary>
    /// <param name="secretBytes">The secret bytes.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    private static async Task AssertSecretBytesClearedAsync(byte[] secretBytes)
    {
        for (var index = 0; index < secretBytes.Length; index++)
        {
            await Assert.That(secretBytes[index]).IsEqualTo(ZeroSecretByte);
        }
    }

    /// <summary>Creates a valid replay MAC for deterministic session tests.</summary>
    /// <param name="options">The replay protection options.</param>
    /// <param name="sessionSecret">The replay session secret.</param>
    /// <returns>The replay MAC.</returns>
    private static string CreateMac(HttpReplayProtectionOptions options, string sessionSecret)
    {
        HttpReplayEnvelopeHasher hasher = new(options);
        return hasher.ComputeMac(Encoding.UTF8.GetBytes(sessionSecret), MacInput);
    }
}
