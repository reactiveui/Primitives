// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Issues and verifies endpoint-owned replay sessions.</summary>
internal sealed class HttpReplaySessionRegistry : IAsyncDisposable
{
    /// <summary>The replay token byte count.</summary>
    private const int SessionTokenBytes = 16;

    /// <summary>The smallest retained session list capacity.</summary>
    private const int MinimumSessionEntryCapacity = 1;

    /// <summary>The retained session list growth multiplier.</summary>
    private const int SessionEntryCapacityGrowthMultiplier = 2;

    /// <summary>The synchronization gate.</summary>
    private readonly Lock _gate = new();

    /// <summary>The retained replay sessions.</summary>
    private readonly List<SessionEntry> _entries = [];

    /// <summary>The replay envelope hasher.</summary>
    private readonly HttpReplayEnvelopeHasher _hasher;

    /// <summary>The replay protection options.</summary>
    private readonly HttpReplayProtectionOptions _options;

    /// <summary>The shared retained-byte budget.</summary>
    private readonly HttpReplayRetentionBudget _budget;

    /// <summary>Whether this registry has been disposed.</summary>
    private bool _disposed;

    /// <summary>Initializes a new instance of the <see cref="HttpReplaySessionRegistry"/> class.</summary>
    /// <param name="options">The replay protection options.</param>
    /// <param name="budget">The shared retained-byte budget.</param>
    internal HttpReplaySessionRegistry(HttpReplayProtectionOptions options, HttpReplayRetentionBudget budget)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        ArgumentExceptionHelper.ThrowIfNull(budget);
        options.Validate();
        _options = options;
        _budget = budget;
        _hasher = new(options);
    }

    /// <summary>Gets the retained replay session count.</summary>
    internal int Count
    {
        get
        {
            lock (_gate)
            {
                return _entries.Count;
            }
        }
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            _disposed = true;
            for (var index = 0; index < _entries.Count; index++)
            {
                _entries[index].Dispose();
            }

            _entries.Clear();
        }

        return default;
    }

    /// <summary>Adds a bounded retention window to a timestamp.</summary>
    /// <param name="value">The timestamp.</param>
    /// <param name="window">The retention window.</param>
    /// <param name="parameterName">The parameter name for overflow failures.</param>
    /// <returns>The resulting timestamp.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The timestamp addition overflows.</exception>
    internal static DateTimeOffset AddChecked(DateTimeOffset value, TimeSpan window, string parameterName)
    {
        try
        {
            return value.Add(window);
        }
        catch (ArgumentOutOfRangeException exception)
        {
            throw new ArgumentOutOfRangeException(parameterName, value, exception.Message);
        }
    }

    /// <summary>Issues a fresh replay session for the trusted principal.</summary>
    /// <param name="principal">The trusted session owner.</param>
    /// <param name="observedUtc">The monotonic observed timestamp.</param>
    /// <returns>The issued replay session header values.</returns>
    /// <exception cref="HttpRemoteTransportException">Input is invalid or capacity is unavailable.</exception>
    internal HttpReplayIssuedSession Issue(HttpReplayPrincipal principal, DateTimeOffset observedUtc)
    {
        ValidatePrincipal(principal);
        var expiresAtUtc = AddChecked(observedUtc, _options.ReplaySessionRetention, nameof(observedUtc));
        var session = new HttpReplayIssuedSession { SessionId = CreateToken(), SessionSecret = CreateToken(), ExpiresAtUtc = expiresAtUtc };
        RegisterIssued(principal, session, observedUtc);
        return session;
    }

    /// <summary>Registers an endpoint-issued replay session.</summary>
    /// <param name="principal">The trusted session owner.</param>
    /// <param name="session">The issued replay session.</param>
    /// <param name="observedUtc">The monotonic observed timestamp.</param>
    /// <exception cref="HttpRemoteTransportException">Input is invalid or capacity is unavailable.</exception>
    internal void RegisterIssued(HttpReplayPrincipal principal, HttpReplayIssuedSession session, DateTimeOffset observedUtc)
    {
        ArgumentExceptionHelper.ThrowIfNull(session);
        ValidatePrincipal(principal);
        ValidateHeader(session.SessionId);
        ValidateHeader(session.SessionSecret);
        ValidateSessionExpiry(session.ExpiresAtUtc, observedUtc);
        var charge = GetSessionCharge(principal, session);
        using var sessionSecret = new HttpReplaySessionSecretCandidate();
        lock (_gate)
        {
            ThrowIfDisposed();
            RemoveExpiredCore(observedUtc);
            var existingIndex = FindSessionIndexCore(session.SessionId);
            if (existingIndex >= 0)
            {
                var existing = _entries[existingIndex];
                if (!existing.IsOwnedBy(principal))
                {
                    throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected, HttpStatusCode.Conflict);
                }

                ReplaceExistingSessionCore(existing, sessionSecret, session.SessionSecret, session.ExpiresAtUtc, charge);
                return;
            }

            if (_entries.Count >= _options.MaximumReplaySessions)
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.Transient, HttpTransportStatus.TooManyRequests);
            }

            EnsureEntryCapacityForAddCore();
            using var lease = _budget.Reserve(charge);
            sessionSecret.Retain(session.SessionSecret);
            _entries.Add(new(principal, session.SessionId, sessionSecret, session.ExpiresAtUtc, lease));
        }
    }

    /// <summary>Verifies replay session ownership and MAC for a non-connect request.</summary>
    /// <param name="principal">The trusted request owner.</param>
    /// <param name="replaySessionId">The replay session identifier.</param>
    /// <param name="macInput">The canonical replay MAC input.</param>
    /// <param name="replayMac">The replay MAC header value.</param>
    /// <param name="observedUtc">The monotonic observed timestamp.</param>
    /// <returns>The verified replay session proof.</returns>
    /// <exception cref="HttpRemoteTransportException">Input is invalid or authentication fails.</exception>
    internal HttpReplaySessionProof Verify(
        HttpReplayPrincipal principal,
        string replaySessionId,
        ReadOnlyMemory<byte> macInput,
        string replayMac,
        DateTimeOffset observedUtc)
    {
        ValidatePrincipal(principal);
        ValidateHeader(replaySessionId);
        ValidateHeader(replayMac);
        if (macInput.Length > _options.MaximumCanonicalRequestBytes)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.PayloadTooLarge, HttpStatusCode.RequestEntityTooLarge);
        }

        byte[]? sessionSecret = null;
        string? verifiedSessionId = null;
        var verifiedExpiresAtUtc = DateTimeOffset.MinValue;
        lock (_gate)
        {
            ThrowIfDisposed();
            RemoveExpiredCore(observedUtc);
            var entry = FindSessionCore(replaySessionId);
            if (entry is not null && entry.IsOwnedBy(principal))
            {
                sessionSecret = entry.CopySessionSecret();
                verifiedSessionId = entry.SessionId;
                verifiedExpiresAtUtc = entry.ExpiresAtUtc;
            }
        }

        if (sessionSecret is null || verifiedSessionId is null)
        {
            throw new HttpRemoteTransportException(HttpTransportFailureKind.Authentication, HttpStatusCode.Unauthorized);
        }

        try
        {
            var expectedMac = _hasher.ComputeMac(sessionSecret, macInput);
            if (!FixedTimeEquals(expectedMac, replayMac))
            {
                throw new HttpRemoteTransportException(HttpTransportFailureKind.Authentication, HttpStatusCode.Unauthorized);
            }

            return new() { SessionId = verifiedSessionId, ExpiresAtUtc = verifiedExpiresAtUtc };
        }
        finally
        {
            HttpReplayCryptography.ZeroMemory(sessionSecret);
        }
    }

    /// <summary>Creates a random replay token.</summary>
    /// <returns>The replay token text.</returns>
    private static string CreateToken()
    {
        var bytes = new byte[SessionTokenBytes];
        using var generator = RandomNumberGenerator.Create();
        generator.GetBytes(bytes);
        return HttpReplayBase64Url.Encode(bytes);
    }

    /// <summary>Compares two replay MAC strings without data-dependent early exit.</summary>
    /// <param name="left">The left MAC.</param>
    /// <param name="right">The right MAC.</param>
    /// <returns>Whether the MACs match.</returns>
    private static bool FixedTimeEquals(string left, string right)
    {
        var leftBytes = Encoding.UTF8.GetBytes(left);
        var rightBytes = Encoding.UTF8.GetBytes(right);
        return leftBytes.Length == rightBytes.Length && HttpReplayCryptography.FixedTimeEquals(leftBytes, rightBytes);
    }

    /// <summary>Gets the retained byte charge for a replay session.</summary>
    /// <param name="principal">The trusted session owner.</param>
    /// <param name="session">The session values.</param>
    /// <returns>The retained byte charge.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long GetSessionCharge(HttpReplayPrincipal principal, HttpReplayIssuedSession session) =>
        HttpReplayRetainedSizeCalculator.SumTextBytes(
            Encoding.UTF8.GetByteCount(principal.TenantId),
            Encoding.UTF8.GetByteCount(principal.ClientId),
            Encoding.UTF8.GetByteCount(session.SessionId),
            Encoding.UTF8.GetByteCount(session.SessionSecret));

    /// <summary>Rejects replacement sessions whose retained charge cannot fit in a single budget lease.</summary>
    /// <param name="charge">The retained session charge.</param>
    /// <param name="budget">The retained-byte budget.</param>
    /// <exception cref="HttpRemoteTransportException">The charge exceeds the total retained-byte budget.</exception>
    private static void ThrowIfSingleLeaseExceedsBudget(long charge, HttpReplayRetentionBudget budget)
    {
        if (charge <= budget.MaximumBytes)
        {
            return;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.Transient, HttpTransportStatus.TooManyRequests);
    }

    /// <summary>Validates a trusted principal.</summary>
    /// <param name="principal">The trusted principal.</param>
    /// <exception cref="HttpRemoteTransportException">The principal contains invalid header text.</exception>
    private static void ValidatePrincipal(HttpReplayPrincipal principal)
    {
        ValidateHeader(principal.TenantId);
        ValidateHeader(principal.ClientId);
    }

    /// <summary>Validates an opaque header value.</summary>
    /// <param name="value">The candidate header.</param>
    /// <exception cref="HttpRemoteTransportException">The header is empty or contains a control character.</exception>
    private static void ValidateHeader(string value)
    {
        if (!string.IsNullOrWhiteSpace(value) && !ContainsControl(value))
        {
            return;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected, HttpStatusCode.BadRequest);
    }

    /// <summary>Checks whether text contains a control character.</summary>
    /// <param name="value">The candidate text.</param>
    /// <returns>Whether a control character is present.</returns>
    private static bool ContainsControl(string value)
    {
        for (var index = 0; index < value.Length; index++)
        {
            if (char.IsControl(value[index]))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Validates a replay session expiry.</summary>
    /// <param name="expiresAtUtc">The session expiry.</param>
    /// <param name="observedUtc">The observed timestamp.</param>
    /// <exception cref="HttpRemoteTransportException">The session expiry is invalid.</exception>
    private static void ValidateSessionExpiry(DateTimeOffset expiresAtUtc, DateTimeOffset observedUtc)
    {
        if (expiresAtUtc >= observedUtc)
        {
            return;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.ValidationRejected, HttpStatusCode.BadRequest);
    }

    /// <summary>Throws when this registry has been disposed.</summary>
    /// <exception cref="HttpRemoteTransportException">This registry has been disposed.</exception>
    private void ThrowIfDisposed()
    {
        if (!_disposed)
        {
            return;
        }

        throw new HttpRemoteTransportException(HttpTransportFailureKind.Transient);
    }

    /// <summary>Finds a retained replay session.</summary>
    /// <param name="sessionId">The session identifier.</param>
    /// <returns>The retained session, when found.</returns>
    private SessionEntry? FindSessionCore(string sessionId)
    {
        var index = FindSessionIndexCore(sessionId);
        return index >= 0 ? _entries[index] : null;
    }

    /// <summary>Finds a retained session index by identifier.</summary>
    /// <param name="sessionId">The session identifier.</param>
    /// <returns>The retained session index or -1.</returns>
    private int FindSessionIndexCore(string sessionId)
    {
        for (var index = 0; index < _entries.Count; index++)
        {
            if (string.Equals(_entries[index].SessionId, sessionId, StringComparison.Ordinal))
            {
                return index;
            }
        }

        return -1;
    }

    /// <summary>Removes expired retained sessions.</summary>
    /// <param name="observedUtc">The observed timestamp.</param>
    private void RemoveExpiredCore(DateTimeOffset observedUtc)
    {
        for (var index = _entries.Count - 1; index >= 0; index--)
        {
            if (observedUtc <= _entries[index].ExpiresAtUtc)
            {
                continue;
            }

            _entries[index].Dispose();
            _entries.RemoveAt(index);
        }
    }

    /// <summary>Grows retained session storage before ownership is transferred into a new entry.</summary>
    private void EnsureEntryCapacityForAddCore()
    {
        var requiredCapacity = _entries.Count + 1;
        if (requiredCapacity <= _entries.Capacity)
        {
            return;
        }

        var doubledCapacity = _entries.Capacity > _options.MaximumReplaySessions / SessionEntryCapacityGrowthMultiplier
            ? _options.MaximumReplaySessions
            : _entries.Capacity * SessionEntryCapacityGrowthMultiplier;
        var grownCapacity = Math.Max(MinimumSessionEntryCapacity, Math.Max(requiredCapacity, doubledCapacity));
        _entries.Capacity = Math.Min(_options.MaximumReplaySessions, grownCapacity);
    }

    /// <summary>Replaces an existing session while keeping staged secret ownership inside the registry gate.</summary>
    /// <param name="existing">The retained session entry.</param>
    /// <param name="sessionSecret">The replacement session secret candidate.</param>
    /// <param name="sessionSecretText">The replacement session secret text.</param>
    /// <param name="expiresAtUtc">The replacement session expiry.</param>
    /// <param name="charge">The retained replacement charge.</param>
    private void ReplaceExistingSessionCore(
        SessionEntry existing,
        HttpReplaySessionSecretCandidate sessionSecret,
        string sessionSecretText,
        DateTimeOffset expiresAtUtc,
        long charge)
    {
        ThrowIfSingleLeaseExceedsBudget(charge, _budget);
        sessionSecret.Retain(sessionSecretText);
        existing.Replace(sessionSecret, expiresAtUtc, charge, _budget);
    }

    /// <summary>Represents one retained replay session.</summary>
    private sealed class SessionEntry : IDisposable
    {
        /// <summary>The retained byte budget lease.</summary>
        private readonly IDisposable _lease;

        /// <summary>The retained session secret owner.</summary>
        private HttpReplaySessionSecretOwner _sessionSecret;

        /// <summary>Initializes a new instance of the <see cref="SessionEntry"/> class.</summary>
        /// <param name="principal">The trusted owner.</param>
        /// <param name="sessionId">The session identifier.</param>
        /// <param name="sessionSecret">The session secret candidate.</param>
        /// <param name="expiresAtUtc">The inclusive expiry.</param>
        /// <param name="lease">The retained byte lease candidate.</param>
        internal SessionEntry(
            HttpReplayPrincipal principal,
            string sessionId,
            HttpReplaySessionSecretCandidate sessionSecret,
            DateTimeOffset expiresAtUtc,
            HttpReplayRetentionBudget.Reservation lease)
        {
            Principal = principal;
            SessionId = sessionId;

            // The registry grows list capacity before creating this entry, so ownership transfer is followed only by storing into preallocated list storage.
            _sessionSecret = sessionSecret.Transfer();
            ExpiresAtUtc = expiresAtUtc;
            _lease = lease.Transfer();
        }

        /// <summary>Gets the inclusive expiry.</summary>
        internal DateTimeOffset ExpiresAtUtc { get; private set; }

        /// <summary>Gets the trusted owner.</summary>
        internal HttpReplayPrincipal Principal { get; }

        /// <summary>Gets the session identifier.</summary>
        internal string SessionId { get; }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
            _sessionSecret.Dispose();
            _lease.Dispose();
        }

        /// <summary>Copies the retained secret for MAC verification outside the registry lock.</summary>
        /// <returns>An owned temporary secret copy.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal byte[] CopySessionSecret() => _sessionSecret.Copy();

        /// <summary>Checks whether this session is owned by the supplied principal.</summary>
        /// <param name="principal">The trusted principal.</param>
        /// <returns>Whether the principal owns this session.</returns>
        internal bool IsOwnedBy(HttpReplayPrincipal principal) =>
            string.Equals(Principal.TenantId, principal.TenantId, StringComparison.Ordinal)
            && string.Equals(Principal.ClientId, principal.ClientId, StringComparison.Ordinal);

        /// <summary>Replaces this session's secret and expiry after retained-byte capacity is secured.</summary>
        /// <param name="sessionSecret">The replacement session secret candidate.</param>
        /// <param name="expiresAtUtc">The replacement expiry.</param>
        /// <param name="retainedBytes">The replacement retained byte count.</param>
        /// <param name="budget">The retained-byte budget.</param>
        internal void Replace(
            HttpReplaySessionSecretCandidate sessionSecret,
            DateTimeOffset expiresAtUtc,
            long retainedBytes,
            HttpReplayRetentionBudget budget)
        {
            budget.Update(_lease, retainedBytes);
            _sessionSecret.Dispose();
            _sessionSecret = sessionSecret.Transfer();
            ExpiresAtUtc = expiresAtUtc;
        }
    }
}
