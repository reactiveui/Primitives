// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Tracks bounded request fingerprints within one authenticated server process.</summary>
/// <remarks>
/// Identities must come from authentication and bytes must cover the complete canonical request. This component does
/// not authorize requests, cache responses or persist replay history across process restarts. The hosting protocol must
/// provide durable replay history or invalidate its authentication sessions on restart before advertising replay protection.
/// Request bytes are borrowed immutably for the synchronous call; only their fingerprint is retained.
/// </remarks>
internal sealed class ReplayNonceRegistry
{
    /// <summary>The largest authenticated identity in UTF-16 characters.</summary>
    private const int MaximumIdentityCharacters = 256;

    /// <summary>The largest nonce in UTF-16 characters.</summary>
    private const int MaximumNonceCharacters = 128;

    /// <summary>The fingerprint and two timestamps retained per record, excluding its encoded key.</summary>
    private const int FixedRecordBytes = 48;

    /// <summary>The canonical key encoding.</summary>
    private static readonly Encoding KeyEncoding = new UTF8Encoding(false, true);

    /// <summary>Protects replay admission and accounting.</summary>
    private readonly Lock _gate = new();

    /// <summary>The bounded authenticated nonce records.</summary>
    private readonly Dictionary<NonceKey, NonceEntry> _entries = [];

    /// <summary>The maximum retained record count.</summary>
    private readonly int _maximumEntries;

    /// <summary>The maximum retained encoded keys, fingerprints and timestamps.</summary>
    private readonly long _maximumRetainedBytes;

    /// <summary>The largest accepted canonical request.</summary>
    private readonly int _maximumRequestBytes;

    /// <summary>The accepted timestamp skew in either direction.</summary>
    private readonly TimeSpan _freshnessWindow;

    /// <summary>The minimum interval for which an admitted nonce is retained.</summary>
    private readonly TimeSpan _nonceRetention;

    /// <summary>The clock sampled outside the registry gate.</summary>
    private readonly TimeProvider _timeProvider;

    /// <summary>The retained encoded record bytes.</summary>
    private long _retainedBytes;

    /// <summary>The latest observed clock value, preventing rollback from reopening expired windows.</summary>
    private DateTimeOffset _latestUtc = DateTimeOffset.MinValue;

    /// <summary>Initializes a new instance of the <see cref="ReplayNonceRegistry"/> class.</summary>
    /// <param name="maximumEntries">The positive record limit.</param>
    /// <param name="maximumRetainedBytes">The positive encoded retention limit.</param>
    /// <param name="maximumRequestBytes">The positive canonical request limit.</param>
    /// <param name="freshnessWindow">The positive timestamp window.</param>
    /// <param name="nonceRetention">The finite minimum retention period covering the freshness window.</param>
    /// <param name="timeProvider">The clock.</param>
    /// <exception cref="ArgumentNullException">The clock is missing.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A limit is not positive.</exception>
    internal ReplayNonceRegistry(
        int maximumEntries,
        long maximumRetainedBytes,
        int maximumRequestBytes,
        TimeSpan freshnessWindow,
        TimeSpan nonceRetention,
        TimeProvider timeProvider)
    {
        ArgumentExceptionHelper.ThrowIfNull(timeProvider);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(maximumEntries);
        ThrowIfNegativeOrZero(maximumRetainedBytes, nameof(maximumRetainedBytes));
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(maximumRequestBytes);
        if (freshnessWindow <= TimeSpan.Zero || freshnessWindow == TimeSpan.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(freshnessWindow),
                freshnessWindow,
                "Freshness windows must be positive and finite.");
        }

        if (nonceRetention < freshnessWindow || nonceRetention == TimeSpan.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(nonceRetention), nonceRetention, "Nonce retention must be finite and cover the freshness window.");
        }

        _maximumEntries = maximumEntries;
        _maximumRetainedBytes = maximumRetainedBytes;
        _maximumRequestBytes = maximumRequestBytes;
        _freshnessWindow = freshnessWindow;
        _nonceRetention = nonceRetention;
        _timeProvider = timeProvider;
    }

    /// <summary>Gets the retained record count.</summary>
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

    /// <summary>Gets retained encoded key, fingerprint and timestamp bytes.</summary>
    internal long RetainedBytes
    {
        get
        {
            lock (_gate)
            {
                return _retainedBytes;
            }
        }
    }

    /// <summary>Checks whether an authenticated request has already been registered.</summary>
    /// <param name="authenticatedTenant">The tenant supplied by authentication.</param>
    /// <param name="authenticatedClient">The client supplied by authentication.</param>
    /// <param name="nonce">The request nonce.</param>
    /// <param name="requestTimestamp">The timestamp covered by authentication and the canonical request.</param>
    /// <param name="requestBytes">The complete immutable canonical request bytes.</param>
    /// <returns>Whether the identical authenticated request was previously registered.</returns>
    /// <exception cref="ArgumentException">An identity, nonce or request size is invalid.</exception>
    /// <exception cref="ArgumentNullException">An identity or nonce is missing.</exception>
    /// <exception cref="InvalidOperationException">Freshness, replay integrity or retention capacity is violated.</exception>
    internal bool IsReplay(
        string authenticatedTenant,
        string authenticatedClient,
        string nonce,
        DateTimeOffset requestTimestamp,
        ReadOnlyMemory<byte> requestBytes)
    {
        ValidateIdentifier(authenticatedTenant, MaximumIdentityCharacters, nameof(authenticatedTenant));
        ValidateIdentifier(authenticatedClient, MaximumIdentityCharacters, nameof(authenticatedClient));
        ValidateIdentifier(nonce, MaximumNonceCharacters, nameof(nonce));
        ValidateRequestSize(requestBytes);

        var key = new NonceKey(authenticatedTenant, authenticatedClient, nonce);
        var retainedBytes = FixedRecordBytes
            + GetIdentifierByteCount(authenticatedTenant)
            + GetIdentifierByteCount(authenticatedClient)
            + GetIdentifierByteCount(nonce);
        var fingerprint = Hash(requestBytes);
        var observedUtc = _timeProvider.GetUtcNow();
        lock (_gate)
        {
            if (observedUtc > _latestUtc)
            {
                _latestUtc = observedUtc;
            }

            EnsureFresh(requestTimestamp);
            PruneExpired(_latestUtc);
            if (_entries.TryGetValue(key, out var existing))
            {
                EnsureReplayMatches(existing, requestTimestamp, fingerprint);
                return true;
            }

            EnsureCapacity(retainedBytes);
            _entries.Add(key, new(fingerprint, requestTimestamp, GetExpiry(requestTimestamp), retainedBytes));
            _retainedBytes += retainedBytes;
            return false;
        }
    }

    /// <summary>Throws when a long value is negative or zero.</summary>
    /// <param name="value">The value to validate.</param>
    /// <param name="parameterName">The source parameter name.</param>
    /// <exception cref="ArgumentOutOfRangeException">The value is not positive.</exception>
    private static void ThrowIfNegativeOrZero(long value, string parameterName)
    {
        if (value > 0)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(parameterName, value, null);
    }

    /// <summary>Rejects nonce reuse that changes the authenticated request.</summary>
    /// <param name="existing">The retained nonce entry.</param>
    /// <param name="requestTimestamp">The requested timestamp.</param>
    /// <param name="fingerprint">The requested canonical request fingerprint.</param>
    /// <exception cref="InvalidOperationException">The replay did not match the retained request.</exception>
    private static void EnsureReplayMatches(NonceEntry existing, DateTimeOffset requestTimestamp, byte[] fingerprint)
    {
        if (existing.RequestTimestamp == requestTimestamp && Matches(existing.Fingerprint, fingerprint))
        {
            return;
        }

        throw new InvalidOperationException("Nonce reuse changed the authenticated request.");
    }

    /// <summary>Validates a bounded textual key before encoding or hashing.</summary>
    /// <param name="value">The key value.</param>
    /// <param name="maximumCharacters">The character bound.</param>
    /// <param name="parameterName">The source parameter name.</param>
    /// <exception cref="ArgumentNullException">The key is missing.</exception>
    /// <exception cref="ArgumentException">The key is blank or oversized.</exception>
    private static void ValidateIdentifier(string value, int maximumCharacters, string parameterName)
    {
        ArgumentExceptionHelper.ThrowIfNull(value, parameterName);
        if (string.IsNullOrWhiteSpace(value) || value.Length > maximumCharacters)
        {
            throw new ArgumentException("Authenticated request key is invalid.", parameterName);
        }

        ThrowIfMalformedSurrogate(value, parameterName);
    }

    /// <summary>Rejects malformed surrogate pairs before strict UTF-8 key accounting.</summary>
    /// <param name="value">The key value.</param>
    /// <param name="parameterName">The source parameter name.</param>
    /// <exception cref="ArgumentException">The key contains an unpaired surrogate.</exception>
    private static void ThrowIfMalformedSurrogate(string value, string parameterName)
    {
        if (!HasMalformedSurrogate(value))
        {
            return;
        }

        throw new ArgumentException("Authenticated request key is invalid.", parameterName);
    }

    /// <summary>Detects malformed surrogate pairs before strict UTF-8 key accounting.</summary>
    /// <param name="value">The key value.</param>
    /// <returns>Whether the key contains an unpaired surrogate.</returns>
    private static bool HasMalformedSurrogate(string value)
    {
        for (var index = 0; index < value.Length; index++)
        {
            var character = value[index];
            if (char.IsLowSurrogate(character))
            {
                return true;
            }

            if (!char.IsHighSurrogate(character))
            {
                continue;
            }

            if (index + 1 < value.Length && char.IsLowSurrogate(value[index + 1]))
            {
                index++;
                continue;
            }

            return true;
        }

        return false;
    }

    /// <summary>Counts UTF-8 bytes for a validated key.</summary>
    /// <param name="value">The validated key.</param>
    /// <returns>The exact UTF-8 byte count.</returns>
    /// <exception cref="ArgumentException">The key cannot be encoded as strict UTF-8.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int GetIdentifierByteCount(string value) => KeyEncoding.GetByteCount(value);

    /// <summary>Hashes a bounded request without retaining its body.</summary>
    /// <param name="request">The canonical bytes.</param>
    /// <returns>The owned SHA-256 fingerprint.</returns>
    private static byte[] Hash(ReadOnlyMemory<byte> request)
    {
#if NET5_0_OR_GREATER
        return SHA256.HashData(request.Span);
#else
        using var hash = SHA256.Create();
        return hash.ComputeHash(request.ToArray());
#endif
    }

    /// <summary>Compares fingerprints without data-dependent early exit.</summary>
    /// <param name="stored">The stored fingerprint.</param>
    /// <param name="requested">The requested fingerprint.</param>
    /// <returns>Whether the fingerprints match.</returns>
    private static bool Matches(byte[] stored, byte[] requested)
    {
#if NET5_0_OR_GREATER
        return CryptographicOperations.FixedTimeEquals(stored, requested);
#else
        var difference = 0;
        for (var index = 0; index < stored.Length; index++)
        {
            difference |= stored[index] ^ requested[index];
        }

        return difference == 0;
#endif
    }

    /// <summary>Validates the canonical request size before hashing.</summary>
    /// <param name="requestBytes">The request bytes.</param>
    /// <exception cref="ArgumentException">The request is empty or too large.</exception>
    private void ValidateRequestSize(ReadOnlyMemory<byte> requestBytes)
    {
        if (!requestBytes.IsEmpty && requestBytes.Length <= _maximumRequestBytes)
        {
            return;
        }

        throw new ArgumentException("Canonical request size is invalid.", nameof(requestBytes));
    }

    /// <summary>Rejects timestamps outside the inclusive freshness window.</summary>
    /// <param name="requestTimestamp">The authenticated request timestamp.</param>
    /// <exception cref="InvalidOperationException">The timestamp is outside the accepted window.</exception>
    private void EnsureFresh(DateTimeOffset requestTimestamp)
    {
        var age = _latestUtc - requestTimestamp;
        if (age <= _freshnessWindow && age >= -_freshnessWindow)
        {
            return;
        }

        throw new InvalidOperationException("Request timestamp is outside the freshness window.");
    }

    /// <summary>Rejects a new entry when retained count or byte limits would be exceeded.</summary>
    /// <param name="retainedBytes">The bytes required by the new entry.</param>
    /// <exception cref="InvalidOperationException">No capacity remains for a distinct nonce.</exception>
    private void EnsureCapacity(int retainedBytes)
    {
        if (_entries.Count < _maximumEntries && retainedBytes <= _maximumRetainedBytes - _retainedBytes)
        {
            return;
        }

        throw new InvalidOperationException("Replay retention capacity is exhausted.");
    }

    /// <summary>Retains a nonce throughout the interval in which its timestamp remains acceptable.</summary>
    /// <param name="requestTimestamp">The authenticated timestamp.</param>
    /// <returns>The inclusive expiry timestamp.</returns>
    private DateTimeOffset GetExpiry(DateTimeOffset requestTimestamp)
    {
        try
        {
            var freshnessExpiry = requestTimestamp.Add(_freshnessWindow);
            var retentionExpiry = _latestUtc.Add(_nonceRetention);
            return freshnessExpiry >= retentionExpiry ? freshnessExpiry : retentionExpiry;
        }
        catch (ArgumentOutOfRangeException)
        {
            return DateTimeOffset.MaxValue;
        }
    }

    /// <summary>Reclaims records whose inclusive freshness interval has ended.</summary>
    /// <param name="now">The latest observed UTC timestamp.</param>
    private void PruneExpired(DateTimeOffset now)
    {
        List<NonceKey> expired = [];
        foreach (var entry in _entries)
        {
            if (entry.Value.ExpiresAt < now)
            {
                expired.Add(entry.Key);
            }
        }

        foreach (var key in expired)
        {
            _retainedBytes -= _entries[key].RetainedBytes;
            _ = _entries.Remove(key);
        }
    }

    /// <summary>Identifies a nonce within its authenticated scope.</summary>
    /// <param name="Tenant">The authenticated tenant.</param>
    /// <param name="Client">The authenticated client.</param>
    /// <param name="Nonce">The request nonce.</param>
    private readonly record struct NonceKey(string Tenant, string Client, string Nonce);

    /// <summary>Retains a fingerprint and its bounded replay interval.</summary>
    /// <param name="Fingerprint">The owned request hash.</param>
    /// <param name="RequestTimestamp">The authenticated request timestamp.</param>
    /// <param name="ExpiresAt">The inclusive expiry.</param>
    /// <param name="RetainedBytes">The encoded record size.</param>
    private sealed record NonceEntry(byte[] Fingerprint, DateTimeOffset RequestTimestamp, DateTimeOffset ExpiresAt, int RetainedBytes);
}
