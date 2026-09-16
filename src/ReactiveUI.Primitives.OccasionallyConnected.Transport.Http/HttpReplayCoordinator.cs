// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Coordinates replay sessions, nonce cache admission, waiters, and retained responses.</summary>
/// <remarks>
/// The retained-byte budget covers replay state kept after admission. Canonical request hashing is limited per request
/// and is composed with the owning endpoint's concurrent request gate.
/// </remarks>
internal sealed class HttpReplayCoordinator : IAsyncDisposable
{
    /// <summary>The connect replay session identifier response header.</summary>
    private const string ReplaySessionIdHeader = "X-ReactiveUI-Replay-Session-Id";

    /// <summary>The connect replay session secret response header.</summary>
    private const string ReplaySessionSecretHeader = "X-ReactiveUI-Replay-Session-Secret";

    /// <summary>The connect replay session expiry response header.</summary>
    private const string ReplaySessionExpiresHeader = "X-ReactiveUI-Replay-Session-Expires";

    /// <summary>The retained status metadata byte count.</summary>
    private const int StatusCodeRetainedBytes = sizeof(int);

    /// <summary>The synchronization gate.</summary>
    private readonly Lock _gate = new();

    /// <summary>The retained replay entries.</summary>
    private readonly List<ReplayEntry> _entries = [];

    /// <summary>The replay envelope hasher.</summary>
    private readonly HttpReplayEnvelopeHasher _hasher;

    /// <summary>The owned replay session registry.</summary>
    private readonly HttpReplaySessionRegistry _sessions;

    /// <summary>The replay protection options.</summary>
    private readonly HttpReplayProtectionOptions _options;

    /// <summary>The shared replay retained-byte budget.</summary>
    private readonly HttpReplayRetentionBudget _budget;

    /// <summary>The next replay entry identifier.</summary>
    private long _nextEntryId;

    /// <summary>The current monotonic high-water time.</summary>
    private DateTimeOffset _highWaterUtc;

    /// <summary>The active duplicate waiter count.</summary>
    private int _activeReplayWaiters;

    /// <summary>Whether this coordinator has been disposed.</summary>
    private bool _disposed;

    /// <summary>Initializes a new instance of the <see cref="HttpReplayCoordinator"/> class.</summary>
    /// <param name="options">The replay protection options.</param>
    internal HttpReplayCoordinator(HttpReplayProtectionOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        options.Validate();
        _options = options;
        _budget = new(options.MaximumRetainedBytes);
        _hasher = new(options);
        _sessions = new(options, _budget);
        _highWaterUtc = DateTimeOffset.MinValue;
    }

    /// <summary>Gets the owned replay session registry.</summary>
    internal HttpReplaySessionRegistry Sessions => _sessions;

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        List<ReplayWaiter> waiters = [];
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            for (var index = 0; index < _entries.Count; index++)
            {
                DetachWaitersCore(_entries[index], waiters);
                _entries[index].Dispose();
            }

            _entries.Clear();
        }

        CompleteWaiters(waiters, CreateTransientDecision(HttpStatusCode.ServiceUnavailable));
        await _sessions.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Admits a replay request after invoking current authorization outside internal locks.</summary>
    /// <param name="request">The replay request.</param>
    /// <param name="authorizeAsync">The current authorization callback.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The replay admission decision.</returns>
    /// <exception cref="HttpRemoteTransportException">Replay input is invalid.</exception>
    /// <remarks>The owning endpoint remains responsible for bounding concurrent requests before admission.</remarks>
    internal async ValueTask<HttpReplayDecision> AdmitAsync(
        HttpReplayRequest request,
        Func<CancellationToken, ValueTask<HttpReplayAuthorizationResult>> authorizeAsync,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        ArgumentExceptionHelper.ThrowIfNull(authorizeAsync);
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            if (_disposed)
            {
                return CreateTransientDecision(HttpStatusCode.ServiceUnavailable);
            }
        }

        var envelope = _hasher.Create(request);
        var authorization = await authorizeAsync(cancellationToken).ConfigureAwait(false);
        var observedUtc = ObserveHighWater();
        var failureDecision = CreatePreAdmissionFailureDecision(request, envelope, authorization, observedUtc, out var sessionProof);
        if (failureDecision is not null)
        {
            return failureDecision;
        }

        ReplayWaiter? waiter = null;
        HttpReplayDecision decision;
        lock (_gate)
        {
            if (_disposed)
            {
                return CreateTransientDecision(HttpStatusCode.ServiceUnavailable);
            }

            observedUtc = ObserveHighWater();
            if (sessionProof is not null && observedUtc > sessionProof.ExpiresAtUtc)
            {
                return CreateRejectDecision(HttpStatusCode.Unauthorized, HttpTransportFailureKind.Authentication);
            }

            var existingIndex = FindEntryIndexCore(request);
            decision = existingIndex >= 0
                ? AdmitExistingCore(existingIndex, request, envelope, observedUtc, out waiter)
                : AdmitNewCore(request, envelope, observedUtc, sessionProof?.ExpiresAtUtc);
        }

        return waiter is not null
            ? await AwaitWaiterAsync(waiter, cancellationToken).ConfigureAwait(false)
            : decision;
    }

    /// <summary>Completes an owner execution after endpoint effects have run.</summary>
    /// <param name="owner">The replay owner.</param>
    /// <param name="completion">The execution completion.</param>
    /// <returns>The asynchronous completion operation.</returns>
    internal ValueTask CompleteAsync(HttpReplayOwner owner, HttpReplayCompletion completion)
    {
        ArgumentExceptionHelper.ThrowIfNull(owner);
        ArgumentExceptionHelper.ThrowIfNull(completion);
        if (!owner.IsOwnedBy(this) || !owner.TryClose())
        {
            return default;
        }

        var registrationFailureStatus = RegisterConnectSession(owner, completion);
        List<ReplayWaiter> waiters = [];
        HttpReplayDecision? waiterDecision = null;
        lock (_gate)
        {
            var entry = FindEntryByIdCore(owner.EntryId);
            if (!_disposed && entry is not null && entry.IsInFlight)
            {
                CompleteEntryCore(entry, completion, registrationFailureStatus);
                waiterDecision = entry.CreateCurrentDecision();
                DetachWaitersCore(entry, waiters);
            }
        }

        if (waiterDecision is null)
        {
            return default;
        }

        CompleteWaiters(waiters, waiterDecision);
        return default;
    }

    /// <summary>Abandons an owner execution and transitions retained state to uncached replay when needed.</summary>
    /// <param name="owner">The replay owner.</param>
    /// <param name="_">The cancellation token used only while waiting for drain.</param>
    /// <returns>The asynchronous abandonment operation.</returns>
    internal ValueTask AbandonAsync(HttpReplayOwner owner, CancellationToken _)
    {
        ArgumentExceptionHelper.ThrowIfNull(owner);
        if (!owner.IsOwnedBy(this) || !owner.TryClose())
        {
            return default;
        }

        List<ReplayWaiter> waiters = [];
        HttpReplayDecision? waiterDecision = null;
        lock (_gate)
        {
            var entry = FindEntryByIdCore(owner.EntryId);
            if (!_disposed && entry is not null && entry.IsInFlight)
            {
                entry.MarkTransient(HttpStatusCode.ServiceUnavailable, CanReexecute(entry.Request.Operation));
                waiterDecision = entry.CreateCurrentDecision();
                DetachWaitersCore(entry, waiters);
            }
        }

        if (waiterDecision is null)
        {
            return default;
        }

        CompleteWaiters(waiters, waiterDecision);
        return default;
    }

    /// <summary>Creates a safe validation rejection.</summary>
    /// <param name="statusCode">The HTTP status code.</param>
    /// <param name="kind">The failure kind.</param>
    /// <returns>The replay decision.</returns>
    private static HttpReplayDecision CreateRejectDecision(HttpStatusCode statusCode, HttpTransportFailureKind kind) =>
        new() { Kind = HttpReplayAdmissionKind.Reject, Failure = new(statusCode, kind) };

    /// <summary>Creates a safe transient decision.</summary>
    /// <param name="statusCode">The HTTP status code.</param>
    /// <returns>The replay decision.</returns>
    private static HttpReplayDecision CreateTransientDecision(HttpStatusCode statusCode) =>
        new() { Kind = HttpReplayAdmissionKind.ReplayTransient, Failure = new(statusCode, HttpTransportFailureKind.Transient) };

    /// <summary>Creates a replay decision from an internal HTTP transport exception.</summary>
    /// <param name="exception">The transport exception.</param>
    /// <returns>The replay decision.</returns>
    private static HttpReplayDecision CreateTransportFailureDecision(HttpRemoteTransportException exception)
    {
        // Session verification supplies explicit statuses for validation and authentication failures. The statusless
        // internal path is disposed registry state, which is reported as a transient service outage.
        var statusCode = exception.StatusCode ?? HttpStatusCode.ServiceUnavailable;
        return exception.IsTransient
            ? CreateTransientDecision(statusCode)
            : CreateRejectDecision(statusCode, exception.Kind);
    }

    /// <summary>Checks whether an uncached operation may be reexecuted.</summary>
    /// <param name="operation">The replay operation.</param>
    /// <returns>Whether the operation may reexecute.</returns>
    private static bool CanReexecute(HttpReplayOperationKind operation) =>
        operation is HttpReplayOperationKind.Push or HttpReplayOperationKind.Acknowledge;

    /// <summary>Completes all detached waiters.</summary>
    /// <param name="waiters">The waiters.</param>
    /// <param name="decision">The replay decision.</param>
    private static void CompleteWaiters(IReadOnlyList<ReplayWaiter> waiters, HttpReplayDecision decision)
    {
        for (var index = 0; index < waiters.Count; index++)
        {
            waiters[index].SetResult(decision);
        }
    }

    /// <summary>Creates exact connect replay proof headers.</summary>
    /// <param name="session">The issued replay session.</param>
    /// <returns>The retained response headers.</returns>
    private static KeyValuePair<string, string>[] CreateConnectHeaders(HttpReplayIssuedSession? session) =>
        session is null
            ? []
            : [
            new(ReplaySessionIdHeader, session.SessionId),
            new(ReplaySessionSecretHeader, session.SessionSecret),
            new(ReplaySessionExpiresHeader, session.ExpiresAtUtc.ToString("O", CultureInfo.InvariantCulture)),
            ];

    /// <summary>Completes an entry whose response is too large to cache.</summary>
    /// <param name="entry">The retained entry.</param>
    /// <param name="completion">The owner completion.</param>
    private static void CompleteOversizedEntry(ReplayEntry entry, HttpReplayCompletion completion)
    {
        if (completion.FailedBeforeEffect)
        {
            entry.MarkRejected(HttpStatusCode.RequestEntityTooLarge, HttpTransportFailureKind.PayloadTooLarge);
            return;
        }

        entry.MarkTransient(HttpStatusCode.ServiceUnavailable, CanReexecute(entry.Request.Operation));
    }

    /// <summary>Gets the retained byte charge for a cached response representation.</summary>
    /// <param name="completion">The owner completion.</param>
    /// <param name="headers">The retained response headers.</param>
    /// <returns>The retained byte charge.</returns>
    private static long GetCachedResponseCharge(HttpReplayCompletion completion, KeyValuePair<string, string>[] headers)
    {
        var charge = (long)StatusCodeRetainedBytes + completion.ResponseBytes.Length;
        if (completion.ContentType is not null)
        {
            charge += Encoding.UTF8.GetByteCount(completion.ContentType);
        }

        for (var index = 0; index < headers.Length; index++)
        {
            charge += Encoding.UTF8.GetByteCount(headers[index].Key);
            charge += Encoding.UTF8.GetByteCount(headers[index].Value);
        }

        return charge;
    }

    /// <summary>Gets the retained byte charge for a replay entry.</summary>
    /// <param name="request">The replay request.</param>
    /// <param name="envelope">The replay envelope.</param>
    /// <returns>The retained byte charge.</returns>
    private static long GetEntryCharge(HttpReplayRequest request, HttpReplayEnvelope envelope) =>
        envelope.CanonicalByteCount
        + envelope.RequestHash.Length
        + envelope.MacInput.Length
        + envelope.EnvelopeFingerprint.Length
        + Encoding.UTF8.GetByteCount(request.Principal.TenantId)
        + Encoding.UTF8.GetByteCount(request.Principal.ClientId)
        + Encoding.UTF8.GetByteCount(request.MessageId)
        + Encoding.UTF8.GetByteCount(request.Nonce)
        + Encoding.UTF8.GetByteCount(envelope.ReplaySessionId)
        + Encoding.UTF8.GetByteCount(envelope.ReplayMac);

    /// <summary>Gets the later of two timestamps.</summary>
    /// <param name="left">The first timestamp.</param>
    /// <param name="right">The second timestamp.</param>
    /// <returns>The later timestamp.</returns>
    private static DateTimeOffset Max(DateTimeOffset left, DateTimeOffset right) => left >= right ? left : right;

    /// <summary>Gets the earlier of two timestamps.</summary>
    /// <param name="left">The first timestamp.</param>
    /// <param name="right">The second timestamp.</param>
    /// <returns>The earlier timestamp.</returns>
    private static DateTimeOffset Min(DateTimeOffset left, DateTimeOffset right) => left <= right ? left : right;

    /// <summary>Admits a new replay entry.</summary>
    /// <param name="request">The replay request.</param>
    /// <param name="envelope">The replay envelope.</param>
    /// <param name="observedUtc">The observed timestamp.</param>
    /// <param name="sessionExpiresAtUtc">The optional verified session expiry.</param>
    /// <returns>The replay decision.</returns>
    private HttpReplayDecision AdmitNewCore(
        HttpReplayRequest request,
        HttpReplayEnvelope envelope,
        DateTimeOffset observedUtc,
        DateTimeOffset? sessionExpiresAtUtc)
    {
        if (!IsFresh(request.SentAtUtc, observedUtc))
        {
            return CreateRejectDecision(HttpStatusCode.BadRequest, HttpTransportFailureKind.ValidationRejected);
        }

        RemoveExpiredCore(observedUtc, null);
        return _entries.Count < _options.MaximumEntries
            ? CreateExecutionOwnerCore(request, envelope, observedUtc, sessionExpiresAtUtc)
            : CreateTransientDecision(HttpTransportStatus.TooManyRequests);
    }

    /// <summary>Admits a retained entry.</summary>
    /// <param name="index">The retained entry index.</param>
    /// <param name="request">The replay request.</param>
    /// <param name="envelope">The replay envelope.</param>
    /// <param name="observedUtc">The observed timestamp.</param>
    /// <param name="waiter">The created waiter, when admission joins an in-flight owner.</param>
    /// <returns>The replay decision.</returns>
    private HttpReplayDecision AdmitExistingCore(
        int index,
        HttpReplayRequest request,
        HttpReplayEnvelope envelope,
        DateTimeOffset observedUtc,
        out ReplayWaiter? waiter)
    {
        waiter = null;
        var entry = _entries[index];
        if (observedUtc > entry.ExpiresAtUtc)
        {
            return AdmitExpiredExistingCore(index, request, observedUtc);
        }

        if (!IsFresh(request.SentAtUtc, observedUtc))
        {
            return CreateRejectDecision(HttpStatusCode.BadRequest, HttpTransportFailureKind.ValidationRejected);
        }

        if (!_hasher.FixedTimeEquals(entry.EnvelopeFingerprint, envelope.EnvelopeFingerprint))
        {
            return CreateRejectDecision(HttpStatusCode.Conflict, HttpTransportFailureKind.ValidationRejected);
        }

        if (entry.IsInFlight)
        {
            return TryCreateWaiterCore(entry, out waiter);
        }

        if (entry.HasCachedResponse)
        {
            return entry.CreateCurrentDecision();
        }

        if (entry.CanReexecute && CanReexecute(request.Operation))
        {
            entry.StartReexecution();
            return new() { Kind = HttpReplayAdmissionKind.Execute, Owner = new(this, entry.EntryId) };
        }

        return entry.FailureKind != HttpTransportFailureKind.Transient
            ? CreateRejectDecision(entry.TransientStatusCode, entry.FailureKind)
            : CreateTransientDecision(entry.TransientStatusCode);
    }

    /// <summary>Admits an expired retained entry.</summary>
    /// <param name="index">The retained entry index.</param>
    /// <param name="request">The replay request.</param>
    /// <param name="observedUtc">The observed timestamp.</param>
    /// <returns>The replay decision.</returns>
    private HttpReplayDecision AdmitExpiredExistingCore(int index, HttpReplayRequest request, DateTimeOffset observedUtc)
    {
        var entry = _entries[index];
        if (entry.IsInFlight)
        {
            return IsFresh(request.SentAtUtc, observedUtc)
                ? CreateTransientDecision(HttpTransportStatus.TooManyRequests)
                : CreateRejectDecision(HttpStatusCode.BadRequest, HttpTransportFailureKind.ValidationRejected);
        }

        RemoveEntryAtCore(index);
        return request.Operation == HttpReplayOperationKind.Connect && entry.HasConnectSession
            ? CreateRejectDecision(HttpStatusCode.Conflict, HttpTransportFailureKind.ValidationRejected)
            : CreateRejectDecision(HttpStatusCode.BadRequest, HttpTransportFailureKind.ValidationRejected);
    }

    /// <summary>Awaits an in-flight duplicate waiter with cancellation cleanup.</summary>
    /// <param name="waiter">The replay waiter.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The eventual replay decision.</returns>
    private async ValueTask<HttpReplayDecision> AwaitWaiterAsync(ReplayWaiter waiter, CancellationToken cancellationToken)
    {
        if (!cancellationToken.CanBeCanceled)
        {
            return await waiter.Task.ConfigureAwait(false);
        }

#if NET8_0_OR_GREATER
        await using var registration = cancellationToken.UnsafeRegister(_ => CancelWaiter(waiter, cancellationToken), null);
#else
        using var registration = cancellationToken.Register(_ => CancelWaiter(waiter, cancellationToken), null);
#endif
        return await waiter.Task.ConfigureAwait(false);
    }

    /// <summary>Cancels and removes a waiter from its retained entry.</summary>
    /// <param name="waiter">The waiter to cancel.</param>
    /// <param name="cancellationToken">The cancellation token that fired.</param>
    private void CancelWaiter(ReplayWaiter waiter, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            if (!waiter.Entry.RemoveWaiter(waiter))
            {
                return;
            }

            _activeReplayWaiters--;
        }

        waiter.SetCanceled(cancellationToken);
    }

    /// <summary>Completes a retained entry.</summary>
    /// <param name="entry">The retained entry.</param>
    /// <param name="completion">The owner completion.</param>
    /// <param name="registrationFailureStatus">The optional connect-session registration failure status.</param>
    private void CompleteEntryCore(
        ReplayEntry entry,
        HttpReplayCompletion completion,
        HttpStatusCode? registrationFailureStatus)
    {
        if (registrationFailureStatus is { } failureStatus)
        {
            entry.MarkTransient(failureStatus, false);
            return;
        }

        if (completion.ConnectSession is not null)
        {
            entry.ExtendExpiry(completion.ConnectSession.ExpiresAtUtc);
        }

        if (completion.ResponseBytes.Length > _options.MaximumCachedResponseBytes)
        {
            CompleteOversizedEntry(entry, completion);
            return;
        }

        var headers = CreateConnectHeaders(completion.ConnectSession);
        try
        {
            var cachedResponse = new HttpReplayCachedResponse(completion.StatusCode, completion.ContentType, completion.ResponseBytes, headers);
            using var responseLease = _budget.Reserve(GetCachedResponseCharge(completion, headers));
            entry.StoreCachedResponse(
                cachedResponse,
                responseLease.Transfer(),
                completion.ConnectSession is not null);
        }
        catch (HttpRemoteTransportException)
        {
            entry.MarkTransient(HttpStatusCode.ServiceUnavailable, CanReexecute(entry.Request.Operation));
        }
    }

    /// <summary>Creates first-execution ownership for an admitted entry.</summary>
    /// <param name="request">The replay request.</param>
    /// <param name="envelope">The replay envelope.</param>
    /// <param name="observedUtc">The observed timestamp.</param>
    /// <param name="sessionExpiresAtUtc">The optional verified session expiry.</param>
    /// <returns>The replay decision.</returns>
    private HttpReplayDecision CreateExecutionOwnerCore(
        HttpReplayRequest request,
        HttpReplayEnvelope envelope,
        DateTimeOffset observedUtc,
        DateTimeOffset? sessionExpiresAtUtc)
    {
        try
        {
            using var lease = _budget.Reserve(GetEntryCharge(request, envelope));
            var entryId = ++_nextEntryId;
            var expiresAtUtc = CreateEntryExpiry(request.SentAtUtc, observedUtc, sessionExpiresAtUtc);
            var owner = new HttpReplayOwner(this, entryId);
            _entries.Add(new(entryId, request, envelope.EnvelopeFingerprint, expiresAtUtc, lease.Transfer()));
            return new() { Kind = HttpReplayAdmissionKind.Execute, Owner = owner };
        }
        catch (HttpRemoteTransportException)
        {
            return CreateTransientDecision(HttpTransportStatus.TooManyRequests);
        }
    }

    /// <summary>Creates the inclusive retention expiry for a replay entry.</summary>
    /// <param name="sentAtUtc">The request timestamp.</param>
    /// <param name="observedUtc">The observed timestamp.</param>
    /// <param name="sessionExpiresAtUtc">The optional verified session expiry.</param>
    /// <returns>The inclusive expiry timestamp.</returns>
    private DateTimeOffset CreateEntryExpiry(DateTimeOffset sentAtUtc, DateTimeOffset observedUtc, DateTimeOffset? sessionExpiresAtUtc)
    {
        var nonceExpiry = HttpReplaySessionRegistry.AddChecked(observedUtc, _options.NonceRetention, nameof(observedUtc));
        var freshnessExpiry = HttpReplaySessionRegistry.AddChecked(sentAtUtc, _options.FreshnessWindow, nameof(sentAtUtc));
        var expiry = Max(nonceExpiry, freshnessExpiry);
        if (sessionExpiresAtUtc is { } sessionExpiry)
        {
            expiry = Min(expiry, sessionExpiry);
        }

        return expiry;
    }

    /// <summary>Creates authorization or session-proof failure before retained replay state is touched.</summary>
    /// <param name="request">The replay request.</param>
    /// <param name="envelope">The replay envelope.</param>
    /// <param name="authorization">The current authorization result.</param>
    /// <param name="observedUtc">The observed timestamp.</param>
    /// <param name="sessionProof">The verified session proof, when one is required.</param>
    /// <returns>The failure decision, or null when admission can continue.</returns>
    private HttpReplayDecision? CreatePreAdmissionFailureDecision(
        HttpReplayRequest request,
        HttpReplayEnvelope envelope,
        HttpReplayAuthorizationResult authorization,
        DateTimeOffset observedUtc,
        out HttpReplaySessionProof? sessionProof)
    {
        sessionProof = null;
        if (!authorization.IsAuthorized)
        {
            return new() { Kind = HttpReplayAdmissionKind.Reject, Failure = authorization.Failure ?? new(HttpStatusCode.Forbidden, HttpTransportFailureKind.AuthorizationDenied) };
        }

        if (request.Operation == HttpReplayOperationKind.Connect)
        {
            return null;
        }

        try
        {
            sessionProof = VerifyReplaySession(request, envelope, observedUtc);
            return null;
        }
        catch (HttpRemoteTransportException exception)
        {
            return CreateTransportFailureDecision(exception);
        }
    }

    /// <summary>Detaches waiters from an entry and updates the active waiter count.</summary>
    /// <param name="entry">The replay entry.</param>
    /// <param name="waiters">The destination waiter list.</param>
    private void DetachWaitersCore(ReplayEntry entry, List<ReplayWaiter> waiters)
    {
        var detached = entry.DetachWaiters();
        _activeReplayWaiters -= detached.Length;
        for (var index = 0; index < detached.Length; index++)
        {
            waiters.Add(detached[index]);
        }
    }

    /// <summary>Finds a retained entry by owner identifier.</summary>
    /// <param name="entryId">The owner entry identifier.</param>
    /// <returns>The retained entry, when found.</returns>
    private ReplayEntry? FindEntryByIdCore(long entryId)
    {
        for (var index = 0; index < _entries.Count; index++)
        {
            if (_entries[index].EntryId == entryId)
            {
                return _entries[index];
            }
        }

        return null;
    }

    /// <summary>Finds a retained entry by replay key.</summary>
    /// <param name="request">The replay request.</param>
    /// <returns>The retained entry index, or -1 when not found.</returns>
    private int FindEntryIndexCore(HttpReplayRequest request)
    {
        for (var index = 0; index < _entries.Count; index++)
        {
            if (_entries[index].Matches(request))
            {
                return index;
            }
        }

        return -1;
    }

    /// <summary>Checks inclusive replay freshness.</summary>
    /// <param name="sentAtUtc">The request timestamp.</param>
    /// <param name="observedUtc">The observed timestamp.</param>
    /// <returns>Whether the timestamp is fresh.</returns>
    private bool IsFresh(DateTimeOffset sentAtUtc, DateTimeOffset observedUtc)
    {
        var earliest = observedUtc.Add(-_options.FreshnessWindow);
        var latest = HttpReplaySessionRegistry.AddChecked(observedUtc, _options.FreshnessWindow, nameof(observedUtc));
        return sentAtUtc >= earliest && sentAtUtc <= latest;
    }

    /// <summary>Observes monotonic high-water UTC time.</summary>
    /// <returns>The monotonic observed time.</returns>
    private DateTimeOffset ObserveHighWater()
    {
        var observedUtc = _options.TimeProvider.GetUtcNow();
        lock (_gate)
        {
            if (observedUtc > _highWaterUtc)
            {
                _highWaterUtc = observedUtc;
            }

            return _highWaterUtc;
        }
    }

    /// <summary>Registers an issued connect session before publishing a connect replay response.</summary>
    /// <param name="owner">The replay owner.</param>
    /// <param name="completion">The owner completion.</param>
    /// <returns>The failure status, when registration fails.</returns>
    private HttpStatusCode? RegisterConnectSession(HttpReplayOwner owner, HttpReplayCompletion completion)
    {
        if (completion.ConnectSession is null)
        {
            return null;
        }

        ReplayEntry? entry;
        lock (_gate)
        {
            entry = !_disposed && owner.IsOwnedBy(this) ? FindEntryByIdCore(owner.EntryId) : null;
        }

        if (entry?.Request.Operation != HttpReplayOperationKind.Connect)
        {
            return null;
        }

        try
        {
            _sessions.RegisterIssued(entry.Request.Principal, completion.ConnectSession, ObserveHighWater());
            return null;
        }
        catch (HttpRemoteTransportException)
        {
            return HttpStatusCode.ServiceUnavailable;
        }
    }

    /// <summary>Removes expired retained entries.</summary>
    /// <param name="observedUtc">The observed timestamp.</param>
    /// <param name="excluded">The excluded index.</param>
    private void RemoveExpiredCore(DateTimeOffset observedUtc, int? excluded)
    {
        for (var index = _entries.Count - 1; index >= 0; index--)
        {
            if (excluded == index || _entries[index].IsInFlight || observedUtc <= _entries[index].ExpiresAtUtc)
            {
                continue;
            }

            RemoveEntryAtCore(index);
        }
    }

    /// <summary>Removes a retained entry.</summary>
    /// <param name="index">The entry index.</param>
    private void RemoveEntryAtCore(int index)
    {
        _entries[index].Dispose();
        _entries.RemoveAt(index);
    }

    /// <summary>Creates an in-flight waiter when capacity allows it.</summary>
    /// <param name="entry">The retained in-flight entry.</param>
    /// <param name="waiter">The created waiter.</param>
    /// <returns>The replay decision when no waiter could be created.</returns>
    private HttpReplayDecision TryCreateWaiterCore(ReplayEntry entry, out ReplayWaiter? waiter)
    {
        waiter = null;
        if (_activeReplayWaiters >= _options.MaximumActiveReplayWaiters)
        {
            return CreateTransientDecision(HttpTransportStatus.TooManyRequests);
        }

        waiter = new(entry);
        entry.AddWaiter(waiter);
        _activeReplayWaiters++;
        return CreateTransientDecision(HttpStatusCode.ServiceUnavailable);
    }

    /// <summary>Verifies a replay session for non-connect operations.</summary>
    /// <param name="request">The replay request.</param>
    /// <param name="envelope">The replay envelope.</param>
    /// <param name="observedUtc">The observed timestamp.</param>
    /// <returns>The verified session proof.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private HttpReplaySessionProof VerifyReplaySession(HttpReplayRequest request, HttpReplayEnvelope envelope, DateTimeOffset observedUtc) =>
        _sessions.Verify(request.Principal, envelope.ReplaySessionId, envelope.MacInput, envelope.ReplayMac, observedUtc);

    /// <summary>Represents one retained replay nonce entry.</summary>
    private sealed class ReplayEntry : IDisposable
    {
        /// <summary>The retained byte budget lease.</summary>
        private readonly IDisposable _lease;

        /// <summary>The in-flight duplicate waiters.</summary>
        private readonly List<ReplayWaiter> _waiters = [];

        /// <summary>The retained response budget lease.</summary>
        private IDisposable? _responseLease;

        /// <summary>The cached response when one is available.</summary>
        private HttpReplayCachedResponse? _cachedResponse;

        /// <summary>Initializes a new instance of the <see cref="ReplayEntry"/> class.</summary>
        /// <param name="entryId">The entry identifier.</param>
        /// <param name="request">The replay request.</param>
        /// <param name="envelopeFingerprint">The replay envelope fingerprint.</param>
        /// <param name="expiresAtUtc">The inclusive expiry timestamp.</param>
        /// <param name="lease">The retained entry byte lease.</param>
        internal ReplayEntry(
            long entryId,
            HttpReplayRequest request,
            ReadOnlyMemory<byte> envelopeFingerprint,
            DateTimeOffset expiresAtUtc,
            IDisposable lease)
        {
            EntryId = entryId;
            Request = request;
            EnvelopeFingerprint = Copy(envelopeFingerprint);
            ExpiresAtUtc = expiresAtUtc;
            _lease = lease;
            IsInFlight = true;
            FailureKind = HttpTransportFailureKind.Transient;
            TransientStatusCode = HttpStatusCode.ServiceUnavailable;
        }

        /// <summary>Gets whether this entry can issue one reexecution owner.</summary>
        internal bool CanReexecute { get; private set; }

        /// <summary>Gets the entry identifier.</summary>
        internal long EntryId { get; }

        /// <summary>Gets the replay envelope fingerprint.</summary>
        internal ReadOnlyMemory<byte> EnvelopeFingerprint { get; }

        /// <summary>Gets the inclusive expiry timestamp.</summary>
        internal DateTimeOffset ExpiresAtUtc { get; private set; }

        /// <summary>Gets the failure kind associated with uncached state.</summary>
        internal HttpTransportFailureKind FailureKind { get; private set; }

        /// <summary>Gets whether this connect entry issued a retained replay session.</summary>
        internal bool HasConnectSession { get; private set; }

        /// <summary>Gets whether this entry has a retained cached response.</summary>
        internal bool HasCachedResponse => _cachedResponse is not null;

        /// <summary>Gets whether this entry already issued its single reexecution owner.</summary>
        internal bool HasReexecuted { get; private set; }

        /// <summary>Gets whether the first execution owner is still in flight.</summary>
        internal bool IsInFlight { get; private set; }

        /// <summary>Gets the original replay request.</summary>
        internal HttpReplayRequest Request { get; }

        /// <summary>Gets the transient replay status code.</summary>
        internal HttpStatusCode TransientStatusCode { get; private set; }

        /// <inheritdoc />
        public void Dispose()
        {
            ClearCachedResponse();
            _lease.Dispose();
        }

        /// <summary>Adds a duplicate waiter.</summary>
        /// <param name="waiter">The waiter to add.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void AddWaiter(ReplayWaiter waiter) => _waiters.Add(waiter);

        /// <summary>Creates a replay decision for the current completed state.</summary>
        /// <returns>The replay decision.</returns>
        internal HttpReplayDecision CreateCurrentDecision()
        {
            if (_cachedResponse is not null)
            {
                return new() { Kind = HttpReplayAdmissionKind.ReplayCached, CachedResponse = _cachedResponse.CreateSnapshot() };
            }

            return FailureKind != HttpTransportFailureKind.Transient
                ? CreateRejectDecision(TransientStatusCode, FailureKind)
                : CreateTransientDecision(TransientStatusCode);
        }

        /// <summary>Detaches all waiters.</summary>
        /// <returns>The detached waiters.</returns>
        internal ReplayWaiter[] DetachWaiters()
        {
            if (_waiters.Count == 0)
            {
                return [];
            }

            var waiters = _waiters.ToArray();
            _waiters.Clear();
            return waiters;
        }

        /// <summary>Extends the entry expiry to cover an issued connect session.</summary>
        /// <param name="expiresAtUtc">The session expiry.</param>
        internal void ExtendExpiry(DateTimeOffset expiresAtUtc)
        {
            if (expiresAtUtc <= ExpiresAtUtc)
            {
                return;
            }

            ExpiresAtUtc = expiresAtUtc;
        }

        /// <summary>Marks this entry as a pre-effect rejection.</summary>
        /// <param name="statusCode">The rejection status code.</param>
        /// <param name="kind">The failure kind.</param>
        internal void MarkRejected(HttpStatusCode statusCode, HttpTransportFailureKind kind)
        {
            ClearCachedResponse();
            IsInFlight = false;
            CanReexecute = false;
            TransientStatusCode = statusCode;
            FailureKind = kind;
        }

        /// <summary>Marks this entry as an uncached transient replay.</summary>
        /// <param name="statusCode">The transient status code.</param>
        /// <param name="canReexecute">Whether one safe reexecution is available.</param>
        internal void MarkTransient(HttpStatusCode statusCode, bool canReexecute)
        {
            ClearCachedResponse();
            IsInFlight = false;
            CanReexecute = canReexecute && !HasReexecuted;
            TransientStatusCode = statusCode;
            FailureKind = HttpTransportFailureKind.Transient;
        }

        /// <summary>Checks whether this entry matches a replay request key.</summary>
        /// <param name="request">The replay request.</param>
        /// <returns>Whether this entry matches the key.</returns>
        internal bool Matches(HttpReplayRequest request) =>
            Request.Operation == request.Operation
            && string.Equals(Request.Principal.TenantId, request.Principal.TenantId, StringComparison.Ordinal)
            && string.Equals(Request.Principal.ClientId, request.Principal.ClientId, StringComparison.Ordinal)
            && string.Equals(Request.MessageId, request.MessageId, StringComparison.Ordinal)
            && string.Equals(Request.Nonce, request.Nonce, StringComparison.Ordinal);

        /// <summary>Removes a duplicate waiter.</summary>
        /// <param name="waiter">The waiter to remove.</param>
        /// <returns>Whether the waiter was removed.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool RemoveWaiter(ReplayWaiter waiter) => _waiters.Remove(waiter);

        /// <summary>Starts a single safe reexecution.</summary>
        internal void StartReexecution()
        {
            ClearCachedResponse();
            IsInFlight = true;
            CanReexecute = false;
            HasReexecuted = true;
            FailureKind = HttpTransportFailureKind.Transient;
            TransientStatusCode = HttpStatusCode.ServiceUnavailable;
        }

        /// <summary>Stores a cached response for byte-identical replay.</summary>
        /// <param name="response">The cached response.</param>
        /// <param name="responseLease">The response byte lease.</param>
        /// <param name="hasConnectSession">Whether the response includes connect proof headers.</param>
        internal void StoreCachedResponse(HttpReplayCachedResponse response, IDisposable responseLease, bool hasConnectSession)
        {
            ClearCachedResponse();
            _responseLease = responseLease;
            _cachedResponse = response;
            HasConnectSession = hasConnectSession;
            IsInFlight = false;
            CanReexecute = false;
            FailureKind = HttpTransportFailureKind.Transient;
            TransientStatusCode = HttpStatusCode.ServiceUnavailable;
        }

        /// <summary>Copies memory into an owned byte array.</summary>
        /// <param name="source">The source bytes.</param>
        /// <returns>The copied bytes.</returns>
        private static byte[] Copy(ReadOnlyMemory<byte> source)
        {
            var copy = new byte[source.Length];
            source.CopyTo(copy);
            return copy;
        }

        /// <summary>Clears cached response state and releases its budget lease.</summary>
        private void ClearCachedResponse()
        {
            _cachedResponse?.Clear();
            _responseLease?.Dispose();
            _responseLease = null;
            _cachedResponse = null;
        }
    }

    /// <summary>Represents one in-flight duplicate waiter.</summary>
    /// <param name="entry">The retained entry.</param>
    private sealed class ReplayWaiter(ReplayEntry entry)
    {
        /// <summary>The asynchronous completion source.</summary>
        private readonly TaskCompletionSource<HttpReplayDecision> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the owning retained entry.</summary>
        internal ReplayEntry Entry { get; } = entry;

        /// <summary>Gets the completion task.</summary>
        internal Task<HttpReplayDecision> Task => _completion.Task;

        /// <summary>Cancels this waiter.</summary>
        /// <param name="cancellationToken">The cancellation token that fired.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetCanceled(CancellationToken cancellationToken) => _completion.TrySetCanceled(cancellationToken);

        /// <summary>Completes this waiter.</summary>
        /// <param name="decision">The replay decision.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetResult(HttpReplayDecision decision) => _completion.TrySetResult(decision);
    }
}
