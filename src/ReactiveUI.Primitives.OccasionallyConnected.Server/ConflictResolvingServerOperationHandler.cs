// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Adapts public conflict/domain contracts to the internal server operation processor.</summary>
internal sealed class ConflictResolvingServerOperationHandler : IServerOperationHandler
{
    /// <summary>The configured streams.</summary>
    private readonly Dictionary<StreamId, ServerConflictStreamRegistration> _streams;

    /// <summary>The maximum event proposals retained for one operation.</summary>
    private readonly int _maximumProducedEvents;

    /// <summary>Initializes a new instance of the <see cref="ConflictResolvingServerOperationHandler"/> class.</summary>
    /// <param name="options">The conflict handler options.</param>
    internal ConflictResolvingServerOperationHandler(ServerConflictHandlerOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        options.Validate();
        var streams = new Dictionary<StreamId, ServerConflictStreamRegistration>();
        for (var index = 0; index < options.Streams.Count; index++)
        {
            var registration = options.Streams[index];
            streams.Add(registration.StreamId, registration);
        }

        _streams = streams;
        _maximumProducedEvents = options.MaximumProducedEvents;
    }

    /// <inheritdoc/>
    public async ValueTask<ServerOperationPreparation> PrepareAsync(
        ServerOperationContext context,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(context);
        cancellationToken.ThrowIfCancellationRequested();
        if (!_streams.TryGetValue(context.Operation.StreamId, out var registration))
        {
            return Rejected(context.Operation.OperationId, "unregistered-stream");
        }

        var current = await ResolveCurrentStateAsync(context, registration, cancellationToken).ConfigureAwait(false);
        var conflict = CreateConflictContext(context, current);
        var resolution = await ResolveAsync(context, registration, conflict, cancellationToken).ConfigureAwait(false);
        var rejected = ValidateResolution(context.Operation, resolution);
        if (rejected is not null)
        {
            return Rejected(context.Operation.OperationId, rejected.ReasonCode, resolution.ServerVersion);
        }

        var domain = await registration.DomainHandler.ApplyAsync(
            new() { Client = context.Client, Operation = context.Operation, Conflict = conflict, Resolution = resolution },
            cancellationToken).ConfigureAwait(false);

        ValidateDomainResult(context, resolution, domain);
        return Accepted(context.Operation.OperationId, resolution, domain, _maximumProducedEvents);
    }

    /// <summary>Creates a rejected preparation without durable effects.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="reasonCode">The stable reason code.</param>
    /// <param name="serverVersion">The optional server version to return with the rejection.</param>
    /// <returns>The rejected preparation.</returns>
    private static ServerOperationPreparation Rejected(OperationId operationId, string reasonCode, string? serverVersion = null) =>
        new(new(operationId, OperationResultKind.Rejected, reasonCode, serverVersion), null, [], []);

    /// <summary>Creates an accepted or conflict preparation.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="resolution">The validated resolution.</param>
    /// <param name="domain">The validated domain result.</param>
    /// <param name="maximumProducedEvents">The maximum event proposals to retain.</param>
    /// <returns>The server operation preparation.</returns>
    private static ServerOperationPreparation Accepted(
        OperationId operationId,
        ConflictResolutionResult resolution,
        ServerDomainApplyResult domain,
        int maximumProducedEvents)
    {
        var kind = resolution.Conflicts.Count == 0 ? OperationResultKind.Accepted : OperationResultKind.Conflict;
        var reasonCode = kind == OperationResultKind.Conflict ? resolution.Conflicts[0].ResolutionCode : null;
        return new(
            new(operationId, kind, reasonCode, resolution.ServerVersion),
            domain.NewState,
            resolution.Conflicts,
            CaptureEvents(resolution.ProducedEvents, domain.Events, maximumProducedEvents));
    }

    /// <summary>Copies resolver and domain event proposals into internal prepared events.</summary>
    /// <param name="resolverEvents">The resolver event proposals.</param>
    /// <param name="domainEvents">The domain event proposals.</param>
    /// <param name="maximumProducedEvents">The maximum event proposals to retain.</param>
    /// <returns>The prepared events.</returns>
    private static ReadOnlyCollection<ServerPreparedEvent> CaptureEvents(
        IReadOnlyList<RemoteEvent> resolverEvents,
        IReadOnlyList<ServerProducedEvent> domainEvents,
        int maximumProducedEvents)
    {
        ArgumentExceptionHelper.ThrowIfNull(resolverEvents);
        ArgumentExceptionHelper.ThrowIfNull(domainEvents);
        var eventCount = CountProducedEvents(resolverEvents.Count, domainEvents.Count, maximumProducedEvents);
        var events = new ServerPreparedEvent[eventCount];
        for (var index = 0; index < resolverEvents.Count; index++)
        {
            var produced = resolverEvents[index];
            ArgumentExceptionHelper.ThrowIfNull(produced, nameof(resolverEvents));
            events[index] = new(produced.EventId, produced.Payload, produced.Metadata);
        }

        for (var index = 0; index < domainEvents.Count; index++)
        {
            var produced = domainEvents[index];
            ArgumentExceptionHelper.ThrowIfNull(produced, nameof(domainEvents));
            events[resolverEvents.Count + index] = new(produced.EventId, produced.Payload, produced.Metadata);
        }

        return Array.AsReadOnly(events);
    }

    /// <summary>Creates the public conflict context with trusted server provenance.</summary>
    /// <param name="context">The internal server context.</param>
    /// <param name="current">The current server state.</param>
    /// <returns>The conflict context.</returns>
    private static ConflictContext CreateConflictContext(ServerOperationContext context, ServerState current) =>
        new(
            current,
            [context.Operation],
            context.Client,
            new()
            {
                CandidateWrite = ToConflictWriteStamp(context.CandidateWrite),
                CurrentWrite = context.Snapshot.LastWriteStamp.HasValue ? ToConflictWriteStamp(context.Snapshot.LastWriteStamp.Value) : null,
            });

    /// <summary>Maps an internal write stamp to public conflict provenance.</summary>
    /// <param name="stamp">The internal write stamp.</param>
    /// <returns>The public write stamp.</returns>
    private static ConflictWriteStamp ToConflictWriteStamp(in ServerWriteStamp stamp) =>
        new() { CommittedAtUtc = stamp.CommittedAtUtc, ClientId = stamp.ClientId, OperationId = stamp.OperationId };

    /// <summary>Validates a resolver result and returns the rejected operation when present.</summary>
    /// <param name="operation">The operation.</param>
    /// <param name="resolution">The resolver result.</param>
    /// <returns>The rejected operation, or null for accepted/conflict results.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="resolution"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">A resolver field contains invalid text.</exception>
    /// <exception cref="InvalidOperationException">The resolution does not decide the operation exactly once.</exception>
    private static RejectedOperation? ValidateResolution(SyncOperation operation, ConflictResolutionResult resolution)
    {
        ArgumentExceptionHelper.ThrowIfNull(resolution);
        ServerCommitJournalGuard.ValidateText(resolution.ServerVersion, nameof(resolution.ServerVersion));
        var acceptedCount = CountAccepted(operation.OperationId, resolution.AcceptedOperations);
        var rejected = GetRejected(operation.OperationId, resolution.RejectedOperations);
        if (acceptedCount + (rejected is null ? 0 : 1) == 1)
        {
            ValidateResolutionEffects(operation, resolution, rejected is not null);
            return rejected;
        }

        throw new InvalidOperationException("A conflict resolution must decide the operation exactly once.");
    }

    /// <summary>Counts accepted entries for one operation while rejecting foreign entries.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="acceptedOperations">The accepted operation list.</param>
    /// <returns>The accepted operation count.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="acceptedOperations"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">The accepted operation list contains a foreign operation.</exception>
    private static int CountAccepted(
        OperationId operationId,
        IReadOnlyList<OperationId> acceptedOperations)
    {
        ArgumentExceptionHelper.ThrowIfNull(acceptedOperations);
        var count = 0;
        for (var index = 0; index < acceptedOperations.Count; index++)
        {
            if (acceptedOperations[index] != operationId)
            {
                throw new InvalidOperationException("A conflict resolution cannot decide a foreign operation.");
            }

            count++;
        }

        return count;
    }

    /// <summary>Gets the rejected operation for this operation while rejecting foreign entries.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="rejectedOperations">The rejected operation list.</param>
    /// <returns>The rejected operation, or null when not rejected.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="rejectedOperations"/> or one of its entries is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">A rejected operation reason contains invalid text.</exception>
    /// <exception cref="InvalidOperationException">The rejected operation list contains a foreign or duplicate operation.</exception>
    private static RejectedOperation? GetRejected(
        OperationId operationId,
        IReadOnlyList<RejectedOperation> rejectedOperations)
    {
        ArgumentExceptionHelper.ThrowIfNull(rejectedOperations);
        RejectedOperation? rejected = null;
        for (var index = 0; index < rejectedOperations.Count; index++)
        {
            var candidate = rejectedOperations[index];
            ArgumentExceptionHelper.ThrowIfNull(candidate, nameof(rejectedOperations));
            if (candidate.OperationId != operationId || rejected is not null)
            {
                throw new InvalidOperationException("A conflict resolution cannot reject a foreign or duplicate operation.");
            }

            ServerCommitJournalGuard.ValidateText(candidate.ReasonCode, nameof(candidate.ReasonCode));
            rejected = candidate;
        }

        return rejected;
    }

    /// <summary>Validates resolver effects that may later be retained.</summary>
    /// <param name="operation">The operation.</param>
    /// <param name="resolution">The resolver result.</param>
    /// <param name="isRejected">Whether the resolver rejected the operation.</param>
    /// <exception cref="InvalidOperationException">A rejected resolution contains durable effects.</exception>
    private static void ValidateResolutionEffects(
        SyncOperation operation,
        ConflictResolutionResult resolution,
        bool isRejected)
    {
        if (isRejected && (resolution.Conflicts.Count != 0 || resolution.ProducedEvents.Count != 0))
        {
            throw new InvalidOperationException("A rejected conflict resolution cannot produce durable conflict or event effects.");
        }

        ValidateConflicts(operation.OperationId, resolution.Conflicts);
        ValidateResolverEvents(operation, resolution.ProducedEvents);
    }

    /// <summary>Validates resolver conflicts.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="conflicts">The resolved conflicts.</param>
    /// <exception cref="ArgumentNullException">A conflict entry is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">A conflict resolution code contains invalid text.</exception>
    /// <exception cref="InvalidOperationException">A conflict belongs to a different operation.</exception>
    private static void ValidateConflicts(
        OperationId operationId,
        IReadOnlyList<ResolvedConflict> conflicts)
    {
        for (var index = 0; index < conflicts.Count; index++)
        {
            var conflict = conflicts[index];
            ArgumentExceptionHelper.ThrowIfNull(conflict, nameof(conflicts));
            if (conflict.OperationId != operationId)
            {
                throw new InvalidOperationException("A resolver conflict must match the resolved operation.");
            }

            ServerCommitJournalGuard.ValidateText(conflict.ResolutionCode, nameof(conflict.ResolutionCode));
        }
    }

    /// <summary>Validates resolver event proposals.</summary>
    /// <param name="operation">The operation.</param>
    /// <param name="producedEvents">The resolver event proposals.</param>
    /// <exception cref="ArgumentNullException">An event entry is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">An event stream or cause belongs to a different operation.</exception>
    /// <remarks>
    /// The event identifier, payload, metadata, stream, and optional cause are validated. The proposal's cursor, commit timestamp, and origin are
    /// ignored; the server stamps authoritative values after journal admission.
    /// </remarks>
    private static void ValidateResolverEvents(
        SyncOperation operation,
        IReadOnlyList<RemoteEvent> producedEvents)
    {
        for (var index = 0; index < producedEvents.Count; index++)
        {
            var produced = producedEvents[index];
            ArgumentExceptionHelper.ThrowIfNull(produced, nameof(producedEvents));
            if (produced.StreamId != operation.StreamId)
            {
                throw new InvalidOperationException("A resolver event stream must match the resolved operation stream.");
            }

            if (produced.CausedByOperationId.HasValue && produced.CausedByOperationId.Value != operation.OperationId)
            {
                throw new InvalidOperationException("A resolver event cause must match the resolved operation.");
            }
        }
    }

    /// <summary>Counts event proposals before retaining them.</summary>
    /// <param name="resolverEventCount">The resolver event count.</param>
    /// <param name="domainEventCount">The domain event count.</param>
    /// <param name="maximumProducedEvents">The maximum supported count.</param>
    /// <returns>The total event proposal count.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The event proposal count is outside the supported bounds.</exception>
    private static int CountProducedEvents(
        int resolverEventCount,
        int domainEventCount,
        int maximumProducedEvents)
    {
        if (resolverEventCount < 0 || resolverEventCount > maximumProducedEvents)
        {
            throw new ArgumentOutOfRangeException(nameof(resolverEventCount), resolverEventCount, "The resolver event proposal count is outside the supported bounds.");
        }

        var remaining = maximumProducedEvents - resolverEventCount;
        if (domainEventCount >= 0 && domainEventCount <= remaining)
        {
            return resolverEventCount + domainEventCount;
        }

        throw new ArgumentOutOfRangeException(nameof(domainEventCount), domainEventCount, "The domain event proposal count is outside the supported bounds.");
    }

    /// <summary>Validates a domain result before conversion to a preparation.</summary>
    /// <param name="context">The internal operation context.</param>
    /// <param name="resolution">The resolver result.</param>
    /// <param name="domain">The domain result.</param>
    /// <exception cref="ArgumentNullException"><paramref name="domain"/> or its state is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException">The domain state does not match the operation or resolved version.</exception>
    private static void ValidateDomainResult(
        ServerOperationContext context,
        ConflictResolutionResult resolution,
        ServerDomainApplyResult domain)
    {
        ArgumentExceptionHelper.ThrowIfNull(domain);
        ArgumentExceptionHelper.ThrowIfNull(domain.NewState);
        if (domain.NewState.StreamId != context.Operation.StreamId)
        {
            throw new InvalidOperationException("A domain result state must belong to the operation stream.");
        }

        if (StringComparer.Ordinal.Equals(domain.NewState.Version, resolution.ServerVersion))
        {
            return;
        }

        throw new InvalidOperationException("A domain result state version must match the conflict resolution version.");
    }

    /// <summary>Resolves the current state for the operation stream.</summary>
    /// <param name="context">The server operation context.</param>
    /// <param name="registration">The stream registration.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The current server state.</returns>
    /// <exception cref="ArgumentNullException">The initial state factory returned <see langword="null"/>.</exception>
    /// <exception cref="ArgumentException">The initial state version contains invalid text.</exception>
    /// <exception cref="InvalidOperationException">The initial state belongs to a different stream.</exception>
    private static async ValueTask<ServerState> ResolveCurrentStateAsync(
        ServerOperationContext context,
        ServerConflictStreamRegistration registration,
        CancellationToken cancellationToken)
    {
        if (context.Snapshot.State is { } state)
        {
            return state;
        }

        var initial = await registration.InitialStateFactory.CreateInitialStateAsync(context.Operation.StreamId, cancellationToken).ConfigureAwait(false);
        ArgumentExceptionHelper.ThrowIfNull(initial);
        if (initial.StreamId != context.Operation.StreamId)
        {
            throw new InvalidOperationException("Initial state must belong to the operation stream.");
        }

        ServerCommitJournalGuard.ValidateText(initial.Version, nameof(initial.Version));
        return initial;
    }

    /// <summary>Runs the resolver selected by the operation policy for every base-version shape.</summary>
    /// <param name="context">The server operation context.</param>
    /// <param name="registration">The stream registration.</param>
    /// <param name="conflict">The public conflict context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The conflict resolution result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ValueTask<ConflictResolutionResult> ResolveAsync(
        ServerOperationContext context,
        ServerConflictStreamRegistration registration,
        ConflictContext conflict,
        CancellationToken cancellationToken) =>
        SelectResolver(context.Operation.Policy.ConflictPolicy, registration).ResolveAsync(conflict, cancellationToken);

    /// <summary>Selects the resolver for a conflict policy.</summary>
    /// <param name="policy">The conflict policy.</param>
    /// <param name="registration">The stream registration.</param>
    /// <returns>The selected resolver.</returns>
    /// <exception cref="InvalidOperationException">The policy is not supported.</exception>
    private static IConflictResolver SelectResolver(
        ConflictPolicy policy,
        ServerConflictStreamRegistration registration) =>
        policy switch
        {
            ConflictPolicy.LastWriterWins => registration.LastWriterWinsResolver,
            ConflictPolicy.Merge => registration.MergeResolver,
            ConflictPolicy.Custom => registration.CustomResolver,
            _ => throw new InvalidOperationException("The operation conflict policy is not supported."),
        };
}
