// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Applies accepted CRDT conflict decisions to server state and authoritative events.</summary>
[System.Diagnostics.DebuggerDisplay("{_options.Kind,nq}")]
public sealed class CrdtServerDomainHandler : IServerDomainHandler
{
    /// <summary>The handler options.</summary>
    private readonly CrdtServerDomainHandlerOptions _options;

    /// <summary>Initializes a new instance of the <see cref="CrdtServerDomainHandler"/> class.</summary>
    /// <param name="options">The handler options.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    public CrdtServerDomainHandler(CrdtServerDomainHandlerOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        CrdtServerGuards.ValidateKind(options.Kind, nameof(options.Kind));
        ArgumentExceptionHelper.ThrowIfNull(options.Bounds);
        options.Bounds.Validate();
        _options = options;
    }

    /// <inheritdoc/>
    public ValueTask<ServerDomainApplyResult> ApplyAsync(
        ServerDomainApplyContext context,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(context);
        cancellationToken.ThrowIfCancellationRequested();
        var conflict = FindResolvedConflict(context);
        if (conflict.ResolvedPayload is not { } payload)
        {
            throw new InvalidOperationException("The CRDT resolved state payload is missing.");
        }

        if (!CrdtServerPayloads.TryDecodeState(payload, _options.Kind, _options.Bounds, out var state, out var reason))
        {
            throw new InvalidOperationException($"The CRDT resolved state payload is invalid: {reason}");
        }

        var authoritative = CrdtServerPayloads.CreateInput(CrdtInput.ForAuthoritativeState(state), _options.Bounds);
        return new(new ServerDomainApplyResult
        {
            NewState = new(context.Operation.StreamId, context.Resolution.ServerVersion, payload),
            Events = [new() { EventId = Guid.NewGuid(), Payload = authoritative }],
        });
    }

    /// <summary>Finds the single CRDT merge conflict for the operation.</summary>
    /// <param name="context">The domain context.</param>
    /// <returns>The resolved conflict.</returns>
    /// <exception cref="InvalidOperationException">The resolution does not contain the expected CRDT merge conflict.</exception>
    private static ResolvedConflict FindResolvedConflict(ServerDomainApplyContext context)
    {
        if (context.Resolution.Conflicts.Count == 1
            && context.Resolution.Conflicts[0].OperationId == context.Operation.OperationId
            && string.Equals(context.Resolution.Conflicts[0].ResolutionCode, "crdt.merge", StringComparison.Ordinal))
        {
            return context.Resolution.Conflicts[0];
        }

        throw new InvalidOperationException("The CRDT domain handler requires one matching crdt.merge conflict.");
    }
}
