// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Registers conflict and domain behavior for one server stream.</summary>
[System.Diagnostics.DebuggerDisplay("{StreamId,nq}")]
public sealed record ServerConflictStreamRegistration
{
    /// <summary>Gets the stream served by this registration.</summary>
    public required StreamId StreamId { get; init; }

    /// <summary>Gets the initial state factory used when the journal has no canonical state.</summary>
    public required IServerInitialStateFactory InitialStateFactory { get; init; }

    /// <summary>Gets the conflict resolver used for last-writer-wins operations.</summary>
    public required IConflictResolver LastWriterWinsResolver { get; init; }

    /// <summary>Gets the conflict resolver used for merge operations.</summary>
    public required IConflictResolver MergeResolver { get; init; }

    /// <summary>Gets the conflict resolver used for custom operations.</summary>
    public required IConflictResolver CustomResolver { get; init; }

    /// <summary>Gets the domain handler that materializes accepted conflict decisions.</summary>
    public required IServerDomainHandler DomainHandler { get; init; }

    /// <summary>Validates the stream registration.</summary>
    /// <exception cref="ArgumentException">The stream identifier is malformed.</exception>
    /// <exception cref="ArgumentNullException">A required registration component is missing.</exception>
    public void Validate()
    {
        if (StreamId.Value is null)
        {
            throw new ArgumentException("A registered stream identifier is required.", nameof(StreamId));
        }

        ServerCommitJournalGuard.ValidateText(StreamId.Value, nameof(StreamId));
        ArgumentExceptionHelper.ThrowIfNull(InitialStateFactory);
        ArgumentExceptionHelper.ThrowIfNull(LastWriterWinsResolver);
        ArgumentExceptionHelper.ThrowIfNull(MergeResolver);
        ArgumentExceptionHelper.ThrowIfNull(CustomResolver);
        ArgumentExceptionHelper.ThrowIfNull(DomainHandler);
    }
}
