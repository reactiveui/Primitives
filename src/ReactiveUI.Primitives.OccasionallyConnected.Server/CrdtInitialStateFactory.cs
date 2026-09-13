// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Creates empty built-in CRDT server states.</summary>
[System.Diagnostics.DebuggerDisplay("{_options.Kind,nq} {_options.InitialVersion,nq}")]
public sealed class CrdtInitialStateFactory : IServerInitialStateFactory
{
    /// <summary>The factory options.</summary>
    private readonly CrdtInitialStateFactoryOptions _options;

    /// <summary>Initializes a new instance of the <see cref="CrdtInitialStateFactory"/> class.</summary>
    /// <param name="options">The factory options.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    public CrdtInitialStateFactory(CrdtInitialStateFactoryOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        CrdtServerGuards.ValidateKind(options.Kind, nameof(options.Kind));
        ServerCommitJournalGuard.ValidateText(options.InitialVersion, nameof(options.InitialVersion));
        ArgumentExceptionHelper.ThrowIfNull(options.Bounds);
        options.Bounds.Validate();
        _options = options;
    }

    /// <inheritdoc/>
    public ValueTask<ServerState> CreateInitialStateAsync(
        StreamId streamId,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var state = CrdtFunctions.Empty(_options.Kind);
        return new(new ServerState(
            streamId,
            _options.InitialVersion,
            CrdtServerPayloads.CreateState(state, _options.Bounds)));
    }
}
