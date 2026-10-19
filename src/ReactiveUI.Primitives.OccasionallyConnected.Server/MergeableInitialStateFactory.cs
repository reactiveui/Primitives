// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Creates empty built-in CRDT server states.</summary>
[System.Diagnostics.DebuggerDisplay("{_options.Kind,nq} {_options.InitialVersion,nq}")]
public sealed class MergeableInitialStateFactory : IServerInitialStateFactory
{
    /// <summary>The factory options.</summary>
    private readonly MergeableInitialStateFactoryOptions _options;

    /// <summary>Initializes a new instance of the <see cref="MergeableInitialStateFactory"/> class.</summary>
    /// <param name="options">The factory options.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is <see langword="null"/>.</exception>
    public MergeableInitialStateFactory(MergeableInitialStateFactoryOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        MergeableServerGuards.ValidateKind(options.Kind, nameof(options.Kind));
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
        var state = MergeableFunctions.Empty(_options.Kind);
        return new(new ServerState(
            streamId,
            _options.InitialVersion,
            MergeableServerPayloads.CreateState(state, _options.Bounds)));
    }
}
