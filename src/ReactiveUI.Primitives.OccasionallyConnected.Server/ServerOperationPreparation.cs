// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Contains a side-effect-free prepared operation result.</summary>
internal sealed class ServerOperationPreparation
{
    /// <summary>The maximum conflict count accepted for one prepared operation.</summary>
    private const int MaximumPreparedConflicts = 512;

    /// <summary>The maximum event count accepted for one prepared operation.</summary>
    private const int MaximumPreparedEvents = 512;

    /// <summary>The copied conflicts.</summary>
    private readonly ReadOnlyCollection<ResolvedConflict> _conflicts;

    /// <summary>The copied events.</summary>
    private readonly ReadOnlyCollection<ServerPreparedEvent> _events;

    /// <summary>Initializes a new instance of the <see cref="ServerOperationPreparation"/> class.</summary>
    /// <param name="result">The terminal operation result.</param>
    /// <param name="newState">The optional new server state.</param>
    /// <param name="conflicts">The complete resolved conflicts.</param>
    /// <param name="events">The complete prepared events.</param>
    internal ServerOperationPreparation(
        OperationSyncResult result,
        ServerState? newState,
        IReadOnlyList<ResolvedConflict> conflicts,
        IReadOnlyList<ServerPreparedEvent> events)
    {
        ArgumentExceptionHelper.ThrowIfNull(result);
        ArgumentExceptionHelper.ThrowIfNull(conflicts);
        ArgumentExceptionHelper.ThrowIfNull(events);
        Result = result;
        NewState = newState;
        _conflicts = Copy(conflicts, MaximumPreparedConflicts, nameof(conflicts));
        _events = Copy(events, MaximumPreparedEvents, nameof(events));
    }

    /// <summary>Gets the terminal operation result.</summary>
    internal OperationSyncResult Result { get; }

    /// <summary>Gets the optional new server state.</summary>
    internal ServerState? NewState { get; }

    /// <summary>Gets the complete resolved conflicts.</summary>
    internal IReadOnlyList<ResolvedConflict> Conflicts => _conflicts;

    /// <summary>Gets the complete prepared events.</summary>
    internal IReadOnlyList<ServerPreparedEvent> Events => _events;

    /// <summary>Copies a list while preserving item identity.</summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="source">The source list.</param>
    /// <param name="maximumCount">The maximum accepted item count.</param>
    /// <param name="parameterName">The source parameter name.</param>
    /// <returns>The owned copy.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The source count exceeds <paramref name="maximumCount"/>.</exception>
    private static ReadOnlyCollection<T> Copy<T>(
        IReadOnlyList<T> source,
        int maximumCount,
        string parameterName)
    {
        var count = source.Count;
        if (count > maximumCount)
        {
            throw new ArgumentOutOfRangeException(parameterName, count, "The prepared collection count is outside the configured bounds.");
        }

        var copy = new T[count];
        for (var index = 0; index < copy.Length; index++)
        {
            copy[index] = source[index];
        }

        return Array.AsReadOnly(copy);
    }
}
