// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Contains one processed operation result and replayable events.</summary>
internal sealed class ServerOperationReceipt
{
    /// <summary>The copied events.</summary>
    private readonly ReadOnlyCollection<RemoteEvent> _events;

    /// <summary>Initializes a new instance of the <see cref="ServerOperationReceipt"/> class.</summary>
    /// <param name="result">The operation result.</param>
    /// <param name="events">The replayable events.</param>
    internal ServerOperationReceipt(OperationSyncResult result, IReadOnlyList<RemoteEvent> events)
    {
        ArgumentExceptionHelper.ThrowIfNull(result);
        ArgumentExceptionHelper.ThrowIfNull(events);
        Result = result;
        _events = Copy(events);
    }

    /// <summary>Gets the operation result.</summary>
    internal OperationSyncResult Result { get; }

    /// <summary>Gets the replayable events.</summary>
    internal IReadOnlyList<RemoteEvent> Events => _events;

    /// <summary>Copies an event list.</summary>
    /// <param name="source">The source events.</param>
    /// <returns>The copied events.</returns>
    private static ReadOnlyCollection<RemoteEvent> Copy(IReadOnlyList<RemoteEvent> source)
    {
        var copy = new RemoteEvent[source.Count];
        for (var index = 0; index < copy.Length; index++)
        {
            copy[index] = source[index];
        }

        return Array.AsReadOnly(copy);
    }
}
