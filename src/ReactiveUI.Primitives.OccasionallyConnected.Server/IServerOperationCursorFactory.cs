// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Creates canonical server cursors for prepared operation events.</summary>
internal interface IServerOperationCursorFactory
{
    /// <summary>Creates a cursor for the event at <paramref name="eventIndex"/>.</summary>
    /// <param name="context">The authorized operation context.</param>
    /// <param name="eventIndex">The zero-based event index in the prepared result.</param>
    /// <returns>The canonical cursor.</returns>
    string CreateCursor(ServerOperationContext context, int eventIndex);
}
