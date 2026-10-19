// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Creates deterministic per-stream cursors from the snapshot event sequence.</summary>
internal sealed class ServerOperationCursorFactory : IServerOperationCursorFactory
{
    /// <summary>The cursor segment separator.</summary>
    private const string Separator = ":";

    /// <inheritdoc/>
    public string CreateCursor(ServerOperationContext context, int eventIndex)
    {
        ArgumentExceptionHelper.ThrowIfNull(context);
        if (eventIndex < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(eventIndex), eventIndex, "Event indexes cannot be negative.");
        }

        var sequence = checked(context.Snapshot.LastEventSequence + eventIndex + 1);
        return string.Concat(
            context.StreamKey.TenantId,
            Separator,
            context.StreamKey.StreamId.Value,
            Separator,
            sequence.ToString(CultureInfo.InvariantCulture),
            Separator,
            context.OperationKey.OperationId.Value.ToString("N"));
    }
}
