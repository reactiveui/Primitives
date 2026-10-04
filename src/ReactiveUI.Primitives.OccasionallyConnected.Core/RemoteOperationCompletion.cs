// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Identifies the complete event set produced by one client operation.</summary>
/// <remarks>An empty event set explicitly represents an operation that produced no events. This record does not authenticate its origin.</remarks>
[System.Diagnostics.DebuggerDisplay("{Origin,nq}")]
public sealed record RemoteOperationCompletion
{
    /// <summary>Initializes a new instance of the <see cref="RemoteOperationCompletion"/> class.</summary>
    /// <param name="origin">The originating client operation.</param>
    /// <param name="eventIds">The complete event identifiers, copied before returning.</param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public RemoteOperationCompletion(RemoteEventOrigin origin, IReadOnlyList<Guid> eventIds)
    {
        ArgumentExceptionHelper.ThrowIfNull(origin);
        Origin = origin;
        EventIds = CollectionCopy.List(eventIds);
    }

    /// <summary>Gets the originating client operation.</summary>
    public RemoteEventOrigin Origin { get; }

    /// <summary>Gets the complete event identifiers.</summary>
    public IReadOnlyList<Guid> EventIds { get; }
}
