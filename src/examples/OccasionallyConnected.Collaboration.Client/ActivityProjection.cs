// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

/// <summary>Projects local optimistic and remote canonical activity updates into a durable activity view.</summary>
internal sealed class ActivityProjection : ILocalProjection<ActivityView, ActivityUpdate>
{
    /// <summary>Gets the shared projection instance.</summary>
    public static ActivityProjection Instance { get; } = new();

    /// <inheritdoc />
    public ActivityView InitialState => ActivityView.Empty();

    /// <inheritdoc />
    public ActivityView ApplyLocal(ActivityView state, ActivityUpdate input, SyncOperation operation)
    {
        ArgumentNullException.ThrowIfNull(state);
        return state.ApplyLocal(input, operation);
    }

    /// <inheritdoc />
    public ActivityView ApplyRemote(ActivityView state, ActivityUpdate input, RemoteEvent remoteEvent)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(remoteEvent);
        return ActivityView.FromRemote(input, state);
    }

    /// <inheritdoc />
    public ActivityView Reconcile(ActivityView state, ConflictResolutionResult result)
    {
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(result);
        for (var index = 0; index < result.ProducedEvents.Count; index++)
        {
            if (ActivityPayloadSerializer.TryReadView(result.ProducedEvents[index].Payload, out var view))
            {
                return view;
            }
        }

        for (var index = 0; index < result.Conflicts.Count; index++)
        {
            var payload = result.Conflicts[index].ResolvedPayload;
            if (payload is not null && ActivityPayloadSerializer.TryReadView(payload, out var view))
            {
                return view;
            }
        }

        return state;
    }
}
