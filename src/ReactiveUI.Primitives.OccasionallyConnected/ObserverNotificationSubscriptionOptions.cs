// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures one observer notification queue.</summary>
/// <param name="Capacity">The maximum queued data notification count.</param>
/// <param name="CapacityBytes">The maximum queued data notification bytes.</param>
/// <param name="OverflowMode">The overflow behavior for latest-state notifications. Event overflow disconnects.</param>
internal readonly record struct ObserverNotificationSubscriptionOptions(
    int Capacity,
    long CapacityBytes,
    ObserverNotificationOverflowMode OverflowMode)
{
    /// <summary>Validates the subscription options.</summary>
    /// <exception cref="InvalidOperationException">The option record contains invalid values.</exception>
    internal void Validate()
    {
        if (Capacity <= 0)
        {
            throw new InvalidOperationException("Capacity must be positive.");
        }

        if (CapacityBytes <= 0)
        {
            throw new InvalidOperationException("CapacityBytes must be positive.");
        }

        if (OverflowMode is ObserverNotificationOverflowMode.CoalesceLatest or ObserverNotificationOverflowMode.Disconnect)
        {
            return;
        }

        throw new InvalidOperationException("OverflowMode must be a defined observer notification overflow mode.");
    }
}
