// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Maui.Networking;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile.Tests;

/// <summary>Provides deterministic OS connectivity hints through the real Essentials interface.</summary>
internal sealed class MobileTestConnectivity : IConnectivity
{
    /// <inheritdoc/>
    public event EventHandler<ConnectivityChangedEventArgs>? ConnectivityChanged;

    /// <inheritdoc/>
    public NetworkAccess NetworkAccess { get; private set; } = NetworkAccess.Internet;

    /// <inheritdoc/>
    public IEnumerable<ConnectionProfile> ConnectionProfiles => [ConnectionProfile.WiFi];

    /// <summary>Publishes the next platform hint.</summary>
    /// <param name="access">The next network access hint.</param>
    internal void SetAccess(NetworkAccess access)
    {
        NetworkAccess = access;
        ConnectivityChanged?.Invoke(this, new(access, ConnectionProfiles));
    }
}
