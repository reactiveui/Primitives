// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile;

/// <summary>Reports network availability hints, not proof that a remote peer is reachable.</summary>
public interface IMobileConnectivityHint
{
    /// <summary>Occurs when the network availability hint changes.</summary>
    event EventHandler? Changed;

    /// <summary>Gets a value indicating whether the host reports internet access.</summary>
    bool NetworkAvailable { get; }
}
