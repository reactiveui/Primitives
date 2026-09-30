// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Web;

/// <summary>Describes a browser network hint, not authenticated server connectivity.</summary>
public enum BrowserConnectivityHint
{
    /// <summary>No browser hint has been received.</summary>
    Unknown = 0,

    /// <summary>The browser reports that no network path is available.</summary>
    Unavailable = 1,

    /// <summary>The browser reports a possible network path; the server may still be unreachable.</summary>
    PossiblyAvailable = 2,
}
