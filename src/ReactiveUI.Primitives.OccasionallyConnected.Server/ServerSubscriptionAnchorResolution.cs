// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Describes how an initial subscription position resolved against retained stream history.</summary>
internal enum ServerSubscriptionAnchorResolution
{
    /// <summary>A concrete complete-group anchor was resolved.</summary>
    Resolved = 0,

    /// <summary>The requested start threshold has not appeared in the stream yet.</summary>
    Pending = 1,

    /// <summary>The requested start threshold cannot be proven from retained history.</summary>
    RetentionGap = 2,
}
