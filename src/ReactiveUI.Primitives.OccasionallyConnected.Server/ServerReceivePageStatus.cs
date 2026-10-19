// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Describes the outcome of a server receive page read.</summary>
internal enum ServerReceivePageStatus
{
    /// <summary>A complete receive page was returned.</summary>
    Page = 0,

    /// <summary>The request cursor is already at the retained high-water mark.</summary>
    EndOfStream = 1,

    /// <summary>Retained operation groups no longer cover the requested cursor.</summary>
    RetentionGap = 2,
}
