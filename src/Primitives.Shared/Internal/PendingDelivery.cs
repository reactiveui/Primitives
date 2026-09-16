// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Internal;
#else
namespace ReactiveUI.Primitives.Internal;
#endif

/// <summary>What a coordinator's drain took from its pending work.</summary>
internal enum PendingDelivery
{
    /// <summary>Nothing is waiting.</summary>
    None = 0,

    /// <summary>A queued value.</summary>
    Value = 1,

    /// <summary>The terminal notification, taken once every queued value has been delivered.</summary>
    Terminal = 2,
}
