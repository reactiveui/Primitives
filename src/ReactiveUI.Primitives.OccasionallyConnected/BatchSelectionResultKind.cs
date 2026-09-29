// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.OccasionallyConnected.Reactive;
#else
namespace ReactiveUI.Primitives.OccasionallyConnected;
#endif

/// <summary>Describes why a batch prefix can or cannot proceed.</summary>
internal enum BatchSelectionResultKind
{
    /// <summary>The prefix can be submitted.</summary>
    Ready = 0,

    /// <summary>The prefix remains eligible but should wait for dwell.</summary>
    WaitForDwell = 1,

    /// <summary>The FIFO head exceeds the effective byte ceiling.</summary>
    OversizedHead = 2,
}
