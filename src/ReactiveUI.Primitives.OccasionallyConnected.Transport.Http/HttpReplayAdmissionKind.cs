// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Classifies replay admission outcomes.</summary>
internal enum HttpReplayAdmissionKind
{
    /// <summary>The caller owns first execution for the admitted nonce.</summary>
    Execute = 0,

    /// <summary>The caller should receive a retained byte-identical response.</summary>
    ReplayCached = 1,

    /// <summary>The caller should reexecute an uncached idempotent request or observe a safe transient result.</summary>
    ReplayTransient = 2,

    /// <summary>The caller should receive a safe rejection.</summary>
    Reject = 3,
}
