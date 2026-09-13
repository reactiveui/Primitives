// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Holds final authoritative stream pages received by one client.</summary>
/// <param name="GCounter">The G-counter stream page.</param>
/// <param name="PNCounter">The PN-counter stream page.</param>
/// <param name="ORSet">The OR-set stream page.</param>
/// <param name="Lww">The LWW register stream page.</param>
[DebuggerDisplay("GCounter={GCounter.Cursor,nq}; PNCounter={PNCounter.Cursor,nq}")]
internal sealed record CrdtLoopbackReceivedStates(
    CrdtLoopbackReceivedStream GCounter,
    CrdtLoopbackReceivedStream PNCounter,
    CrdtLoopbackReceivedStream ORSet,
    CrdtLoopbackReceivedStream Lww);
