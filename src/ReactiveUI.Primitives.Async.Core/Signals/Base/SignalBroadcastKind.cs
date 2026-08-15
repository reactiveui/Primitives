// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>Observer broadcast mode for signal helper operations.</summary>
internal enum SignalBroadcastKind
{
    /// <summary>Notify observers one at a time.</summary>
    Serial = 0,

    /// <summary>Notify observers one at a time with multi-observer value semantics.</summary>
    SerialMulti = 1,

    /// <summary>Notify observers concurrently.</summary>
    Concurrent = 2,
}
