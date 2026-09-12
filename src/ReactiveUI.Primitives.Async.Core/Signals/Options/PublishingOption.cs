// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>Specifies the available options for publishing operations, indicating whether actions are performed serially or concurrently.</summary>
public enum PublishingOption
{
    /// <summary>Awaits each observer notification before invoking the next observer.</summary>
    Serial = 0,

    /// <summary>Invokes observer notifications concurrently.</summary>
    Concurrent = 1,
}
