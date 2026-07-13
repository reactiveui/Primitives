// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions;

/// <summary>Kind of upstream notification enqueued for a <c>ScheduledDrainState&lt;T&gt;</c> drain pass.</summary>
public enum DrainNotificationKind
{
    /// <summary>OnNext with a value.</summary>
    Next,

    /// <summary>OnError with an exception.</summary>
    Error,

    /// <summary>OnCompleted (no value).</summary>
    Completed
}
