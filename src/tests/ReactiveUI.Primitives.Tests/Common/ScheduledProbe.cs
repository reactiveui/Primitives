// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Creates <see cref="ScheduledItem{TAbsolute}"/> probes that delegate invocation to a supplied factory.</summary>
internal static class ScheduledProbe
{
    /// <summary>Creates a scheduled item whose invocation calls the supplied factory.</summary>
    /// <param name="dueTime">The due time for the scheduled item.</param>
    /// <param name="invoke">The factory invoked when the item runs.</param>
    /// <returns>The scheduled item.</returns>
    public static ScheduledItem<int> Create(int dueTime, Func<IDisposable> invoke) =>
        new(dueTime, Comparer<int>.Default, _ => invoke());
}
