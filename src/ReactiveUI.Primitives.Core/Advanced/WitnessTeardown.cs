// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Shared run-once teardown for witness sinks.</summary>
public static class WitnessTeardown
{
    /// <summary>Disposes the upstream subscription exactly once.</summary>
    /// <param name="disposed">The run-once latch; 0 when alive, 1 once disposed.</param>
    /// <param name="cancel">The upstream subscription to dispose.</param>
    /// <returns><see langword="true"/> on the first disposal; otherwise <see langword="false"/>.</returns>
    public static bool Dispose(ref int disposed, ref IDisposable? cancel)
    {
        // Atomic run-once latch so concurrent disposal cannot double-tear-down.
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return false;
        }

        Interlocked.Exchange(ref cancel, null)?.Dispose();
        return true;
    }
}
