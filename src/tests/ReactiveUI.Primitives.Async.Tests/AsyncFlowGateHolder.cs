// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internal;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Holds an <see cref="AsyncFlowGate"/> from a separate async flow, so the caller's flow is not the holder.</summary>
internal static class AsyncFlowGateHolder
{
    /// <summary>Enters <paramref name="gate"/> and keeps it until <paramref name="release"/> completes.</summary>
    /// <param name="gate">The gate to hold.</param>
    /// <param name="release">Completes when the gate should be released.</param>
    /// <returns>A task that completes once the gate is released.</returns>
    /// <remarks>The hold is entered synchronously when the gate is free, so it is in place when this method returns.</remarks>
    internal static async Task HoldAsync(AsyncFlowGate gate, Task release)
    {
        using (await gate.EnterAsync().ConfigureAwait(false))
        {
            await release.ConfigureAwait(false);
        }
    }
}
