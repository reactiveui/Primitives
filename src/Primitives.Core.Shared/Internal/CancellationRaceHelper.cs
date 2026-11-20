// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if PRIMITIVES_CORE
namespace ReactiveUI.Primitives.Core.Internal;
#elif REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Internal;
#else
namespace ReactiveUI.Primitives.Internal;
#endif

/// <summary>Synchronous cancellation helpers tolerant of concurrent source disposal.</summary>
internal static class CancellationRaceHelper
{
    /// <summary>Cancels <paramref name="cts"/>, swallowing <see cref="ObjectDisposedException"/> when another path disposed it first.</summary>
    /// <param name="cts">The cancellation token source to cancel.</param>
    /// <returns><see langword="true"/> when cancellation completed; <see langword="false"/> when <paramref name="cts"/> was already disposed.</returns>
    internal static bool TryCancel(CancellationTokenSource cts)
    {
        try
        {
            cts.Cancel();
            return true;
        }
        catch (ObjectDisposedException)
        {
            return false;
        }
    }
}
