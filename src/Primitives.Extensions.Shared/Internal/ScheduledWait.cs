// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Extensions.Reactive.Internal;
#else
namespace ReactiveUI.Primitives.Extensions.Internal;
#endif

/// <summary>Runs an action after a synchronous wait.</summary>
internal static class ScheduledWait
{
    /// <summary>Runs an action after the specified delay.</summary>
    /// <param name="dueTime">The delay.</param>
    /// <param name="action">The action to invoke.</param>
    /// <returns>An empty disposable after the action returns.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static IDisposable Run(TimeSpan dueTime, Action action) => Run(dueTime, action, Wait);

    /// <summary>Invokes an action after the supplied wait completes.</summary>
    /// <param name="dueTime">The delay passed to the wait operation.</param>
    /// <param name="action">The action to invoke.</param>
    /// <param name="wait">The wait operation; a failure prevents the action.</param>
    /// <returns>An empty disposable after the action returns.</returns>
    internal static IDisposable Run(TimeSpan dueTime, Action action, Action<TimeSpan> wait)
    {
        wait(dueTime);
        action();
        return EmptyDisposable.Instance;
    }

    /// <summary>Blocks the current thread for the requested delay.</summary>
    /// <param name="dueTime">The delay.</param>
    [ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Wait(TimeSpan dueTime) => Thread.Sleep(dueTime);
}
