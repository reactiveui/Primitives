// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives;

/// <summary>Rethrows a captured failure from an expression, so the caller carries no unreachable code after the throw.</summary>
internal static class CapturedFailure
{
    /// <summary>Rethrows <paramref name="error"/> with its original stack, typed by <paramref name="fallback"/> so a value-returning caller can return the call.</summary>
    /// <typeparam name="TResult">The caller's return type.</typeparam>
    /// <param name="error">The failure to rethrow.</param>
    /// <param name="fallback">The value the caller would otherwise return; never returned.</param>
    /// <returns>Never returns; the return after the throw is unreachable.</returns>
    [ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static TResult Rethrow<TResult>(Exception error, TResult fallback)
    {
        ExceptionDispatchInfo.Capture(error).Throw();
        return fallback;
    }
}
