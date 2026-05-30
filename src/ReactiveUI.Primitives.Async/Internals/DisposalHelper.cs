// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Internals;

/// <summary>
/// Provides helper methods for idempotent disposal patterns using an integer flag.
/// </summary>
[ExcludeFromCodeCoverage]
internal static class DisposalHelper
{
    /// <summary>
    /// Checks whether the disposed flag indicates disposal has occurred.
    /// </summary>
    /// <param name="disposed">The disposed flag value.</param>
    /// <returns><see langword="true"/> if disposed; otherwise <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool IsDisposed(int disposed) => disposed == 1;

    /// <summary>
    /// Atomically sets the disposed flag and returns whether it was already set.
    /// </summary>
    /// <param name="disposed">A reference to the disposed flag.</param>
    /// <returns><see langword="true"/> if already disposed; otherwise <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool TrySetDisposed(ref int disposed) => Interlocked.Exchange(ref disposed, 1) == 1;
}
