// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.Async.Helpers;

/// <summary>Rethrows a failure captured before asynchronous cleanup, so the cleanup runs outside the <see langword="catch"/> block.</summary>
internal static class CapturedFailure
{
    /// <summary>Rethrows <paramref name="failure"/> with its original stack, typed so a value-returning caller can return the call.</summary>
    /// <typeparam name="TResult">The caller's return type.</typeparam>
    /// <param name="failure">The captured failure.</param>
    /// <returns>Never returns; the return after the throw is unreachable.</returns>
    [ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static TResult Rethrow<TResult>(ExceptionDispatchInfo failure)
    {
        failure.Throw();
        return default!;
    }
}
