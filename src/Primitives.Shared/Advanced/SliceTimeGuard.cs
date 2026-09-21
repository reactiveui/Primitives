// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Argument checks shared by the time-based window operators.</summary>
internal static class SliceTimeGuard
{
    /// <summary>Throws when <paramref name="value"/> is zero or negative.</summary>
    /// <param name="value">The duration to validate.</param>
    /// <param name="paramName">The parameter name.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="value"/> is zero or negative.</exception>
    internal static void ThrowIfNotPositive(TimeSpan value, [CallerArgumentExpression(nameof(value))] string? paramName = null)
    {
        if (value > TimeSpan.Zero)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(paramName, value, null);
    }
}
