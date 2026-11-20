// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Advanced;
#else
using ReactiveUI.Primitives.Advanced;
#endif

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Internal;
#else
namespace ReactiveUI.Primitives.Internal;
#endif

/// <summary>Validates and attaches octets of observable sources for wide-arity combine-latest signals.</summary>
internal static class CombineLatestWideHelper
{
    /// <summary>Throws <see cref="ArgumentNullException"/> if any source in <paramref name="octet"/> is <see langword="null"/>, preserving parameter names.</summary>
    /// <typeparam name="T1">The first source element type.</typeparam>
    /// <typeparam name="T2">The second source element type.</typeparam>
    /// <typeparam name="T3">The third source element type.</typeparam>
    /// <typeparam name="T4">The fourth source element type.</typeparam>
    /// <typeparam name="T5">The fifth source element type.</typeparam>
    /// <typeparam name="T6">The sixth source element type.</typeparam>
    /// <typeparam name="T7">The seventh source element type.</typeparam>
    /// <typeparam name="T8">The eighth source element type.</typeparam>
    /// <param name="octet">The eight sources to validate.</param>
    /// <param name="firstName">Parameter name for the first source.</param>
    /// <param name="startIndex">1-based index of the first source in the caller's parameter list.</param>
    /// <returns>The validated <paramref name="octet"/>.</returns>
    /// <exception cref="ArgumentNullException">Any source in <paramref name="octet"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static CombineLatestOctet<T1, T2, T3, T4, T5, T6, T7, T8> ValidateOctet<T1, T2, T3, T4, T5, T6, T7, T8>(
        in CombineLatestOctet<T1, T2, T3, T4, T5, T6, T7, T8> octet,
        string firstName = "source",
        int startIndex = 1)
    {
        var idx = startIndex;
        ArgumentExceptionHelper.ThrowIfNull(octet.Source1, firstName);
        idx++;
        ArgumentExceptionHelper.ThrowIfNull(octet.Source2, $"source{idx}");
        idx++;
        ArgumentExceptionHelper.ThrowIfNull(octet.Source3, $"source{idx}");
        idx++;
        ArgumentExceptionHelper.ThrowIfNull(octet.Source4, $"source{idx}");
        idx++;
        ArgumentExceptionHelper.ThrowIfNull(octet.Source5, $"source{idx}");
        idx++;
        ArgumentExceptionHelper.ThrowIfNull(octet.Source6, $"source{idx}");
        idx++;
        ArgumentExceptionHelper.ThrowIfNull(octet.Source7, $"source{idx}");
        idx++;
        ArgumentExceptionHelper.ThrowIfNull(octet.Source8, $"source{idx}");
        return octet;
    }

    /// <summary>Attaches all eight sources in <paramref name="octet"/> to <paramref name="coordinator"/> in source order.</summary>
    /// <typeparam name="TResult">The combined result type.</typeparam>
    /// <typeparam name="T1">The first source element type.</typeparam>
    /// <typeparam name="T2">The second source element type.</typeparam>
    /// <typeparam name="T3">The third source element type.</typeparam>
    /// <typeparam name="T4">The fourth source element type.</typeparam>
    /// <typeparam name="T5">The fifth source element type.</typeparam>
    /// <typeparam name="T6">The sixth source element type.</typeparam>
    /// <typeparam name="T7">The seventh source element type.</typeparam>
    /// <typeparam name="T8">The eighth source element type.</typeparam>
    /// <param name="coordinator">The combine-latest coordinator.</param>
    /// <param name="octet">The eight validated sources.</param>
    /// <returns>The eight attached slots in source order.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static CombineLatestOctetSlots<TResult, T1, T2, T3, T4, T5, T6, T7, T8> AttachOctet<TResult, T1, T2, T3, T4, T5, T6, T7, T8>(
        CombineLatestCoordinator<TResult> coordinator,
        in CombineLatestOctet<T1, T2, T3, T4, T5, T6, T7, T8> octet) =>
        new(
            coordinator.Attach(octet.Source1),
            coordinator.Attach(octet.Source2),
            coordinator.Attach(octet.Source3),
            coordinator.Attach(octet.Source4),
            coordinator.Attach(octet.Source5),
            coordinator.Attach(octet.Source6),
            coordinator.Attach(octet.Source7),
            coordinator.Attach(octet.Source8));
}
