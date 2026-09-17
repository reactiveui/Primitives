// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if NET8_0_OR_GREATER
using System.Text;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Core;
#else
namespace ReactiveUI.Primitives.Core;
#endif

/// <summary>Shares parsed spark formats across all closed generic types.</summary>
internal static class SparkCompositeFormats
{
    /// <summary>The format an <see cref="SparkKind.OnNext"/> spark renders with.</summary>
    internal static readonly CompositeFormat OnNext = CompositeFormat.Parse("OnNext({0})");

    /// <summary>The format an <see cref="SparkKind.OnError"/> spark renders with.</summary>
    internal static readonly CompositeFormat OnError = CompositeFormat.Parse("OnError({0})");
}
#endif
