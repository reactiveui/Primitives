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

/// <summary>Holds the parsed composite formats <see cref="Spark{T}"/> renders with.</summary>
/// <remarks>
/// The formats sit on a non-generic type on purpose. A static field inside <see cref="Spark{T}"/> is
/// a field of each closed generic, so the format would be parsed once per <c>T</c>; one shared instance
/// serves every one of them.
/// </remarks>
internal static class SparkCompositeFormats
{
    /// <summary>The format an <see cref="SparkKind.OnNext"/> spark renders with.</summary>
    internal static readonly CompositeFormat OnNext = CompositeFormat.Parse("OnNext({0})");

    /// <summary>The format an <see cref="SparkKind.OnError"/> spark renders with.</summary>
    internal static readonly CompositeFormat OnError = CompositeFormat.Parse("OnError({0})");
}
#endif
