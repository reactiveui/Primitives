// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if NET8_0_OR_GREATER
using System.Text;

namespace ReactiveUI.Primitives.Core;

/// <summary>Holds the parsed composite formats the core value types render with.</summary>
/// <remarks>
/// The formats sit on a non-generic type on purpose. A static field inside <see cref="Moment{T}"/> is
/// a field of each closed generic, so the format would be parsed once per <c>T</c>; one shared instance
/// serves every one of them.
/// </remarks>
internal static class CoreCompositeFormats
{
    /// <summary>The format <see cref="Moment{T}"/> renders with.</summary>
    public static readonly CompositeFormat Moment = CompositeFormat.Parse("{0}@{1:o}");

    /// <summary>The format <see cref="TimeInterval{T}"/> renders with.</summary>
    public static readonly CompositeFormat TimeInterval = CompositeFormat.Parse("{0}@{1}");
}
#endif
