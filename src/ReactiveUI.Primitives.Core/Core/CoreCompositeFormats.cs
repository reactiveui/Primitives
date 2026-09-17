// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
#if NET8_0_OR_GREATER
using System.Text;

namespace ReactiveUI.Primitives.Core;

/// <summary>Shares parsed value formats across all closed generic types.</summary>
internal static class CoreCompositeFormats
{
    /// <summary>The format <see cref="Moment{T}"/> renders with.</summary>
    internal static readonly CompositeFormat Moment = CompositeFormat.Parse("{0}@{1:o}");

    /// <summary>The format <see cref="TimeInterval{T}"/> renders with.</summary>
    internal static readonly CompositeFormat TimeInterval = CompositeFormat.Parse("{0}@{1}");
}
#endif
