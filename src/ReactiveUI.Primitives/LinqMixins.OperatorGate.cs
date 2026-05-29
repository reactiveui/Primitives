// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>
/// SelectMixins.
/// </summary>
public static partial class LinqMixins
{
    /// <summary>
    /// Per-subscription synchronization gate for operators that coordinate callbacks from multiple sources.
    /// </summary>
    private sealed class OperatorGate
    {
        /// <summary>
        /// Gets the stable synchronization gate for the subscription. Typed as <c>Lock</c> so the
        /// lock statement uses <c>System.Threading.Lock</c> on .NET 9+ and a plain monitor elsewhere.
        /// </summary>
        internal Lock SyncRoot { get; } = new();
    }
}
