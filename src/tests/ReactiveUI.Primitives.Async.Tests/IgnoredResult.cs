// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Consumes return values that are intentionally ignored in tests.</summary>
internal static class IgnoredResult
{
    /// <summary>Consumes an ignored return value.</summary>
    /// <typeparam name="T">The ignored value type.</typeparam>
    /// <param name="value">The ignored value.</param>
    internal static void Of<T>(T value) => _ = value;
}
