// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Helpers;

/// <summary>Caches identity selectors by source type for dictionary materialization.</summary>
/// <typeparam name="T">The source value type.</typeparam>
internal static class DictionaryIdentity<T>
{
    /// <summary>The cached identity selector.</summary>
    internal static readonly Func<T, T> Instance = static value => value;
}
