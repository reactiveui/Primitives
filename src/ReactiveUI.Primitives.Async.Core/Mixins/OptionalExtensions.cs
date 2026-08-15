// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with the Optional{T} type.</summary>
public static class OptionalExtensions
{
    /// <summary>Value-retrieval operators for an <see cref="Optional{T}"/> instance.</summary>
    /// <typeparam name="T">The type of the value contained in the <see cref="Optional{T}"/>.</typeparam>
    /// <param name="optional">The <see cref="Optional{T}"/> instance from which to retrieve the value.</param>
    extension<T>(Optional<T> optional)
    {
        /// <summary>Attempts to retrieve the value contained in the specified <see cref="Optional{T}"/> instance.</summary>
        /// <param name="value">When this method returns, contains the value if the <see cref="Optional{T}"/> has a value; otherwise, the
        /// default value for type <typeparamref name="T"/>. This parameter is passed uninitialized.</param>
        /// <returns><see langword="true"/> if the <see cref="Optional{T}"/> has a value; otherwise, <see langword="false"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetValue([NotNullWhen(true)] out T? value)
        {
            var hasValue = optional.HasValue;
            value = hasValue ? optional.Value : default;
            return hasValue;
        }
    }
}
