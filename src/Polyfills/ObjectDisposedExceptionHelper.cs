// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Internal;

/// <summary>
/// Polyfill for <c>ObjectDisposedException.ThrowIf</c> on target frameworks (net462-net481) that predate it.
/// On net8.0 and later this type is not compiled; consuming projects alias the <c>ObjectDisposedExceptionHelper</c>
/// identifier directly to <see cref="ObjectDisposedException"/> so the call sites bind to the BCL method.
/// </summary>
[ExcludeFromCodeCoverage]
internal static class ObjectDisposedExceptionHelper
{
    /// <summary>Throws an <see cref="ObjectDisposedException"/> if <paramref name="condition"/> is <see langword="true"/>.</summary>
    /// <param name="condition">The condition to evaluate for a disposed instance.</param>
    /// <param name="instance">The object whose type name is used to build the exception message.</param>
    internal static void ThrowIf(bool condition, object instance)
    {
        if (!condition)
        {
            return;
        }

        throw new ObjectDisposedException(instance?.GetType().FullName);
    }
}
