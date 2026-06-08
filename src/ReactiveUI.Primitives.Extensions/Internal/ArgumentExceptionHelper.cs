// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Extensions.Internal;

/// <summary>
/// Provides helper methods for argument validation.
/// These methods serve as polyfills for ArgumentExceptionHelper.ThrowIfNull and related methods
/// that are only available in newer .NET versions.
/// </summary>
[ExcludeFromCodeCoverage]
internal static class ArgumentExceptionHelper
{
    /// <summary>Throws an <see cref="ArgumentNullException"/> if <paramref name="argument"/> is null.</summary>
    /// <param name="argument">The reference type argument to validate as non-null.</param>
    /// <param name="paramName">The name of the parameter with which <paramref name="argument"/> corresponds.</param>
    public static void ThrowIfNull(
        [NotNull] object? argument,
        [CallerArgumentExpression(nameof(argument))]
        string? paramName = null)
    {
        if (argument is not null)
        {
            return;
        }

        throw new ArgumentNullException(paramName);
    }

    /// <summary>
    /// Validates an argument and returns it if it is not null, otherwise throws an <see cref="ArgumentNullException"/>.
    /// Designed for use in constructor initializers.
    /// </summary>
    /// <typeparam name="T">The type of the argument.</typeparam>
    /// <param name="argument">The argument to validate.</param>
    /// <param name="paramName">Captured automatically via <see cref="CallerArgumentExpressionAttribute"/>.</param>
    /// <returns>The non-null argument.</returns>
    public static T Check<T>(
        [NotNull] T? argument,
        [CallerArgumentExpression(nameof(argument))]
        string? paramName = null)
        where T : class
    {
        if (argument is not null)
        {
            return argument;
        }

        throw new ArgumentNullException(paramName);
    }

    /// <summary>
    /// Validates a string argument and returns it if it is not null or empty, otherwise throws an <see cref="ArgumentNullException"/> or <see cref="ArgumentException"/>.
    /// Designed for use in constructor initializers.
    /// </summary>
    /// <param name="argument">The argument to validate.</param>
    /// <param name="paramName">Captured automatically via <see cref="CallerArgumentExpressionAttribute"/>.</param>
    /// <returns>The non-null, non-empty argument.</returns>
    public static string Check(
        [NotNull] string? argument,
        [CallerArgumentExpression(nameof(argument))]
        string? paramName = null)
    {
        if (argument is null)
        {
            throw new ArgumentNullException(paramName);
        }

        if (argument.Length == 0)
        {
            throw new ArgumentException("The value cannot be an empty string.", paramName);
        }

        return argument;
    }

    /// <summary>Throws an exception if <paramref name="argument"/> is null or empty.</summary>
    /// <param name="argument">The string argument to validate as non-null and non-empty.</param>
    /// <param name="paramName">The name of the parameter with which <paramref name="argument"/> corresponds.</param>
    /// <exception cref="ArgumentNullException"><paramref name="argument"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="argument"/> is empty.</exception>
    public static void ThrowIfNullOrEmpty(
        [NotNull] string? argument,
        [CallerArgumentExpression(nameof(argument))]
        string? paramName = null)
    {
        if (argument is null)
        {
            throw new ArgumentNullException(paramName);
        }

        if (argument.Length != 0)
        {
            return;
        }

        throw new ArgumentException("The value cannot be an empty string.", paramName);
    }
}
