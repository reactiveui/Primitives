// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Extensions.Internal;

/// <summary>
/// Provides helper methods for throwing <see cref="InvalidOperationException"/> when
/// constructor-supplied state on an operator is missing at the time it is consumed.
/// The thrown message is composed of the captured member name and the caller member
/// (typically <c>Subscribe</c> or the enclosing type), so call sites just pass the
/// field being validated.
/// </summary>
[ExcludeFromCodeCoverage]
internal static class InvalidOperationExceptionHelper
{
    /// <summary>
    /// Throws an <see cref="InvalidOperationException"/> if <paramref name="argument"/> is null.
    /// The exception message is composed from the captured argument expression and the
    /// caller member, e.g. <c>"'source' was not supplied to 'Subscribe'."</c>.
    /// </summary>
    /// <param name="argument">The reference type field to validate as non-null.</param>
    /// <param name="memberName">The validated member's name, captured from the <paramref name="argument"/> expression via <see cref="CallerArgumentExpressionAttribute"/>.</param>
    /// <param name="operation">The void-throwing caller's name, captured via <see cref="CallerMemberNameAttribute"/>.</param>
    public static void ThrowIfNull(
        [NotNull] object? argument,
        [CallerArgumentExpression(nameof(argument))]
        string? memberName = null,
        [CallerMemberName] string? operation = null)
    {
        if (argument is not null)
        {
            return;
        }

        throw new InvalidOperationException(
            $"'{memberName}' was not supplied to '{operation}'.");
    }

    /// <summary>
    /// Validates an argument and returns it if it is not null, otherwise throws an <see cref="InvalidOperationException"/>.
    /// Designed for use in primary constructor initializers.
    /// </summary>
    /// <typeparam name="T">The type of the argument.</typeparam>
    /// <param name="argument">The argument to validate.</param>
    /// <param name="memberName">The validated reference-type argument's name, captured from the <paramref name="argument"/> expression via <see cref="CallerArgumentExpressionAttribute"/>.</param>
    /// <param name="operation">The reference-type-checking caller's name, captured via <see cref="CallerMemberNameAttribute"/>.</param>
    /// <returns>The non-null argument.</returns>
    public static T Check<T>(
        [NotNull] T? argument,
        [CallerArgumentExpression(nameof(argument))]
        string? memberName = null,
        [CallerMemberName] string? operation = null)
        where T : class
    {
        if (argument is not null)
        {
            return argument;
        }

        throw new InvalidOperationException(
            $"'{memberName}' was not supplied to '{operation}'.");
    }

    /// <summary>
    /// Validates a string argument and returns it if it is not null or empty, otherwise throws an <see cref="InvalidOperationException"/>.
    /// Designed for use in primary constructor initializers.
    /// </summary>
    /// <param name="argument">The argument to validate.</param>
    /// <param name="memberName">The validated string argument's name, captured from the <paramref name="argument"/> expression via <see cref="CallerArgumentExpressionAttribute"/>.</param>
    /// <param name="operation">The string-checking caller's name, captured via <see cref="CallerMemberNameAttribute"/>.</param>
    /// <returns>The non-null, non-empty argument.</returns>
    public static string Check(
        [NotNull] string? argument,
        [CallerArgumentExpression(nameof(argument))]
        string? memberName = null,
        [CallerMemberName] string? operation = null)
    {
        if (argument is null || string.IsNullOrEmpty(argument))
        {
            throw new InvalidOperationException(
                $"'{memberName}' was not supplied to '{operation}'.");
        }

        return argument;
    }
}
