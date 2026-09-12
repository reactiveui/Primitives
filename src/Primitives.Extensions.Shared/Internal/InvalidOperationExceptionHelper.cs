// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Extensions;

/// <summary>Rejects missing operator state with the member and operation names.</summary>
[ExcludeFromCodeCoverage]
internal static class InvalidOperationExceptionHelper
{
    /// <summary>Throws <see cref="InvalidOperationException"/> when <paramref name="argument"/> is null.</summary>
    /// <param name="argument">The reference type field to validate as non-null.</param>
    /// <param name="memberName">The validated member's name, captured from the <paramref name="argument"/> expression.</param>
    /// <param name="operation">The calling member's name.</param>
    /// <exception cref="InvalidOperationException"><paramref name="argument"/> is <see langword="null"/>.</exception>
    internal static void ThrowIfNull(
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

    /// <summary>Returns the argument or throws if it is null.</summary>
    /// <typeparam name="T">The type of the argument.</typeparam>
    /// <param name="argument">The argument to validate.</param>
    /// <param name="memberName">The validated argument's name, captured from the <paramref name="argument"/> expression.</param>
    /// <param name="operation">The calling member's name.</param>
    /// <returns>The non-null argument.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="argument"/> is <see langword="null"/>.</exception>
    internal static T Check<T>(
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

    /// <summary>Returns the argument or throws if it is null or empty.</summary>
    /// <param name="argument">The argument to validate.</param>
    /// <param name="memberName">The validated argument's name, captured from the <paramref name="argument"/> expression.</param>
    /// <param name="operation">The calling member's name.</param>
    /// <returns>The non-null, non-empty argument.</returns>
    /// <exception cref="InvalidOperationException"><paramref name="argument"/> is <see langword="null"/> or empty.</exception>
    internal static string Check(
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
