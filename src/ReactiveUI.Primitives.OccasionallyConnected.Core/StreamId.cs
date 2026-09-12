// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Identifies a stable, tenant-scoped logical stream.</summary>
[DebuggerDisplay("{Value,nq}")]
public readonly record struct StreamId
{
    /// <summary>Defines the maximum allowed UTF-8 byte count.</summary>
    private const int MaximumUtf8Bytes = 256;

    /// <summary>Initializes a new instance of the <see cref="StreamId"/> struct.</summary>
    /// <param name="value">The stream identifier value.</param>
    /// <exception cref="ArgumentNullException">The identifier is null.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="value"/> is not a valid stream identifier.</exception>
    public StreamId(string value)
    {
        ArgumentExceptionHelper.ThrowIfNull(value);
        ValidateUtf16(value);
        var normalizedValue = value.Normalize(NormalizationForm.FormC);
        ValidateGrammar(normalizedValue);
        if (Encoding.UTF8.GetByteCount(normalizedValue) > MaximumUtf8Bytes)
        {
            throw new ArgumentException("A stream identifier cannot exceed 256 UTF-8 bytes.", nameof(value));
        }

        Value = normalizedValue;
    }

    /// <summary>Gets the normalized stream identifier value.</summary>
    public string Value { get; }

    /// <inheritdoc/>
    public override string ToString() => Value ?? string.Empty;

    /// <summary>Ensures that a value contains only well-formed UTF-16 code units.</summary>
    /// <param name="value">The identifier to validate.</param>
    /// <exception cref="ArgumentException">The identifier contains malformed Unicode.</exception>
    private static void ValidateUtf16(string value)
    {
        for (int index = 0; index < value.Length; index++)
        {
            var character = value[index];
            if (!char.IsSurrogate(character))
            {
                continue;
            }

            var validPair = char.IsHighSurrogate(character) && index + 1 < value.Length && char.IsLowSurrogate(value[index + 1]);
            if (!validPair)
            {
                throw new ArgumentException("A stream identifier cannot contain malformed Unicode.", nameof(value));
            }

            index++;
        }
    }

    /// <summary>Ensures that a value conforms to the stream identifier grammar.</summary>
    /// <param name="value">The normalized identifier.</param>
    /// <exception cref="ArgumentException">The identifier violates the grammar.</exception>
    private static void ValidateGrammar(string value)
    {
        if (value.Length == 0)
        {
            throw new ArgumentException("A stream identifier cannot be empty.", nameof(value));
        }

#if NETFRAMEWORK
        var endsWithSlash = value.EndsWith("/", StringComparison.Ordinal);
#else
        var endsWithSlash = value.EndsWith('/');
#endif
        if (value[0] == '/' || endsWithSlash || value.Contains("//"))
        {
            throw new ArgumentException("A stream identifier cannot contain empty path segments.", nameof(value));
        }

        if (value.Contains(".."))
        {
            throw new ArgumentException("A stream identifier cannot contain '..'.", nameof(value));
        }

        ValidateCharacters(value);
    }

    /// <summary>Checks the permitted characters in a normalized identifier.</summary>
    /// <param name="value">The identifier to validate.</param>
    /// <exception cref="ArgumentException">The identifier contains a forbidden character.</exception>
    private static void ValidateCharacters(string value)
    {
        for (var index = 0; index < value.Length; index++)
        {
            var character = value[index];
            if (character is '/' or '.' or '_' or '-')
            {
                continue;
            }

            var category = CharUnicodeInfo.GetUnicodeCategory(value, index);
            if (!IsLetterOrDigit(category))
            {
                throw new ArgumentException("A stream identifier contains an unsupported character.", nameof(value));
            }

            if (char.IsHighSurrogate(character))
            {
                index++;
            }
        }
    }

    /// <summary>Identifies the Unicode letter and decimal digit categories.</summary>
    /// <param name="category">The Unicode category.</param>
    /// <returns>Whether the category is allowed in an identifier.</returns>
    private static bool IsLetterOrDigit(UnicodeCategory category) => category is
        UnicodeCategory.UppercaseLetter or UnicodeCategory.LowercaseLetter or UnicodeCategory.TitlecaseLetter or
        UnicodeCategory.ModifierLetter or UnicodeCategory.OtherLetter or UnicodeCategory.DecimalDigitNumber;
}
