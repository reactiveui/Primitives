// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>
/// Mutates retained protocol-v1 JSON messages for bounded fuzz tests. Each mutation is chosen by a
/// <see cref="ProtocolFuzzRandom"/>, so the same case seed always produces the same bytes.
/// </summary>
internal static class ProtocolFuzzMutator
{
    /// <summary>The number of mutation strategies.</summary>
    private const int StrategyCount = 12;

    /// <summary>The maximum number of stacked mutations per case.</summary>
    private const int MaximumMutations = 4;

    /// <summary>The number of bits in a byte.</summary>
    private const int BitsPerByte = 8;

    /// <summary>The maximum slice length used by duplication and deletion.</summary>
    private const int MaximumSliceLength = 64;

    /// <summary>The maximum nesting depth written by the deep-nesting strategy.</summary>
    private const int MaximumNestingDepth = 160;

    /// <summary>The long string length used by the string strategy.</summary>
    private const int LongStringLength = 5000;

    /// <summary>The odds that the deep-nesting strategy writes objects instead of arrays.</summary>
    private const int ObjectNestingOdds = 2;

    /// <summary>The bit-flip strategy.</summary>
    private const int BitFlipStrategy = 0;

    /// <summary>The interesting-byte strategy.</summary>
    private const int InterestingByteStrategy = 1;

    /// <summary>The truncation strategy.</summary>
    private const int TruncateStrategy = 2;

    /// <summary>The slice duplication strategy.</summary>
    private const int DuplicateSliceStrategy = 3;

    /// <summary>The slice deletion strategy.</summary>
    private const int DeleteSliceStrategy = 4;

    /// <summary>The huge-number strategy.</summary>
    private const int HugeNumberStrategy = 5;

    /// <summary>The deep-nesting strategy.</summary>
    private const int DeepNestingStrategy = 6;

    /// <summary>The invalid UTF-8 strategy.</summary>
    private const int InvalidUtf8Strategy = 7;

    /// <summary>The unknown-member strategy.</summary>
    private const int UnknownMemberStrategy = 8;

    /// <summary>The type-confusion strategy.</summary>
    private const int TypeConfusionStrategy = 9;

    /// <summary>The duplicate-member strategy.</summary>
    private const int DuplicateMemberStrategy = 10;

    /// <summary>The strict UTF-8 encoding used to recognize text mutations.</summary>
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    /// <summary>Bytes that commonly break JSON readers.</summary>
    private static readonly byte[] InterestingBytes = [0x00, 0x0A, 0x22, 0x2C, 0x2D, 0x30, 0x3A, 0x5B, 0x5C, 0x5D, 0x7B, 0x7D, 0x7F, 0x80, 0xC0, 0xFF];

    /// <summary>Invalid UTF-8 sequences.</summary>
    private static readonly byte[][] InvalidUtf8Sequences = [[0x80], [0xC0, 0xAF], [0xED, 0xA0, 0x80], [0xF8, 0x88, 0x80, 0x80, 0x80], [0xFF, 0xFE], [0xE2, 0x82]];

    /// <summary>Number tokens that overflow, underflow or change the numeric type.</summary>
    private static readonly string[] HugeNumbers =
    [
        "99999999999999999999999999999", "-9223372036854775809", "9223372036854775808", "2147483648", "-2147483649", "1e400",
        "-1e-400", "1.5", "-0", "0.0", "1E2", "00", "-1", "4294967296", "1797693134862315708145274237317043567981",
    ];

    /// <summary>Replacement values of every JSON type.</summary>
    private static readonly string[] TypeConfusionValues =
    [
        "null", "true", "false", "0", "-1", "\"\"", "\"x\"", "{}", "[]", "[null]", "{\"a\":1}", "\"00000000-0000-0000-0000-000000000000\"",
        "\"\\ud800\"", "\"\\u0000\"", "\"not base64!\"", "\"9999-12-31T23:59:59.9999999+14:00\"", "\"0001-01-01T00:00:00+00:00\"",
    ];

    /// <summary>Unknown members injected into objects.</summary>
    private static readonly string[] UnknownMembers =
    [
        "\"futureField\":1,", "\"futureField\":{\"nested\":[1,2,3]},", "\"\":null,", "\"__proto__\":{},", "\"$type\":\"System.Object\",",
    ];

    /// <summary>Applies one to four stacked mutations to a message.</summary>
    /// <param name="original">The original message bytes.</param>
    /// <param name="random">The case generator.</param>
    /// <returns>The mutated bytes.</returns>
    internal static byte[] Mutate(byte[] original, ProtocolFuzzRandom random)
    {
        var bytes = original;
        var count = random.Next(1, MaximumMutations + 1);
        for (var index = 0; index < count; index++)
        {
            bytes = MutateOnce(bytes, random);
        }

        return bytes;
    }

    /// <summary>Applies one mutation strategy.</summary>
    /// <param name="bytes">The message bytes.</param>
    /// <param name="random">The case generator.</param>
    /// <returns>The mutated bytes.</returns>
    private static byte[] MutateOnce(byte[] bytes, ProtocolFuzzRandom random)
    {
        if (bytes.Length == 0)
        {
            return [(byte)'{'];
        }

        var strategy = random.Next(StrategyCount);
        return strategy switch
        {
            BitFlipStrategy => FlipBit(bytes, random),
            InterestingByteStrategy => SetInterestingByte(bytes, random),
            TruncateStrategy => bytes.AsSpan(0, random.Next(bytes.Length)).ToArray(),
            DuplicateSliceStrategy => DuplicateSlice(bytes, random),
            DeleteSliceStrategy => DeleteSlice(bytes, random),
            InvalidUtf8Strategy => Insert(bytes, random.Next(bytes.Length + 1), random.Pick(InvalidUtf8Sequences)),
            _ => MutateText(bytes, strategy, random),
        };
    }

    /// <summary>Applies a JSON-aware mutation when the message is valid UTF-8 text.</summary>
    /// <param name="bytes">The message bytes.</param>
    /// <param name="strategy">The strategy.</param>
    /// <param name="random">The case generator.</param>
    /// <returns>The mutated bytes.</returns>
    private static byte[] MutateText(byte[] bytes, int strategy, ProtocolFuzzRandom random)
    {
        string text;
        try
        {
            text = StrictUtf8.GetString(bytes);
        }
        catch (DecoderFallbackException)
        {
            return FlipBit(bytes, random);
        }

        var mutated = strategy switch
        {
            HugeNumberStrategy => ReplaceValue(text, random, static value => value.Length > 0 && (value[0] == '-' || char.IsDigit(value[0])), random.Pick(HugeNumbers)),
            DeepNestingStrategy => ReplaceValue(text, random, static _ => true, CreateNesting(random)),
            UnknownMemberStrategy => InsertUnknownMember(text, random),
            TypeConfusionStrategy => ReplaceValue(text, random, static _ => true, random.Pick(TypeConfusionValues)),
            DuplicateMemberStrategy => DuplicateMember(text, random),
            _ => ReplaceValue(text, random, static value => value.Length > 0 && value[0] == '"', CreateChaoticString(random)),
        };
        return Encoding.UTF8.GetBytes(mutated);
    }

    /// <summary>Flips one random bit.</summary>
    /// <param name="bytes">The message bytes.</param>
    /// <param name="random">The case generator.</param>
    /// <returns>The mutated bytes.</returns>
    private static byte[] FlipBit(byte[] bytes, ProtocolFuzzRandom random)
    {
        var copy = (byte[])bytes.Clone();
        var index = random.Next(copy.Length);
        copy[index] ^= (byte)(1 << random.Next(BitsPerByte));
        return copy;
    }

    /// <summary>Overwrites one random byte with a byte that commonly breaks readers.</summary>
    /// <param name="bytes">The message bytes.</param>
    /// <param name="random">The case generator.</param>
    /// <returns>The mutated bytes.</returns>
    private static byte[] SetInterestingByte(byte[] bytes, ProtocolFuzzRandom random)
    {
        var copy = (byte[])bytes.Clone();
        copy[random.Next(copy.Length)] = random.Pick(InterestingBytes);
        return copy;
    }

    /// <summary>Duplicates a random slice in place.</summary>
    /// <param name="bytes">The message bytes.</param>
    /// <param name="random">The case generator.</param>
    /// <returns>The mutated bytes.</returns>
    private static byte[] DuplicateSlice(byte[] bytes, ProtocolFuzzRandom random)
    {
        var start = random.Next(bytes.Length);
        var length = Math.Min(random.Next(1, MaximumSliceLength), bytes.Length - start);
        return Insert(bytes, start, bytes.AsSpan(start, length).ToArray());
    }

    /// <summary>Deletes a random slice.</summary>
    /// <param name="bytes">The message bytes.</param>
    /// <param name="random">The case generator.</param>
    /// <returns>The mutated bytes.</returns>
    private static byte[] DeleteSlice(byte[] bytes, ProtocolFuzzRandom random)
    {
        var start = random.Next(bytes.Length);
        var length = Math.Min(random.Next(1, MaximumSliceLength), bytes.Length - start);
        var result = new byte[bytes.Length - length];
        bytes.AsSpan(0, start).CopyTo(result);
        bytes.AsSpan(start + length).CopyTo(result.AsSpan(start));
        return result;
    }

    /// <summary>Inserts bytes at a position.</summary>
    /// <param name="bytes">The message bytes.</param>
    /// <param name="position">The insert position.</param>
    /// <param name="inserted">The inserted bytes.</param>
    /// <returns>The mutated bytes.</returns>
    private static byte[] Insert(byte[] bytes, int position, byte[] inserted)
    {
        var result = new byte[bytes.Length + inserted.Length];
        bytes.AsSpan(0, position).CopyTo(result);
        inserted.CopyTo(result.AsSpan(position));
        bytes.AsSpan(position).CopyTo(result.AsSpan(position + inserted.Length));
        return result;
    }

    /// <summary>Creates a nested array or object value.</summary>
    /// <param name="random">The case generator.</param>
    /// <returns>The nested JSON value.</returns>
    private static string CreateNesting(ProtocolFuzzRandom random)
    {
        var depth = random.Next(1, MaximumNestingDepth);
        var useObjects = random.OneIn(ObjectNestingOdds);
        var builder = new StringBuilder();
        for (var index = 0; index < depth; index++)
        {
            _ = builder.Append(useObjects ? "{\"a\":" : "[");
        }

        _ = builder.Append('0');
        for (var index = 0; index < depth; index++)
        {
            _ = builder.Append(useObjects ? '}' : ']');
        }

        return builder.ToString();
    }

    /// <summary>Creates a hostile JSON string value.</summary>
    /// <param name="random">The case generator.</param>
    /// <returns>The JSON string literal.</returns>
    private static string CreateChaoticString(ProtocolFuzzRandom random)
    {
        string[] values =
        [
            "\"\"", "\" \"", $"\"{new string('a', LongStringLength)}\"", "\"\\udc00\\ud800\"", "\"\\u0001\\u001f\"", "\"..\\/..\\/etc\"",
            "\"%00\"", "\"\\\"\"", "\"\u00e9\u0301\"", "\"\U0001F600\"", $"\"{new string('\u00e9', LongStringLength)}\"",
        ];
        return random.Pick(values);
    }

    /// <summary>Replaces one random member value that matches a predicate.</summary>
    /// <param name="text">The JSON text.</param>
    /// <param name="random">The case generator.</param>
    /// <param name="predicate">The value filter applied to the original value text.</param>
    /// <param name="replacement">The replacement value.</param>
    /// <returns>The mutated JSON text.</returns>
    private static string ReplaceValue(string text, ProtocolFuzzRandom random, Func<string, bool> predicate, string replacement)
    {
        List<(int Start, int End)> candidates = [];
        foreach (var start in FindMemberValueStarts(text))
        {
            var end = FindValueEnd(text, start);
            if (end > start && predicate(text.Substring(start, end - start)))
            {
                candidates.Add((start, end));
            }
        }

        if (candidates.Count == 0)
        {
            return text + replacement;
        }

        var (valueStart, valueEnd) = random.Pick(candidates);
        return string.Concat(text.AsSpan(0, valueStart), replacement, text.AsSpan(valueEnd));
    }

    /// <summary>Inserts an unknown member at the start of a random object.</summary>
    /// <param name="text">The JSON text.</param>
    /// <param name="random">The case generator.</param>
    /// <returns>The mutated JSON text.</returns>
    private static string InsertUnknownMember(string text, ProtocolFuzzRandom random)
    {
        var openings = FindStructuralCharacters(text, '{');
        if (openings.Count == 0)
        {
            return text;
        }

        var position = random.Pick(openings) + 1;
        var member = random.Pick(UnknownMembers);
        var closesImmediately = position < text.Length && text[position] == '}';
        return text.Insert(position, closesImmediately ? member.TrimEnd(',') : member);
    }

    /// <summary>Repeats one random member after itself.</summary>
    /// <param name="text">The JSON text.</param>
    /// <param name="random">The case generator.</param>
    /// <returns>The mutated JSON text.</returns>
    private static string DuplicateMember(string text, ProtocolFuzzRandom random)
    {
        var starts = FindMemberValueStarts(text);
        if (starts.Count == 0)
        {
            return text;
        }

        var valueStart = random.Pick(starts);
        var nameEnd = text.LastIndexOf('"', valueStart - 1);
        var nameStart = nameEnd > 0 ? text.LastIndexOf('"', nameEnd - 1) : -1;
        var valueEnd = FindValueEnd(text, valueStart);
        return nameStart < 0 ? text : text.Insert(valueEnd, string.Concat(",", text.AsSpan(nameStart, valueEnd - nameStart)));
    }

    /// <summary>Finds the first character of every object member value outside strings.</summary>
    /// <param name="text">The JSON text.</param>
    /// <returns>The value start positions.</returns>
    private static List<int> FindMemberValueStarts(string text)
    {
        var colons = FindStructuralCharacters(text, ':');
        for (var index = 0; index < colons.Count; index++)
        {
            colons[index]++;
        }

        return colons;
    }

    /// <summary>Finds a structural character outside JSON strings.</summary>
    /// <param name="text">The JSON text.</param>
    /// <param name="target">The structural character.</param>
    /// <returns>The positions of the character.</returns>
    private static List<int> FindStructuralCharacters(string text, char target)
    {
        List<int> positions = [];
        var inString = false;
        for (var index = 0; index < text.Length; index++)
        {
            var character = text[index];
            if (inString)
            {
                index += character == '\\' ? 1 : 0;
                inString = character != '"';
                continue;
            }

            inString = character == '"';
            if (character == target)
            {
                positions.Add(index);
            }
        }

        return positions;
    }

    /// <summary>Finds the exclusive end of a JSON value that starts at a position.</summary>
    /// <param name="text">The JSON text.</param>
    /// <param name="start">The value start.</param>
    /// <returns>The exclusive value end.</returns>
    private static int FindValueEnd(string text, int start) =>
        start >= text.Length
            ? start
            : text[start] switch
            {
                '"' => FindStringEnd(text, start),
                '{' or '[' => FindContainerEnd(text, start),
                _ => FindScalarEnd(text, start),
            };

    /// <summary>Finds the exclusive end of a JSON string.</summary>
    /// <param name="text">The JSON text.</param>
    /// <param name="start">The opening quote position.</param>
    /// <returns>The exclusive end.</returns>
    private static int FindStringEnd(string text, int start)
    {
        for (var index = start + 1; index < text.Length; index++)
        {
            if (text[index] == '\\')
            {
                index++;
                continue;
            }

            if (text[index] == '"')
            {
                return index + 1;
            }
        }

        return text.Length;
    }

    /// <summary>Finds the exclusive end of a JSON object or array.</summary>
    /// <param name="text">The JSON text.</param>
    /// <param name="start">The opening bracket position.</param>
    /// <returns>The exclusive end.</returns>
    private static int FindContainerEnd(string text, int start)
    {
        var depth = 0;
        for (var index = start; index < text.Length; index++)
        {
            var character = text[index];
            if (character == '"')
            {
                index = FindStringEnd(text, index) - 1;
                continue;
            }

            depth += character is '{' or '[' ? 1 : 0;
            depth -= character is '}' or ']' ? 1 : 0;
            if (depth == 0)
            {
                return index + 1;
            }
        }

        return text.Length;
    }

    /// <summary>Finds the exclusive end of a JSON scalar.</summary>
    /// <param name="text">The JSON text.</param>
    /// <param name="start">The scalar start.</param>
    /// <returns>The exclusive end.</returns>
    private static int FindScalarEnd(string text, int start)
    {
        var index = start;
        while (index < text.Length && text[index] is not (',' or '}' or ']'))
        {
            index++;
        }

        return index;
    }
}
