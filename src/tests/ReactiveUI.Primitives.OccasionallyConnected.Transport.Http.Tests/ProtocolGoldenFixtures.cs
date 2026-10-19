// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Loads the protocol-v1 golden fixtures that are retained in source control.</summary>
internal static class ProtocolGoldenFixtures
{
    /// <summary>The fixture root folder name.</summary>
    private const string RootFolderName = "GoldenFixtures";

    /// <summary>The protocol-v1 fixture folder name.</summary>
    private const string ProtocolFolderName = "protocol-v1";

    /// <summary>The strict UTF-8 encoding used for fixture text.</summary>
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    /// <summary>Reads a fixture as canonical text with line endings normalized and the final line break removed.</summary>
    /// <param name="name">The fixture file name.</param>
    /// <returns>The canonical fixture text.</returns>
    internal static string ReadText(string name)
    {
        var text = StrictUtf8.GetString(File.ReadAllBytes(GetPath(name)));
        return NormalizeLineEndings(text).TrimEnd('\n');
    }

    /// <summary>Reads a fixture as canonical UTF-8 bytes.</summary>
    /// <param name="name">The fixture file name.</param>
    /// <returns>The canonical fixture bytes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static byte[] ReadBytes(string name) => StrictUtf8.GetBytes(ReadText(name));

    /// <summary>Reads the non-empty, non-comment lines of a line-oriented fixture.</summary>
    /// <param name="name">The fixture file name.</param>
    /// <returns>The fixture lines.</returns>
    internal static string[] ReadLines(string name)
    {
        var lines = ReadText(name).Split('\n');
        List<string> result = [];
        for (var index = 0; index < lines.Length; index++)
        {
            var line = lines[index];
            if (line.Length is 0 || line[0] == '#')
            {
                continue;
            }

            result.Add(line);
        }

        return [.. result];
    }

    /// <summary>Decodes encoder output as canonical text for byte-identical comparison.</summary>
    /// <param name="bytes">The encoded bytes.</param>
    /// <returns>The encoded text.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static string ToText(byte[] bytes) => StrictUtf8.GetString(bytes);

    /// <summary>Gets the absolute path of a fixture copied to the test output folder.</summary>
    /// <param name="name">The fixture file name.</param>
    /// <returns>The fixture path.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string GetPath(string name) => Path.Combine(AppContext.BaseDirectory, RootFolderName, ProtocolFolderName, name);

    /// <summary>Normalizes Windows and classic Mac line endings to LF.</summary>
    /// <param name="text">The text to normalize.</param>
    /// <returns>The normalized text.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string NormalizeLineEndings(string text) =>
        text.Replace("\r\n", "\n", StringComparison.Ordinal).Replace('\r', '\n');
}
