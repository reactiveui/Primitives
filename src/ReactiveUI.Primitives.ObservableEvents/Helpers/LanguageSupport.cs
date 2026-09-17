// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace ReactiveUI.Primitives.ObservableEvents.Helpers;

/// <summary>What the consumer's language version lets the generated source say.</summary>
internal static class LanguageSupport
{
    /// <summary>Determines whether the C# language version supports nullable annotations and directives.</summary>
    /// <param name="tree">A syntax tree from the consumer, which carries the language version it was parsed at.</param>
    /// <returns><see langword="true"/> when annotations and the nullable directive may be emitted.</returns>
    internal static bool SupportsNullableAnnotations(SyntaxTree tree) =>
        ((CSharpParseOptions)tree.Options).LanguageVersion >= LanguageVersion.CSharp8;
}
