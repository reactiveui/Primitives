// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace ReactiveUI.Primitives.ObservableEvents.Helpers;

/// <summary>What the consumer's language version lets the generated source say.</summary>
internal static class LanguageSupport
{
    /// <summary>Determines whether the consumer's language can express a nullable reference type.</summary>
    /// <param name="tree">A syntax tree from the consumer, which carries the language version it was parsed at.</param>
    /// <returns><see langword="true"/> when annotations and the nullable directive may be emitted.</returns>
    /// <remarks>
    /// Everything about the generated file's nullability follows from this one answer: whether it opens with
    /// <c>#nullable enable</c>, and whether the handler signatures carry the annotations that make them match the
    /// delegates they are assigned to. Emitting either against an older language version is a compile error in the
    /// consumer's build, so both are decided together and from the same place.
    /// </remarks>
    /// <remarks>
    /// The cast is safe by registration: the generator is declared for C# only, so every tree it is ever handed
    /// was parsed with C# options.
    /// </remarks>
    internal static bool SupportsNullableAnnotations(SyntaxTree tree) =>
        ((CSharpParseOptions)tree.Options).LanguageVersion >= LanguageVersion.CSharp8;
}
