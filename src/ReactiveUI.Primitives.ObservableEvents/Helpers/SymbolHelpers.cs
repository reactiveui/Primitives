// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using ReactiveUI.Primitives.ObservableEvents.CodeGeneration;

namespace ReactiveUI.Primitives.ObservableEvents.Helpers;

/// <summary>Renders symbols as the source fragments a model carries, so no symbol reaches the emitter.</summary>
internal static class SymbolHelpers
{
    /// <summary>How much room an escaped documentation string is expected to need beyond the original.</summary>
    private const int EscapeGrowthFactor = 2;

    /// <summary>The characters that cannot appear literally in a generated documentation comment.</summary>
    private static readonly char[] XmlSpecialCharacters = ['&', '<', '>', '"', '\''];

    /// <summary>Renders a type without nullable annotations, for a consumer whose language predates them.</summary>
    private static readonly SymbolDisplayFormat ObliviousFormat = SymbolDisplayFormat.FullyQualifiedFormat;

    /// <summary>Renders a type with its nullable annotations.</summary>
    /// <remarks>
    /// The generated handler has to match the delegate it is assigned to exactly. A delegate declared with an
    /// annotated parameter - <c>EventHandler</c> and its sender being the one nearly every event goes through -
    /// does not match a handler that declares the same parameter unannotated, and the consumer's build says so.
    /// </remarks>
    private static readonly SymbolDisplayFormat AnnotatedFormat =
        SymbolDisplayFormat.FullyQualifiedFormat.AddMiscellaneousOptions(
            SymbolDisplayMiscellaneousOptions.IncludeNullableReferenceTypeModifier);

    /// <summary>Renders a type as a fully qualified reference, substituting renamed type parameters.</summary>
    /// <param name="symbol">The type to render.</param>
    /// <param name="typeParameterNames">The generated type-parameter names, or null when none were renamed.</param>
    /// <param name="supportsNullableAnnotations">Whether the consumer's language can express an annotation.</param>
    /// <returns>The fully qualified type reference.</returns>
    /// <remarks>
    /// Walking display parts rather than the finished string is what makes the substitution safe: a part carrying a
    /// type parameter is identified by its symbol, so a parameter named <c>T</c> is replaced while a type whose name
    /// merely contains <c>T</c> is left alone.
    /// </remarks>
    internal static string Display(
        ITypeSymbol symbol,
        IReadOnlyDictionary<ITypeParameterSymbol, string>? typeParameterNames,
        bool supportsNullableAnnotations)
    {
        var format = supportsNullableAnnotations ? AnnotatedFormat : ObliviousFormat;
        if (typeParameterNames is null || typeParameterNames.Count == 0)
        {
            return symbol.ToDisplayString(format);
        }

        var builder = new PooledStringBuilder();
        foreach (var part in symbol.ToDisplayParts(format))
        {
            if (part.Symbol is ITypeParameterSymbol parameter
                && typeParameterNames.TryGetValue(parameter, out var replacement))
            {
                _ = builder.Append(replacement);
                continue;
            }

            _ = builder.Append(part.ToString());
        }

        return builder.ToStringAndReturn();
    }

    /// <summary>Collects the type parameters a wrapper has to redeclare, outermost container first.</summary>
    /// <param name="type">The host type.</param>
    /// <returns>The complete ordered type-parameter list.</returns>
    /// <remarks>
    /// A wrapper for a nested generic sits at namespace level, so it has to redeclare every parameter its host
    /// inherits from its containing types as well as its own.
    /// </remarks>
    internal static List<ITypeParameterSymbol> CollectTypeParameters(INamedTypeSymbol type)
    {
        var containers = new Stack<INamedTypeSymbol>();
        for (INamedTypeSymbol? current = type; current is not null; current = current.ContainingType)
        {
            containers.Push(current);
        }

        var result = new List<ITypeParameterSymbol>();
        while (containers.Count > 0)
        {
            result.AddRange(containers.Pop().TypeParameters);
        }

        return result;
    }

    /// <summary>Assigns each type parameter a name that is unique across the flattened list.</summary>
    /// <param name="typeParameters">The ordered type parameters.</param>
    /// <returns>The symbol-to-generated-name mapping.</returns>
    /// <remarks>
    /// Flattening a nested generic can collide two parameters that were distinct in their own scopes -
    /// <c>Outer&lt;T&gt;.Inner&lt;T&gt;</c> being the usual shape - so the second one is suffixed rather than
    /// silently shadowing the first.
    /// </remarks>
    internal static Dictionary<ITypeParameterSymbol, string> CreateTypeParameterNames(
        List<ITypeParameterSymbol> typeParameters)
    {
        var result = new Dictionary<ITypeParameterSymbol, string>(SymbolEqualityComparer.Default);
        var usedNames = new HashSet<string>(StringComparer.Ordinal);
        foreach (var parameter in typeParameters)
        {
            var name = parameter.Name;
            var suffix = 1;
            while (!usedNames.Add(name))
            {
                suffix++;
                name = parameter.Name + suffix.ToString(CultureInfo.InvariantCulture);
            }

            result.Add(parameter, EscapeIdentifier(name));
        }

        return result;
    }

    /// <summary>Renders the type-parameter declaration list.</summary>
    /// <param name="typeParameters">The ordered type parameters.</param>
    /// <param name="typeParameterNames">The generated type-parameter names.</param>
    /// <returns>The declaration list, or an empty string for a non-generic host.</returns>
    internal static string BuildTypeParameterList(
        List<ITypeParameterSymbol> typeParameters,
        IReadOnlyDictionary<ITypeParameterSymbol, string> typeParameterNames)
    {
        if (typeParameters.Count == 0)
        {
            return string.Empty;
        }

        var builder = new PooledStringBuilder();
        _ = builder.Append('<');
        for (var index = 0; index < typeParameters.Count; index++)
        {
            if (index > 0)
            {
                _ = builder.Append(", ");
            }

            _ = builder.Append(typeParameterNames[typeParameters[index]]);
        }

        return builder.Append('>').ToStringAndReturn();
    }

    /// <summary>Renders the generic constraint clauses, one per line and without indentation.</summary>
    /// <param name="typeParameters">The ordered type parameters.</param>
    /// <param name="typeParameterNames">The generated type-parameter names.</param>
    /// <param name="supportsNullableAnnotations">Whether the consumer's language can express an annotation.</param>
    /// <returns>The constraint clauses, or an empty string when nothing is constrained.</returns>
    /// <remarks>
    /// Left unindented because the same clauses are emitted at two different depths - once on the wrapper class and
    /// once on the activation overload - and the emitter is what knows which.
    /// </remarks>
    internal static string BuildConstraints(
        List<ITypeParameterSymbol> typeParameters,
        IReadOnlyDictionary<ITypeParameterSymbol, string> typeParameterNames,
        bool supportsNullableAnnotations)
    {
        var builder = new PooledStringBuilder();
        foreach (var parameter in typeParameters)
        {
            var clause = new PooledStringBuilder();
            AppendConstraintClause(clause, parameter, typeParameterNames, supportsNullableAnnotations);
            if (clause.Length == 0)
            {
                clause.Return();
                continue;
            }

            _ = builder.Append("where ").Append(typeParameterNames[parameter]).Append(" : ")
                .Append(clause).AppendLine();
        }

        return builder.ToStringAndReturn();
    }

    /// <summary>Escapes an identifier that collides with a C# keyword.</summary>
    /// <param name="value">The identifier text.</param>
    /// <returns>The escaped identifier.</returns>
    internal static string EscapeIdentifier(string value) =>
        SyntaxFacts.GetKeywordKind(value) == SyntaxKind.None ? value : $"@{value}";

    /// <summary>Escapes text destined for a generated documentation comment.</summary>
    /// <param name="value">The text to escape.</param>
    /// <returns>The escaped text, or the original instance when nothing needed escaping.</returns>
    internal static string EscapeXml(string value)
    {
        // Most references have nothing to escape, so the scan is what keeps the common case allocation-free.
        if (value.IndexOfAny(XmlSpecialCharacters) < 0)
        {
            return value;
        }

        var builder = new PooledStringBuilder(value.Length * EscapeGrowthFactor);
        for (var current = 0; current < value.Length; current++)
        {
            _ = value[current] switch
            {
                '&' => builder.Append("&amp;"),
                '<' => builder.Append("&lt;"),
                '>' => builder.Append("&gt;"),
                '"' => builder.Append("&quot;"),
                '\'' => builder.Append("&apos;"),
                var character => builder.Append(character),
            };
        }

        return builder.ToStringAndReturn();
    }

    /// <summary>Appends one type parameter's comma-separated constraints.</summary>
    /// <param name="builder">The destination builder.</param>
    /// <param name="parameter">The constrained type parameter.</param>
    /// <param name="typeParameterNames">The generated type-parameter names.</param>
    /// <param name="supportsNullableAnnotations">Whether the consumer's language can express an annotation.</param>
    /// <remarks>
    /// The primary constraint has to come first and only one of the four forms may appear, which is why they are
    /// tested in order rather than accumulated.
    /// </remarks>
    private static void AppendConstraintClause(
        PooledStringBuilder builder,
        ITypeParameterSymbol parameter,
        IReadOnlyDictionary<ITypeParameterSymbol, string> typeParameterNames,
        bool supportsNullableAnnotations)
    {
        var primary = SelectPrimaryConstraint(parameter, supportsNullableAnnotations);
        if (primary.Length > 0)
        {
            _ = builder.Append(primary);
        }

        foreach (var constraintType in parameter.ConstraintTypes)
        {
            _ = AppendSeparator(builder)
                .Append(Display(constraintType, typeParameterNames, supportsNullableAnnotations));
        }

        if (!parameter.HasConstructorConstraint)
        {
            return;
        }

        _ = AppendSeparator(builder).Append("new()");
    }

    /// <summary>Selects the single primary constraint a type parameter may carry.</summary>
    /// <param name="parameter">The type parameter.</param>
    /// <param name="supportsNullableAnnotations">Whether the consumer's language can express an annotation.</param>
    /// <returns>The primary constraint keyword, or an empty string when there is none.</returns>
    private static string SelectPrimaryConstraint(ITypeParameterSymbol parameter, bool supportsNullableAnnotations)
    {
        if (parameter.HasUnmanagedTypeConstraint)
        {
            return "unmanaged";
        }

        if (parameter.HasValueTypeConstraint)
        {
            return "struct";
        }

        if (parameter.HasReferenceTypeConstraint)
        {
            // A referenced assembly can declare `class?` whatever the consumer's language version is, so the
            // annotation has to be dropped rather than repeated when the consumer could not have written it.
            return supportsNullableAnnotations
                && parameter.ReferenceTypeConstraintNullableAnnotation == NullableAnnotation.Annotated
                ? "class?"
                : "class";
        }

        return parameter.HasNotNullConstraint ? "notnull" : string.Empty;
    }

    /// <summary>Appends the constraint separator when something has already been written.</summary>
    /// <param name="builder">The destination builder.</param>
    /// <returns>The builder, for chaining.</returns>
    private static PooledStringBuilder AppendSeparator(PooledStringBuilder builder) =>
        builder.Length == 0 ? builder : builder.Append(", ");
}
