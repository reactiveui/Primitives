// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using ReactiveUI.Primitives.ObservableEvents.Models;

namespace ReactiveUI.Primitives.ObservableEvents.Helpers;

/// <summary>Turns each <c>GenerateStaticEventObservables</c> application into the model of the host it names.</summary>
/// <remarks>
/// Matched on how the attribute is written rather than on the symbol it binds to. The attribute is declared by this
/// generator's own output, and output is not visible to the pipeline that produced it, so there is no symbol to
/// match against while the pipeline runs. What the request actually needs - the host type - comes from the
/// <c>typeof</c> argument, which binds on its own.
/// </remarks>
internal static class StaticTargetExtractor
{
    /// <summary>Cheaply rejects syntax that cannot be a static generation request.</summary>
    /// <param name="node">The node under consideration.</param>
    /// <param name="cancellationToken">A token that cancels the check.</param>
    /// <returns><see langword="true"/> when the node is an assembly-targeted request attribute.</returns>
    internal static bool IsStaticRequestAttribute(SyntaxNode node, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        return node is AttributeSyntax attribute
            && attribute.Parent is AttributeListSyntax { Target: { } target }
            && target.Identifier.IsKind(SyntaxKind.AssemblyKeyword)
            && IsRequestAttributeName(attribute.Name);
    }

    /// <summary>Resolves one static request into the host it names.</summary>
    /// <param name="context">The semantic context for the candidate attribute.</param>
    /// <param name="cancellationToken">A token that cancels the resolution.</param>
    /// <returns>The requested host, or <see langword="null"/> when the attribute names nothing usable.</returns>
    /// <remarks>
    /// An attribute that names nothing usable - written without an argument, or with one that is not a type - is
    /// skipped rather than diagnosed: the consumer is already being told about it by the compiler, and a half-typed
    /// attribute should not add a second complaint on every keystroke.
    /// </remarks>
    internal static StaticTargetModel? Extract(GeneratorSyntaxContext context, CancellationToken cancellationToken)
    {
        var attribute = (AttributeSyntax)context.Node;
        return attribute.ArgumentList is { Arguments.Count: 1 } arguments
            && arguments.Arguments[0].Expression is TypeOfExpressionSyntax typeOfExpression
            && context.SemanticModel.GetSymbolInfo(typeOfExpression.Type, cancellationToken).Symbol
                is INamedTypeSymbol host
            ? Create(
                host.OriginalDefinition,
                LocationInfo.From(attribute.GetLocation()),
                LanguageSupport.SupportsNullableAnnotations(context.SemanticModel.SyntaxTree),
                WellKnownTypes.From(context.SemanticModel.Compilation),
                cancellationToken)
            : null;
    }

    /// <summary>Determines whether an attribute is written with this generator's request name.</summary>
    /// <param name="name">The attribute name as written.</param>
    /// <returns><see langword="true"/> when the name matches, with or without the suffix.</returns>
    private static bool IsRequestAttributeName(NameSyntax name)
    {
        var identifier = SelectRightmostIdentifier(name);
        return identifier is Constants.StaticRequestAttributeName
            or Constants.StaticRequestAttributeQualifiedName;
    }

    /// <summary>Gets the last identifier of a possibly qualified attribute name.</summary>
    /// <param name="name">The attribute name as written.</param>
    /// <returns>The rightmost identifier, or an empty string for a name with none.</returns>
    private static string SelectRightmostIdentifier(NameSyntax name) => name switch
    {
        IdentifierNameSyntax identifier => identifier.Identifier.ValueText,
        QualifiedNameSyntax qualified => SelectRightmostIdentifier(qualified.Right),
        AliasQualifiedNameSyntax aliased => aliased.Name.Identifier.ValueText,
        _ => string.Empty,
    };

    /// <summary>Builds the model for one requested static host.</summary>
    /// <param name="host">The host to expose, reduced to its original definition.</param>
    /// <param name="location">The attribute application, for diagnostics.</param>
    /// <param name="supportsNullableAnnotations">Whether the consumer's language can express an annotation.</param>
    /// <param name="wellKnownTypes">The task types resolved from the consumer compilation.</param>
    /// <param name="cancellationToken">A token that cancels the walk.</param>
    /// <returns>The host model.</returns>
    /// <remarks>
    /// A generic host is refused outright: its static events belong to each closed construction rather than to the
    /// open type, and the generated class has no receiver to infer type arguments from.
    /// </remarks>
    private static StaticTargetModel Create(
        INamedTypeSymbol host,
        LocationInfo? location,
        bool supportsNullableAnnotations,
        WellKnownTypes wellKnownTypes,
        CancellationToken cancellationToken)
    {
        var identity = host.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        var displayName = host.ToDisplayString();
        var namespaceName = host.ContainingNamespace.IsGlobalNamespace
            ? string.Empty
            : host.ContainingNamespace.ToDisplayString();

        if (host.IsGenericType)
        {
            return new(
                identity,
                displayName,
                namespaceName,
                supportsNullableAnnotations,
                EquatableArray<EventModel>.Empty,
                new([
                    new DiagnosticInfo(
                        DiagnosticWarnings.UnsupportedEvent,
                        location,
                        displayName,
                        DiagnosticWarnings.GenericStaticHostReason),
                ]),
                location);
        }

        var diagnostics = new List<DiagnosticInfo>();
        var events = EventExtractor.Collect(
            new(host, true, null, supportsNullableAnnotations, location, wellKnownTypes),
            diagnostics,
            cancellationToken);

        if (events.IsEmpty)
        {
            diagnostics.Add(new(
                DiagnosticWarnings.NoEvents,
                location,
                DiagnosticWarnings.StaticHostKind,
                displayName));
        }

        return new(
            identity,
            displayName,
            namespaceName,
            supportsNullableAnnotations,
            events,
            diagnostics.Count == 0 ? EquatableArray<DiagnosticInfo>.Empty : new([.. diagnostics]),
            location);
    }
}
