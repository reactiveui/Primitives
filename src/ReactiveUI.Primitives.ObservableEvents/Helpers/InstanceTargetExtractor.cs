// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using ReactiveUI.Primitives.ObservableEvents.CodeGeneration;
using ReactiveUI.Primitives.ObservableEvents.Models;

namespace ReactiveUI.Primitives.ObservableEvents.Helpers;

/// <summary>Turns an <c>Events()</c> call site into the model of the wrapper it asks for.</summary>
internal static class InstanceTargetExtractor
{
    /// <summary>Cheaply rejects syntax that cannot be an activation call.</summary>
    /// <param name="node">The node under consideration.</param>
    /// <param name="cancellationToken">A token that cancels the check.</param>
    /// <returns><see langword="true"/> when the node is a parameterless <c>Events()</c> member invocation.</returns>
    /// <remarks>
    /// This runs on every node of every edited file, so it only looks at shape and spelling. Deciding whether the
    /// call is really ours needs the semantic model, and is left to the transform that runs on the survivors.
    /// </remarks>
    internal static bool IsActivationInvocation(SyntaxNode node, CancellationToken cancellationToken)
    {
        _ = cancellationToken;
        return node is InvocationExpressionSyntax
        {
            ArgumentList.Arguments.Count: 0,
            Expression: MemberAccessExpressionSyntax memberAccess,
        }
        && memberAccess.Name.Identifier.ValueText == Constants.EventMethodName;
    }

    /// <summary>Resolves an activation call into the host it wraps.</summary>
    /// <param name="context">The semantic context for the candidate call.</param>
    /// <param name="cancellationToken">A token that cancels the resolution.</param>
    /// <returns>The requested host, or <see langword="null"/> for an unrelated call.</returns>
    /// <remarks>
    /// The activation placeholder this call will eventually bind to is this generator's own output, and output is
    /// not visible to the pipeline that produced it - so during a run the call resolves to nothing. That absence is
    /// the signal: a call that <em>does</em> resolve belongs to somebody else and is left alone, and a call that
    /// does not is ours to answer. What the request needs is the receiver's type, which binds on its own.
    /// </remarks>
    internal static InstanceTargetModel? Extract(GeneratorSyntaxContext context, CancellationToken cancellationToken)
    {
        var invocation = (InvocationExpressionSyntax)context.Node;
        if (context.SemanticModel.GetSymbolInfo(invocation, cancellationToken).Symbol is IMethodSymbol method
            && method.ContainingType.ToDisplayString() != Constants.ActivationExtensionsDisplayName)
        {
            return null;
        }

        var memberAccess = (MemberAccessExpressionSyntax)invocation.Expression;
        return context.SemanticModel.GetTypeInfo(memberAccess.Expression, cancellationToken).Type
            is INamedTypeSymbol { TypeKind: not TypeKind.Error } receiver
            ? Create(
                receiver.OriginalDefinition,
                LocationInfo.From(invocation.GetLocation()),
                LanguageSupport.SupportsNullableAnnotations(context.SemanticModel.SyntaxTree),
                WellKnownTypes.From(context.SemanticModel.Compilation),
                cancellationToken)
            : null;
    }

    /// <summary>Builds the model for one requested host.</summary>
    /// <param name="host">The host to wrap, reduced to its original definition.</param>
    /// <param name="location">The call site, for diagnostics.</param>
    /// <param name="supportsNullableAnnotations">Whether the consumer's language can express an annotation.</param>
    /// <param name="wellKnownTypes">The task types resolved from the consumer compilation.</param>
    /// <param name="cancellationToken">A token that cancels the walk.</param>
    /// <returns>The host model.</returns>
    /// <remarks>
    /// A generic host is reduced to its original definition so that <c>Foo&lt;int&gt;</c> and
    /// <c>Foo&lt;string&gt;</c> share one wrapper, generic in the same parameters the host is.
    /// </remarks>
    private static InstanceTargetModel Create(
        INamedTypeSymbol host,
        LocationInfo? location,
        bool supportsNullableAnnotations,
        WellKnownTypes wellKnownTypes,
        CancellationToken cancellationToken)
    {
        var typeParameters = SymbolHelpers.CollectTypeParameters(host);
        var typeParameterNames = SymbolHelpers.CreateTypeParameterNames(typeParameters);
        var typeParameterList = SymbolHelpers.BuildTypeParameterList(typeParameters, typeParameterNames);
        var typeReference = SymbolHelpers.Display(host, typeParameterNames, supportsNullableAnnotations);

        // Keyed on the unannotated name, so a generated file does not change identity with the language version.
        var identity = host.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        var displayName = host.ToDisplayString();
        var namespaceName = host.ContainingNamespace.IsGlobalNamespace
            ? string.Empty
            : host.ContainingNamespace.ToDisplayString();
        var wrapperName = GeneratedNames.WrapperName(identity);

        var diagnostics = new List<DiagnosticInfo>();
        var events = EventExtractor.Collect(
            new(host, false, typeParameterNames, supportsNullableAnnotations, location, wellKnownTypes),
            diagnostics,
            cancellationToken);

        if (events.IsEmpty)
        {
            diagnostics.Add(new(
                DiagnosticWarnings.NoEvents,
                location,
                DiagnosticWarnings.InstanceHostKind,
                displayName));
        }

        return new(
            identity,
            displayName,
            SymbolHelpers.EscapeXml(typeReference),
            GeneratedNames.InstanceHintName(identity),
            namespaceName,
            wrapperName,
            BuildWrapperReference(namespaceName, wrapperName, typeParameterList),
            typeReference,
            typeParameterList,
            SymbolHelpers.BuildConstraints(typeParameters, typeParameterNames, supportsNullableAnnotations),
            supportsNullableAnnotations,
            events,
            diagnostics.Count == 0 ? EquatableArray<DiagnosticInfo>.Empty : new([.. diagnostics]),
            location);
    }

    /// <summary>Builds the fully qualified reference to a generated wrapper.</summary>
    /// <param name="namespaceName">The wrapper's namespace, or empty for the global namespace.</param>
    /// <param name="wrapperName">The wrapper class name.</param>
    /// <param name="typeParameterList">The wrapper's type parameter list.</param>
    /// <returns>The fully qualified wrapper reference.</returns>
    private static string BuildWrapperReference(
        string namespaceName,
        string wrapperName,
        string typeParameterList)
    {
        var builder = new PooledStringBuilder();
        _ = builder.Append("global::");
        if (namespaceName.Length > 0)
        {
            _ = builder.Append(namespaceName).Append('.');
        }

        return builder.Append(wrapperName).Append(typeParameterList).ToStringAndReturn();
    }
}
