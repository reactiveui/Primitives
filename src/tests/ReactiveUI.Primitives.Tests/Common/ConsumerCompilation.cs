// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Compiles a snippet of consumer code against the shipped Primitives assemblies so a test can assert what a
/// downstream project's compiler resolves. Overload resolution against a referenced assembly is not observable
/// from inside this assembly, where the same names bind through source rather than metadata.
/// </summary>
public static class ConsumerCompilation
{
    /// <summary>Compiles consumer source with the platform assemblies, System.Reactive, and Primitives referenced.</summary>
    /// <param name="source">The C# source to compile.</param>
    /// <returns>The compilation, its syntax tree, and any compilation errors.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    [RequiresAssemblyFiles("Builds metadata references from loaded assembly locations.")]
    public static (CSharpCompilation Compilation, SyntaxTree SyntaxTree, string[] Errors) Compile(string source)
    {
        ArgumentNullException.ThrowIfNull(source);

        List<MetadataReference> references = [];
        foreach (var path in AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!.ToString()!.Split(Path.PathSeparator))
        {
            references.Add(MetadataReference.CreateFromFile(path));
        }

        references.Add(MetadataReference.CreateFromFile(typeof(System.Reactive.Unit).Assembly.Location));
        references.Add(MetadataReference.CreateFromFile(typeof(SubscribeExtensions).Assembly.Location));
        references.Add(MetadataReference.CreateFromFile(typeof(LinqExtensions).Assembly.Location));

        var syntaxTree = CSharpSyntaxTree.ParseText(
            source,
            CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview));
        var compilation = CSharpCompilation.Create(
            "PrimitivesConsumer",
            [syntaxTree],
            references,
            new(OutputKind.DynamicallyLinkedLibrary));

        List<string> errors = [];
        foreach (var diagnostic in compilation.GetDiagnostics())
        {
            if (diagnostic.Severity == DiagnosticSeverity.Error)
            {
                errors.Add(diagnostic.ToString());
            }
        }

        return (compilation, syntaxTree, errors.ToArray());
    }

    /// <summary>Resolves the method symbol the first invocation of the named method binds to.</summary>
    /// <param name="compilation">The consumer compilation.</param>
    /// <param name="syntaxTree">The consumer syntax tree.</param>
    /// <param name="methodName">The invoked method name.</param>
    /// <returns>The resolved method symbol, or <see langword="null"/> when resolution failed.</returns>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
    public static IMethodSymbol? ResolveInvocation(
        CSharpCompilation compilation,
        SyntaxTree syntaxTree,
        string methodName)
    {
        ArgumentNullException.ThrowIfNull(compilation);

        ArgumentNullException.ThrowIfNull(syntaxTree);

        ArgumentNullException.ThrowIfNull(methodName);

        var model = compilation.GetSemanticModel(syntaxTree);
        foreach (var node in syntaxTree.GetRoot().DescendantNodes())
        {
            if (node is not InvocationExpressionSyntax invocation || InvokedName(invocation) != methodName)
            {
                continue;
            }

            return model.GetSymbolInfo(invocation).Symbol as IMethodSymbol;
        }

        return null;
    }

    /// <summary>Reads the invoked method's simple name, ignoring any receiver and type arguments.</summary>
    /// <param name="invocation">The invocation to read.</param>
    /// <returns>The invoked name, or <see langword="null"/> for an invocation with no simple name.</returns>
    private static string? InvokedName(InvocationExpressionSyntax invocation) => invocation.Expression switch
    {
        MemberAccessExpressionSyntax member => member.Name.Identifier.ValueText,
        SimpleNameSyntax simple => simple.Identifier.ValueText,
        _ => null,
    };
}
