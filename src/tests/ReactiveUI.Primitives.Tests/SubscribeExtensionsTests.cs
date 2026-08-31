// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for observable subscription extension methods.</summary>
public class SubscribeExtensionsTests
{
    /// <summary>A consumer that uses the explicit Primitives subscription name with both extension namespaces globally imported.</summary>
    private const string SubscribePrimitivesConsumerWithGlobalUsings = """
        global using System;
        global using ReactiveUI.Primitives;
        global using System.Reactive.Linq;

        public static class Consumer
        {
            public static IDisposable SubscribeWithoutCallbacks(IObservable<Exception> source) =>
                source.SubscribePrimitives();

            public static IDisposable SubscribeNext(IObservable<Exception> source, Action<Exception> onNext) =>
                source.SubscribePrimitives(onNext);

            public static IDisposable SubscribeNextError(
                IObservable<Exception> source,
                Action<Exception> onNext,
                Action<Exception> onError) =>
                source.SubscribePrimitives(onNext, onError);

            public static IDisposable SubscribeNextCompleted(
                IObservable<Exception> source,
                Action<Exception> onNext,
                Action onCompleted) =>
                source.SubscribePrimitives(onNext, onCompleted);

            public static IDisposable SubscribeAll(
                IObservable<Exception> source,
                Action<Exception> onNext,
                Action<Exception> onError,
                Action onCompleted) =>
                source.SubscribePrimitives(onNext, onError, onCompleted);
        }
        """;

    /// <summary>Verifies every callback shape preserves value and completion delivery.</summary>
    /// <param name="overload">The subscription callback shape to use.</param>
    /// <param name="expectedValues">The number of value callbacks expected.</param>
    /// <param name="expectedCompletions">The number of completion callbacks expected.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments("none", 0, 0)]
    [Arguments("next", 1, 0)]
    [Arguments("next-error", 1, 0)]
    [Arguments("next-completed", 1, 1)]
    [Arguments("all", 1, 1)]
    public async Task SubscribePrimitivesPreservesCallbackShapes(
        string overload,
        int expectedValues,
        int expectedCompletions)
    {
        using Signal<int> source = new();
        List<int> values = [];
        List<Exception> errors = [];
        var completions = 0;
        using var subscription = overload switch
        {
            "none" => source.SubscribePrimitives(),
            "next" => source.SubscribePrimitives(values.Add),
            "next-error" => source.SubscribePrimitives(values.Add, errors.Add),
            "next-completed" => source.SubscribePrimitives(values.Add, () => completions++),
            _ => source.SubscribePrimitives(values.Add, errors.Add, () => completions++),
        };

        source.OnNext(1);
        source.OnCompleted();
        await Assert.That(values.Count).IsEqualTo(expectedValues);
        await Assert.That(completions).IsEqualTo(expectedCompletions);
        await Assert.That(errors).IsEmpty();
    }

    /// <summary>Verifies exception values stay distinct from terminal errors and disposal removes the observer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribePrimitivesHandlesExceptionValuesAndDisposal()
    {
        using Signal<Exception> source = new();
        var value = new InvalidOperationException("value");
        var error = new InvalidOperationException("error");
        List<Exception> values = [];
        List<Exception> errors = [];
        var subscription = source.SubscribePrimitives(values.Add, errors.Add);

        source.OnNext(value);
        await Assert.That(values.Single()).IsSameReferenceAs(value);
        await Assert.That(errors).IsEmpty();

        subscription.Dispose();
        await Assert.That(source.HasObservers).IsFalse();
        source.OnError(error);
        await Assert.That(errors).IsEmpty();
    }

    /// <summary>Verifies the explicit Primitives subscription name avoids System.Reactive Subscribe ambiguity.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles("Builds metadata references from loaded assembly locations.")]
    public async Task SubscribePrimitivesCallbacksCompileWithSystemReactiveAndPrimitivesGlobalUsings()
    {
        var (compilation, syntaxTree, errors) = CompileConsumer(SubscribePrimitivesConsumerWithGlobalUsings);

        await Assert.That(errors).IsEmpty();
        var root = await syntaxTree.GetRootAsync();
        var invocation = root
            .DescendantNodes()
            .OfType<InvocationExpressionSyntax>()
            .First(static node => node.Expression.ToString().EndsWith(".SubscribePrimitives", StringComparison.Ordinal));
        var symbol = compilation.GetSemanticModel(syntaxTree).GetSymbolInfo(invocation).Symbol;

        await Assert.That(symbol?.ContainingNamespace.ToDisplayString()).IsEqualTo("ReactiveUI.Primitives");
        await Assert.That(symbol?.ContainingType.Name).IsEqualTo(nameof(SubscribeExtensions));
    }

    /// <summary>Compiles a consumer with references matching a DynamicData-style transitive System.Reactive dependency.</summary>
    /// <param name="source">The C# source to compile.</param>
    /// <returns>Compilation errors.</returns>
    [RequiresAssemblyFiles("Builds metadata references from loaded assembly locations.")]
    private static (CSharpCompilation Compilation, SyntaxTree SyntaxTree, string[] Errors) CompileConsumer(string source)
    {
        var references = AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!.ToString()!
            .Split(Path.PathSeparator)
            .Select(static path => MetadataReference.CreateFromFile(path))
            .Cast<MetadataReference>()
            .ToList();
        references.Add(MetadataReference.CreateFromFile(typeof(System.Reactive.Unit).Assembly.Location));
        references.Add(MetadataReference.CreateFromFile(typeof(SubscribeExtensions).Assembly.Location));

        var syntaxTree = CSharpSyntaxTree.ParseText(
            source,
            CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview));
        var compilation = CSharpCompilation.Create(
            "SubscribeExtensionsConsumer",
            [syntaxTree],
            references,
            new(OutputKind.DynamicallyLinkedLibrary));

        var errors = compilation.GetDiagnostics()
            .Where(static diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)
            .Select(static diagnostic => diagnostic.ToString())
            .ToArray();
        return (compilation, syntaxTree, errors);
    }
}
