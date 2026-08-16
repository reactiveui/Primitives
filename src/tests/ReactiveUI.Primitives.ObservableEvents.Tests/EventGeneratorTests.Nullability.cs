// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace ReactiveUI.Primitives.ObservableEvents.Tests;

/// <summary>Verifies generated handlers match the nullability of the delegates they are assigned to.</summary>
/// <remarks>
/// A handler whose parameter nullability differs from its delegate's is assignable but warns (CS8622), and a
/// consumer building warnings-as-errors cannot use the generated wrapper at all. The annotations are therefore part
/// of the contract - but only where the consumer's language version can express them, which is what these pin down.
/// </remarks>
public sealed partial class EventGeneratorTests
{
    /// <summary>The nullable directive a generated file opens with when the consumer's language allows it.</summary>
    private const string NullableDirective = "#nullable enable";

    /// <summary>The annotated sender the conventional event delegate declares.</summary>
    private const string AnnotatedSenderHandler = "void Handler(object? sender, global::System.EventArgs e)";

    /// <summary>The same handler with the annotation dropped, as an older language version requires.</summary>
    private const string ObliviousSenderHandler = "void Handler(object sender, global::System.EventArgs e)";

    /// <summary>Consumer source valid on every language version the generator supports.</summary>
    /// <remarks>
    /// Requests both an instance wrapper and a static one, so every generated file the language gate touches - the
    /// wrapper, the shared overloads, and the namespace's static class - is exercised at both language versions.
    /// </remarks>
    private const string ConventionalEventSource = """
        using System;
        using ReactiveUI.Primitives.ObservableEvents;

        [assembly: GenerateStaticEventObservables(typeof(Samples.StaticHost))]

        namespace Samples
        {
            public sealed class EventSource
            {
                public event EventHandler<EventArgs> Changed;

                public void Raise() => Changed?.Invoke(this, EventArgs.Empty);
            }

            public static class StaticHost
            {
                public static event EventHandler<EventArgs> GlobalChanged;
            }

            public static class Consumer
            {
                public static IObservable<EventArgs> Observe(EventSource source) => source.Events().Changed;
            }
        }
        """;

    /// <summary>Verifies a modern consumer gets a handler matching the delegate's annotated sender.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorAnnotatesHandlersWhenTheLanguageSupportsIt()
    {
        var result = RunGeneratorAt(ConventionalEventSource, LanguageVersion.Preview);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedText).Contains(AnnotatedSenderHandler);
        await Assert.That(result.GeneratedText).Contains(NullableDirective);
    }

    /// <summary>Verifies the generated wrapper raises no nullability mismatch against the delegate it wraps.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorProducesNoNullabilityMismatchWarnings()
    {
        var result = RunGeneratorAt(ConventionalEventSource, LanguageVersion.Preview, NullableContextOptions.Enable);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.CompilationWarnings.Where(static id => id == "CS8622")).IsEmpty();
    }

    /// <summary>Verifies a consumer predating nullable reference types gets source it can actually compile.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorOmitsAnnotationsWhenTheLanguagePredatesThem()
    {
        var result = RunGeneratorAt(ConventionalEventSource, LanguageVersion.CSharp7_3);

        await Assert.That(result.Errors).IsEmpty();
        await Assert.That(result.GeneratedText).Contains(ObliviousSenderHandler);
        await Assert.That(result.GeneratedText).DoesNotContain(NullableDirective);
        await Assert.That(result.GeneratedText).DoesNotContain("object?");
    }

    /// <summary>Runs the generator for one source at a chosen language version.</summary>
    /// <param name="source">The consumer source to compile.</param>
    /// <param name="languageVersion">The language version the consumer is compiled at.</param>
    /// <param name="nullableContext">The consumer's nullable context.</param>
    /// <returns>The compile errors, warning identifiers, and generated source.</returns>
    [RequiresAssemblyFiles("Calls System.Reflection.Assembly.Location")]
    private static LanguageVersionResult RunGeneratorAt(
        string source,
        LanguageVersion languageVersion,
        NullableContextOptions nullableContext = NullableContextOptions.Disable)
    {
        var parseOptions = CSharpParseOptions.Default.WithLanguageVersion(languageVersion);
        var compilation = CSharpCompilation.Create(
            $"ObservableEventsNullability_{languageVersion}",
            [CSharpSyntaxTree.ParseText(source, parseOptions)],
            CreateReferences(ProviderMode.Lean, []),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
                .WithNullableContextOptions(nullableContext));
        var driver = CSharpGeneratorDriver.Create(
            [new EventGenerator().AsSourceGenerator()],
            parseOptions: parseOptions);
        _ = driver.RunGeneratorsAndUpdateCompilation(compilation, out var updated, out _);

        var diagnostics = updated.GetDiagnostics();
        return new(
            [.. diagnostics.Where(static diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)],
            [.. diagnostics
                .Where(static diagnostic => diagnostic.Severity == DiagnosticSeverity.Warning)
                .Select(static diagnostic => diagnostic.Id)],
            string.Join(
                Environment.NewLine,
                updated.SyntaxTrees.Select(static tree => tree.ToString())));
    }

    /// <summary>What one language-version run produced.</summary>
    /// <param name="Errors">The compile errors after generation.</param>
    /// <param name="CompilationWarnings">The identifiers of every warning the consumer would see.</param>
    /// <param name="GeneratedText">Every tree in the updated compilation, generated ones included.</param>
    private sealed record LanguageVersionResult(
        ImmutableArray<Diagnostic> Errors,
        ImmutableArray<string> CompilationWarnings,
        string GeneratedText);
}
