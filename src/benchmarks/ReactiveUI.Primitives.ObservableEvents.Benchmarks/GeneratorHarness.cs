// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace ReactiveUI.Primitives.ObservableEvents.Benchmarks;

/// <summary>Builds the compilations and driver states the generator benchmarks run against.</summary>
internal static class GeneratorHarness
{
    /// <summary>The assembly name given to the throwaway compilation the generator runs against.</summary>
    private const string CompilationAssemblyName = "ObservableEventsCorpus";

    /// <summary>The host whose file the event-edit case rewrites.</summary>
    private const int EditedHostIndex = 0;

    /// <summary>The parse options every corpus tree is parsed with.</summary>
    private static readonly CSharpParseOptions ParseOptions =
        CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Latest);

    /// <summary>The compilation options the corpus is compiled with, matching a modern consumer.</summary>
    private static readonly CSharpCompilationOptions CompilationOptions =
        new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
            .WithNullableContextOptions(NullableContextOptions.Enable);

    /// <summary>Creates a compilation and a driver that has never run, so nothing is cached.</summary>
    /// <param name="size">The corpus size.</param>
    /// <returns>The compilation and a fresh driver.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static (Compilation Compilation, CSharpGeneratorDriver Driver) CreateColdState(CorpusSize size) =>
        (BuildCompilation(size), CreateDriver());

    /// <summary>Runs the generator with unchanged inputs to measure cache reuse.</summary>
    /// <param name="size">The corpus size.</param>
    /// <returns>The unchanged compilation and a driver that has already generated once.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static (Compilation Compilation, CSharpGeneratorDriver Driver) CreateUnchangedState(CorpusSize size) =>
        RunOnce(size);

    /// <summary>Runs the generator after editing a file unrelated to event activation.</summary>
    /// <param name="size">The corpus size.</param>
    /// <returns>The edited compilation and a driver that has already generated once.</returns>
    internal static (Compilation Compilation, CSharpGeneratorDriver Driver) CreateUnrelatedEditState(CorpusSize size)
    {
        var primed = RunOnce(size);
        var edited = primed.Compilation.AddSyntaxTrees(
            CSharpSyntaxTree.ParseText(EventCorpus.UnrelatedSource, ParseOptions, EventCorpus.UnrelatedFileName));
        return (edited, primed.Driver);
    }

    /// <summary>Runs the generator after changing one host's event declarations.</summary>
    /// <param name="size">The corpus size.</param>
    /// <returns>The edited compilation and a driver that has already generated once.</returns>
    internal static (Compilation Compilation, CSharpGeneratorDriver Driver) CreateEventEditState(CorpusSize size)
    {
        var primed = RunOnce(size);
        var fileName = EventCorpus.HostFileName(EditedHostIndex);
        var original = primed.Compilation.SyntaxTrees.First(tree =>
            string.Equals(tree.FilePath, fileName, StringComparison.Ordinal));
        var edited = primed.Compilation.ReplaceSyntaxTree(
            original,
            CSharpSyntaxTree.ParseText(
                EventCorpus.HostSourceWithAddedEvent(EditedHostIndex),
                ParseOptions,
                fileName));
        return (edited, primed.Driver);
    }

    /// <summary>Runs every corpus size once and reports what came out, so a broken corpus fails loudly.</summary>
    /// <exception cref="InvalidOperationException">A corpus does not compile, making its measurements worthless.</exception>
    internal static void ValidateCorpus()
    {
        foreach (var size in Enum.GetValues<CorpusSize>())
        {
            var cold = CreateColdState(size);
            var updated = cold.Driver.RunGeneratorsAndUpdateCompilation(cold.Compilation, out var result, out _);
            var errors = result.GetDiagnostics()
                .Where(static diagnostic => diagnostic.Severity == DiagnosticSeverity.Error)
                .ToArray();
            var generated = ((CSharpGeneratorDriver)updated).GetRunResult().GeneratedTrees.Length;

            Console.WriteLine(string.Create(
                CultureInfo.InvariantCulture,
                $"{size}: {EventCorpus.HostCountFor(size)} hosts in {cold.Compilation.SyntaxTrees.Count()} files, "
                + $"{generated} generated files, {errors.Length} errors"));

            foreach (var error in errors)
            {
                Console.WriteLine(error.ToString());
            }

            if (errors.Length > 0)
            {
                throw new InvalidOperationException($"The {size} corpus does not compile; the benchmark is invalid.");
            }
        }
    }

    /// <summary>Builds a compilation over one file per host, as real code is laid out.</summary>
    /// <param name="size">The corpus size.</param>
    /// <returns>The compilation.</returns>
    private static CSharpCompilation BuildCompilation(CorpusSize size)
    {
        var files = EventCorpus.FilesFor(size);
        var trees = new SyntaxTree[files.Count];
        for (var index = 0; index < files.Count; index++)
        {
            trees[index] = CSharpSyntaxTree.ParseText(files[index].Text, ParseOptions, files[index].Path);
        }

        return CSharpCompilation.Create(CompilationAssemblyName, trees, CreateReferences(), CompilationOptions);
    }

    /// <summary>Creates a driver with only the observable-event generator loaded.</summary>
    /// <returns>The driver.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static CSharpGeneratorDriver CreateDriver() =>
        CSharpGeneratorDriver.Create(
            [new EventGenerator().AsSourceGenerator()],
            parseOptions: ParseOptions);

    /// <summary>Runs the generator once so the driver has something cached to compare against.</summary>
    /// <param name="size">The corpus size.</param>
    /// <returns>The compilation and the primed driver.</returns>
    private static (Compilation Compilation, CSharpGeneratorDriver Driver) RunOnce(CorpusSize size)
    {
        var cold = CreateColdState(size);
        return (cold.Compilation, (CSharpGeneratorDriver)cold.Driver.RunGenerators(cold.Compilation));
    }

    /// <summary>Collects the metadata references the corpus compiles against.</summary>
    /// <returns>The metadata references, including the lean provider the generated wrappers name.</returns>
    private static List<MetadataReference> CreateReferences() =>
        [.. AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!.ToString()!
            .Split(Path.PathSeparator)
            .Select(static path => (MetadataReference)MetadataReference.CreateFromFile(path))];
}
