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

    /// <summary>The diagnostic reported when no observable provider is referenced.</summary>
    private const string MissingProviderId = "RXOE001";

    /// <summary>The diagnostic reported for a host without supported events.</summary>
    private const string NoEventsId = "RXOE002";

    /// <summary>The diagnostic reported for an event host the generator cannot represent.</summary>
    private const string UnsupportedEventId = "RXOE003";

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
    /// <returns>The unchanged compilation and the primed driver.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static (Compilation Compilation, CSharpGeneratorDriver Driver) CreateUnchangedState(CorpusSize size) =>
        RunOnce(size);

    /// <summary>Runs the generator after editing a file unrelated to event activation.</summary>
    /// <param name="size">The corpus size.</param>
    /// <returns>The edited compilation and the primed driver.</returns>
    internal static (Compilation Compilation, CSharpGeneratorDriver Driver) CreateUnrelatedEditState(CorpusSize size)
    {
        var primed = RunOnce(size);
        var edited = primed.Compilation.AddSyntaxTrees(
            CSharpSyntaxTree.ParseText(EventCorpus.UnrelatedSource, ParseOptions, EventCorpus.UnrelatedFileName));
        return (edited, primed.Driver);
    }

    /// <summary>Runs the generator after changing one host's event declarations.</summary>
    /// <param name="size">The corpus size.</param>
    /// <returns>The edited compilation and the primed driver.</returns>
    internal static (Compilation Compilation, CSharpGeneratorDriver Driver) CreateEventEditState(CorpusSize size)
    {
        var primed = RunOnce(size);
        var fileName = EventCorpus.HostFileName(EditedHostIndex);
        var original = FindTree(primed.Compilation, fileName);
        var edited = primed.Compilation.ReplaceSyntaxTree(
            original,
            CSharpSyntaxTree.ParseText(
                EventCorpus.HostSourceWithAddedEvent(EditedHostIndex),
                ParseOptions,
                fileName));
        return (edited, primed.Driver);
    }

    /// <summary>Creates a fresh driver over the corpus plus requests the generator can only answer with diagnostics.</summary>
    /// <param name="size">The corpus size.</param>
    /// <param name="withProvider">Whether the compilation references an observable provider.</param>
    /// <returns>The compilation and a fresh driver.</returns>
    internal static (Compilation Compilation, CSharpGeneratorDriver Driver) CreateUnservableState(CorpusSize size, bool withProvider)
    {
        var compilation = BuildCompilation(size, withProvider).AddSyntaxTrees(
            CSharpSyntaxTree.ParseText(EventCorpus.UnservableSource, ParseOptions, EventCorpus.UnservableFileName));
        return (compilation, CreateDriver());
    }

    /// <summary>Runs every corpus size once and reports what came out, so a broken corpus fails loudly.</summary>
    /// <exception cref="InvalidOperationException">A corpus does not compile, or the unservable requests report nothing, making the measurements worthless.</exception>
    internal static void ValidateCorpus()
    {
        ExpectDiagnostics(CreateUnservableState(CorpusSize.Small, true), [NoEventsId, UnsupportedEventId], "with a provider");
        ExpectDiagnostics(CreateUnservableState(CorpusSize.Small, false), [MissingProviderId], "without a provider");

        foreach (var size in Enum.GetValues<CorpusSize>())
        {
            var cold = CreateColdState(size);
            var updated = cold.Driver.RunGeneratorsAndUpdateCompilation(cold.Compilation, out var result, out _);
            List<Diagnostic> errors = [];
            foreach (var diagnostic in result.GetDiagnostics())
            {
                if (diagnostic.Severity == DiagnosticSeverity.Error)
                {
                    errors.Add(diagnostic);
                }
            }

            var generated = ((CSharpGeneratorDriver)updated).GetRunResult().GeneratedTrees.Length;

            Console.WriteLine(string.Create(
                CultureInfo.InvariantCulture,
                $"{size}: {EventCorpus.HostCountFor(size)} hosts in {EventCorpus.FilesFor(size).Count} files, "
                + $"{generated} generated files, {errors.Count} errors"));

            foreach (var error in errors)
            {
                Console.WriteLine(error.ToString());
            }

            if (errors.Count > 0)
            {
                throw new InvalidOperationException($"The {size} corpus does not compile; the benchmark is invalid.");
            }
        }
    }

    /// <summary>Builds a compilation over one file per host, as real code is laid out.</summary>
    /// <param name="size">The corpus size.</param>
    /// <returns>The compilation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static CSharpCompilation BuildCompilation(CorpusSize size) => BuildCompilation(size, true);

    /// <summary>Builds a compilation over one file per host, optionally without any observable provider referenced.</summary>
    /// <param name="size">The corpus size.</param>
    /// <param name="withProvider">Whether the compilation references an observable provider.</param>
    /// <returns>The compilation.</returns>
    private static CSharpCompilation BuildCompilation(CorpusSize size, bool withProvider)
    {
        var files = EventCorpus.FilesFor(size);
        var trees = new SyntaxTree[files.Count];
        for (var index = 0; index < files.Count; index++)
        {
            trees[index] = CSharpSyntaxTree.ParseText(files[index].Text, ParseOptions, files[index].Path);
        }

        return CSharpCompilation.Create(CompilationAssemblyName, trees, CreateReferences(withProvider), CompilationOptions);
    }

    /// <summary>Runs a fresh driver and checks it reported every expected diagnostic.</summary>
    /// <param name="state">The compilation and fresh driver.</param>
    /// <param name="expectedIds">The diagnostic identifiers the run must report.</param>
    /// <param name="description">How the compilation differs, for the report.</param>
    /// <exception cref="InvalidOperationException">An expected diagnostic was not reported.</exception>
    private static void ExpectDiagnostics(
        (Compilation Compilation, CSharpGeneratorDriver Driver) state,
        string[] expectedIds,
        string description)
    {
        var result = state.Driver.RunGenerators(state.Compilation).GetRunResult();
        HashSet<string> reported = [with(StringComparer.Ordinal)];
        foreach (var diagnostic in result.Diagnostics)
        {
            _ = reported.Add(diagnostic.Id);
        }

        Console.WriteLine($"Unservable requests {description}: {string.Join(", ", reported)}");
        foreach (var id in expectedIds)
        {
            if (!reported.Contains(id))
            {
                throw new InvalidOperationException($"The unservable requests {description} did not report {id}; the benchmark is invalid.");
            }
        }
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
    /// <param name="withProvider">Whether to keep the observable providers the generated wrappers name.</param>
    /// <returns>The metadata references.</returns>
    private static List<MetadataReference> CreateReferences(bool withProvider)
    {
        var paths = AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!.ToString()!.Split(Path.PathSeparator);
        List<MetadataReference> references = [with(capacity: paths.Length)];
        foreach (var path in paths)
        {
            if (!withProvider && IsObservableProvider(Path.GetFileName(path)))
            {
                continue;
            }

            references.Add(MetadataReference.CreateFromFile(path));
        }

        return references;
    }

    /// <summary>Reports whether an assembly file carries an observable provider the generator can target.</summary>
    /// <param name="fileName">The assembly file name.</param>
    /// <returns><see langword="true"/> for the Primitives and System.Reactive assemblies.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsObservableProvider(string fileName) =>
        fileName.StartsWith("ReactiveUI.", StringComparison.Ordinal)
        || fileName.StartsWith("System.Reactive", StringComparison.Ordinal);

    /// <summary>Finds the corpus tree with the given file path.</summary>
    /// <param name="compilation">The compilation to search.</param>
    /// <param name="fileName">The file path of the tree.</param>
    /// <returns>The matching tree.</returns>
    /// <exception cref="InvalidOperationException">No tree has that file path.</exception>
    private static SyntaxTree FindTree(Compilation compilation, string fileName)
    {
        foreach (var tree in compilation.SyntaxTrees)
        {
            if (string.Equals(tree.FilePath, fileName, StringComparison.Ordinal))
            {
                return tree;
            }
        }

        throw new InvalidOperationException($"The corpus has no file named {fileName}.");
    }
}
