// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace ReactiveUI.Primitives.ObservableEvents.Tests;

/// <summary>Verifies the generator's pipeline caches, so an unrelated edit costs nothing to recompute.</summary>
/// <remarks>
/// Correct output alone does not prove a generator is incremental: a pipeline that reruns everything on every
/// keystroke produces exactly the same files. What proves it is the driver's own record of why each step ran, which
/// is what these tests read. They are the guard against a model quietly regaining a symbol, a syntax node, or the
/// compilation, any of which makes every step compare unequal and every file regenerate on every keystroke.
/// </remarks>
public sealed partial class EventGeneratorTests
{
    /// <summary>Consumer source that exercises both the instance and the static request routes at once.</summary>
    private const string IncrementalSource = """
        using System;
        using ReactiveUI.Primitives.ObservableEvents;

        [assembly: GenerateStaticEventObservables(typeof(Samples.EventSource))]

        namespace Samples;

        public sealed class EventSource
        {
            public event EventHandler<EventArgs>? Changed;

            public static event Action<int>? GlobalChanged;
        }

        public static class Consumer
        {
            public static IObservable<EventArgs> Observe(EventSource source) => source.Events().Changed;
        }
        """;

    /// <summary>Source in a second file that no request depends on.</summary>
    private const string UnrelatedSource = """
        namespace Samples;

        public static class Unrelated
        {
            public static int Value => 1;
        }
        """;

    /// <summary>The same source after an edit that changes nothing any request depends on.</summary>
    private const string EditedUnrelatedSource = """
        namespace Samples;

        public static class Unrelated
        {
            public static int Value => 2;

            public static string Name => "added";
        }
        """;

    /// <summary>The event declaration the incremental-change test adds an event alongside.</summary>
    private const string ExistingEventDeclaration = "public event EventHandler<EventArgs>? Changed;";

    /// <summary>The same declaration with a second event added after it.</summary>
    private const string AddedEventDeclaration =
        "public event EventHandler<EventArgs>? Changed;\n\n    public event Action? Added;";

    /// <summary>The pipeline steps whose caching these tests assert.</summary>
    private static readonly string[] TrackedStepNames =
    [
        GeneratorStepNames.Provider,
        GeneratorStepNames.InstanceTargets,
        GeneratorStepNames.StaticTargets,
        GeneratorStepNames.ActivationOverloads,
        GeneratorStepNames.StaticNamespaces,
    ];

    /// <summary>The steps that must not recompute at all when an unrelated file is edited.</summary>
    /// <remarks>
    /// These are the two that run the semantic model, and they are the expensive half of the generator. Accepting
    /// <c>Unchanged</c> here would let a regression through: a transform that re-runs and happens to produce an
    /// equal value still paid for every symbol walk, which is the cost this pipeline exists to avoid.
    /// </remarks>
    private static readonly string[] SemanticStepNames =
    [
        GeneratorStepNames.InstanceTargets,
        GeneratorStepNames.StaticTargets,
    ];

    /// <summary>The Roslyn types a pipeline model must never carry, because they defeat the caching.</summary>
    private static readonly Type[] UncacheableTypes =
    [
        typeof(ISymbol),
        typeof(SyntaxNode),
        typeof(Compilation),
        typeof(SemanticModel),
        typeof(SyntaxTree),
        typeof(Location),
    ];

    /// <summary>Gets the parse options every compilation in these tests uses.</summary>
    private static CSharpParseOptions IncrementalParseOptions =>
        CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview);

    /// <summary>Verifies a second run over an equivalent compilation recomputes no step's value.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorCachesEveryPipelineStepWhenNothingChanges()
    {
        var compilation = CreateCompilation([IncrementalSource]);
        GeneratorDriver driver = CreateTrackingDriver();
        driver = driver.RunGenerators(compilation);

        // A clone is a different compilation object holding the same trees, so every step is asked again and
        // every step has to answer that its value is unchanged.
        driver = driver.RunGenerators(compilation.Clone());
        var reasons = CollectTrackedStepReasons(driver.GetRunResult());

        await Assert.That(reasons).IsNotEmpty();
        await Assert.That(reasons.FindAll(static reason => !IsCached(reason.Reason))).IsEmpty();
    }

    /// <summary>Verifies editing a file no request depends on leaves every requested host's model equal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorCachesPipelineStepsAcrossAnUnrelatedEdit()
    {
        var compilation = CreateCompilation([IncrementalSource, UnrelatedSource]);
        GeneratorDriver driver = CreateTrackingDriver();
        driver = driver.RunGenerators(compilation);
        var firstSources = CollectGeneratedSources(driver.GetRunResult());

        var edited = compilation.ReplaceSyntaxTree(
            compilation.SyntaxTrees[^1],
            ParseSource(EditedUnrelatedSource));
        driver = driver.RunGenerators(edited);
        var runResult = driver.GetRunResult();
        var reasons = CollectTrackedStepReasons(runResult);

        await Assert.That(reasons).IsNotEmpty();
        await Assert.That(reasons.FindAll(static reason => !IsCached(reason.Reason))).IsEmpty();
        await Assert.That(CollectGeneratedSources(runResult)).IsEqualTo(firstSources);

        // The semantic transforms must have been skipped outright, not re-run to an equal answer.
        var semantic = reasons.FindAll(static reason => Array.Exists(
            SemanticStepNames,
            name => name == reason.StepName));
        await Assert.That(semantic).IsNotEmpty();
        await Assert.That(semantic.FindAll(
            static reason => reason.Reason != IncrementalStepRunReason.Cached)).IsEmpty();
    }

    /// <summary>Verifies the generator emits no post-initialization output, which would defeat all caching.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Post-initialization source is added to the compilation the pipeline runs against, so producing any at all -
    /// even one file nothing refers to - makes that compilation new on every run and discards every semantic result
    /// cached against the previous one. Measured at roughly a hundredfold on an unchanged re-run, so this is worth
    /// a test of its own: the activation API has to arrive as an ordinary source output instead.
    /// </remarks>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorEmitsNoPostInitializationOutput()
    {
        // Ordinary source output is switched off, so anything left is post-initialization output.
        var driver = CSharpGeneratorDriver.Create(
            [new EventGenerator().AsSourceGenerator()],
            parseOptions: IncrementalParseOptions,
            driverOptions: new(IncrementalGeneratorOutputKind.Source, trackIncrementalGeneratorSteps: false));

        var runResult = driver.RunGenerators(CreateCompilation([IncrementalSource])).GetRunResult();

        await Assert.That(runResult.GeneratedTrees).IsEmpty();
    }

    /// <summary>Verifies re-running against the very same compilation recomputes nothing at all.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// The strongest statement the pipeline can make, and the one that only holds while no post-initialization
    /// output exists: handed back the identical compilation, every semantic transform is skipped outright.
    /// </remarks>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorRecomputesNothingForTheSameCompilation()
    {
        var compilation = CreateCompilation([IncrementalSource]);
        var primed = CreateTrackingDriver().RunGenerators(compilation);

        var reasons = CollectTrackedStepReasons(primed.RunGenerators(compilation).GetRunResult());

        var semantic = reasons.FindAll(static reason => Array.Exists(
            SemanticStepNames,
            name => name == reason.StepName));
        await Assert.That(semantic).IsNotEmpty();
        await Assert.That(semantic.FindAll(
            static reason => reason.Reason != IncrementalStepRunReason.Cached)).IsEmpty();
    }

    /// <summary>Verifies no step's value carries a Roslyn object that would defeat the caching.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorPipelineStepsCarryNoRoslynObjects()
    {
        GeneratorDriver driver = CreateTrackingDriver();
        driver = driver.RunGenerators(CreateCompilation([IncrementalSource]));

        var leaked = CollectTrackedStepValues(driver.GetRunResult())
            .FindAll(static value => Array.Exists(UncacheableTypes, type => type.IsInstanceOfType(value)));

        await Assert.That(leaked).IsEmpty();
    }

    /// <summary>Verifies a change to a requested host's events re-emits only what that change affects.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task EventGeneratorRecomputesOnlyTheStepsAnEventChangeAffects()
    {
        var compilation = CreateCompilation([IncrementalSource]);
        GeneratorDriver driver = CreateTrackingDriver();
        driver = driver.RunGenerators(compilation);

        var edited = compilation.ReplaceSyntaxTree(
            compilation.SyntaxTrees[0],
            ParseSource(IncrementalSource.Replace(
                ExistingEventDeclaration,
                AddedEventDeclaration,
                StringComparison.Ordinal)));
        driver = driver.RunGenerators(edited);
        var reasons = CollectTrackedStepReasons(driver.GetRunResult());

        // The added event changes what the wrapper exposes, so the host's own model has to be recomputed.
        await Assert.That(reasons.Exists(static reason =>
            reason.StepName == GeneratorStepNames.InstanceTargets && !IsCached(reason.Reason))).IsTrue();

        // Its signature did not move, so the shared overload file must not be rebuilt, and neither the static
        // request nor the resolved provider has anything to do with an instance event being added.
        await Assert.That(reasons.FindAll(static reason =>
                reason.StepName != GeneratorStepNames.InstanceTargets && !IsCached(reason.Reason)))
            .IsEmpty();
    }

    /// <summary>Creates a driver that records why each tracked step ran.</summary>
    /// <returns>The step-tracking driver.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static CSharpGeneratorDriver CreateTrackingDriver() =>
        CSharpGeneratorDriver.Create(
            [new EventGenerator().AsSourceGenerator()],
            parseOptions: IncrementalParseOptions,
            driverOptions: new(IncrementalGeneratorOutputKind.None, trackIncrementalGeneratorSteps: true));

    /// <summary>Creates a consumer compilation referencing the lean provider.</summary>
    /// <param name="sources">The consumer source files.</param>
    /// <returns>The consumer compilation.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [RequiresAssemblyFiles("Calls System.Reflection.Assembly.Location")]
    private static CSharpCompilation CreateCompilation(string[] sources) =>
        CSharpCompilation.Create(
            "ObservableEventsIncremental",
            Array.ConvertAll(sources, ParseSource),
            CreateReferences(ProviderMode.Lean, []),
            new(OutputKind.DynamicallyLinkedLibrary));

    /// <summary>Parses one consumer source file.</summary>
    /// <param name="source">The source text.</param>
    /// <returns>The parsed tree.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static SyntaxTree ParseSource(string source) =>
        CSharpSyntaxTree.ParseText(source, IncrementalParseOptions);

    /// <summary>Collects why each tracked step produced each of its values.</summary>
    /// <param name="runResult">The driver run result.</param>
    /// <returns>One entry per tracked step output.</returns>
    private static List<TrackedStepReason> CollectTrackedStepReasons(GeneratorDriverRunResult runResult)
    {
        var reasons = new List<TrackedStepReason>();
        foreach (var result in runResult.Results)
        {
            foreach (var tracked in result.TrackedSteps)
            {
                if (!Array.Exists(TrackedStepNames, name => name == tracked.Key))
                {
                    continue;
                }

                foreach (var step in tracked.Value)
                {
                    foreach (var output in step.Outputs)
                    {
                        reasons.Add(new(tracked.Key, output.Reason));
                    }
                }
            }
        }

        return reasons;
    }

    /// <summary>Collects the value every tracked step produced.</summary>
    /// <param name="runResult">The driver run result.</param>
    /// <returns>The tracked step values.</returns>
    private static List<object> CollectTrackedStepValues(GeneratorDriverRunResult runResult)
    {
        var values = new List<object>();
        foreach (var result in runResult.Results)
        {
            foreach (var tracked in result.TrackedSteps)
            {
                if (!Array.Exists(TrackedStepNames, name => name == tracked.Key))
                {
                    continue;
                }

                foreach (var step in tracked.Value)
                {
                    foreach (var output in step.Outputs)
                    {
                        values.Add(output.Value);
                    }
                }
            }
        }

        return values;
    }

    /// <summary>Renders every generated file as one comparable string, so two runs can be compared.</summary>
    /// <param name="runResult">The driver run result.</param>
    /// <returns>The generated files, in hint-name order.</returns>
    private static string CollectGeneratedSources(GeneratorDriverRunResult runResult)
    {
        var entries = new List<string>();
        foreach (var result in runResult.Results)
        {
            foreach (var generated in result.GeneratedSources)
            {
                entries.Add($"{generated.HintName}\n{generated.SourceText}");
            }
        }

        entries.Sort(StringComparer.Ordinal);
        return string.Join("\n", entries);
    }

    /// <summary>Determines whether a step reused its previous value rather than producing a new one.</summary>
    /// <param name="reason">The reason the driver recorded.</param>
    /// <returns><see langword="true"/> when nothing downstream had to be recomputed.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsCached(IncrementalStepRunReason reason) =>
        reason is IncrementalStepRunReason.Cached or IncrementalStepRunReason.Unchanged;

    /// <summary>Why one tracked step produced one of its values.</summary>
    /// <param name="StepName">The tracked step name.</param>
    /// <param name="Reason">The reason the driver recorded.</param>
    private sealed record TrackedStepReason(string StepName, IncrementalStepRunReason Reason);
}
