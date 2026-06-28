// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.R3Bridge.Generator;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies the R3 bridge source generator.</summary>
public class R3BridgeGeneratorTests
{
    /// <summary>Generated R3 bridge type marker.</summary>
    private const string R3BridgeName = "R3SignalBridge";

    /// <summary>Generated R3 async bridge type marker.</summary>
    private const string R3AsyncBridgeName = "R3AsyncBridge";

    /// <summary>Generated marker attribute type name.</summary>
    private const string GeneratedMarkerName = "PrimitivesR3BridgeGeneratedAttribute";

    /// <summary>Compiler diagnostic raised when a source type conflicts with an imported type.</summary>
    private const string ConflictingTypeDiagnosticId = "CS0436";

    /// <summary>Verifies the R3 bridge generator emits adapters when the R3 shapes are present.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    [SuppressMessage(
        "Major Code Smell",
        "S138:Functions should not have too many lines",
        Justification = "Embedded generator smoke source keeps the emitted API contract local to the test.")]
    public async Task R3BridgeGeneratorEmitsWhenR3ShapesArePresentAndCompilesSmokeAdapters()
    {
        const string Source = """
                              using System;
                              using ReactiveUI.Primitives;
                              using ReactiveUI.Primitives.Concurrency;
                              using ReactiveUI.Primitives.Disposables;
                              using ReactiveUI.Primitives.Signals;
                              using ReactiveUI.Primitives.R3Bridge;

                              namespace R3
                              {
                                  public readonly struct Result
                                  {
                                      public static Result Success => default;

                                      public static Result Failure(Exception exception) => new Result(exception);

                                      private Result(Exception exception) => Exception = exception;

                                      public Exception Exception { get; }

                                      public bool IsFailure => Exception != null;
                                  }

                                  public abstract class Observer<T> : IDisposable
                                  {
                                      public void OnNext(T value) => OnNextCore(value);

                                      public void OnErrorResume(Exception error) => OnErrorResumeCore(error);

                                      public void OnCompleted(Result result) => OnCompletedCore(result);

                                      public void Dispose() { }

                                      protected abstract void OnNextCore(T value);

                                      protected abstract void OnErrorResumeCore(Exception error);

                                      protected abstract void OnCompletedCore(Result result);
                                  }

                                  public abstract class Observable<T>
                                  {
                                      public abstract IDisposable Subscribe(Observer<T> observer);
                                  }

                                  public static class Observable
                                  {
                                      public static Observable<T> Create<T>(Func<Observer<T>, IDisposable> subscribe) => new DelegateObservable<T>(subscribe);

                                      private sealed class DelegateObservable<TValue> : Observable<TValue>
                                      {
                                          private readonly Func<Observer<TValue>, IDisposable> _subscribe;
                                          public DelegateObservable(Func<Observer<TValue>, IDisposable> subscribe) => _subscribe = subscribe;
                                          public override IDisposable Subscribe(Observer<TValue> observer) => _subscribe(observer);
                                      }
                                  }
                              }

                              public static class BridgeSmoke
                              {
                                  public static void Use(R3.Observable<int> r3)
                                  {
                                      IObservable<int> PrimitivesFromR3 = r3.AsPrimitivesSignal();
                                      R3.Observable<int> r3Again = PrimitivesFromR3.AsR3Observable();
                                  }
                              }
                              """;
        var (diagnostics, generatedSources) = RunGenerators(Source);
        await Assert.That(diagnostics.Length).IsEqualTo(0);
        await Assert.That(Array.Exists(
            generatedSources,
            static text => text.Contains(R3BridgeName, StringComparison.Ordinal))).IsTrue();
    }

    /// <summary>Verifies the generated marker stays private across project-reference-like compilations.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task R3BridgeGeneratedMarkerDoesNotConflictAcrossProjectReferenceCompilations()
    {
        const string LibrarySource = """
                                     public static class GeneratedLibrary
                                     {
                                         public static int Value => 42;
                                     }
                                     """;
        const string ConsumerSource = """
                                      public static class GeneratedConsumer
                                      {
                                          public static int Use() => GeneratedLibrary.Value;
                                      }
                                      """;
        var libraryRun = RunGeneratorsCore("GeneratedLibrary", LibrarySource, []);
        var libraryEmit = EmitToMetadataReference(libraryRun.Compilation);
        await Assert.That(ContainsError(libraryRun.GeneratorDiagnostics.Concat(libraryEmit.Diagnostics))).IsFalse();
        await Assert.That(libraryEmit.Reference).IsNotNull();
        await Assert.That(GeneratedMarkerIsInternal(libraryRun.GeneratedSources)).IsTrue();
        var consumerRun = RunGeneratorsCore("GeneratedConsumer", ConsumerSource, [libraryEmit.Reference!]);
        var consumerDiagnostics = consumerRun.GeneratorDiagnostics
            .Concat(consumerRun.Compilation.GetDiagnostics())
            .ToArray();
        await Assert.That(Array.Exists(consumerDiagnostics, IsConflictingMarkerTypeDiagnostic)).IsFalse();
        await Assert.That(Array.Exists(consumerDiagnostics, IsErrorDiagnostic)).IsFalse();
    }

    /// <summary>Verifies the generator skips bridge sources when required R3 shapes are incomplete.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task R3BridgeGeneratorDoesNotEmitAdaptersWhenR3ShapesAreIncomplete()
    {
        const string Source = """
                              namespace R3
                              {
                                  public abstract class Observable<T>
                                  {
                                  }
                              }
                              """;
        var (diagnostics, generatedSources) = RunGenerators(Source, includeAsyncReference: true);
        await Assert.That(diagnostics.Length).IsEqualTo(0);
        await Assert.That(GeneratedBridgeTypeExists(generatedSources, R3BridgeName)).IsFalse();
        await Assert.That(GeneratedBridgeTypeExists(generatedSources, R3AsyncBridgeName)).IsFalse();
    }

    /// <summary>Verifies the R3 bridge generator skips adapters when the R3 package is absent.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task R3BridgeGeneratorDoesNotEmitAdaptersWhenR3IsAbsent()
    {
        const string Source = """
                              using System;
                              using ReactiveUI.Primitives.Signals;

                              public static class CoreOnlySmoke
                              {
                                  public static IObservable<int> Use() => Signal.Emit(1);
                              }
                              """;
        var (diagnostics, generatedSources) = RunGenerators(Source);
        await Assert.That(diagnostics.Length).IsEqualTo(0);
        await Assert.That(Array.Exists(
            generatedSources,
            static text => text.Contains(R3BridgeName, StringComparison.Ordinal))).IsFalse();
    }

    /// <summary>Runs the R3 bridge source generator for the supplied source.</summary>
    /// <param name="source">Source code to compile.</param>
    /// <param name="includeAsyncReference">Whether to include the async primitives assembly reference.</param>
    /// <returns>Compilation diagnostics and generated source text.</returns>
    [RequiresAssemblyFiles("Calls System.Reflection.Assembly.Location")]
    private static (ImmutableArray<Diagnostic> Diagnostics, string[] GeneratedSources) RunGenerators(
        string source,
        bool includeAsyncReference = false)
    {
        var run = RunGeneratorsCore("R3BridgeGeneratorSmoke", source, [], includeAsyncReference);
        ImmutableArray<Diagnostic> diagnostics = [
            ..run.GeneratorDiagnostics
                .Concat(run.Compilation.GetDiagnostics()
                    .Where(IsErrorDiagnostic))
        ];
        return (diagnostics, run.GeneratedSources);
    }

    /// <summary>Runs the R3 bridge source generator and keeps the updated compilation.</summary>
    /// <param name="assemblyName">Compilation assembly name.</param>
    /// <param name="source">Source code to compile.</param>
    /// <param name="additionalReferences">Additional metadata references for project-reference scenarios.</param>
    /// <param name="includeAsyncReference">Whether to include the async primitives assembly reference.</param>
    /// <returns>The updated compilation, generator diagnostics, and generated source text.</returns>
    [RequiresAssemblyFiles("Calls System.Reflection.Assembly.Location")]
    private static (
        Compilation Compilation,
        ImmutableArray<Diagnostic> GeneratorDiagnostics,
        string[] GeneratedSources) RunGeneratorsCore(
        string assemblyName,
        string source,
        IEnumerable<MetadataReference> additionalReferences,
        bool includeAsyncReference = false)
    {
        var parseOptions = CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview);
        var references = CreateReferences(additionalReferences, includeAsyncReference);
        var compilation = CSharpCompilation.Create(
            assemblyName,
            [CSharpSyntaxTree.ParseText(source, parseOptions)],
            references,
            new(OutputKind.DynamicallyLinkedLibrary));
        var driver = CSharpGeneratorDriver.Create(
            [new R3BridgeGenerator().AsSourceGenerator()],
            parseOptions: parseOptions);
        driver = (CSharpGeneratorDriver)driver.RunGeneratorsAndUpdateCompilation(
            compilation,
            out var updatedCompilation,
            out var generatorDiagnostics);
        var generatedSources = driver.GetRunResult().Results
            .SelectMany(result => result.GeneratedSources)
            .Select(sourceText => sourceText.SourceText.ToString())
            .ToArray();
        return (updatedCompilation, generatorDiagnostics, generatedSources);
    }

    /// <summary>Creates the metadata references needed by in-memory generator smoke compilations.</summary>
    /// <param name="additionalReferences">Additional metadata references for project-reference scenarios.</param>
    /// <param name="includeAsyncReference">Whether to include the async primitives assembly reference.</param>
    /// <returns>Metadata references for the smoke compilation.</returns>
    [RequiresAssemblyFiles("Calls System.Reflection.Assembly.Location")]
    private static List<MetadataReference> CreateReferences(
        IEnumerable<MetadataReference> additionalReferences,
        bool includeAsyncReference)
    {
        var references = AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!.ToString()!
            .Split(Path.PathSeparator)
            .Where(path => !Path.GetFileName(path).StartsWith("System.Reactive", StringComparison.OrdinalIgnoreCase)
                           && !Path.GetFileName(path).StartsWith("R3", StringComparison.OrdinalIgnoreCase))
            .Select(path => MetadataReference.CreateFromFile(path))
            .Cast<MetadataReference>()
            .ToList();
        references.Add(MetadataReference.CreateFromFile(typeof(Signal).Assembly.Location));
        references.Add(MetadataReference.CreateFromFile(typeof(StateSignal<>).Assembly.Location));
        if (includeAsyncReference)
        {
            references.Add(MetadataReference.CreateFromFile(typeof(IObservableAsync<>).Assembly.Location));
        }

        references.AddRange(additionalReferences);
        return references;
    }

    /// <summary>Emits an in-memory compilation as a metadata reference.</summary>
    /// <param name="compilation">Compilation to emit.</param>
    /// <returns>The emit diagnostics and metadata reference when emission succeeds.</returns>
    private static (ImmutableArray<Diagnostic> Diagnostics, PortableExecutableReference? Reference) EmitToMetadataReference(
        Compilation compilation)
    {
        using MemoryStream stream = new();
        var result = compilation.Emit(stream);
        PortableExecutableReference? reference = result.Success
            ? MetadataReference.CreateFromImage(stream.ToArray())
            : null;
        return (result.Diagnostics, reference);
    }

    /// <summary>Checks whether diagnostics contain any compiler errors.</summary>
    /// <param name="diagnostics">Diagnostics to inspect.</param>
    /// <returns><see langword="true"/> when an error diagnostic is present.</returns>
    private static bool ContainsError(IEnumerable<Diagnostic> diagnostics) => diagnostics.Any(IsErrorDiagnostic);

    /// <summary>Checks whether a diagnostic is an error.</summary>
    /// <param name="diagnostic">Diagnostic to inspect.</param>
    /// <returns><see langword="true"/> when the diagnostic is an error.</returns>
    private static bool IsErrorDiagnostic(Diagnostic diagnostic) => diagnostic.Severity == DiagnosticSeverity.Error;

    /// <summary>Checks whether a diagnostic is the marker-attribute type conflict.</summary>
    /// <param name="diagnostic">Diagnostic to inspect.</param>
    /// <returns><see langword="true"/> when the diagnostic is the expected conflict shape.</returns>
    private static bool IsConflictingMarkerTypeDiagnostic(Diagnostic diagnostic) =>
        diagnostic.Id == ConflictingTypeDiagnosticId &&
        diagnostic.GetMessage().Contains(GeneratedMarkerName, StringComparison.Ordinal);

    /// <summary>Checks whether generated source contains the named bridge type.</summary>
    /// <param name="generatedSources">Generated source text to inspect.</param>
    /// <param name="typeName">Bridge type name.</param>
    /// <returns><see langword="true"/> when the bridge type is emitted.</returns>
    private static bool GeneratedBridgeTypeExists(string[] generatedSources, string typeName) => Array.Exists(
        generatedSources,
        text => text.Contains($"internal static class {typeName}", StringComparison.Ordinal));

    /// <summary>Checks whether the generated marker attribute type is internal.</summary>
    /// <param name="generatedSources">Generated source text to inspect.</param>
    /// <returns><see langword="true"/> when the marker source declares the attribute as internal.</returns>
    private static bool GeneratedMarkerIsInternal(string[] generatedSources) => Array.Exists(
        generatedSources,
        static text => text.Contains($"internal sealed class {GeneratedMarkerName}", StringComparison.Ordinal));
}
