// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using ReactiveUI.Primitives.R3Bridge.Generator;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies the R3 bridge source generator.</summary>
public class R3BridgeGeneratorTests
{
    /// <summary>Generated R3 bridge type marker.</summary>
    private const string R3BridgeName = "R3SignalBridge";

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
    /// <returns>Compilation diagnostics and generated source text.</returns>
    [RequiresAssemblyFiles("Calls System.Reflection.Assembly.Location")]
    private static (ImmutableArray<Diagnostic> Diagnostics, string[] GeneratedSources) RunGenerators(string source)
    {
        var parseOptions = CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview);
        var references = AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!.ToString()!
            .Split(Path.PathSeparator)
            .Where(path => !Path.GetFileName(path).StartsWith("System.Reactive", StringComparison.OrdinalIgnoreCase)
                           && !Path.GetFileName(path).StartsWith("R3", StringComparison.OrdinalIgnoreCase))
            .Select(path => MetadataReference.CreateFromFile(path))
            .Cast<MetadataReference>()
            .ToList();
        references.Add(MetadataReference.CreateFromFile(typeof(Signal).Assembly.Location));
        references.Add(MetadataReference.CreateFromFile(typeof(StateSignal<>).Assembly.Location));
        var compilation = CSharpCompilation.Create(
            "R3BridgeGeneratorSmoke",
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
        ImmutableArray<Diagnostic> diagnostics = [
            ..generatorDiagnostics
                .Concat(updatedCompilation.GetDiagnostics()
                    .Where(diagnostic => diagnostic.Severity == DiagnosticSeverity.Error))
        ];
        var generatedSources = driver.GetRunResult().Results
            .SelectMany(result => result.GeneratedSources)
            .Select(sourceText => sourceText.SourceText.ToString())
            .ToArray();
        return (diagnostics, generatedSources);
    }
}
