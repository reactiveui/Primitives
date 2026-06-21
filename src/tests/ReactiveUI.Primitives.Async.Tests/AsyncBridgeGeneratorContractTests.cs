// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.R3Bridge.Generator;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Contract tests for the R3 bridge source generators that conditionally emit async adapter APIs.</summary>
public sealed class AsyncBridgeGeneratorContractTests
{
    /// <summary>The generated R3-to-async bridge type name.</summary>
    private const string R3AsyncBridgeName = "R3AsyncBridge";

    /// <summary>The generated R3Async package bridge type name.</summary>
    private const string R3AsyncObservableBridgeName = "R3AsyncObservableBridge";

    /// <summary>Base imports for in-memory bridge generator smoke compilations.</summary>
    private const string BaseSmokeUsings = """
                                           using System;
                                           using System.Threading;
                                           using System.Threading.Tasks;

                                           """;

    /// <summary>Imports needed when the smoke compilation includes async primitives.</summary>
    private const string AsyncBridgeSmokeUsings = """
                                                  using ReactiveUI.Primitives.Async;
                                                  using ReactiveUI.Primitives.R3Bridge;

                                                  """;

    /// <summary>Imports needed when the smoke compilation intentionally omits async primitives.</summary>
    private const string CoreBridgeSmokeUsings = """
                                                 using ReactiveUI.Primitives.R3Bridge;

                                                 """;

    /// <summary>Minimal R3 and R3Async contract shapes consumed by the generated bridge source.</summary>
    private const string BridgeShapeSource = """
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

                                             namespace R3Async
                                             {
                                                 public readonly struct Result
                                                 {
                                                     public static Result Success => default;

                                                     public static Result Failure(Exception exception) => new Result(exception);

                                                     private Result(Exception exception) => Exception = exception;

                                                     public Exception? Exception { get; }

                                                     public bool IsFailure => Exception != null;
                                                 }

                                                 public abstract class AsyncObserver<T> : IAsyncDisposable
                                                 {
                                                     public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
                                                         OnNextAsyncCore(value, cancellationToken);

                                                     public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
                                                         OnErrorResumeAsyncCore(error, cancellationToken);

                                                     public ValueTask OnCompletedAsync(Result result) => OnCompletedAsyncCore(result);

                                                     public ValueTask DisposeAsync() => default;

                                                     protected abstract ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken);

                                                     protected abstract ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken);

                                                     protected abstract ValueTask OnCompletedAsyncCore(Result result);
                                                 }

                                                 public abstract class AsyncObservable<T>
                                                 {
                                                     public ValueTask<IAsyncDisposable> SubscribeAsync(AsyncObserver<T> observer, CancellationToken cancellationToken) =>
                                                         SubscribeAsyncCore(observer, cancellationToken);

                                                     protected abstract ValueTask<IAsyncDisposable> SubscribeAsyncCore(
                                                         AsyncObserver<T> observer,
                                                         CancellationToken cancellationToken);
                                                 }
                                             }

                                             """;

    /// <summary>Smoke code that compiles only when async R3 bridge adapters are emitted.</summary>
    private const string AsyncBridgeSmokeSource = """
                                                  public static class AsyncBridgeSmoke
                                                  {
                                                      public static void Use(
                                                          IObservableAsync<int> asyncSource,
                                                          R3.Observable<int> r3,
                                                          R3Async.AsyncObservable<int> r3Async)
                                                      {
                                                          IObservable<int> fromR3 = r3.AsPrimitivesSignal();
                                                          R3.Observable<int> toR3 = fromR3.AsR3Observable();
                                                          IObservableAsync<int> asyncFromR3 = r3.AsPrimitivesAsyncObservable();
                                                          R3.Observable<int> asyncToR3 = asyncSource.AsR3Observable();
                                                          IObservableAsync<int> fromR3Async = r3Async.AsPrimitivesAsyncObservable();
                                                          R3Async.AsyncObservable<int> toR3Async = asyncSource.AsR3AsyncObservable();
                                                      }
                                                  }
                                                  """;

    /// <summary>Smoke code that compiles when async primitives are intentionally absent.</summary>
    private const string CoreOnlySmokeSource = """
                                               public static class CoreOnlySmoke
                                               {
                                                   public static IObservable<int> Use(R3.Observable<int> r3) =>
                                                       r3.AsPrimitivesSignal();
                                               }
                                               """;

    /// <summary>The platform assemblies needed by the in-memory generator smoke compilation.</summary>
    private static readonly string[] PlatformReferenceNames =
    [
        "System.Collections.dll", "System.Linq.dll", "System.Private.CoreLib.dll", "System.Runtime.dll",
        "System.Runtime.Extensions.dll", "System.Threading.dll", "System.Threading.Tasks.dll"
    ];

    /// <summary>Verifies the R3 bridge generators emit async adapter extensions when async primitives are referenced.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task BridgeGeneratorsEmitAsyncAdaptersOnlyWhenAsyncShapesArePresent()
    {
        const string Source = BaseSmokeUsings + AsyncBridgeSmokeUsings + BridgeShapeSource + AsyncBridgeSmokeSource;
        var (diagnostics, generatedSources) = RunGenerators(Source, true);
        await Assert.That(diagnostics.Length).IsEqualTo(0);
        await Assert.That(GeneratedBridgeTypeExists(generatedSources, R3AsyncBridgeName)).IsTrue();
        await Assert.That(GeneratedBridgeTypeExists(generatedSources, R3AsyncObservableBridgeName)).IsTrue();
    }

    /// <summary>Verifies the R3 bridge generators skip async adapter extensions when async primitives are absent.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresAssemblyFiles]
    public async Task BridgeGeneratorsSkipAsyncAdaptersWhenAsyncAssemblyIsAbsent()
    {
        const string Source = BaseSmokeUsings + CoreBridgeSmokeUsings + BridgeShapeSource + CoreOnlySmokeSource;
        var (diagnostics, generatedSources) = RunGenerators(Source, false);
        await Assert.That(diagnostics.Length).IsEqualTo(0);
        await Assert.That(GeneratedBridgeTypeExists(generatedSources, R3AsyncBridgeName)).IsFalse();
        await Assert.That(GeneratedBridgeTypeExists(generatedSources, R3AsyncObservableBridgeName)).IsFalse();
    }

    /// <summary>Checks whether generated source contains the named bridge type rather than a marker attribute substring.</summary>
    /// <param name = "generatedSources">The generated source texts.</param>
    /// <param name = "typeName">The bridge type name.</param>
    /// <returns><see langword="true"/> when the bridge type is emitted.</returns>
    private static bool GeneratedBridgeTypeExists(string[] generatedSources, string typeName) => Array.Exists(
        generatedSources,
        text => text.Contains($"internal static class {typeName}", StringComparison.Ordinal));

    /// <summary>Runs the R3 bridge generators against an in-memory compilation.</summary>
    /// <param name = "source">The source text to compile.</param>
    /// <param name = "includeAsyncReference">Whether to include the async primitives assembly reference.</param>
    /// <returns>The diagnostics and generated source texts produced by the generator run.</returns>
    [RequiresAssemblyFiles("Calls System.Reflection.Assembly.Location")]
    private static (ImmutableArray<Diagnostic> Diagnostics, string[] GeneratedSources) RunGenerators(
        string source,
        bool includeAsyncReference)
    {
        var parseOptions = CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview);
        var references = CreateReferences(includeAsyncReference);
        var compilation = CSharpCompilation.Create(
            "AsyncBridgeGeneratorSmoke",
            [CSharpSyntaxTree.ParseText(source, parseOptions)],
            references,
            new(OutputKind.DynamicallyLinkedLibrary));
        var driver = CSharpGeneratorDriver.Create(
            [new R3BridgeGenerator().AsSourceGenerator(), new R3AsyncBridgeGenerator().AsSourceGenerator()],
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
        var generatedSources = driver.GetRunResult().Results.SelectMany(result => result.GeneratedSources)
            .Select(sourceText => sourceText.SourceText.ToString()).ToArray();
        return (diagnostics, generatedSources);
    }

    /// <summary>Creates the bounded metadata reference set required by the generator smoke compilation.</summary>
    /// <param name = "includeAsyncReference">Whether to include the async primitives assembly reference.</param>
    /// <returns>The metadata references for the in-memory Roslyn compilation.</returns>
    [RequiresAssemblyFiles("Calls System.Reflection.Assembly.Location")]
    private static List<MetadataReference> CreateReferences(bool includeAsyncReference)
    {
        Dictionary<string, string> platformAssemblies = new(StringComparer.OrdinalIgnoreCase);
        foreach (var path in AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!.ToString()!.Split(Path.PathSeparator))
        {
            var name = Path.GetFileName(path);
            if (!string.IsNullOrEmpty(name))
            {
                _ = platformAssemblies.TryAdd(name, path);
            }
        }

        List<MetadataReference> references = new(PlatformReferenceNames.Length + 2);
        foreach (var name in PlatformReferenceNames)
        {
            if (platformAssemblies.TryGetValue(name, out var path))
            {
                references.Add(MetadataReference.CreateFromFile(path));
            }
        }

        references.Add(MetadataReference.CreateFromFile(typeof(Signal).Assembly.Location));
        references.Add(MetadataReference.CreateFromFile(typeof(StateSignal<>).Assembly.Location));
        if (includeAsyncReference)
        {
            references.Add(MetadataReference.CreateFromFile(typeof(IObservableAsync<>).Assembly.Location));
        }

        return references;
    }
}
