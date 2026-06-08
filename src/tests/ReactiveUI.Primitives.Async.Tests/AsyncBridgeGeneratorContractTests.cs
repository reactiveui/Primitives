// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.R3Bridge.Generator;
using ReactiveUI.Primitives.Signals;
using ReactiveUI.Primitives.SystemReactiveBridge.Generator;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Contract tests for bridge source generators that conditionally emit async adapter APIs.</summary>
public sealed class AsyncBridgeGeneratorContractTests
{
    /// <summary>The generated System.Reactive async bridge type name.</summary>
    private const string SystemReactiveAsyncBridgeName = "SystemReactiveAsyncBridge";

    /// <summary>The generated R3 async bridge type name.</summary>
    private const string R3AsyncBridgeName = "R3AsyncBridge";

    /// <summary>Verifies bridge generators emit async adapter extensions when async primitives are referenced.</summary>
    [Test]
    [RequiresAssemblyFiles]
    public void BridgeGeneratorsEmitAsyncAdaptersOnlyWhenAsyncShapesArePresent()
    {
        const string Source = """
using System;
using System.Threading;
using System.Threading.Tasks;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.SystemReactiveBridge;
using ReactiveUI.Primitives.R3Bridge;

namespace System.Reactive.Linq
{
    public static class Observable { }
}

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

public static class AsyncBridgeSmoke
{
    public static void Use(IObservable<int> source, IObservableAsync<int> asyncSource, R3.Observable<int> r3)
    {
        IObservableAsync<int> fromSystem = source.ToObservableAsync();
        IObservable<int> toSystem = asyncSource.ToObservable();
        IObservableAsync<int> fromR3 = r3.AsPrimitivesAsyncObservable();
        R3.Observable<int> toR3 = asyncSource.AsR3Observable();
    }
}
""";

        var (diagnostics, generatedSources) = RunGenerators(Source, includeAsyncReference: true);

        Assert.Equal(0, diagnostics.Length);
        Assert.True(Array.Exists(generatedSources, static text => text.Contains(SystemReactiveAsyncBridgeName, StringComparison.Ordinal)));
        Assert.True(Array.Exists(generatedSources, static text => text.Contains(R3AsyncBridgeName, StringComparison.Ordinal)));
    }

    /// <summary>Verifies bridge generators skip async adapter extensions when async primitives are absent.</summary>
    [Test]
    [RequiresAssemblyFiles]
    public void BridgeGeneratorsSkipAsyncAdaptersWhenAsyncAssemblyIsAbsent()
    {
        const string Source = """
using System;
using ReactiveUI.Primitives.SystemReactiveBridge;
using ReactiveUI.Primitives.R3Bridge;

namespace System.Reactive.Linq
{
    public static class Observable { }
}

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

public static class CoreOnlySmoke
{
    public static (IObservable<int> System, IObservable<int> R3) Use(IObservable<int> source, R3.Observable<int> r3) =>
        (source.AsSystemObservable(), r3.AsPrimitivesSignal());
}
""";

        var (diagnostics, generatedSources) = RunGenerators(Source, includeAsyncReference: false);

        Assert.Equal(0, diagnostics.Length);
        Assert.False(Array.Exists(generatedSources, static text => text.Contains(SystemReactiveAsyncBridgeName, StringComparison.Ordinal)));
        Assert.False(Array.Exists(generatedSources, static text => text.Contains(R3AsyncBridgeName, StringComparison.Ordinal)));
    }

    /// <summary>Runs the System.Reactive and R3 bridge generators against an in-memory compilation.</summary>
    /// <param name="source">The source text to compile.</param>
    /// <param name="includeAsyncReference">Whether to include the async primitives assembly reference.</param>
    /// <returns>The diagnostics and generated source texts produced by the generator run.</returns>
    [RequiresAssemblyFiles("Calls System.Reflection.Assembly.Location")]
    private static (ImmutableArray<Diagnostic> Diagnostics, string[] GeneratedSources) RunGenerators(
        string source,
        bool includeAsyncReference)
    {
        var parseOptions = CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview);
        var references = AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!
            .ToString()!
            .Split(Path.PathSeparator)
            .Where(path =>
                !Path.GetFileName(path).StartsWith("System.Reactive", StringComparison.OrdinalIgnoreCase) &&
                !Path.GetFileName(path).StartsWith("R3", StringComparison.OrdinalIgnoreCase) &&
                !Path.GetFileName(path).StartsWith("ReactiveUI.Primitives.Async", StringComparison.OrdinalIgnoreCase))
            .Select(path => MetadataReference.CreateFromFile(path))
            .Cast<MetadataReference>()
            .ToList();

        references.Add(MetadataReference.CreateFromFile(typeof(Signal).Assembly.Location));
        if (includeAsyncReference)
        {
            references.Add(MetadataReference.CreateFromFile(typeof(IObservableAsync<>).Assembly.Location));
        }

        var compilation = CSharpCompilation.Create(
            "AsyncBridgeGeneratorSmoke",
            [CSharpSyntaxTree.ParseText(source, parseOptions)],
            references,
            new(OutputKind.DynamicallyLinkedLibrary));

        var driver = CSharpGeneratorDriver.Create(
            [
                new SystemReactiveBridgeGenerator().AsSourceGenerator(),
                new R3BridgeGenerator().AsSourceGenerator(),
            ],
            parseOptions: parseOptions);

        driver = (CSharpGeneratorDriver)driver.RunGeneratorsAndUpdateCompilation(compilation, out var updatedCompilation, out var generatorDiagnostics);
        var diagnostics = generatorDiagnostics
            .Concat(updatedCompilation.GetDiagnostics().Where(diagnostic => diagnostic.Severity == DiagnosticSeverity.Error))
            .ToImmutableArray();
        var generatedSources = driver.GetRunResult().Results
            .SelectMany(result => result.GeneratedSources)
            .Select(sourceText => sourceText.SourceText.ToString())
            .ToArray();

        return (diagnostics, generatedSources);
    }
}
