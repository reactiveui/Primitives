// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#pragma warning disable SA1600, SA1611, SA1615, SA1618, S1118, S1144, S125, CA1034, CA1812, CA1822, IDE0051

using System;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.R3Bridge.Generator;
using ReactiveUI.Primitives.Signals;
using ReactiveUI.Primitives.SystemReactiveBridge.Generator;
using TUnit.Core;

namespace ReactiveUI.Primitives.Tests;

public sealed class AsyncBridgeGeneratorContractTests
{
    private const string SystemReactiveAsyncBridgeName = "SystemReactiveAsyncBridge";

    private const string R3AsyncBridgeName = "R3AsyncBridge";

    [Test]
    [RequiresAssemblyFiles]
    public void BridgeGeneratorsEmitAsyncAdaptersOnlyWhenAsyncShapesArePresent()
    {
        const string source = """
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

        var (diagnostics, generatedSources) = RunGenerators(source, includeAsyncReference: true);

        Assert.Equal(0, diagnostics.Length);
        Assert.True(Array.Exists(generatedSources, static text => text.Contains(SystemReactiveAsyncBridgeName, StringComparison.Ordinal)));
        Assert.True(Array.Exists(generatedSources, static text => text.Contains(R3AsyncBridgeName, StringComparison.Ordinal)));
    }

    [Test]
    [RequiresAssemblyFiles]
    public void BridgeGeneratorsSkipAsyncAdaptersWhenAsyncAssemblyIsAbsent()
    {
        const string source = """
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

        var (diagnostics, generatedSources) = RunGenerators(source, includeAsyncReference: false);

        Assert.Equal(0, diagnostics.Length);
        Assert.False(Array.Exists(generatedSources, static text => text.Contains(SystemReactiveAsyncBridgeName, StringComparison.Ordinal)));
        Assert.False(Array.Exists(generatedSources, static text => text.Contains(R3AsyncBridgeName, StringComparison.Ordinal)));
    }

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
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

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
