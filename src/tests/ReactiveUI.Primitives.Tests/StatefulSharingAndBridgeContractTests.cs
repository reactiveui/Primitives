// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.R3Bridge.Generator;
using ReactiveUI.Primitives.Signals;
using ReactiveUI.Primitives.SystemReactiveBridge.Generator;
using TUnit.Core;

namespace ReactiveUI.Primitives.Tests;

public class StatefulSharingAndBridgeContractTests
{
    [Test]
    public void StatefulSignalsExposeLatestValuesAndReadOnlyProjections()
    {
        var state = new StateSignal<int>(10);
        var values = new List<int>();
        var readonlyValues = new List<string>();

        state.Changed.Subscribe(values.Add);
        using var readOnly = state.ToReadOnlyState(value => $"v:{value}");
        readOnly.Changed.Subscribe(readonlyValues.Add);

        state.Value = 11;
        state.Refresh();

        Assert.Equal(11, state.Value);
        Assert.Equal("v:11", readOnly.Value);
        Assert.Equal(new[] { 10, 11, 11 }, values);
        Assert.Equal(new[] { "v:10", "v:11", "v:11" }, readonlyValues);
    }

    [Test]
    public void ConnectableShareAndReplayLiveControlSourceSubscriptions()
    {
        var source = new Signal<int>();
        var sourceSubscriptions = 0;
        var cold = Signal.Create<int>(observer =>
        {
            sourceSubscriptions++;
            return source.Subscribe(observer);
        });

        var shared = cold.ShareLive();
        var first = new List<int>();
        var second = new List<int>();

        using var firstSubscription = shared.Subscribe(first.Add);
        using var secondSubscription = shared.Subscribe(second.Add);
        source.OnNext(1);
        firstSubscription.Dispose();
        source.OnNext(2);
        secondSubscription.Dispose();
        source.OnNext(3);

        Assert.Equal(1, sourceSubscriptions);
        Assert.Equal(new[] { 1 }, first);
        Assert.Equal(new[] { 1, 2 }, second);

        var replayed = cold.ReplayLive(1);
        var replayConnection = replayed.Connect();
        var replayFirst = new List<int>();
        var replaySecond = new List<int>();
        replayed.Subscribe(replayFirst.Add);
        source.OnNext(4);
        replayed.Subscribe(replaySecond.Add);
        source.OnNext(5);
        replayConnection.Dispose();

        Assert.Equal(new[] { 4, 5 }, replayFirst);
        Assert.Equal(new[] { 4, 5 }, replaySecond);
    }

    [Test]
    public async Task CommandSignalPublishesResultsFailuresAndRunningState()
    {
        var canRun = new StateSignal<bool>(true);
        var command = new CommandSignal<int>(
            async token =>
        {
            await Task.Yield();
            token.ThrowIfCancellationRequested();
            return 42;
        }, canRun);

        var results = new List<int>();
        var running = new List<bool>();
        command.Results.Subscribe(results.Add);
        command.IsRunning.Changed.Subscribe(running.Add);

        var executed = await command.ExecuteAsync();
        canRun.Value = false;
        var rejected = Assert.Throws<InvalidOperationException>(() => command.ExecuteAsync().GetAwaiter().GetResult());

        Assert.Equal(42, executed);
        Assert.Equal(new[] { 42 }, results);
        Assert.Equal(new[] { false, true, false }, running);
        Assert.Equal("Command cannot run.", rejected.Message);
    }

    [Test]
    [RequiresAssemblyFiles()]
    public void BridgeGeneratorsEmitOnlyWhenExternalShapesArePresentAndCompileSmokeAdapters()
    {
        const string source = """
using System;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Signals;
using ReactiveUI.Primitives.SystemReactiveBridge;
using ReactiveUI.Primitives.R3Bridge;

namespace System.Reactive.Linq
{
    public static class Observable { }
}

namespace R3
{
    public abstract class Observable<T>
    {
        public abstract IDisposable Subscribe(IObserver<T> observer);

        public static Observable<T> Create(Func<IObserver<T>, IDisposable> subscribe) => new DelegateObservable<T>(subscribe);

        private sealed class DelegateObservable<TValue> : Observable<TValue>
        {
            private readonly Func<IObserver<TValue>, IDisposable> _subscribe;
            public DelegateObservable(Func<IObserver<TValue>, IDisposable> subscribe) => _subscribe = subscribe;
            public override IDisposable Subscribe(IObserver<TValue> observer) => _subscribe(observer);
        }
    }
}

public static class BridgeSmoke
{
    public static void Use(IObservable<int> source, R3.Observable<int> r3)
    {
        IObservable<int> PrimitivesFromSystem = source.AsPrimitivesSignal();
        IObservable<int> PrimitivesFromR3 = r3.AsPrimitivesSignal();
        IObservable<int> system = PrimitivesFromSystem.AsSystemObservable();
        R3.Observable<int> r3Again = PrimitivesFromR3.AsR3Observable();
    }
}
""";

        var (diagnostics, generatedSources) = RunGenerators(source);

        Assert.Equal(0, diagnostics.Length);
        Assert.True(generatedSources.Any(text => text.Contains("SystemReactiveSignalBridge")));
        Assert.True(generatedSources.Any(text => text.Contains("R3SignalBridge")));
    }

    [Test]
    [RequiresAssemblyFiles()]
    public void BridgeGeneratorsDoNotEmitExternalAdaptersWhenExternalPackagesAreAbsent()
    {
        const string source = """
using System;
using ReactiveUI.Primitives.Signals;

public static class CoreOnlySmoke
{
    public static IObservable<int> Use() => Signal.Return(1);
}
""";

        var (diagnostics, generatedSources) = RunGenerators(source);

        Assert.Equal(0, diagnostics.Length);
        Assert.False(generatedSources.Any(text => text.Contains("SystemReactiveSignalBridge")));
        Assert.False(generatedSources.Any(text => text.Contains("R3SignalBridge")));
    }

    [RequiresAssemblyFiles("Calls System.Reflection.Assembly.Location")]
    private static (ImmutableArray<Diagnostic> Diagnostics, string[] GeneratedSources) RunGenerators(string source)
    {
        var parseOptions = CSharpParseOptions.Default.WithLanguageVersion(LanguageVersion.Preview);
        var references = AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!
            .ToString()!
            .Split(Path.PathSeparator)
            .Where(path =>
                !Path.GetFileName(path).StartsWith("System.Reactive", StringComparison.OrdinalIgnoreCase) &&
                !Path.GetFileName(path).StartsWith("R3", StringComparison.OrdinalIgnoreCase))
            .Select(path => MetadataReference.CreateFromFile(path))
            .Cast<MetadataReference>()
            .ToList();

        references.Add(MetadataReference.CreateFromFile(typeof(Signal).Assembly.Location));
        references.Add(MetadataReference.CreateFromFile(typeof(StateSignal<>).Assembly.Location));

        var compilation = CSharpCompilation.Create(
            "BridgeGeneratorSmoke",
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
