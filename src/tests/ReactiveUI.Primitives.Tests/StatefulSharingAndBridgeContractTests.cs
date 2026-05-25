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

/// <summary>
/// Tests stateful signals, sharing helpers, and bridge source generators.
/// </summary>
public class StatefulSharingAndBridgeContractTests
{
    /// <summary>
    /// Initial state value used by projection tests.
    /// </summary>
    private const int InitialStateValue = 10;

    /// <summary>
    /// Updated state value used by projection tests.
    /// </summary>
    private const int UpdatedStateValue = 11;

    /// <summary>
    /// First value observed through a shared signal.
    /// </summary>
    private const int FirstSharedValue = 1;

    /// <summary>
    /// Second value observed through a shared signal.
    /// </summary>
    private const int SecondSharedValue = 2;

    /// <summary>
    /// Value emitted after shared subscriptions are disposed.
    /// </summary>
    private const int UnobservedSharedValue = 3;

    /// <summary>
    /// First value observed through replay.
    /// </summary>
    private const int FirstReplayValue = 4;

    /// <summary>
    /// Second value observed through replay.
    /// </summary>
    private const int SecondReplayValue = 5;

    /// <summary>
    /// Successful command result.
    /// </summary>
    private const int CommandResult = 42;

    /// <summary>
    /// Generated System.Reactive bridge type marker.
    /// </summary>
    private const string SystemReactiveBridgeName = "SystemReactiveSignalBridge";

    /// <summary>
    /// Generated R3 bridge type marker.
    /// </summary>
    private const string R3BridgeName = "R3SignalBridge";

    /// <summary>
    /// Expected mutable state values.
    /// </summary>
    private static readonly int[] ExpectedStateValues = [InitialStateValue, UpdatedStateValue, UpdatedStateValue];

    /// <summary>
    /// Expected projected read-only state values.
    /// </summary>
    private static readonly string[] ExpectedReadOnlyValues = ["v:10", "v:11", "v:11"];

    /// <summary>
    /// Expected values for the first shared subscription.
    /// </summary>
    private static readonly int[] ExpectedFirstSharedValues = [FirstSharedValue];

    /// <summary>
    /// Expected values for the second shared subscription.
    /// </summary>
    private static readonly int[] ExpectedSecondSharedValues = [FirstSharedValue, SecondSharedValue];

    /// <summary>
    /// Expected replayed values.
    /// </summary>
    private static readonly int[] ExpectedReplayValues = [FirstReplayValue, SecondReplayValue];

    /// <summary>
    /// Expected command results.
    /// </summary>
    private static readonly int[] ExpectedCommandResults = [CommandResult];

    /// <summary>
    /// Expected command running-state notifications.
    /// </summary>
    private static readonly bool[] ExpectedRunningValues = [false, true, false];

    /// <summary>
    /// Verifies mutable state exposes latest values and read-only projected values.
    /// </summary>
    [Test]
    public void StatefulSignalsExposeLatestValuesAndReadOnlyProjections()
    {
        var state = new StateSignal<int>(InitialStateValue);
        var values = new List<int>();
        var readonlyValues = new List<string>();

        state.Changed.Subscribe(values.Add);
        using var readOnly = state.ToReadOnlyState(value => $"v:{value}");
        readOnly.Changed.Subscribe(readonlyValues.Add);

        state.Value = UpdatedStateValue;
        state.Refresh();

        Assert.Equal(UpdatedStateValue, state.Value);
        Assert.Equal("v:11", readOnly.Value);
        Assert.Equal(ExpectedStateValues, values);
        Assert.Equal(ExpectedReadOnlyValues, readonlyValues);
    }

    /// <summary>
    /// Verifies shared and replayed connectable signals control source subscriptions.
    /// </summary>
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
        source.OnNext(FirstSharedValue);
        firstSubscription.Dispose();
        source.OnNext(SecondSharedValue);
        secondSubscription.Dispose();
        source.OnNext(UnobservedSharedValue);

        Assert.Equal(1, sourceSubscriptions);
        Assert.Equal(ExpectedFirstSharedValues, first);
        Assert.Equal(ExpectedSecondSharedValues, second);

        var replayed = cold.ReplayLive(1);
        var replayConnection = replayed.Connect();
        var replayFirst = new List<int>();
        var replaySecond = new List<int>();
        replayed.Subscribe(replayFirst.Add);
        source.OnNext(FirstReplayValue);
        replayed.Subscribe(replaySecond.Add);
        source.OnNext(SecondReplayValue);
        replayConnection.Dispose();

        Assert.Equal(ExpectedReplayValues, replayFirst);
        Assert.Equal(ExpectedReplayValues, replaySecond);
    }

    /// <summary>
    /// Verifies command signals publish results, failures, and running state.
    /// </summary>
    /// <returns>A task that completes when the command assertions finish.</returns>
    [Test]
    public async Task CommandSignalPublishesResultsFailuresAndRunningState()
    {
        var canRun = new StateSignal<bool>(true);
        var command = new CommandSignal<int>(
            async token =>
            {
                await Task.Yield();
                token.ThrowIfCancellationRequested();
                return CommandResult;
            },
            canRun);

        var results = new List<int>();
        var running = new List<bool>();
        command.Results.Subscribe(results.Add);
        command.IsRunning.Changed.Subscribe(running.Add);

        var executed = await command.ExecuteAsync();
        canRun.Value = false;
        InvalidOperationException? rejected = null;
        try
        {
            await command.ExecuteAsync();
        }
        catch (InvalidOperationException error)
        {
            rejected = error;
        }

        Assert.NotNull(rejected);
        Assert.Equal(CommandResult, executed);
        Assert.Equal(ExpectedCommandResults, results);
        Assert.Equal(ExpectedRunningValues, running);
        Assert.Equal("Command cannot run.", rejected!.Message);
    }

    /// <summary>
    /// Verifies bridge generators emit adapters when external shapes are present.
    /// </summary>
    [Test]
    [RequiresAssemblyFiles]
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
        Assert.True(Array.Exists(generatedSources, static text => text.Contains(SystemReactiveBridgeName, StringComparison.Ordinal)));
        Assert.True(Array.Exists(generatedSources, static text => text.Contains(R3BridgeName, StringComparison.Ordinal)));
    }

    /// <summary>
    /// Verifies bridge generators skip adapters when external packages are absent.
    /// </summary>
    [Test]
    [RequiresAssemblyFiles]
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
        Assert.False(Array.Exists(generatedSources, static text => text.Contains(SystemReactiveBridgeName, StringComparison.Ordinal)));
        Assert.False(Array.Exists(generatedSources, static text => text.Contains(R3BridgeName, StringComparison.Ordinal)));
    }

    /// <summary>
    /// Runs the bridge source generators for the supplied source.
    /// </summary>
    /// <param name="source">Source code to compile.</param>
    /// <returns>Compilation diagnostics and generated source text.</returns>
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
