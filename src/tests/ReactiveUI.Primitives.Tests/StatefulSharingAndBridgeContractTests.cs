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
using ReactiveUI.Primitives.Disposables;
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
    /// Generated System.Reactive scheduler bridge type marker.
    /// </summary>
    private const string SystemReactiveSchedulerBridgeName = "SystemReactiveSchedulerBridge";

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
    /// Expected scheduler bridge recursive ordering.
    /// </summary>
    private static readonly int[] ExpectedBridgeScheduleValues = [FirstSharedValue, SecondSharedValue];

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

        var shared = cold.ShareLatest();
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
    /// Verifies connectable aliases, auto-connect validation, and replay window overloads.
    /// </summary>
    [Test]
    public void ConnectableAliasesValidateAndConnectAtThreshold()
    {
        var source = new Signal<int>();
        var sourceSubscriptions = 0;
        var cold = Signal.Create<int>(observer =>
        {
            sourceSubscriptions++;
            return source.Subscribe(observer);
        });

        var auto = cold.Share().AutoConnect(2);
        var first = new List<int>();
        var second = new List<int>();
        using var firstSubscription = auto.Subscribe(first.Add);
        source.OnNext(FirstSharedValue);
        using var secondSubscription = auto.Subscribe(second.Add);
        source.OnNext(SecondSharedValue);

        Assert.Equal(1, sourceSubscriptions);
        Assert.Equal(ExpectedSecondSharedValues[1..], first);
        Assert.Equal(ExpectedSecondSharedValues[1..], second);
        Assert.Throws<ArgumentNullException>(() => ConnectableSignalMixins.Multicast(null!, new Signal<int>()));
        Assert.Throws<ArgumentNullException>(() => Signal.Silent<int>().Multicast(null!));
        Assert.Throws<ArgumentNullException>(() => ConnectableSignalMixins.AutoShare<int>(null!));
        Assert.Throws<ArgumentNullException>(() => ConnectableSignalMixins.AutoConnect<int>(null!));
        Assert.Throws<ArgumentOutOfRangeException>(() => cold.ShareLive().AutoConnect(-1));

        var replayed = cold.Replay(1, TimeSpan.FromSeconds(1));
        using var connection = replayed.Connect();
        source.OnNext(FirstReplayValue);
        var replayValues = new List<int>();
        replayed.Subscribe(replayValues.Add);

        Assert.Equal(ExpectedReplayValues[..1], replayValues);
    }

    /// <summary>
    /// Verifies command aliases, sync execution failures, and disposal branches.
    /// </summary>
    /// <returns>A task that completes when command assertions finish.</returns>
    [Test]
    public async Task CommandSignalCoversSyncFaultAndDisposalBranches()
    {
        var behavior = new BehaviorSignal<int>(InitialStateValue);
        var disposable = new MultipleDisposable(Disposable.Empty);
        var fault = new InvalidOperationException("sync failed");
        var command = new CommandSignal<int>(() => throw fault);
        var results = new List<int>();
        var faults = new List<Exception>();

        command.Results.Subscribe(results.Add);
        command.Faults.Subscribe(faults.Add);
        behavior.OnNext(UpdatedStateValue);
        disposable.Dispose();

        InvalidOperationException? observed = null;
        try
        {
            await command.ExecuteAsync();
        }
        catch (InvalidOperationException error)
        {
            observed = error;
        }

        command.Dispose();
        command.Dispose();
        ObjectDisposedException? disposed = null;
        try
        {
            await command.ExecuteAsync();
        }
        catch (ObjectDisposedException error)
        {
            disposed = error;
        }

        Assert.Same(fault, observed!);
        Assert.Equal(0, results.Count);
        Assert.Equal(1, faults.Count);
        Assert.Same(fault, faults[0]);
        Assert.Equal(UpdatedStateValue, behavior.Value);
        Assert.True(disposable.IsDisposed);
        Assert.NotNull(disposed);
    }

    /// <summary>
    /// Verifies bridge generators emit adapters when external shapes are present.
    /// </summary>
    [Test]
    [RequiresAssemblyFiles]
    [SuppressMessage("Major Code Smell", "S138:Functions should not have too many lines", Justification = "Embedded generator smoke source keeps the emitted API contract local to the test.")]
    public void BridgeGeneratorsEmitOnlyWhenExternalShapesArePresentAndCompileSmokeAdapters()
    {
        const string source = """
using System;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;
using ReactiveUI.Primitives.SystemReactiveBridge;
using ReactiveUI.Primitives.R3Bridge;

namespace System.Reactive.Linq
{
    public static class Observable { }
}

namespace System.Reactive.Concurrency
{
    public interface IScheduler
    {
        DateTimeOffset Now { get; }

        IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action);

        IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<IScheduler, TState, IDisposable> action);

        IDisposable Schedule<TState>(TState state, DateTimeOffset dueTime, Func<IScheduler, TState, IDisposable> action);
    }
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

public static class BridgeSmoke
{
    public static void Use(IObservable<int> source, R3.Observable<int> r3, System.Reactive.Concurrency.IScheduler scheduler, ISequencer sequencer)
    {
        IObservable<int> PrimitivesFromSystem = source.AsPrimitivesSignal();
        IObservable<int> PrimitivesFromR3 = r3.AsPrimitivesSignal();
        IObservable<int> system = PrimitivesFromSystem.AsSystemObservable();
        R3.Observable<int> r3Again = PrimitivesFromR3.AsR3Observable();
        ISequencer fromSystemScheduler = scheduler.AsSequencer();
        System.Reactive.Concurrency.IScheduler systemScheduler = sequencer.AsSystemScheduler();
        systemScheduler.Schedule(1, static (_, _) => Disposable.Empty);
    }
}
""";

        var (diagnostics, generatedSources) = RunGenerators(source);

        Assert.Equal(0, diagnostics.Length);
        Assert.True(Array.Exists(generatedSources, static text => text.Contains(SystemReactiveBridgeName, StringComparison.Ordinal)));
        Assert.True(Array.Exists(generatedSources, static text => text.Contains(SystemReactiveSchedulerBridgeName, StringComparison.Ordinal)));
        Assert.True(Array.Exists(generatedSources, static text => text.Contains(R3BridgeName, StringComparison.Ordinal)));
    }

    /// <summary>
    /// Verifies generated System.Reactive scheduler adapters preserve immediate recursive scheduling order.
    /// </summary>
    [Test]
    [RequiresAssemblyFiles]
    [SuppressMessage("Major Code Smell", "S138:Functions should not have too many lines", Justification = "Embedded generator smoke source keeps the emitted API contract local to the test.")]
    public void SystemReactiveSchedulerBridgePreservesImmediateRecursiveOrdering()
    {
        const string source = """
using System;
using System.Collections.Generic;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.SystemReactiveBridge;

namespace System.Reactive.Concurrency
{
    public interface IScheduler
    {
        DateTimeOffset Now { get; }

        IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action);

        IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<IScheduler, TState, IDisposable> action);

        IDisposable Schedule<TState>(TState state, DateTimeOffset dueTime, Func<IScheduler, TState, IDisposable> action);
    }
}

public static class BridgeSchedulerRuntimeSmoke
{
    public static int[] RunSystemSchedulerToSequencer()
    {
        var sequencer = new ImmediateScheduler().AsSequencer();
        var calls = new List<int>();

        sequencer.Schedule(new ReentrantWorkItem(sequencer, calls, 1));

        return calls.ToArray();
    }

    public static int[] RunSequencerToSystemScheduler()
    {
        var scheduler = new ImmediateSequencer().AsSystemScheduler();
        var calls = new List<int>();

        scheduler.Schedule(1, (self, state) =>
        {
            calls.Add(state);
            self.Schedule(2, (_, next) =>
            {
                calls.Add(next);
                return Disposable.Empty;
            });

            return Disposable.Empty;
        });

        return calls.ToArray();
    }

    private sealed class ImmediateScheduler : System.Reactive.Concurrency.IScheduler
    {
        public DateTimeOffset Now => DateTimeOffset.UnixEpoch;

        public IDisposable Schedule<TState>(TState state, Func<System.Reactive.Concurrency.IScheduler, TState, IDisposable> action) =>
            action(this, state);

        public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<System.Reactive.Concurrency.IScheduler, TState, IDisposable> action) =>
            action(this, state);

        public IDisposable Schedule<TState>(TState state, DateTimeOffset dueTime, Func<System.Reactive.Concurrency.IScheduler, TState, IDisposable> action) =>
            action(this, state);
    }

    private sealed class ImmediateSequencer : ISequencer
    {
        public DateTimeOffset Now => DateTimeOffset.UnixEpoch;

        public long Timestamp => 0;

        public void Schedule(IWorkItem item) => item.Execute();

        public void Schedule(IWorkItem item, long dueTimestamp) => item.Execute();
    }

    private sealed class ReentrantWorkItem : IWorkItem
    {
        private readonly ISequencer _sequencer;
        private readonly List<int> _calls;
        private readonly int _value;

        public ReentrantWorkItem(ISequencer sequencer, List<int> calls, int value)
        {
            _sequencer = sequencer;
            _calls = calls;
            _value = value;
        }

        public void Execute()
        {
            _calls.Add(_value);
            if (_value == 1)
            {
                _sequencer.Schedule(new ReentrantWorkItem(_sequencer, _calls, 2));
            }
        }
    }
}
""";

        var systemToPrimitives = (int[])InvokeGeneratedBridge(source, "BridgeSchedulerRuntimeSmoke", "RunSystemSchedulerToSequencer");
        var primitivesToSystem = (int[])InvokeGeneratedBridge(source, "BridgeSchedulerRuntimeSmoke", "RunSequencerToSystemScheduler");

        Assert.Equal(ExpectedBridgeScheduleValues.AsEnumerable(), systemToPrimitives);
        Assert.Equal(ExpectedBridgeScheduleValues.AsEnumerable(), primitivesToSystem);
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
    public static IObservable<int> Use() => Signal.Emit(1);
}
""";

        var (diagnostics, generatedSources) = RunGenerators(source);

        Assert.Equal(0, diagnostics.Length);
        Assert.False(Array.Exists(generatedSources, static text => text.Contains(SystemReactiveBridgeName, StringComparison.Ordinal)));
        Assert.False(Array.Exists(generatedSources, static text => text.Contains(SystemReactiveSchedulerBridgeName, StringComparison.Ordinal)));
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
        var (diagnostics, generatedSources, _) = CompileWithGenerators(source);
        return (diagnostics, generatedSources);
    }

    /// <summary>
    /// Runs the bridge source generators and invokes a generated bridge smoke method.
    /// </summary>
    /// <param name="source">Source code to compile.</param>
    /// <param name="typeName">Type containing the method.</param>
    /// <param name="methodName">Static method name.</param>
    /// <returns>The method result.</returns>
    [RequiresAssemblyFiles("Calls System.Reflection.Assembly.Location")]
    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "The test intentionally loads a Roslyn-emitted in-memory assembly.")]
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "The test invokes a known public static method from a Roslyn-emitted smoke type.")]
    private static object InvokeGeneratedBridge(string source, string typeName, string methodName)
    {
        var (diagnostics, _, updatedCompilation) = CompileWithGenerators(source);
        Assert.Equal(0, diagnostics.Length);

        using var stream = new MemoryStream();
        var emit = updatedCompilation.Emit(stream);
        if (!emit.Success)
        {
            throw new InvalidOperationException(string.Join(Environment.NewLine, emit.Diagnostics));
        }

        stream.Position = 0;
        var assembly = System.Reflection.Assembly.Load(stream.ToArray());
        var result = assembly.GetType(typeName)!.GetMethod(methodName)!.Invoke(null, null);
        return result!;
    }

    /// <summary>
    /// Compiles the supplied source with bridge source generators.
    /// </summary>
    /// <param name="source">Source code to compile.</param>
    /// <returns>Compilation diagnostics, generated source text, and updated compilation.</returns>
    [RequiresAssemblyFiles("Calls System.Reflection.Assembly.Location")]
    private static (ImmutableArray<Diagnostic> Diagnostics, string[] GeneratedSources, Compilation UpdatedCompilation) CompileWithGenerators(string source)
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

        return (diagnostics, generatedSources, updatedCompilation);
    }
}
