// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Harness-free allocation probe (<c>--alloc</c>). Measures the exact bytes allocated per operation
/// with <see cref="GC.GetAllocatedBytesForCurrentThread"/>, reusing a single observer so the
/// reported figure is the operator's own allocation -- not the per-op test observer that
/// BenchmarkDotNet's <c>Allocated</c> column folds in. Run with:
/// <c>dotnet run -c Release --project ... -- --alloc</c>.
/// </summary>
internal static class AllocationProbe
{
    /// <summary>
    /// Shared single-character payload reused by reference/type-coercion probes.
    /// </summary>
    private const string ProbeShared = "x";

    /// <summary>
    /// Number of warm-up iterations executed before measurement begins.
    /// </summary>
    private const int Warmup = 50;

    /// <summary>
    /// Number of measured iterations used to compute the per-op allocation average.
    /// </summary>
    private const int Iterations = 1000;

    /// <summary>
    /// Element count emitted by the standard probe sequences.
    /// </summary>
    private const int Count = 32;

    /// <summary>
    /// Number of concurrent subscriptions exercised by the fan-out churn probe.
    /// </summary>
    private const int FanOut = 64;

    /// <summary>
    /// Single sample value emitted by the <c>Return</c> probe.
    /// </summary>
    private const int ReturnValue = 7;

    /// <summary>
    /// Element count to skip / take / buffer in the windowing probes.
    /// </summary>
    private const int Window = 8;

    /// <summary>
    /// Divisor used by the <c>UniqueBy</c> key selector.
    /// </summary>
    private const int KeyDivisor = 2;

    /// <summary>
    /// Inner sequence length used by the <c>FlatMap</c> probe.
    /// </summary>
    private const int InnerCount = 2;

    /// <summary>
    /// Retry count used by the <c>Reattempt</c> probe.
    /// </summary>
    private const int RetryCount = 2;

    /// <summary>
    /// Source length used by the <c>FlatMap</c> probe.
    /// </summary>
    private const int FlatMapSourceCount = 8;

    /// <summary>
    /// Exclusive upper bound used by the <c>TakeWhile</c> probe predicate.
    /// </summary>
    private const int TakeWhileBound = 24;

    /// <summary>
    /// Start value used by the secondary (right-hand) probe sequences.
    /// </summary>
    private const int RightStart = 10;

    /// <summary>
    /// Per-element multiplier used by the <c>FlatMap</c> inner factory.
    /// </summary>
    private const int FlatMapMultiplier = 10;

    /// <summary>
    /// Factor used by the <c>MapWith</c> probe.
    /// </summary>
    private const int MapWithFactor = 3;

    /// <summary>
    /// History buffer capacity used by the <c>ReplaySignal</c> probe.
    /// </summary>
    private const int HistoryCapacity = 16;

    /// <summary>
    /// Width, in characters, of the console separator rule.
    /// </summary>
    private const int SeparatorWidth = 56;

    /// <summary>
    /// Left-padding width applied to the operator name column.
    /// </summary>
    private const int NameColumnWidth = -34;

    /// <summary>
    /// Right-padding width applied to the bytes-per-op column.
    /// </summary>
    private const int ValueColumnWidth = 6;

    /// <summary>
    /// Runs the probe and prints a per-operator allocation table.
    /// </summary>
    public static void Run()
    {
        // Shared, reused observer: allocated once here, never inside a measured op, so its bytes
        // are excluded from every measurement.
        var observer = new IntSignalObserver();
        var sparkObserver = new CountingSignalObserver<Spark<int>>();
        var intervalObserver = new CountingSignalObserver<TimeInterval<int>>();
        var listObserver = new CountingSignalObserver<IList<int>>();
        var arrayObserver = new CountingSignalObserver<int[]>();
        var stringObserver = new CountingSignalObserver<string>();
        var error = new InvalidOperationException("probe");
        var failSource = Signal.Fail<int>(error, Sequencer.Immediate);
        var handles = new IDisposable[FanOut];

        Console.WriteLine("Operator allocation — bytes/op, observer excluded (GC.GetAllocatedBytesForCurrentThread)");
        Console.WriteLine(new string('-', SeparatorWidth));

        Section("Factories / sources");
        ProbeFactories(observer);

        Section("Stateful single-source operators");
        ProbeStatefulOperators(observer);

        Section("Projection / combination");
        ProbeProjection(observer);

        Section("Pass-through / terminal (Tranche B-D)");
        ProbePassThroughTerminal(observer, sparkObserver, intervalObserver, listObserver, arrayObserver);

        Section("Coverage-gap operators / factories");
        ProbeCoverageGap(observer, listObserver, stringObserver, failSource);

        // Calm / Shift / DelayStart need time advancement; their allocation is captured by the
        // OperatorTimeSchedulerBenchmarks BDN "Allocated" column instead of this synchronous probe.
        Section("Subjects (construct + subscribe + one emit)");
        ProbeSubjects(observer);

        Section("Connectable");
        ProbeConnectable(observer);

        Section($"Subscribe/dispose churn (x{FanOut})");
        ProbeChurn(observer, handles);
    }

    /// <summary>
    /// Probes factory and source allocation.
    /// </summary>
    /// <param name="observer">The reused observer.</param>
    private static void ProbeFactories(IntSignalObserver observer)
    {
        Row("RangeSubscribe (baseline)", () => Signal.Sequence(0, Count).Subscribe(observer).Dispose());
        Row("Return", () => Signal.Emit(ReturnValue).Subscribe(observer).Dispose());
        Row("Empty", () => Signal.None<int>().Subscribe(observer).Dispose());
    }

    /// <summary>
    /// Probes stateful single-source operator allocation.
    /// </summary>
    /// <param name="observer">The reused observer.</param>
    private static void ProbeStatefulOperators(IntSignalObserver observer)
    {
        Row("Skip", () => Signal.Sequence(0, Count).Skip(Window).Subscribe(observer).Dispose());
        Row("Distinct", () => Signal.Sequence(0, Count).Distinct().Subscribe(observer).Dispose());
        Row("Unique", () => Signal.Sequence(0, Count).Unique().Subscribe(observer).Dispose());
        Row("UniqueBy", () => Signal.Sequence(0, Count).UniqueBy(static x => x / KeyDivisor).Subscribe(observer).Dispose());
        Row("Fold", () => Signal.Sequence(0, Count).Fold(0, static (a, x) => a + x).Subscribe(observer).Dispose());
        Row("Reduce", () => Signal.Sequence(0, Count).Reduce(0, static (a, x) => a + x).Subscribe(observer).Dispose());
        Row("TakeWhile", () => Signal.Sequence(0, Count).TakeWhile(static x => x < TakeWhileBound).Subscribe(observer).Dispose());
        Row("SkipWhile", () => Signal.Sequence(0, Count).SkipWhile(static x => x < Window).Subscribe(observer).Dispose());
    }

    /// <summary>
    /// Probes projection and combination operator allocation.
    /// </summary>
    /// <param name="observer">The reused observer.</param>
    private static void ProbeProjection(IntSignalObserver observer)
    {
        Row("Map", () => Signal.Sequence(0, Count).Map(static x => x + 1).Subscribe(observer).Dispose());
        Row("Keep", () => Signal.Sequence(0, Count).Keep(static x => (x & 1) == 0).Subscribe(observer).Dispose());
        Row("Map+Keep", () => Signal.Sequence(0, Count).Map(static x => x + 1).Keep(static x => (x & 1) == 0).Subscribe(observer).Dispose());
        Row("Zip", () => Signal.Pair(Signal.Sequence(0, Count), Signal.Sequence(0, Count), static (l, r) => l + r).Subscribe(observer).Dispose());
        Row("WithLatest (Latch)", () => Signal.Sequence(1, Count).Latch(Signal.Sequence(RightStart, Count), static (l, r) => l + r).Subscribe(observer).Dispose());
        Row("CombineLatest (SyncLatest)", () => Signal.Sequence(0, Count).SyncLatest(Signal.Sequence(RightStart, Count), static (l, r) => l + r).Subscribe(observer).Dispose());
        Row("ForkJoin", () => Signal.Sequence(0, Count).ForkJoin(Signal.Sequence(RightStart, Count), static (l, r) => l + r).Subscribe(observer).Dispose());
        Row("FlatMap", () => Signal.Sequence(1, FlatMapSourceCount).FlatMap(static x => Signal.Sequence(x * FlatMapMultiplier, InnerCount)).Subscribe(observer).Dispose());
    }

    /// <summary>
    /// Probes pass-through and terminal operator allocation.
    /// </summary>
    /// <param name="observer">The reused integer observer.</param>
    /// <param name="sparkObserver">The reused spark observer.</param>
    /// <param name="intervalObserver">The reused time-interval observer.</param>
    /// <param name="listObserver">The reused list observer.</param>
    /// <param name="arrayObserver">The reused array observer.</param>
    private static void ProbePassThroughTerminal(
        IntSignalObserver observer,
        CountingSignalObserver<Spark<int>> sparkObserver,
        CountingSignalObserver<TimeInterval<int>> intervalObserver,
        CountingSignalObserver<IList<int>> listObserver,
        CountingSignalObserver<int[]> arrayObserver)
    {
        Row("Tap", () => Signal.Sequence(0, Count).Tap(static _ => { }).Subscribe(observer).Dispose());
        Row("IgnoreValues", () => Signal.Sequence(0, Count).IgnoreValues().Subscribe(observer).Dispose());
        Row("Spark (materialize)", () => Signal.Sequence(0, Count).Spark().Subscribe(sparkObserver).Dispose());
        Row("Unspark (Spark->Unspark)", () => Signal.Sequence(0, Count).Spark().Unspark().Subscribe(observer).Dispose());
        Row("TimeInterval", () => Signal.Sequence(0, Count).TimeInterval(Sequencer.Immediate).Subscribe(intervalObserver).Dispose());
        Row("SubscribeOn (Immediate)", () => Signal.Sequence(0, Count).SubscribeOn(Sequencer.Immediate).Subscribe(observer).Dispose());
        Row("Reattempt", () => Signal.Sequence(0, Count).Reattempt(RetryCount).Subscribe(observer).Dispose());
        Row("CollectList (range)", () => Signal.Sequence(0, Count).CollectList().Subscribe(listObserver).Dispose());
        Row("CollectArray (range)", () => Signal.Sequence(0, Count).CollectArray().Subscribe(arrayObserver).Dispose());
    }

    /// <summary>
    /// Probes coverage-gap operator and factory allocation.
    /// </summary>
    /// <param name="observer">The reused integer observer.</param>
    /// <param name="listObserver">The reused list observer.</param>
    /// <param name="stringObserver">The reused string observer.</param>
    /// <param name="failSource">The shared failing source.</param>
    private static void ProbeCoverageGap(
        IntSignalObserver observer,
        CountingSignalObserver<IList<int>> listObserver,
        CountingSignalObserver<string> stringObserver,
        IObservable<int> failSource)
    {
        Row("Take", () => Signal.Sequence(0, Count).Take(Window).Subscribe(observer).Dispose());
        Row("Buffer", () => Signal.Sequence(0, Count).Buffer(Window).Subscribe(listObserver).Dispose());
        Row("Recover", () => failSource.Recover<int, Exception>(static _ => Signal.Sequence(0, Count)).Subscribe(observer).Dispose());
        Row("Resume", () => failSource.Resume(Signal.Sequence(0, Count)).Subscribe(observer).Dispose());
        Row("MapWith", () => Signal.Sequence(0, Count).MapWith(MapWithFactor, static (f, x) => x * f).Subscribe(observer).Dispose());
        Row("KeepWith", () => Signal.Sequence(0, Count).KeepWith(Window, static (t, x) => x > t).Subscribe(observer).Dispose());
        Row("TapWith", () => Signal.Sequence(0, Count).TapWith(0, static (s, x) => _ = x + s).Subscribe(observer).Dispose());
        Row("KeepNotNull", () => Signal.Sequence(0, Count).Map(static x => (string?)x.ToString()).KeepNotNull().Subscribe(stringObserver).Dispose());
        Row("KeepType", () => Signal.Sequence(0, Count).Map(static _ => (object?)ProbeShared).KeepType<string>().Subscribe(stringObserver).Dispose());
        Row("CastTo", () => Signal.Sequence(0, Count).Map(static _ => (object?)ProbeShared).CastTo<string>().Subscribe(stringObserver).Dispose());
        Row("Iterate", () => Signal.Iterate(0, static s => s < Count, static s => s + 1, static s => s).Subscribe(observer).Dispose());
        Row("OnCleanup", () => Signal.Sequence(0, Count).OnCleanup(static () => { }).Subscribe(observer).Dispose());
        Row("CreateWithState", () => Signal.CreateWithState<int, int>(Count, static (count, target) =>
        {
            for (var i = 0; i < count; i++)
            {
                target.OnNext(i);
            }

            target.OnCompleted();
            return Disposable.Empty;
        }).Subscribe(observer).Dispose());
        Row("Multicast", () =>
        {
            var c = Signal.Sequence(1, Count).Multicast(new Signal<int>());
            using var s = c.Subscribe(observer);
            using var conn = c.Connect();
        });
    }

    /// <summary>
    /// Probes subject construction, subscription, and single-emit allocation.
    /// </summary>
    /// <param name="observer">The reused observer.</param>
    private static void ProbeSubjects(IntSignalObserver observer)
    {
        Row("Signal", () => EmitOnce(new Signal<int>(), observer));
        Row("StateSignal", () => EmitOnce(new StateSignal<int>(0), observer));
        Row("ReplaySignal", () => EmitOnce(new HistorySignal<int>(HistoryCapacity), observer));
    }

    /// <summary>
    /// Probes connectable operator allocation.
    /// </summary>
    /// <param name="observer">The reused observer.</param>
    private static void ProbeConnectable(IntSignalObserver observer)
    {
        Row("Publish", () =>
        {
            var c = Signal.Sequence(1, Count).ShareLive();
            using var s = c.Subscribe(observer);
            using var conn = c.Connect();
        });
        Row("Share", () => Signal.Sequence(1, Count).ShareLatest().Subscribe(observer).Dispose());
        Row("RefCount", () => Signal.Sequence(1, Count).ShareLive().AutoShare().Subscribe(observer).Dispose());
        Row("AutoConnect", () => Signal.Sequence(1, Count).ShareLive().AutoConnect().Subscribe(observer).Dispose());
    }

    /// <summary>
    /// Probes subscribe/dispose churn allocation.
    /// </summary>
    /// <param name="observer">The reused observer.</param>
    /// <param name="handles">The reused subscription-handle buffer.</param>
    private static void ProbeChurn(IntSignalObserver observer, IDisposable[] handles) =>
        Row("Signal fan-out churn", () =>
        {
            using var subject = new Signal<int>();
            for (var i = 0; i < FanOut; i++)
            {
                handles[i] = subject.Subscribe(observer);
            }

            for (var i = 0; i < FanOut; i++)
            {
                handles[i].Dispose();
            }
        });

    /// <summary>
    /// Subscribes, emits a single value, then tears the subscription and subject down.
    /// </summary>
    /// <param name="subject">The signal under test.</param>
    /// <param name="observer">The reused observer to subscribe.</param>
    private static void EmitOnce(ISignal<int> subject, IObserver<int> observer)
    {
        var subscription = subject.Subscribe(observer);
        subject.OnNext(1);
        subscription.Dispose();
        (subject as IDisposable)?.Dispose();
    }

    /// <summary>
    /// Prints a blank line and a section heading.
    /// </summary>
    /// <param name="name">The section heading to print.</param>
    private static void Section(string name)
    {
        Console.WriteLine();
        Console.WriteLine(name);
    }

    /// <summary>
    /// Warms up, measures, and prints the per-operation allocation for a single probe row.
    /// </summary>
    /// <param name="name">The operator label to print.</param>
    /// <param name="op">The operation to measure.</param>
    private static void Row(string name, Action op)
    {
        for (var i = 0; i < Warmup; i++)
        {
            op();
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < Iterations; i++)
        {
            op();
        }

        var perOp = (GC.GetAllocatedBytesForCurrentThread() - before) / Iterations;
        Console.WriteLine($"  {name,NameColumnWidth} {perOp,ValueColumnWidth} B");
    }
}
