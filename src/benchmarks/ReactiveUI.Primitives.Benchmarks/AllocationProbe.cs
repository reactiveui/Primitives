// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using ReactiveUI.Primitives;
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
    private const string ProbeShared = "x";
    private const int Warmup = 50;
    private const int Iterations = 1000;
    private const int Count = 32;
    private const int FanOut = 64;

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
        Console.WriteLine(new string('-', 56));

        Section("Factories / sources");
        Row("RangeSubscribe (baseline)", () => Signal.Sequence(0, Count).Subscribe(observer).Dispose());
        Row("Return", () => Signal.Emit(7).Subscribe(observer).Dispose());
        Row("Empty", () => Signal.None<int>().Subscribe(observer).Dispose());

        Section("Stateful single-source operators");
        Row("Skip", () => Signal.Sequence(0, Count).Skip(8).Subscribe(observer).Dispose());
        Row("Distinct", () => Signal.Sequence(0, Count).Distinct().Subscribe(observer).Dispose());
        Row("Unique", () => Signal.Sequence(0, Count).Unique().Subscribe(observer).Dispose());
        Row("UniqueBy", () => Signal.Sequence(0, Count).UniqueBy(static x => x / 2).Subscribe(observer).Dispose());
        Row("Fold", () => Signal.Sequence(0, Count).Fold(0, static (a, x) => a + x).Subscribe(observer).Dispose());
        Row("Reduce", () => Signal.Sequence(0, Count).Reduce(0, static (a, x) => a + x).Subscribe(observer).Dispose());
        Row("TakeWhile", () => Signal.Sequence(0, Count).TakeWhile(static x => x < 24).Subscribe(observer).Dispose());
        Row("SkipWhile", () => Signal.Sequence(0, Count).SkipWhile(static x => x < 8).Subscribe(observer).Dispose());

        Section("Projection / combination");
        Row("Map", () => Signal.Sequence(0, Count).Map(static x => x + 1).Subscribe(observer).Dispose());
        Row("Keep", () => Signal.Sequence(0, Count).Keep(static x => (x & 1) == 0).Subscribe(observer).Dispose());
        Row("Map+Keep", () => Signal.Sequence(0, Count).Map(static x => x + 1).Keep(static x => (x & 1) == 0).Subscribe(observer).Dispose());
        Row("Zip", () => Signal.Pair(Signal.Sequence(0, Count), Signal.Sequence(0, Count), static (l, r) => l + r).Subscribe(observer).Dispose());
        Row("WithLatest (Latch)", () => Signal.Sequence(1, Count).Latch(Signal.Sequence(10, Count), static (l, r) => l + r).Subscribe(observer).Dispose());
        Row("CombineLatest (SyncLatest)", () => Signal.Sequence(0, Count).SyncLatest(Signal.Sequence(10, Count), static (l, r) => l + r).Subscribe(observer).Dispose());
        Row("ForkJoin", () => Signal.Sequence(0, Count).ForkJoin(Signal.Sequence(10, Count), static (l, r) => l + r).Subscribe(observer).Dispose());
        Row("FlatMap", () => Signal.Sequence(1, 8).FlatMap(static x => Signal.Sequence(x * 10, 2)).Subscribe(observer).Dispose());

        Section("Pass-through / terminal (Tranche B-D)");
        Row("Tap", () => Signal.Sequence(0, Count).Tap(static _ => { }).Subscribe(observer).Dispose());
        Row("IgnoreValues", () => Signal.Sequence(0, Count).IgnoreValues().Subscribe(observer).Dispose());
        Row("Spark (materialize)", () => Signal.Sequence(0, Count).Spark().Subscribe(sparkObserver).Dispose());
        Row("Unspark (Spark->Unspark)", () => Signal.Sequence(0, Count).Spark().Unspark().Subscribe(observer).Dispose());
        Row("TimeInterval", () => Signal.Sequence(0, Count).TimeInterval(Sequencer.Immediate).Subscribe(intervalObserver).Dispose());
        Row("SubscribeOn (Immediate)", () => Signal.Sequence(0, Count).SubscribeOn(Sequencer.Immediate).Subscribe(observer).Dispose());
        Row("Reattempt", () => Signal.Sequence(0, Count).Reattempt(2).Subscribe(observer).Dispose());
        Row("CollectList (range)", () => Signal.Sequence(0, Count).CollectList().Subscribe(listObserver).Dispose());
        Row("CollectArray (range)", () => Signal.Sequence(0, Count).CollectArray().Subscribe(arrayObserver).Dispose());

        Section("Coverage-gap operators / factories");
        Row("Take", () => Signal.Sequence(0, Count).Take(8).Subscribe(observer).Dispose());
        Row("Buffer", () => Signal.Sequence(0, Count).Buffer(8).Subscribe(listObserver).Dispose());
        Row("Recover", () => failSource.Recover<int, Exception>(static _ => Signal.Sequence(0, Count)).Subscribe(observer).Dispose());
        Row("Resume", () => failSource.Resume(Signal.Sequence(0, Count)).Subscribe(observer).Dispose());
        Row("MapWith", () => Signal.Sequence(0, Count).MapWith(3, static (f, x) => x * f).Subscribe(observer).Dispose());
        Row("KeepWith", () => Signal.Sequence(0, Count).KeepWith(8, static (t, x) => x > t).Subscribe(observer).Dispose());
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

        // Calm / Shift / DelayStart need time advancement; their allocation is captured by the
        // OperatorTimeSchedulerBenchmarks BDN "Allocated" column instead of this synchronous probe.
        Section("Subjects (construct + subscribe + one emit)");
        Row("Signal", () => EmitOnce(new Signal<int>(), observer));
        Row("StateSignal", () => EmitOnce(new StateSignal<int>(0), observer));
        Row("ReplaySignal", () => EmitOnce(new HistorySignal<int>(16), observer));

        Section("Connectable");
        Row("Publish", () =>
        {
            var c = Signal.Sequence(1, Count).ShareLive();
            using var s = c.Subscribe(observer);
            using var conn = c.Connect();
        });
        Row("Share", () => Signal.Sequence(1, Count).ShareLatest().Subscribe(observer).Dispose());
        Row("RefCount", () => Signal.Sequence(1, Count).ShareLive().AutoShare().Subscribe(observer).Dispose());
        Row("AutoConnect", () => Signal.Sequence(1, Count).ShareLive().AutoConnect().Subscribe(observer).Dispose());

        Section($"Subscribe/dispose churn (x{FanOut})");
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
    }

    private static void EmitOnce(ISignal<int> subject, IObserver<int> observer)
    {
        var subscription = subject.Subscribe(observer);
        subject.OnNext(1);
        subscription.Dispose();
        (subject as IDisposable)?.Dispose();
    }

    private static void Section(string name)
    {
        Console.WriteLine();
        Console.WriteLine(name);
    }

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
        Console.WriteLine($"  {name,-34} {perOp,6} B");
    }
}
