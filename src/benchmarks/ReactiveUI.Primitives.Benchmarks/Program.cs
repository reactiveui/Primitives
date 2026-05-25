// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;
using System.Reactive.Concurrency;
using RxBehaviorSubject = System.Reactive.Subjects.BehaviorSubject<int>;
using RxCompositeDisposable = System.Reactive.Disposables.CompositeDisposable;
using RxCurrentThreadScheduler = System.Reactive.Concurrency.CurrentThreadScheduler;
using RxDisposable = System.Reactive.Disposables.Disposable;
using RxReplaySubject = System.Reactive.Subjects.ReplaySubject<int>;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

internal static class Program
{
    private static void Main(string[] args)
    {
        if (args.Contains("--smoke", StringComparer.OrdinalIgnoreCase))
        {
            var scalar = new ScalarSignalBenchmarks();
            Console.WriteLine($"Primitives={scalar.PrimitivesReturnSubscribe()}");
            Console.WriteLine($"System.Reactive={scalar.SystemReactiveReturnSubscribe()}");
            Console.WriteLine($"R3={scalar.R3ReturnSubscribe()}");

            var factories = new FactorySignalBenchmarks();
            Console.WriteLine($"PrimitivesEmptySubscribe={factories.PrimitivesEmptySubscribe()}");
            Console.WriteLine($"SystemReactiveEmptySubscribe={factories.SystemReactiveEmptySubscribe()}");
            Console.WriteLine($"R3EmptySubscribe={factories.R3EmptySubscribe()}");
            Console.WriteLine($"PrimitivesRangeSubscribe={factories.PrimitivesRangeSubscribe()}");
            Console.WriteLine($"SystemReactiveRangeSubscribe={factories.SystemReactiveRangeSubscribe()}");
            Console.WriteLine($"R3RangeSubscribe={factories.R3RangeSubscribe()}");
            Console.WriteLine($"PrimitivesRepeatSubscribe={factories.PrimitivesRepeatSubscribe()}");
            Console.WriteLine($"SystemReactiveRepeatSubscribe={factories.SystemReactiveRepeatSubscribe()}");
            Console.WriteLine($"R3RepeatSubscribe={factories.R3RepeatSubscribe()}");

            var core = new CoreRuntimeBenchmarks();
            Console.WriteLine($"PrimitivesPocketDispose={core.PrimitivesPocketDispose()}");
            Console.WriteLine($"SystemReactiveCompositeDispose={core.SystemReactiveCompositeDispose()}");
            Console.WriteLine($"PrimitivesCurrentThreadSchedule={core.PrimitivesCurrentThreadSchedule()}");
            Console.WriteLine($"SystemReactiveCurrentThreadSchedule={core.SystemReactiveCurrentThreadSchedule()}");
            Console.WriteLine($"PrimitivesSafeWitness={core.PrimitivesSafeWitness()}");
            Console.WriteLine($"PrimitivesCompletedSpark={core.PrimitivesCompletedSpark()}");

            var operators = new OperatorBenchmarks();
            Console.WriteLine($"PrimitivesRangeMapKeep={operators.PrimitivesRangeMapKeep()}");
            Console.WriteLine($"SystemReactiveRangeSelectWhere={operators.SystemReactiveRangeSelectWhere()}");
            Console.WriteLine($"R3RangeSelectWhere={operators.R3RangeSelectWhere()}");
            Console.WriteLine($"PrimitivesAggregateAnyCount={operators.PrimitivesAggregateAnyCount()}");
            Console.WriteLine($"SystemReactiveAggregateAnyCount={operators.SystemReactiveAggregateAnyCount()}");

            var parityOperators = new OperatorParityBenchmarks();
            Console.WriteLine($"PrimitivesStartWithAppendDefaultIfEmpty={parityOperators.PrimitivesStartWithAppendDefaultIfEmpty()}");
            Console.WriteLine($"SystemReactiveStartWithAppendDefaultIfEmpty={parityOperators.SystemReactiveStartWithAppendDefaultIfEmpty()}");
            Console.WriteLine($"R3PrependAppendDefaultIfEmpty={parityOperators.R3PrependAppendDefaultIfEmpty()}");
            Console.WriteLine($"PrimitivesSelectManyRange={parityOperators.PrimitivesSelectManyRange()}");
            Console.WriteLine($"SystemReactiveSelectManyRange={parityOperators.SystemReactiveSelectManyRange()}");
            Console.WriteLine($"R3SelectManyRange={parityOperators.R3SelectManyRange()}");
            Console.WriteLine($"PrimitivesZip={parityOperators.PrimitivesZip()}");
            Console.WriteLine($"SystemReactiveZip={parityOperators.SystemReactiveZip()}");
            Console.WriteLine($"R3Zip={parityOperators.R3Zip()}");

            var throughput = new SubjectThroughputBenchmarks { Count = 32 };
            Console.WriteLine($"PrimitivesSubjectEmitN={throughput.PrimitivesSubjectEmitN()}");
            Console.WriteLine($"SystemReactiveSubjectEmitN={throughput.SystemReactiveSubjectEmitN()}");
            Console.WriteLine($"R3SubjectEmitN={throughput.R3SubjectEmitN()}");

            var subjectSubscriptions = new SubjectSubscriptionBenchmarks { Subscribers = 8 };
            Console.WriteLine($"PrimitivesSubjectSubscribeDisposeN={subjectSubscriptions.PrimitivesSubjectSubscribeDisposeN()}");
            Console.WriteLine($"SystemReactiveSubjectSubscribeDisposeN={subjectSubscriptions.SystemReactiveSubjectSubscribeDisposeN()}");
            Console.WriteLine($"R3SubjectSubscribeDisposeN={subjectSubscriptions.R3SubjectSubscribeDisposeN()}");

            var state = new StatefulSignalBenchmarks { Count = 32 };
            Console.WriteLine($"PrimitivesBehaviourSignal={state.PrimitivesBehaviourSignal()}");
            Console.WriteLine($"SystemReactiveBehaviorSubject={state.SystemReactiveBehaviorSubject()}");
            Console.WriteLine($"R3BehaviorSubject={state.R3BehaviorSubject()}");

            var replay = new ReplaySignalBenchmarks();
            Console.WriteLine($"PrimitivesReplaySubscribe={replay.PrimitivesReplaySubscribe()}");
            Console.WriteLine($"SystemReactiveReplaySubscribe={replay.SystemReactiveReplaySubscribe()}");

            var taskBridge = new AsyncBridgeBenchmarks();
            Console.WriteLine($"PrimitivesCompletedTaskBridge={taskBridge.PrimitivesCompletedTaskBridge()}");
            Console.WriteLine($"SystemReactiveCompletedTaskBridge={taskBridge.SystemReactiveCompletedTaskBridge()}");
            return;
        }

        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}

[MemoryDiagnoser]
public class FactorySignalBenchmarks
{
    [Benchmark(Baseline = true)]
    public int PrimitivesEmptySubscribe()
    {
        var completed = 0;
        using var subscription = Signal.Empty<int>().Subscribe(static _ => { }, () => completed++);
        return completed;
    }

    [Benchmark]
    public int SystemReactiveEmptySubscribe()
    {
        var completed = 0;
        using var subscription = System.Reactive.Linq.Observable.Empty<int>().Subscribe(static _ => { }, () => completed++);
        return completed;
    }

    [Benchmark]
    public int R3EmptySubscribe()
    {
        using var observer = new R3CountingObserver<int>();
        using var subscription = R3.Observable.Empty<int>().Subscribe(observer);
        return observer.Completed;
    }

    [Benchmark]
    public int PrimitivesRangeSubscribe()
    {
        var total = 0;
        using var subscription = Signal.Range(4, 32).Subscribe(x => total += x);
        return total;
    }

    [Benchmark]
    public int SystemReactiveRangeSubscribe()
    {
        var total = 0;
        using var subscription = System.Reactive.Linq.Observable.Range(4, 32).Subscribe(x => total += x);
        return total;
    }

    [Benchmark]
    public int R3RangeSubscribe()
    {
        using var observer = new R3CountingObserver<int>();
        using var subscription = R3.Observable.Range(4, 32).Subscribe(observer);
        return observer.Total;
    }

    [Benchmark]
    public int PrimitivesRepeatSubscribe()
    {
        var total = 0;
        using var subscription = Signal.Repeat(7, 32).Subscribe(x => total += x);
        return total;
    }

    [Benchmark]
    public int SystemReactiveRepeatSubscribe()
    {
        var total = 0;
        using var subscription = System.Reactive.Linq.Observable.Repeat(7, 32).Subscribe(x => total += x);
        return total;
    }

    [Benchmark]
    public int R3RepeatSubscribe()
    {
        using var observer = new R3CountingObserver<int>();
        using var subscription = R3.Observable.Repeat(7, 32).Subscribe(observer);
        return observer.Total;
    }
}

[MemoryDiagnoser]
public class ScalarSignalBenchmarks
{
    [Benchmark(Baseline = true)]
    public int PrimitivesReturnSubscribe()
    {
        var value = 0;
        using var subscription = Signal.Return(42).Subscribe(static x => { }, static () => { });
        using var capture = Signal.Return(42).Subscribe(x => value = x);
        return value;
    }

    [Benchmark]
    public int SystemReactiveReturnSubscribe()
    {
        var value = 0;
        using var subscription = System.Reactive.Linq.Observable.Return(42).Subscribe(x => value = x);
        return value;
    }

    [Benchmark]
    public int R3ReturnSubscribe()
    {
        var value = 0;
        using var subscription = R3.Observable.Return(42).Subscribe(new R3ActionObserver<int>(x => value = x));
        return value;
    }
}

[MemoryDiagnoser]
public class OperatorBenchmarks
{
    [Benchmark(Baseline = true)]
    public int PrimitivesRangeMapKeep()
    {
        var total = 0;
        using var subscription = Signal.Range(0, 32).Map(static x => x + 1).Keep(static x => (x & 1) == 0).Subscribe(x => total += x);
        return total;
    }

    [Benchmark]
    public int SystemReactiveRangeSelectWhere()
    {
        var total = 0;
        using var subscription = System.Reactive.Linq.Observable.Where(System.Reactive.Linq.Observable.Select(System.Reactive.Linq.Observable.Range(0, 32), static x => x + 1), static x => (x & 1) == 0).Subscribe(x => total += x);
        return total;
    }

    [Benchmark]
    public int R3RangeSelectWhere()
    {
        var total = 0;
        using var subscription = R3.ObservableExtensions.Where(R3.ObservableExtensions.Select(R3.Observable.Range(0, 32), static (int x) => x + 1), static (int x) => (x & 1) == 0).Subscribe(new R3ActionObserver<int>(x => total += x));
        return total;
    }

    [Benchmark]
    public int PrimitivesAggregateAnyCount()
    {
        var count = 0;
        var any = false;
        using var countSubscription = Signal.Range(0, 32).DistinctBy(static x => x / 2).Count().Subscribe(x => count = x);
        using var anySubscription = Signal.Range(0, 32).Any(static x => x == 31).Subscribe(x => any = x);
        return any ? count : -count;
    }

    [Benchmark]
    public int SystemReactiveAggregateAnyCount()
    {
        var count = 0;
        var any = false;
        using var countSubscription = System.Reactive.Linq.Observable.Count(System.Reactive.Linq.Observable.Distinct(System.Reactive.Linq.Observable.Select(System.Reactive.Linq.Observable.Range(0, 32), static x => x / 2))).Subscribe(x => count = x);
        using var anySubscription = System.Reactive.Linq.Observable.Any(System.Reactive.Linq.Observable.Range(0, 32), static x => x == 31).Subscribe(x => any = x);
        return any ? count : -count;
    }
}

[MemoryDiagnoser]
public class OperatorParityBenchmarks
{
    [Benchmark(Baseline = true)]
    public int PrimitivesStartWithAppendDefaultIfEmpty()
    {
        var total = 0;
        using var subscription = Signal.Empty<int>().DefaultIfEmpty(2).StartWith(1).Append(3).Subscribe(x => total += x);
        return total;
    }

    [Benchmark]
    public int SystemReactiveStartWithAppendDefaultIfEmpty()
    {
        var total = 0;
        using var subscription = System.Reactive.Linq.Observable.Append(
            System.Reactive.Linq.Observable.StartWith(
                System.Reactive.Linq.Observable.DefaultIfEmpty(System.Reactive.Linq.Observable.Empty<int>(), 2),
                1),
            3).Subscribe(x => total += x);
        return total;
    }

    [Benchmark]
    public int R3PrependAppendDefaultIfEmpty()
    {
        using var observer = new R3CountingObserver<int>();
        using var subscription = R3.ObservableExtensions.Append(
            R3.ObservableExtensions.Prepend(
                R3.ObservableExtensions.DefaultIfEmpty(R3.Observable.Empty<int>(), 2),
                1),
            3).Subscribe(observer);
        return observer.Total;
    }

    [Benchmark]
    public int PrimitivesSelectManyRange()
    {
        var total = 0;
        using var subscription = Signal.Range(1, 8).SelectMany(static x => Signal.Range(x * 10, 2)).Subscribe(x => total += x);
        return total;
    }

    [Benchmark]
    public int SystemReactiveSelectManyRange()
    {
        var total = 0;
        using var subscription = System.Reactive.Linq.Observable.SelectMany(
            System.Reactive.Linq.Observable.Range(1, 8),
            static x => System.Reactive.Linq.Observable.Range(x * 10, 2)).Subscribe(x => total += x);
        return total;
    }

    [Benchmark]
    public int R3SelectManyRange()
    {
        using var observer = new R3CountingObserver<int>();
        using var subscription = R3.ObservableExtensions.SelectMany(
            R3.Observable.Range(1, 8),
            static (int x) => R3.Observable.Range(x * 10, 2)).Subscribe(observer);
        return observer.Total;
    }

    [Benchmark]
    public int PrimitivesZip()
    {
        var total = 0;
        using var subscription = Signal.Zip(Signal.Range(1, 16), Signal.Range(10, 16), static (left, right) => left + right).Subscribe(x => total += x);
        return total;
    }

    [Benchmark]
    public int SystemReactiveZip()
    {
        var total = 0;
        using var subscription = System.Reactive.Linq.Observable.Zip(
            System.Reactive.Linq.Observable.Range(1, 16),
            System.Reactive.Linq.Observable.Range(10, 16),
            static (left, right) => left + right).Subscribe(x => total += x);
        return total;
    }

    [Benchmark]
    public int R3Zip()
    {
        using var observer = new R3CountingObserver<int>();
        using var subscription = R3.Observable.Zip(
            R3.Observable.Range(1, 16),
            R3.Observable.Range(10, 16),
            static (left, right) => left + right).Subscribe(observer);
        return observer.Total;
    }
}

[MemoryDiagnoser]
public class SubjectThroughputBenchmarks
{
    [Params(32, 1024)]
    public int Count { get; set; }

    [Benchmark(Baseline = true)]
    public int PrimitivesSubjectEmitN()
    {
        var total = 0;
        using var subject = new Signal<int>();
        using var subscription = subject.Subscribe(x => total += x);
        for (var i = 0; i < Count; i++)
        {
            subject.OnNext(i);
        }

        return total;
    }

    [Benchmark]
    public int SystemReactiveSubjectEmitN()
    {
        var total = 0;
        using var subject = new RxSubject();
        using var subscription = subject.Subscribe(x => total += x);
        for (var i = 0; i < Count; i++)
        {
            subject.OnNext(i);
        }

        return total;
    }

    [Benchmark]
    public int R3SubjectEmitN()
    {
        var total = 0;
        using var subject = new R3.Subject<int>();
        using var subscription = subject.Subscribe(new R3ActionObserver<int>(x => total += x));
        for (var i = 0; i < Count; i++)
        {
            subject.OnNext(i);
        }

        return total;
    }
}

[MemoryDiagnoser]
public class SubjectSubscriptionBenchmarks
{
    [Params(8, 64)]
    public int Subscribers { get; set; }

    [Benchmark(Baseline = true)]
    public int PrimitivesSubjectSubscribeDisposeN()
    {
        using var subject = new Signal<int>();
        var subscriptions = new IDisposable[Subscribers];
        for (var i = 0; i < subscriptions.Length; i++)
        {
            subscriptions[i] = subject.Subscribe(static _ => { });
        }

        var hasObservers = subject.HasObservers ? 1 : 0;
        for (var i = 0; i < subscriptions.Length; i++)
        {
            subscriptions[i].Dispose();
        }

        return hasObservers + (subject.HasObservers ? 1 : 0);
    }

    [Benchmark]
    public int SystemReactiveSubjectSubscribeDisposeN()
    {
        using var subject = new RxSubject();
        var subscriptions = new IDisposable[Subscribers];
        for (var i = 0; i < subscriptions.Length; i++)
        {
            subscriptions[i] = subject.Subscribe(static _ => { });
        }

        for (var i = 0; i < subscriptions.Length; i++)
        {
            subscriptions[i].Dispose();
        }

        return subscriptions.Length;
    }

    [Benchmark]
    public int R3SubjectSubscribeDisposeN()
    {
        using var subject = new R3.Subject<int>();
        var subscriptions = new IDisposable[Subscribers];
        for (var i = 0; i < subscriptions.Length; i++)
        {
            subscriptions[i] = subject.Subscribe(new R3ActionObserver<int>(static _ => { }));
        }

        for (var i = 0; i < subscriptions.Length; i++)
        {
            subscriptions[i].Dispose();
        }

        return subscriptions.Length;
    }
}

[MemoryDiagnoser]
public class StatefulSignalBenchmarks
{
    [Params(32, 1024)]
    public int Count { get; set; }

    [Benchmark(Baseline = true)]
    public int PrimitivesBehaviourSignal()
    {
        var total = 0;
        using var subject = new BehaviourSignal<int>(0);
        using var subscription = subject.Subscribe(x => total += x);
        for (var i = 1; i <= Count; i++)
        {
            subject.OnNext(i);
        }

        return total + subject.Value;
    }

    [Benchmark]
    public int SystemReactiveBehaviorSubject()
    {
        var total = 0;
        using var subject = new RxBehaviorSubject(0);
        using var subscription = subject.Subscribe(x => total += x);
        for (var i = 1; i <= Count; i++)
        {
            subject.OnNext(i);
        }

        return total + subject.Value;
    }

    [Benchmark]
    public int R3BehaviorSubject()
    {
        var total = 0;
        using var subject = new R3.BehaviorSubject<int>(0);
        using var subscription = subject.Subscribe(new R3ActionObserver<int>(x => total += x));
        for (var i = 1; i <= Count; i++)
        {
            subject.OnNext(i);
        }

        return total + subject.Value;
    }
}

[MemoryDiagnoser]
public class ReplaySignalBenchmarks
{
    [Benchmark(Baseline = true)]
    public int PrimitivesReplaySubscribe()
    {
        using var subject = new ReplaySignal<int>(16);
        for (var i = 0; i < 16; i++)
        {
            subject.OnNext(i);
        }

        var total = 0;
        using var subscription = subject.Subscribe(x => total += x);
        return total;
    }

    [Benchmark]
    public int SystemReactiveReplaySubscribe()
    {
        using var subject = new RxReplaySubject(16);
        for (var i = 0; i < 16; i++)
        {
            subject.OnNext(i);
        }

        var total = 0;
        using var subscription = subject.Subscribe(x => total += x);
        return total;
    }
}

[MemoryDiagnoser]
public class AsyncBridgeBenchmarks
{
    private static readonly Task<int> CompletedTask = Task.FromResult(42);

    [Benchmark(Baseline = true)]
    public int PrimitivesCompletedTaskBridge()
    {
        var value = 0;
        using var subscription = Signal.FromTask(CompletedTask).Subscribe(x => value = x);
        return value;
    }

    [Benchmark]
    public int SystemReactiveCompletedTaskBridge()
    {
        var value = 0;
        using var subscription = System.Reactive.Linq.Observable.FromAsync(() => CompletedTask).Subscribe(x => value = x);
        return value;
    }
}

[MemoryDiagnoser]
public class CoreRuntimeBenchmarks
{
    [Benchmark(Baseline = true)]
    public int PrimitivesPocketDispose()
    {
        var disposed = 0;
        var pocket = new Pocket(
            Disposable.Create(() => disposed++),
            Disposable.Create(() => disposed++),
            Disposable.Create(() => disposed++));

        pocket.Dispose();
        return disposed;
    }

    [Benchmark]
    public int SystemReactiveCompositeDispose()
    {
        var disposed = 0;
        var pocket = new RxCompositeDisposable(
            RxDisposable.Create(() => disposed++),
            RxDisposable.Create(() => disposed++),
            RxDisposable.Create(() => disposed++));

        pocket.Dispose();
        return disposed;
    }

    [Benchmark]
    public int PrimitivesCurrentThreadSchedule()
    {
        var value = 0;
        using var scheduled = ReactiveUI.Primitives.Concurrency.Sequencer.CurrentThread.Schedule(() => value = 1);
        return value;
    }

    [Benchmark]
    public int SystemReactiveCurrentThreadSchedule()
    {
        var value = 0;
        using var scheduled = RxCurrentThreadScheduler.Instance.Schedule(() => value = 1);
        return value;
    }

    [Benchmark]
    public int PrimitivesSafeWitness()
    {
        var value = 0;
        var witness = Witness.Safe(Witness.Create<int>(x => value = x));
        witness.OnNext(42);
        witness.OnCompleted();
        return value;
    }

    [Benchmark]
    public int PrimitivesCompletedSpark()
    {
        var spark = Spark.CreateOnCompleted<int>();
        return (int)spark.Kind;
    }
}

internal sealed class R3ActionObserver<T> : R3.Observer<T>
{
    private readonly Action<T> _onNext;

    public R3ActionObserver(Action<T> onNext) => _onNext = onNext;

    protected override void OnNextCore(T value) => _onNext(value);

    protected override void OnErrorResumeCore(Exception error) => throw error;

    protected override void OnCompletedCore(R3.Result result)
    {
    }
}

internal sealed class R3CountingObserver<T> : R3.Observer<T>
{
    public int Completed { get; private set; }

    public int Count { get; private set; }

    public int Total { get; private set; }

    protected override void OnNextCore(T value)
    {
        Count++;
        if (value is int intValue)
        {
            Total += intValue;
        }
    }

    protected override void OnErrorResumeCore(Exception error) => throw error;

    protected override void OnCompletedCore(R3.Result result) => Completed++;
}
