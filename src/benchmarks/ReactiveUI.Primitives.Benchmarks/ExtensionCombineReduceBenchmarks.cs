// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using R3;
using ReactiveUI.Primitives.Signals;
using PackageExtensions = ReactiveUI.Extensions.ReactiveExtensions;
using PackageObservables = ReactiveUI.Extensions.Observables;
using PrimitivesExtensions = ReactiveUI.Primitives.Extensions.ReactiveExtensions;
using PrimitivesObservables = ReactiveUI.Primitives.Extensions.Observables;
using R3IntSubject = R3.Subject<int>;
using RxBoolSubject = System.Reactive.Subjects.Subject<bool>;
using RxIntSubject = System.Reactive.Subjects.Subject<int>;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures combine-latest reductions, two-stage flattening and first-match candidate walks.</summary>
[MemoryDiagnoser]
public class ExtensionCombineReduceBenchmarks
{
    /// <summary>The number of values pushed into each source.</summary>
    private const int Count = 256;

    /// <summary>The multiplier applied by the candidate transform.</summary>
    private const int CandidateMultiplier = 2;

    /// <summary>The transformed value a candidate must reach to match.</summary>
    private const int Match = 480;

    /// <summary>The value emitted when no candidate matches.</summary>
    private const int Fallback = -1;

    /// <summary>The modulus that spaces out the third boolean source's false values.</summary>
    private const int ThirdCycle = 3;

    /// <summary>The ordered candidate keys.</summary>
    private static readonly int[] Candidates = CreateCandidates();

    /// <summary>Emits the maximum of the latest values of two sources.</summary>
    /// <returns>The sum of the emitted maxima.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesGetMaxOfTwo()
    {
        IntSignalWitness observer = new();
        using Signal<int> first = new();
        using Signal<int> second = new();
        using var subscription = PrimitivesExtensions.GetMax(first, second).Subscribe(observer);
        PushCrossing(first, second);
        return observer.Total;
    }

    /// <summary>Emits the maximum of the latest values of two sources using System.Reactive.</summary>
    /// <returns>The sum of the emitted maxima.</returns>
    [Benchmark]
    public int SystemReactiveGetMaxOfTwo()
    {
        IntSignalWitness observer = new();
        using RxIntSubject first = new();
        using RxIntSubject second = new();
        using var subscription = RxObservable.CombineLatest(first, second, Math.Max).Subscribe(observer);
        PushCrossing(first, second);
        return observer.Total;
    }

    /// <summary>Emits the maximum of the latest values of two sources using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the emitted maxima.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsGetMaxOfTwo()
    {
        IntSignalWitness observer = new();
        using RxIntSubject first = new();
        using RxIntSubject second = new();
        using var subscription = PackageExtensions.GetMax(first, second).Subscribe(observer);
        PushCrossing(first, second);
        return observer.Total;
    }

    /// <summary>Emits the maximum of the latest values of two sources using R3.</summary>
    /// <returns>The sum of the emitted maxima.</returns>
    [Benchmark]
    public int R3GetMaxOfTwo()
    {
        IntR3Witness observer = new();
        using R3IntSubject first = new();
        using R3IntSubject second = new();
        using var subscription = first.CombineLatest(second, (Func<int, int, int>)Math.Max).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            first.OnNext(i);
            second.OnNext(Count - i);
        }

        return observer.Total;
    }

    /// <summary>Emits the minimum of the latest values of three sources.</summary>
    /// <returns>The sum of the emitted minima.</returns>
    [Benchmark]
    public int PrimitivesGetMinOfThree()
    {
        IntSignalWitness observer = new();
        using Signal<int> first = new();
        using Signal<int> second = new();
        using Signal<int> third = new();
        using var subscription = PrimitivesExtensions.GetMin(first, second, third).Subscribe(observer);
        PushCrossing(first, second, third);
        return observer.Total;
    }

    /// <summary>Emits the minimum of the latest values of three sources using System.Reactive.</summary>
    /// <returns>The sum of the emitted minima.</returns>
    [Benchmark]
    public int SystemReactiveGetMinOfThree()
    {
        IntSignalWitness observer = new();
        using RxIntSubject first = new();
        using RxIntSubject second = new();
        using RxIntSubject third = new();
        using var subscription = RxObservable.CombineLatest(first, second, third, static (x, y, z) => Math.Min(x, Math.Min(y, z)))
            .Subscribe(observer);
        PushCrossing(first, second, third);
        return observer.Total;
    }

    /// <summary>Emits the minimum of the latest values of three sources using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the emitted minima.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsGetMinOfThree()
    {
        IntSignalWitness observer = new();
        using RxIntSubject first = new();
        using RxIntSubject second = new();
        using RxIntSubject third = new();
        using var subscription = PackageExtensions.GetMin(first, second, third).Subscribe(observer);
        PushCrossing(first, second, third);
        return observer.Total;
    }

    /// <summary>Reports whether the latest values of three boolean sources are all true.</summary>
    /// <returns>The number of true reductions.</returns>
    [Benchmark]
    public int PrimitivesAllTrueOfThree()
    {
        BoolSignalWitness observer = new();
        using Signal<bool> first = new();
        using Signal<bool> second = new();
        using Signal<bool> third = new();
        using var subscription = PrimitivesExtensions.CombineLatestValuesAreAllTrue([first, second, third]).Subscribe(observer);
        PushToggles(first, second, third);
        return observer.Total;
    }

    /// <summary>Reports whether the latest values of three boolean sources are all true using System.Reactive.</summary>
    /// <returns>The number of true reductions.</returns>
    [Benchmark]
    public int SystemReactiveAllTrueOfThree()
    {
        BoolSignalWitness observer = new();
        using RxBoolSubject first = new();
        using RxBoolSubject second = new();
        using RxBoolSubject third = new();
        using var subscription = RxObservable.Select(RxObservable.CombineLatest([first, second, third]), AllTrue).Subscribe(observer);
        PushToggles(first, second, third);
        return observer.Total;
    }

    /// <summary>Reports whether the latest values of three boolean sources are all true using ReactiveUI.Extensions.</summary>
    /// <returns>The number of true reductions.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsAllTrueOfThree()
    {
        BoolSignalWitness observer = new();
        using RxBoolSubject first = new();
        using RxBoolSubject second = new();
        using RxBoolSubject third = new();
        using var subscription = PackageExtensions.CombineLatestValuesAreAllTrue([first, second, third]).Subscribe(observer);
        PushToggles(first, second, third);
        return observer.Total;
    }

    /// <summary>Reports whether the latest values of three boolean sources are all false.</summary>
    /// <returns>The number of true reductions.</returns>
    [Benchmark]
    public int PrimitivesAllFalseOfThree()
    {
        BoolSignalWitness observer = new();
        using Signal<bool> first = new();
        using Signal<bool> second = new();
        using Signal<bool> third = new();
        using var subscription = PrimitivesExtensions.CombineLatestValuesAreAllFalse([first, second, third]).Subscribe(observer);
        PushToggles(first, second, third);
        return observer.Total;
    }

    /// <summary>Reports whether the latest values of three boolean sources are all false using ReactiveUI.Extensions.</summary>
    /// <returns>The number of true reductions.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsAllFalseOfThree()
    {
        BoolSignalWitness observer = new();
        using RxBoolSubject first = new();
        using RxBoolSubject second = new();
        using RxBoolSubject third = new();
        using var subscription = PackageExtensions.CombineLatestValuesAreAllFalse([first, second, third]).Subscribe(observer);
        PushToggles(first, second, third);
        return observer.Total;
    }

    /// <summary>Flattens each value through two successive single-value projections.</summary>
    /// <returns>The sum of the flattened values.</returns>
    [Benchmark]
    public int PrimitivesSelectManyThen()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.SelectManyThen(
                source,
                static value => PrimitivesObservables.Return(value + 1),
                static value => PrimitivesObservables.Return(value * CandidateMultiplier))
            .Subscribe(observer);
        PushRange(source);
        return observer.Total;
    }

    /// <summary>Flattens each value through two successive single-value projections using System.Reactive.</summary>
    /// <returns>The sum of the flattened values.</returns>
    [Benchmark]
    public int SystemReactiveSelectManyThen()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = RxObservable.SelectMany(
                RxObservable.SelectMany(source, static value => RxObservable.Return(value + 1)),
                static value => RxObservable.Return(value * CandidateMultiplier))
            .Subscribe(observer);
        PushRange(source);
        return observer.Total;
    }

    /// <summary>Flattens each value through two successive single-value projections using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the flattened values.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsSelectManyThen()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = PackageExtensions.SelectManyThen(
                source,
                static value => PackageObservables.Return(value + 1),
                static value => PackageObservables.Return(value * CandidateMultiplier))
            .Subscribe(observer);
        PushRange(source);
        return observer.Total;
    }

    /// <summary>Walks candidates in order and emits the first transformed value that matches.</summary>
    /// <returns>The matched value plus completions.</returns>
    [Benchmark]
    public int PrimitivesFirstMatchFromCandidates()
    {
        IntSignalWitness observer = new();
        using var subscription = PrimitivesExtensions.FirstMatchFromCandidates(
                Candidates,
                PrimitivesObservables.Return,
                static raw => raw * CandidateMultiplier,
                static value => value >= Match,
                Fallback)
            .Subscribe(observer);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Walks candidates in order and emits the first transformed value that matches using System.Reactive.</summary>
    /// <returns>The matched value plus completions.</returns>
    [Benchmark]
    public int SystemReactiveFirstMatchFromCandidates()
    {
        IntSignalWitness observer = new();
        using var subscription = RxObservable.DefaultIfEmpty(
                RxObservable.Take(
                    RxObservable.Where(
                        RxObservable.Select(
                            RxObservable.Concat(RxObservable.Select(RxObservable.ToObservable(Candidates), RxObservable.Return)),
                            static raw => raw * CandidateMultiplier),
                        static value => value >= Match),
                    1),
                Fallback)
            .Subscribe(observer);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Walks candidates in order and emits the first transformed value that matches using ReactiveUI.Extensions.</summary>
    /// <returns>The matched value plus completions.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsFirstMatchFromCandidates()
    {
        IntSignalWitness observer = new();
        using var subscription = PackageExtensions.FirstMatchFromCandidates(
                Candidates,
                PackageObservables.Return,
                static raw => raw * CandidateMultiplier,
                static value => value >= Match,
                Fallback)
            .Subscribe(observer);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Determines whether every value is true.</summary>
    /// <param name="values">The values to inspect.</param>
    /// <returns><see langword="true"/> when every value is true; otherwise, <see langword="false"/>.</returns>
    private static bool AllTrue(IList<bool> values)
    {
        for (var i = 0; i < values.Count; i++)
        {
            if (!values[i])
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Pushes the ascending range into the subject.</summary>
    /// <param name="source">The subject to push into.</param>
    private static void PushRange(IObserver<int> source)
    {
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }
    }

    /// <summary>Pushes an ascending and a descending range in lockstep.</summary>
    /// <param name="first">The ascending source.</param>
    /// <param name="second">The descending source.</param>
    private static void PushCrossing(IObserver<int> first, IObserver<int> second)
    {
        for (var i = 0; i < Count; i++)
        {
            first.OnNext(i);
            second.OnNext(Count - i);
        }
    }

    /// <summary>Pushes ascending, descending and constant-mid ranges in lockstep.</summary>
    /// <param name="first">The ascending source.</param>
    /// <param name="second">The descending source.</param>
    /// <param name="third">The source that repeats the midpoint.</param>
    private static void PushCrossing(IObserver<int> first, IObserver<int> second, IObserver<int> third)
    {
        for (var i = 0; i < Count; i++)
        {
            first.OnNext(i);
            second.OnNext(Count - i);
            third.OnNext(Count / CandidateMultiplier);
        }
    }

    /// <summary>Pushes booleans that toggle at different rates into three sources.</summary>
    /// <param name="first">The source that alternates every value.</param>
    /// <param name="second">The source that alternates every two values.</param>
    /// <param name="third">The source that is false every third value.</param>
    private static void PushToggles(IObserver<bool> first, IObserver<bool> second, IObserver<bool> third)
    {
        for (var i = 0; i < Count; i++)
        {
            first.OnNext((i & 1) == 0);
            second.OnNext((i & CandidateMultiplier) == 0);
            third.OnNext(i % ThirdCycle != 0);
        }
    }

    /// <summary>Builds the ascending candidate keys.</summary>
    /// <returns>The candidate keys.</returns>
    private static int[] CreateCandidates()
    {
        var candidates = new int[Count];
        for (var i = 0; i < candidates.Length; i++)
        {
            candidates[i] = i;
        }

        return candidates;
    }
}
