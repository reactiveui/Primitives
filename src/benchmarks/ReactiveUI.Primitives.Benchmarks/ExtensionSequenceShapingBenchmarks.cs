// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using R3;
using ReactiveUI.Primitives.Signals;
using PackageExtensions = ReactiveUI.Extensions.ReactiveExtensions;
using PrimitivesExtensions = ReactiveUI.Primitives.Extensions.ReactiveExtensions;
using R3IntSubject = R3.Subject<int>;
using RxCharSubject = System.Reactive.Subjects.Subject<char>;
using RxIntSubject = System.Reactive.Subjects.Subject<int>;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures stateful shaping operators: pairing, seeded scans, inclusive take, first match, delimiting, shuffling and partitioning.</summary>
[MemoryDiagnoser]
public class ExtensionSequenceShapingBenchmarks
{
    /// <summary>The number of values pushed through each pipeline.</summary>
    private const int Count = 256;

    /// <summary>The value whose arrival ends the take and wait pipelines.</summary>
    private const int StopValue = 200;

    /// <summary>The sliding window size that rebuilds pairs from a buffer.</summary>
    private const int PairWindow = 2;

    /// <summary>The number of arrays pushed through the shuffle pipeline.</summary>
    private const int ArrayCount = 32;

    /// <summary>The length of every shuffled array.</summary>
    private const int ArrayLength = 16;

    /// <summary>The framed character payload used by the delimiter pipelines.</summary>
    private static readonly char[] Framed = CreateFramed();

    /// <summary>Pairs each value with its predecessor.</summary>
    /// <returns>The sum of both halves of every pair.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesPairwise()
    {
        PairWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.Pairwise(source).Subscribe(observer);
        PushRange(source);
        return observer.Total;
    }

    /// <summary>Pairs each value with its predecessor using a System.Reactive sliding buffer.</summary>
    /// <returns>The sum of both halves of every pair.</returns>
    [Benchmark]
    public int SystemReactivePairwise()
    {
        PairWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = RxObservable.Select(
                RxObservable.Where(RxObservable.Buffer(source, PairWindow, 1), static window => window.Count == PairWindow),
                static window => (window[0], window[1]))
            .Subscribe(observer);
        PushRange(source);
        return observer.Total;
    }

    /// <summary>Pairs each value with its predecessor using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of both halves of every pair.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsPairwise()
    {
        PairWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = PackageExtensions.Pairwise(source).Subscribe(observer);
        PushRange(source);
        return observer.Total;
    }

    /// <summary>Pairs each value with its predecessor using R3.</summary>
    /// <returns>The sum of both halves of every pair.</returns>
    [Benchmark]
    public int R3Pairwise()
    {
        var total = 0;
        using R3IntSubject source = new();
        using var subscription = source.Pairwise().Subscribe(pair => total += pair.Previous + pair.Current);
        for (var i = 0; i < Count; i++)
        {
            source.OnNext(i);
        }

        return total;
    }

    /// <summary>Emits a seed and then the running total of the stream.</summary>
    /// <returns>The last running total.</returns>
    [Benchmark]
    public int PrimitivesScanWithInitial()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.ScanWithInitial(source, 0, static (total, value) => total + value)
            .Subscribe(observer);
        PushRange(source);
        return observer.LastValue + observer.NextCount;
    }

    /// <summary>Emits a seed and then the running total of the stream using System.Reactive.</summary>
    /// <returns>The last running total.</returns>
    [Benchmark]
    public int SystemReactiveScanWithInitial()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = RxObservable.StartWith(RxObservable.Scan(source, 0, static (total, value) => total + value), 0)
            .Subscribe(observer);
        PushRange(source);
        return observer.LastValue + observer.NextCount;
    }

    /// <summary>Emits a seed and then the running total of the stream using ReactiveUI.Extensions.</summary>
    /// <returns>The last running total.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsScanWithInitial()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = PackageExtensions.ScanWithInitial(source, 0, static (total, value) => total + value)
            .Subscribe(observer);
        PushRange(source);
        return observer.LastValue + observer.NextCount;
    }

    /// <summary>Forwards values up to and including the stop value, then completes.</summary>
    /// <returns>The sum of the forwarded values plus completions.</returns>
    [Benchmark]
    public int PrimitivesTakeUntilInclusive()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.TakeUntil(source, static value => value == StopValue).Subscribe(observer);
        PushRange(source);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Forwards values up to and including the stop value using System.Reactive.</summary>
    /// <returns>The sum of the forwarded values plus completions.</returns>
    [Benchmark]
    public int SystemReactiveTakeUntilInclusive()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = RxObservable.TakeUntil(source, static value => value == StopValue).Subscribe(observer);
        PushRange(source);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Forwards values up to and including the stop value using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of the forwarded values plus completions.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsTakeUntilInclusive()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = PackageExtensions.TakeUntil(source, static value => value == StopValue).Subscribe(observer);
        PushRange(source);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Emits the first value matching the predicate and completes.</summary>
    /// <returns>The matched value plus completions.</returns>
    [Benchmark]
    public int PrimitivesWaitUntil()
    {
        IntSignalWitness observer = new();
        using Signal<int> source = new();
        using var subscription = PrimitivesExtensions.WaitUntil(source, static value => value == StopValue).Subscribe(observer);
        PushRange(source);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Emits the first value matching the predicate using System.Reactive.</summary>
    /// <returns>The matched value plus completions.</returns>
    [Benchmark]
    public int SystemReactiveWaitUntil()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = RxObservable.Take(RxObservable.Where(source, static value => value == StopValue), 1)
            .Subscribe(observer);
        PushRange(source);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Emits the first value matching the predicate using ReactiveUI.Extensions.</summary>
    /// <returns>The matched value plus completions.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsWaitUntil()
    {
        IntSignalWitness observer = new();
        using RxIntSubject source = new();
        using var subscription = PackageExtensions.WaitUntil(source, static value => value == StopValue).Subscribe(observer);
        PushRange(source);
        return observer.Total + observer.CompletionCount;
    }

    /// <summary>Frames a character stream into bracket-delimited strings.</summary>
    /// <returns>The total length of the framed strings.</returns>
    [Benchmark]
    public int PrimitivesBufferUntilDelimiter()
    {
        FrameWitness observer = new();
        using Signal<char> source = new();
        using var subscription = PrimitivesExtensions.BufferUntil(source, '[', ']').Subscribe(observer);
        PushFramed(source);
        return observer.TotalLength;
    }

    /// <summary>Frames a character stream into bracket-delimited strings using ReactiveUI.Extensions.</summary>
    /// <returns>The total length of the framed strings.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsBufferUntilDelimiter()
    {
        FrameWitness observer = new();
        using RxCharSubject source = new();
        using var subscription = PackageExtensions.BufferUntil(source, '[', ']').Subscribe(observer);
        PushFramed(source);
        return observer.TotalLength;
    }

    /// <summary>Shuffles each emitted array in place.</summary>
    /// <returns>The summed length of the shuffled arrays.</returns>
    [Benchmark]
    public int PrimitivesShuffle()
    {
        ArrayLengthWitness observer = new();
        using Signal<int[]> source = new();
        using var subscription = PrimitivesExtensions.Shuffle(source).Subscribe(observer);
        PushArrays(source);
        return observer.Total;
    }

    /// <summary>Shuffles each emitted array in place using ReactiveUI.Extensions.</summary>
    /// <returns>The summed length of the shuffled arrays.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsShuffle()
    {
        ArrayLengthWitness observer = new();
        using System.Reactive.Subjects.Subject<int[]> source = new();
        using var subscription = PackageExtensions.Shuffle(source).Subscribe(observer);
        PushArrays(source);
        return observer.Total;
    }

    /// <summary>Splits the stream into even and odd outputs sharing one source subscription.</summary>
    /// <returns>The sum of even values minus the sum of odd values.</returns>
    [Benchmark]
    public int PrimitivesPartition()
    {
        IntSignalWitness even = new();
        IntSignalWitness odd = new();
        using Signal<int> source = new();
        var (trueSide, falseSide) = PrimitivesExtensions.Partition(source, static value => (value & 1) == 0);
        using var evenSubscription = trueSide.Subscribe(even);
        using var oddSubscription = falseSide.Subscribe(odd);
        PushRange(source);
        return even.Total - odd.Total;
    }

    /// <summary>Splits the stream into even and odd outputs using System.Reactive.</summary>
    /// <returns>The sum of even values minus the sum of odd values.</returns>
    [Benchmark]
    public int SystemReactivePartition()
    {
        IntSignalWitness even = new();
        IntSignalWitness odd = new();
        using RxIntSubject source = new();
        var shared = RxObservable.RefCount(RxObservable.Publish(source));
        using var evenSubscription = RxObservable.Where(shared, static value => (value & 1) == 0).Subscribe(even);
        using var oddSubscription = RxObservable.Where(shared, static value => (value & 1) != 0).Subscribe(odd);
        PushRange(source);
        return even.Total - odd.Total;
    }

    /// <summary>Splits the stream into even and odd outputs using ReactiveUI.Extensions.</summary>
    /// <returns>The sum of even values minus the sum of odd values.</returns>
    [Benchmark]
    public int ReactiveUIExtensionsPartition()
    {
        IntSignalWitness even = new();
        IntSignalWitness odd = new();
        using RxIntSubject source = new();
        var (trueSide, falseSide) = PackageExtensions.Partition(source, static value => (value & 1) == 0);
        using var evenSubscription = trueSide.Subscribe(even);
        using var oddSubscription = falseSide.Subscribe(odd);
        PushRange(source);
        return even.Total - odd.Total;
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

    /// <summary>Pushes the framed character payload into the subject and completes it.</summary>
    /// <param name="source">The subject to push into.</param>
    private static void PushFramed(IObserver<char> source)
    {
        for (var i = 0; i < Framed.Length; i++)
        {
            source.OnNext(Framed[i]);
        }

        source.OnCompleted();
    }

    /// <summary>Pushes fresh ascending arrays into the subject.</summary>
    /// <param name="source">The subject to push into.</param>
    private static void PushArrays(IObserver<int[]> source)
    {
        for (var i = 0; i < ArrayCount; i++)
        {
            var array = new int[ArrayLength];
            for (var j = 0; j < array.Length; j++)
            {
                array[j] = j;
            }

            source.OnNext(array);
        }
    }

    /// <summary>Builds a character payload of repeated noise-then-bracketed frames.</summary>
    /// <returns>The framed payload.</returns>
    private static char[] CreateFramed()
    {
        const string Frame = "xx[abcdef]y";
        var chars = new char[Count];
        for (var i = 0; i < chars.Length; i++)
        {
            chars[i] = Frame[i % Frame.Length];
        }

        return chars;
    }

    /// <summary>Observer that sums both halves of every emitted pair.</summary>
    private sealed class PairWitness : IObserver<(int Previous, int Current)>
    {
        /// <summary>Gets the summed pair values.</summary>
        public int Total { get; private set; }

        /// <inheritdoc/>
        public void OnNext((int Previous, int Current) value) => Total += value.Previous + value.Current;

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }
    }

    /// <summary>Observer that sums the length of every emitted string.</summary>
    private sealed class FrameWitness : IObserver<string>
    {
        /// <summary>Gets the summed string length.</summary>
        public int TotalLength { get; private set; }

        /// <inheritdoc/>
        public void OnNext(string value) => TotalLength += value.Length;

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }
    }

    /// <summary>Observer that sums the length of every emitted array.</summary>
    private sealed class ArrayLengthWitness : IObserver<int[]>
    {
        /// <summary>Gets the summed array length.</summary>
        public int Total { get; private set; }

        /// <inheritdoc/>
        public void OnNext(int[] value) => Total += value.Length;

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }
    }
}
