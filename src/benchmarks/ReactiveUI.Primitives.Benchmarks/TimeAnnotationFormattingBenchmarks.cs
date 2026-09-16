// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Signals;
using RxHistoricalScheduler = System.Reactive.Concurrency.HistoricalScheduler;
using RxObservable = System.Reactive.Linq.Observable;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures annotating values with virtual time and rendering each annotation as text.</summary>
[MemoryDiagnoser]
public class TimeAnnotationFormattingBenchmarks
{
    /// <summary>The number of values annotated per case.</summary>
    private const int Count = 256;

    /// <summary>The virtual time advanced between values.</summary>
    private static readonly TimeSpan Step = TimeSpan.FromMilliseconds(1);

    /// <summary>Benchmarks timestamping values and formatting each timestamped value.</summary>
    /// <returns>The total formatted length.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesTimestampFormat()
    {
        VirtualClock clock = new();
        FormattedLengthWitness<Moment<int>> observer = new();
        using Signal<int> source = new();
        using var subscription = source.Timestamp(clock).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            clock.AdvanceBy(Step);
            source.OnNext(i);
        }

        return observer.Length;
    }

    /// <summary>Benchmarks timestamping values and formatting each timestamped value using System.Reactive.</summary>
    /// <returns>The total formatted length.</returns>
    [Benchmark]
    public int SystemReactiveTimestampFormat()
    {
        RxHistoricalScheduler scheduler = new();
        FormattedLengthWitness<System.Reactive.Timestamped<int>> observer = new();
        using RxSubject source = new();
        using var subscription = RxObservable.Timestamp(source, scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            scheduler.AdvanceBy(Step);
            source.OnNext(i);
        }

        return observer.Length;
    }

    /// <summary>Benchmarks measuring the interval between values and formatting each annotated value.</summary>
    /// <returns>The total formatted length.</returns>
    [Benchmark]
    public int PrimitivesTimeIntervalFormat()
    {
        VirtualClock clock = new();
        FormattedLengthWitness<TimeInterval<int>> observer = new();
        using Signal<int> source = new();
        using var subscription = source.TimeInterval(clock).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            clock.AdvanceBy(Step);
            source.OnNext(i);
        }

        return observer.Length;
    }

    /// <summary>Benchmarks measuring the interval between values and formatting each annotated value using System.Reactive.</summary>
    /// <returns>The total formatted length.</returns>
    [Benchmark]
    public int SystemReactiveTimeIntervalFormat()
    {
        RxHistoricalScheduler scheduler = new();
        FormattedLengthWitness<System.Reactive.TimeInterval<int>> observer = new();
        using RxSubject source = new();
        using var subscription = RxObservable.TimeInterval(source, scheduler).Subscribe(observer);
        for (var i = 0; i < Count; i++)
        {
            scheduler.AdvanceBy(Step);
            source.OnNext(i);
        }

        return observer.Length;
    }

    /// <summary>Observer that formats every value and accumulates the text length.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class FormattedLengthWitness<T> : IObserver<T>
    {
        /// <summary>Gets the total formatted length.</summary>
        public int Length { get; private set; }

        /// <inheritdoc/>
        public void OnNext(T value) => Length += value?.ToString()?.Length ?? 0;

        /// <inheritdoc/>
        public void OnError(Exception error) => Length = -1;

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }
    }
}
