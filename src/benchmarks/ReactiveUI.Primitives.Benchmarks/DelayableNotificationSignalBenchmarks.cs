// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures a notification signal that forwards immediately or buffers and flushes a de-duplicated batch.</summary>
[MemoryDiagnoser]
public class DelayableNotificationSignalBenchmarks
{
    /// <summary>The number of notifications raised per phase.</summary>
    private const int Count = 1000;

    /// <summary>The number of distinct notifications raised while delayed.</summary>
    private const int DistinctValues = 32;

    /// <summary>Removes duplicates from a buffered batch.</summary>
    private static readonly Func<IList<int>, IEnumerable<int>> Distinct = static items => new HashSet<int>(items);

    /// <summary>Benchmarks buffering notifications during a delay window and flushing them as a distinct batch.</summary>
    /// <returns>The sum of delivered values plus the delivery count.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesDelayedBatchFlush()
    {
        var delayed = true;
        IntSignalWitness observer = new();
        using DelayableNotificationSignal<int> signal = new(() => delayed, Distinct);
        using (signal.Subscribe(observer))
        {
            for (var i = 0; i < Count; i++)
            {
                signal.OnNext(i % DistinctValues);
            }

            delayed = false;
            signal.Flush();
            signal.Flush();
            signal.OnCompleted();
        }

        IntSignalWitness late = new();
        using var lateSubscription = signal.Subscribe(late);
        return observer.Total + observer.NextCount + late.CompletionCount;
    }

    /// <summary>Benchmarks forwarding notifications immediately outside a delay window, ending in an error.</summary>
    /// <returns>The sum of delivered values plus the error counts.</returns>
    [Benchmark]
    public int PrimitivesImmediateForwarding()
    {
        IntSignalWitness observer = new();
        using DelayableNotificationSignal<int> signal = new(static () => false, Distinct);
        using (signal.Subscribe(observer))
        {
            for (var i = 0; i < Count; i++)
            {
                signal.OnNext(i);
            }

            var active = signal.HasObservers ? 1 : 0;
            signal.OnError(new InvalidOperationException());
            signal.OnNext(Count);
            IntSignalWitness late = new();
            using var lateSubscription = signal.Subscribe(late);
            return observer.Total + observer.ErrorCount + late.ErrorCount + active + (signal.IsDisposed ? 1 : 0);
        }
    }
}
