// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;
using RxSubject = System.Reactive.Subjects.Subject<int>;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures a fault reaching a subscriber whose error handler rethrows it to the producer.</summary>
[MemoryDiagnoser]
public class UnhandledFaultRethrowBenchmarks
{
    /// <summary>The number of faults raised per case.</summary>
    private const int Count = 16;

    /// <summary>Ignores values.</summary>
    private static readonly Action<int> IgnoreValue = static _ => { };

    /// <summary>Benchmarks rethrowing a signal fault from a subscriber error handler.</summary>
    /// <returns>The number of faults caught by the producer.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesRethrowFault()
    {
        var caught = 0;
        for (var i = 0; i < Count; i++)
        {
            using Signal<int> source = new();
            using var subscription = source.Subscribe(Witness.Create(IgnoreValue, static error => error.Throw()));
            try
            {
                source.OnError(new InvalidOperationException());
            }
            catch (InvalidOperationException)
            {
                caught++;
            }
        }

        return caught;
    }

    /// <summary>Benchmarks rethrowing a subject fault from a default subscriber error handler using System.Reactive.</summary>
    /// <returns>The number of faults caught by the producer.</returns>
    [Benchmark]
    public int SystemReactiveRethrowFault()
    {
        var caught = 0;
        for (var i = 0; i < Count; i++)
        {
            using RxSubject source = new();
            using var subscription = System.ObservableExtensions.Subscribe(source, IgnoreValue);
            try
            {
                source.OnError(new InvalidOperationException());
            }
            catch (InvalidOperationException)
            {
                caught++;
            }
        }

        return caught;
    }
}
