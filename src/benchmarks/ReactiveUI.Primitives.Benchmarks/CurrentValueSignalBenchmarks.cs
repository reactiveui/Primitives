// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures signals that re-read a value each time an external change hook fires.</summary>
[MemoryDiagnoser]
public class CurrentValueSignalBenchmarks
{
    /// <summary>The number of changes raised per case.</summary>
    private const int Count = 1000;

    /// <summary>The divisor that makes consecutive changes carry an equal value.</summary>
    private const int RepeatFactor = 2;

    /// <summary>Benchmarks a current-value signal delivering every read.</summary>
    /// <returns>The sum of delivered values.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesCurrentValueSignalChanges()
    {
        ChangeSource source = new();
        IntSignalWitness observer = new();
        using (new CurrentValueSignal<int>(source.Read, source.Attach).Subscribe(observer))
        {
            for (var i = 0; i < Count; i++)
            {
                source.Set(i);
            }
        }

        return observer.Total + observer.NextCount;
    }

    /// <summary>Benchmarks a current-value signal that skips a read equal to the last delivered value.</summary>
    /// <returns>The sum of delivered values.</returns>
    [Benchmark]
    public int PrimitivesCurrentValueSignalDistinctChanges()
    {
        ChangeSource source = new();
        IntSignalWitness observer = new();
        using (new CurrentValueSignal<int>(source.Read, source.Attach, EqualityComparer<int>.Default).Subscribe(observer))
        {
            for (var i = 0; i < Count; i++)
            {
                source.Set(i / RepeatFactor);
            }
        }

        return observer.Total + observer.NextCount;
    }

    /// <summary>Benchmarks a current-value witness driven by a reader, completing after the changes.</summary>
    /// <returns>The sum of delivered values plus the completion count.</returns>
    [Benchmark]
    public int PrimitivesCurrentValueWitnessComplete()
    {
        ChangeSource source = new();
        IntSignalWitness observer = new();
        using CurrentValueWitness<int> witness = new(observer, source, EqualityComparer<int>.Default);
        using (source.Attach(witness.Changed))
        {
            witness.Start();
            for (var i = 0; i < Count; i++)
            {
                source.Set(i / RepeatFactor);
            }

            witness.Complete();
        }

        return observer.Total + observer.CompletionCount + (witness.IsTerminated ? 1 : 0);
    }

    /// <summary>Benchmarks a current-value witness delivering every read, faulting after the changes.</summary>
    /// <returns>The sum of delivered values plus the error count.</returns>
    [Benchmark]
    public int PrimitivesCurrentValueWitnessFault()
    {
        ChangeSource source = new();
        IntSignalWitness observer = new();
        using CurrentValueWitness<int> witness = new(observer, source);
        using (source.Attach(witness.Changed))
        {
            witness.Start();
            for (var i = 0; i < Count; i++)
            {
                source.Set(i);
            }

            witness.Fault(new InvalidOperationException());
        }

        return observer.Total + observer.ErrorCount;
    }

    /// <summary>A mutable value that raises a single change hook.</summary>
    private sealed class ChangeSource : ICurrentValueReader<int>
    {
        /// <summary>The current value.</summary>
        private int _value;

        /// <summary>The attached change hook.</summary>
        private Action? _changed;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Read() => _value;

        /// <summary>Stores a value and raises the change hook.</summary>
        /// <param name="value">The new value.</param>
        public void Set(int value)
        {
            _value = value;
            _changed?.Invoke();
        }

        /// <summary>Attaches the change hook.</summary>
        /// <param name="changed">The hook to raise on change.</param>
        /// <returns>A handle that detaches the hook.</returns>
        public IDisposable Attach(Action changed)
        {
            _changed = changed;
            return new Detach(this);
        }

        /// <summary>Detaches the change hook from its source.</summary>
        /// <param name="owner">The source.</param>
        private sealed class Detach(ChangeSource owner) : IDisposable
        {
            /// <inheritdoc/>
            public void Dispose() => owner._changed = null;
        }
    }
}
