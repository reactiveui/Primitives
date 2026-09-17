// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Signals;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures attaching to a callback-style event, raising it, and detaching.</summary>
[MemoryDiagnoser]
public class EventConversionSignalBenchmarks
{
    /// <summary>The number of times the event is raised per subscription.</summary>
    private const int Count = 256;

    /// <summary>Benchmarks converting an argument-only event into a signal.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesFromEventArgument()
    {
        IntSignalWitness observer = new();
        ArgumentEventSource events = new();
        using (Signal.FromEvent<int>(events.Add, events.Remove).Subscribe(observer))
        {
            events.RaiseSequence(Count);
        }

        return observer.Total + events.HandlerCount;
    }

    /// <summary>Benchmarks converting an argument-only event into an observable using System.Reactive.</summary>
    /// <returns>The observed total.</returns>
    [Benchmark]
    public int SystemReactiveFromEventArgument()
    {
        IntSignalWitness observer = new();
        ArgumentEventSource events = new();
        using (RxObservable.FromEvent<int>(events.Add, events.Remove).Subscribe(observer))
        {
            events.RaiseSequence(Count);
        }

        return observer.Total + events.HandlerCount;
    }

    /// <summary>An event that passes only its argument to attached callbacks.</summary>
    private sealed class ArgumentEventSource
    {
        /// <summary>The attached callbacks.</summary>
        private Action<int>? _handlers;

        /// <summary>Gets the number of attached callbacks.</summary>
        internal int HandlerCount => _handlers?.GetInvocationList().Length ?? 0;

        /// <summary>Attaches a callback.</summary>
        /// <param name="handler">The callback to attach.</param>
        internal void Add(Action<int> handler) => _handlers += handler;

        /// <summary>Detaches a callback.</summary>
        /// <param name="handler">The callback to detach.</param>
        internal void Remove(Action<int> handler) => _handlers -= handler;

        /// <summary>Raises the event once for each value from zero up to the count.</summary>
        /// <param name="count">The number of raises.</param>
        internal void RaiseSequence(int count)
        {
            for (var i = 0; i < count; i++)
            {
                _handlers?.Invoke(i);
            }
        }
    }
}
