// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Disposables;
using R3SerialDisposable = R3.SerialDisposable;
using R3SingleAssignmentDisposable = R3.SingleAssignmentDisposable;
using RxBooleanDisposable = System.Reactive.Disposables.BooleanDisposable;
using RxMultipleAssignmentDisposable = System.Reactive.Disposables.MultipleAssignmentDisposable;
using RxSerialDisposable = System.Reactive.Disposables.SerialDisposable;
using RxSingleAssignmentDisposable = System.Reactive.Disposables.SingleAssignmentDisposable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures holders that swap, replace or assign once the subscription they own.</summary>
[MemoryDiagnoser]
public class DisposableSlotBenchmarks
{
    /// <summary>The number of replacements or holders per case.</summary>
    private const int Count = 256;

    /// <summary>Replaces the inner subscription repeatedly, disposing each displaced value, then disposes the holder.</summary>
    /// <returns>The number of inner values observed as disposed.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesSwapDisposableReplace()
    {
        var items = CreatePrimitives(Count + 1);
        SwapDisposable holder = new();
        for (var i = 0; i < Count; i++)
        {
            holder.Disposable = items[i];
        }

        holder.Dispose();
        holder.Disposable = items[Count];
        return CountDisposed(items) + (holder.IsDisposed ? 1 : 0);
    }

    /// <summary>Replaces the inner subscription repeatedly through a replaceable slot.</summary>
    /// <returns>The number of inner values observed as disposed.</returns>
    [Benchmark]
    public int PrimitivesSlotReplace()
    {
        var items = CreatePrimitives(Count + 1);
        var released = 0;
        Slot holder = new(items[0], () => released++);
        for (var i = 1; i < Count; i++)
        {
            holder.Create(items[i]);
        }

        holder.Dispose();
        holder.Create(items[Count]);
        return CountDisposed(items) + released + (holder.IsDisposed ? 1 : 0);
    }

    /// <summary>Replaces the inner subscription repeatedly using System.Reactive.</summary>
    /// <returns>The number of inner values observed as disposed.</returns>
    [Benchmark]
    public int SystemReactiveSerialDisposableReplace()
    {
        var items = CreateReactive(Count + 1);
        RxSerialDisposable holder = new();
        for (var i = 0; i < Count; i++)
        {
            holder.Disposable = items[i];
        }

        holder.Dispose();
        holder.Disposable = items[Count];
        return CountDisposed(items) + (holder.IsDisposed ? 1 : 0);
    }

    /// <summary>Replaces the inner subscription repeatedly using R3.</summary>
    /// <returns>The number of inner values observed as disposed.</returns>
    [Benchmark]
    public int R3SerialDisposableReplace()
    {
        var items = CreateReactive(Count + 1);
        R3SerialDisposable holder = new();
        for (var i = 0; i < Count; i++)
        {
            holder.Disposable = items[i];
        }

        holder.Dispose();
        holder.Disposable = items[Count];
        return CountDisposed(items) + (holder.IsDisposed ? 1 : 0);
    }

    /// <summary>Reassigns the inner subscription without disposing displaced values, then disposes the holder.</summary>
    /// <returns>The number of inner values observed as disposed.</returns>
    [Benchmark]
    public int PrimitivesMutableDisposableReassign()
    {
        var items = CreatePrimitives(Count + 1);
        MutableDisposable holder = new();
        for (var i = 0; i < Count; i++)
        {
            holder.Disposable = items[i];
        }

        holder.Dispose();
        holder.Disposable = items[Count];
        return CountDisposed(items) + (holder.Disposable is null ? 1 : 0) + (holder.IsDisposed ? 1 : 0);
    }

    /// <summary>Reassigns the inner subscription without disposing displaced values using System.Reactive.</summary>
    /// <returns>The number of inner values observed as disposed.</returns>
    [Benchmark]
    public int SystemReactiveMultipleAssignmentReassign()
    {
        var items = CreateReactive(Count + 1);
        RxMultipleAssignmentDisposable holder = new();
        for (var i = 0; i < Count; i++)
        {
            holder.Disposable = items[i];
        }

        holder.Dispose();
        holder.Disposable = items[Count];
        return CountDisposed(items) + (holder.Disposable is null ? 1 : 0) + (holder.IsDisposed ? 1 : 0);
    }

    /// <summary>Assigns one subscription to each single-assignment holder and disposes it.</summary>
    /// <returns>The number of inner values observed as disposed.</returns>
    [Benchmark]
    public int PrimitivesOnceDisposableAssign()
    {
        var items = CreatePrimitives(Count);
        var assigned = 0;
        for (var i = 0; i < Count; i++)
        {
            OnceDisposable holder = new() { Disposable = items[i] };
            if (holder.IsAssigned && holder.Disposable is not null)
            {
                assigned++;
            }

            holder.Dispose();
            holder.Disposable = items[i];
        }

        return CountDisposed(items) + assigned;
    }

    /// <summary>Assigns one subscription to each single-assignment slot and disposes it.</summary>
    /// <returns>The number of inner values observed as disposed.</returns>
    [Benchmark]
    public int PrimitivesAssignmentSlotAssign()
    {
        var items = CreatePrimitives(Count);
        var released = 0;
        for (var i = 0; i < Count; i++)
        {
            AssignmentSlot holder = new(() => released++);
            holder.Create(items[i]);
            holder.Dispose();
            if (holder.IsDisposed)
            {
                released++;
            }
        }

        return CountDisposed(items) + released;
    }

    /// <summary>Assigns one subscription to each single-assignment holder and disposes it using System.Reactive.</summary>
    /// <returns>The number of inner values observed as disposed.</returns>
    [Benchmark]
    public int SystemReactiveSingleAssignmentAssign()
    {
        var items = CreateReactive(Count);
        var assigned = 0;
        for (var i = 0; i < Count; i++)
        {
            RxSingleAssignmentDisposable holder = new() { Disposable = items[i] };
            if (holder.Disposable is not null)
            {
                assigned++;
            }

            holder.Dispose();
        }

        return CountDisposed(items) + assigned;
    }

    /// <summary>Assigns one subscription to each single-assignment holder and disposes it using R3.</summary>
    /// <returns>The number of inner values observed as disposed.</returns>
    [Benchmark]
    public int R3SingleAssignmentAssign()
    {
        var items = CreateReactive(Count);
        var assigned = 0;
        for (var i = 0; i < Count; i++)
        {
            R3SingleAssignmentDisposable holder = new() { Disposable = items[i] };
            if (holder.Disposable is not null)
            {
                assigned++;
            }

            holder.Dispose();
        }

        return CountDisposed(items) + assigned;
    }

    /// <summary>Creates Primitives inner disposables.</summary>
    /// <param name="count">The number to create.</param>
    /// <returns>The disposables.</returns>
    private static BooleanDisposable[] CreatePrimitives(int count)
    {
        var items = new BooleanDisposable[count];
        for (var i = 0; i < count; i++)
        {
            items[i] = new();
        }

        return items;
    }

    /// <summary>Creates System.Reactive inner disposables.</summary>
    /// <param name="count">The number to create.</param>
    /// <returns>The disposables.</returns>
    private static RxBooleanDisposable[] CreateReactive(int count)
    {
        var items = new RxBooleanDisposable[count];
        for (var i = 0; i < count; i++)
        {
            items[i] = new();
        }

        return items;
    }

    /// <summary>Counts the Primitives disposables that report disposed.</summary>
    /// <param name="items">The disposables to inspect.</param>
    /// <returns>The disposed count.</returns>
    private static int CountDisposed(BooleanDisposable[] items)
    {
        var count = 0;
        foreach (var item in items)
        {
            if (item.IsDisposed)
            {
                count++;
            }
        }

        return count;
    }

    /// <summary>Counts the System.Reactive disposables that report disposed.</summary>
    /// <param name="items">The disposables to inspect.</param>
    /// <returns>The disposed count.</returns>
    private static int CountDisposed(RxBooleanDisposable[] items)
    {
        var count = 0;
        foreach (var item in items)
        {
            if (item.IsDisposed)
            {
                count++;
            }
        }

        return count;
    }
}
