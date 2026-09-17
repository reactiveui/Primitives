// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Disposables;
using R3CompositeDisposable = R3.CompositeDisposable;
using R3DisposableBag = R3.DisposableBag;
using RxBooleanDisposable = System.Reactive.Disposables.BooleanDisposable;
using RxCompositeDisposable = System.Reactive.Disposables.CompositeDisposable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures filling a composite disposable with subscriptions and disposing them together.</summary>
[MemoryDiagnoser]
public class DisposableBagBenchmarks
{
    /// <summary>The number of entries the seeded constructor takes.</summary>
    private const int SeededEntries = 3;

    /// <summary>Gets or sets the number of entries added to each composite, enough to grow past the inline slots.</summary>
    [Params(2, 16)]
    public int Entries { get; set; }

    /// <summary>Adds entries to a bag, disposes it, and adds one more after disposal.</summary>
    /// <returns>The number of entries observed as disposed.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesDisposableBagAddDispose()
    {
        var items = new BooleanDisposable[Entries + 1];
        DisposableBag bag = new();
        for (var i = 0; i < Entries; i++)
        {
            items[i] = new();
            bag.Add(items[i]);
        }

        bag.Dispose();
        items[Entries] = new();
        bag.Add(items[Entries]);
        return CountDisposed(items) + (bag.IsDisposed ? 1 : 0);
    }

    /// <summary>Pre-populates a bag through its three-entry constructor, then grows and disposes it.</summary>
    /// <returns>The number of entries observed as disposed.</returns>
    [Benchmark]
    public int PrimitivesDisposableBagSeededAddDispose()
    {
        var items = new BooleanDisposable[Entries + SeededEntries];
        for (var i = 0; i < items.Length; i++)
        {
            items[i] = new();
        }

        DisposableBag bag = new(items[0], items[1], items[SeededEntries - 1]);
        for (var i = SeededEntries; i < items.Length; i++)
        {
            bag.Add(items[i]);
        }

        bag.Dispose();
        bag.Dispose();
        return CountDisposed(items);
    }

    /// <summary>Adds entries to a composite disposable and disposes it using System.Reactive.</summary>
    /// <returns>The number of entries observed as disposed.</returns>
    [Benchmark]
    public int SystemReactiveCompositeAddDispose()
    {
        var items = new RxBooleanDisposable[Entries + 1];
        RxCompositeDisposable bag = new();
        for (var i = 0; i < Entries; i++)
        {
            items[i] = new();
            bag.Add(items[i]);
        }

        bag.Dispose();
        items[Entries] = new();
        bag.Add(items[Entries]);
        return CountDisposed(items) + (bag.IsDisposed ? 1 : 0);
    }

    /// <summary>Adds entries to a struct disposable bag and disposes it using R3.</summary>
    /// <returns>The number of entries observed as disposed.</returns>
    [Benchmark]
    public int R3DisposableBagAddDispose()
    {
        var items = new RxBooleanDisposable[Entries + 1];
        R3DisposableBag bag = default;
        for (var i = 0; i < Entries; i++)
        {
            items[i] = new();
            bag.Add(items[i]);
        }

        bag.Dispose();
        items[Entries] = new();
        bag.Add(items[Entries]);
        return CountDisposed(items) + 1;
    }

    /// <summary>Adds entries to a composite disposable and disposes it using R3.</summary>
    /// <returns>The number of entries observed as disposed.</returns>
    [Benchmark]
    public int R3CompositeAddDispose()
    {
        var items = new RxBooleanDisposable[Entries + 1];
        R3CompositeDisposable bag = new();
        for (var i = 0; i < Entries; i++)
        {
            items[i] = new();
            bag.Add(items[i]);
        }

        bag.Dispose();
        items[Entries] = new();
        bag.Add(items[Entries]);
        return CountDisposed(items) + (bag.IsDisposed ? 1 : 0);
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
