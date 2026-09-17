// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Reactive.Disposables;
using R3CompositeDisposable = R3.CompositeDisposable;
using RxBooleanDisposable = System.Reactive.Disposables.BooleanDisposable;
using RxCompositeDisposable = System.Reactive.Disposables.CompositeDisposable;
using ShimLinq = ReactiveUI.Primitives.Reactive.LinqExtensions;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures collecting subscriptions into a container that System.Reactive code also sees as a composite, then disposing it.</summary>
[MemoryDiagnoser]
public class ContainerDisposableBenchmarks
{
    /// <summary>The number of subscriptions registered with each container.</summary>
    private const int Entries = 16;

    /// <summary>Registers subscriptions through DisposeWith, adds one through the System.Reactive composite view, and disposes the container.</summary>
    /// <returns>The number of subscriptions observed as disposed.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesContainerDisposeWith()
    {
        var items = CreateItems();
        ContainerDisposable container = new();
        for (var i = 0; i < Entries; i++)
        {
            _ = ShimLinq.DisposeWith(items[i], container);
        }

        RxCompositeDisposable composite = container;
        composite.Add(items[Entries]);
        container.Dispose();
        return CountDisposed(items);
    }

    /// <summary>Registers subscriptions with a composite disposable and disposes it using System.Reactive.</summary>
    /// <returns>The number of subscriptions observed as disposed.</returns>
    [Benchmark]
    public int SystemReactiveCompositeAddDispose()
    {
        var items = CreateItems();
        RxCompositeDisposable composite = new();
        foreach (var item in items)
        {
            composite.Add(item);
        }

        composite.Dispose();
        return CountDisposed(items);
    }

    /// <summary>Registers subscriptions with a composite disposable and disposes it using R3.</summary>
    /// <returns>The number of subscriptions observed as disposed.</returns>
    [Benchmark]
    public int R3CompositeAddDispose()
    {
        var items = CreateItems();
        R3CompositeDisposable composite = new();
        foreach (var item in items)
        {
            composite.Add(item);
        }

        composite.Dispose();
        return CountDisposed(items);
    }

    /// <summary>Creates the subscriptions registered by each case.</summary>
    /// <returns>One more subscription than <see cref="Entries"/>.</returns>
    private static RxBooleanDisposable[] CreateItems()
    {
        var items = new RxBooleanDisposable[Entries + 1];
        for (var i = 0; i < items.Length; i++)
        {
            items[i] = new();
        }

        return items;
    }

    /// <summary>Counts the subscriptions that report disposed.</summary>
    /// <param name="items">The subscriptions to inspect.</param>
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
