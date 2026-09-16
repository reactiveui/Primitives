// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Async.Disposables;
using ExtensionsCompositeDisposableAsync = ReactiveUI.Extensions.Async.Disposables.CompositeDisposableAsync;
using ExtensionsDisposableAsync = ReactiveUI.Extensions.Async.Disposables.DisposableAsync;
using ExtensionsDisposableAsyncMixins = ReactiveUI.Extensions.Async.DisposableAsyncMixins;
using ExtensionsSerialDisposableAsync = ReactiveUI.Extensions.Async.Disposables.SerialDisposableAsync;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures owning and releasing groups of async disposables: composite collections, replaceable slots and adapters.</summary>
[MemoryDiagnoser]
public class AsyncDisposableCollectionBenchmarks
{
    /// <summary>The number of disposables handled by each case.</summary>
    private const int Count = 64;

    /// <summary>The step between removed items, so every other item is removed.</summary>
    private const int RemovalStride = 2;

    /// <summary>Adds disposables to a composite one at a time and disposes the composite.</summary>
    /// <returns>The number of disposables released.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesCompositeAddDisposeAsync()
    {
        StrongBox<int> released = new();
        MultipleDisposableAsync composite = new();
        for (var i = 0; i < Count; i++)
        {
            await composite.AddAsync(DisposableAsync.Create(released, Release)).ConfigureAwait(false);
        }

        await composite.DisposeAsync().ConfigureAwait(false);
        return released.Value;
    }

    /// <summary>Adds disposables to a ReactiveUI.Extensions composite one at a time and disposes the composite.</summary>
    /// <returns>The number of disposables released.</returns>
    [Benchmark]
    public async Task<int> ExtensionsCompositeAddDisposeAsync()
    {
        StrongBox<int> released = new();
        ExtensionsCompositeDisposableAsync composite = new();
        for (var i = 0; i < Count; i++)
        {
            await composite.AddAsync(ExtensionsDisposableAsync.Create(released, Release)).ConfigureAwait(false);
        }

        await composite.DisposeAsync().ConfigureAwait(false);
        return released.Value;
    }

    /// <summary>Fills a composite, removes every other item, checks membership, snapshots it and clears the rest.</summary>
    /// <returns>The number of disposables released.</returns>
    [Benchmark]
    public async Task<int> PrimitivesCompositeRemoveClearAsync()
    {
        StrongBox<int> released = new();
        var items = CreateItems(released);
        MultipleDisposableAsync composite = new(Count);
        foreach (var item in items)
        {
            await composite.AddAsync(item).ConfigureAwait(false);
        }

        for (var i = 0; i < items.Length; i += RemovalStride)
        {
            await composite.Remove(items[i]).ConfigureAwait(false);
        }

        var live = 0;
        for (var i = 1; i < items.Length; i += RemovalStride)
        {
            live += composite.Contains(items[i]) ? 1 : 0;
        }

        var snapshot = new IAsyncDisposable[composite.Count];
        composite.CopyTo(snapshot, 0);
        using (var enumerator = composite.GetEnumerator())
        {
            while (enumerator.MoveNext())
            {
                live -= ReferenceEquals(enumerator.Current, snapshot[0]) ? 0 : 1;
            }
        }

        await composite.Clear().ConfigureAwait(false);
        await composite.DisposeAsync().ConfigureAwait(false);
        return released.Value + live;
    }

    /// <summary>Fills a ReactiveUI.Extensions composite, removes every other item, checks membership, snapshots it and clears the rest.</summary>
    /// <returns>The number of disposables released.</returns>
    [Benchmark]
    public async Task<int> ExtensionsCompositeRemoveClearAsync()
    {
        StrongBox<int> released = new();
        var entries = CreateExtensionsItems(released);
        ExtensionsCompositeDisposableAsync composite = new(Count);
        foreach (var entry in entries)
        {
            await composite.AddAsync(entry).ConfigureAwait(false);
        }

        for (var index = 0; index < entries.Length; index += RemovalStride)
        {
            await composite.Remove(entries[index]).ConfigureAwait(false);
        }

        var present = 0;
        for (var index = 1; index < entries.Length; index += RemovalStride)
        {
            present += composite.Contains(entries[index]) ? 1 : 0;
        }

        var copy = new IAsyncDisposable[composite.Count];
        composite.CopyTo(copy, 0);
        foreach (var entry in composite)
        {
            present -= ReferenceEquals(entry, copy[0]) ? 0 : 1;
        }

        await composite.Clear().ConfigureAwait(false);
        await composite.DisposeAsync().ConfigureAwait(false);
        return released.Value + present;
    }

    /// <summary>Builds composites from an array and from a list and disposes both.</summary>
    /// <returns>The number of disposables released.</returns>
    [Benchmark]
    public async Task<int> PrimitivesCompositeFromCollectionsDisposeAsync()
    {
        StrongBox<int> released = new();
        MultipleDisposableAsync fromArray = new(CreateItems(released));
        MultipleDisposableAsync fromList = new(new List<IAsyncDisposable>(CreateItems(released)));
        await fromArray.DisposeAsync().ConfigureAwait(false);
        await fromList.DisposeAsync().ConfigureAwait(false);
        return released.Value;
    }

    /// <summary>Builds ReactiveUI.Extensions composites from an array and from a list and disposes both.</summary>
    /// <returns>The number of disposables released.</returns>
    [Benchmark]
    public async Task<int> ExtensionsCompositeFromCollectionsDisposeAsync()
    {
        StrongBox<int> released = new();
        ExtensionsCompositeDisposableAsync fromArray = new(CreateExtensionsItems(released));
        ExtensionsCompositeDisposableAsync fromList = new(new List<IAsyncDisposable>(CreateExtensionsItems(released)));
        await fromArray.DisposeAsync().ConfigureAwait(false);
        await fromList.DisposeAsync().ConfigureAwait(false);
        return released.Value;
    }

    /// <summary>Replaces the resource held by a single-slot owner repeatedly, releasing each displaced one.</summary>
    /// <returns>The number of disposables released.</returns>
    [Benchmark]
    public async Task<int> PrimitivesReplaceableSlotSwapAsync()
    {
        StrongBox<int> released = new();
        SingleReplaceableDisposableAsync slot = new();
        for (var i = 0; i < Count; i++)
        {
            await slot.SetDisposableAsync(DisposableAsync.Create(released, Release)).ConfigureAwait(false);
        }

        await slot.DisposeAsync().ConfigureAwait(false);
        return released.Value;
    }

    /// <summary>Replaces the resource held by a ReactiveUI.Extensions serial owner repeatedly.</summary>
    /// <returns>The number of disposables released.</returns>
    [Benchmark]
    public async Task<int> ExtensionsSerialSlotSwapAsync()
    {
        StrongBox<int> released = new();
        ExtensionsSerialDisposableAsync slot = new();
        for (var i = 0; i < Count; i++)
        {
            await slot.SetDisposableAsync(ExtensionsDisposableAsync.Create(released, Release)).ConfigureAwait(false);
        }

        await slot.DisposeAsync().ConfigureAwait(false);
        return released.Value;
    }

    /// <summary>Adapts synchronous disposables to async disposables and releases them.</summary>
    /// <returns>The number of disposables released.</returns>
    [Benchmark]
    public async Task<int> PrimitivesDisposableAdapterAsync()
    {
        StrongBox<int> released = new();
        for (var i = 0; i < Count; i++)
        {
            await new CountingDisposable(released).ToDisposableAsync().DisposeAsync().ConfigureAwait(false);
        }

        return released.Value;
    }

    /// <summary>Adapts synchronous disposables to async disposables and releases them in ReactiveUI.Extensions.</summary>
    /// <returns>The number of disposables released.</returns>
    [Benchmark]
    public async Task<int> ExtensionsDisposableAdapterAsync()
    {
        StrongBox<int> released = new();
        for (var i = 0; i < Count; i++)
        {
            await ExtensionsDisposableAsyncMixins.ToDisposableAsync(new CountingDisposable(released))
                .DisposeAsync()
                .ConfigureAwait(false);
        }

        return released.Value;
    }

    /// <summary>Counts one release.</summary>
    /// <param name="released">The release counter.</param>
    /// <returns>A completed task.</returns>
    private static ValueTask Release(StrongBox<int> released)
    {
        released.Value++;
        return default;
    }

    /// <summary>Creates a batch of counting primitives async disposables.</summary>
    /// <param name="released">The release counter.</param>
    /// <returns>The disposables.</returns>
    private static IAsyncDisposable[] CreateItems(StrongBox<int> released)
    {
        var items = new IAsyncDisposable[Count];
        for (var i = 0; i < items.Length; i++)
        {
            items[i] = DisposableAsync.Create(released, Release);
        }

        return items;
    }

    /// <summary>Creates a batch of counting ReactiveUI.Extensions async disposables.</summary>
    /// <param name="released">The release counter.</param>
    /// <returns>The disposables.</returns>
    private static IAsyncDisposable[] CreateExtensionsItems(StrongBox<int> released)
    {
        var entries = new IAsyncDisposable[Count];
        for (var index = 0; index < entries.Length; index++)
        {
            entries[index] = ExtensionsDisposableAsync.Create(released, Release);
        }

        return entries;
    }

    /// <summary>A synchronous disposable that counts its release.</summary>
    /// <param name="released">The release counter.</param>
    [DebuggerDisplay("CountingDisposable")]
    private sealed class CountingDisposable(StrongBox<int> released) : IDisposable
    {
        /// <inheritdoc/>
        public void Dispose() => released.Value++;
    }
}
