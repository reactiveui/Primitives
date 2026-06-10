// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;

using ReactiveUI.Primitives.Async;

using ExtensionsAsyncObservable = ReactiveUI.Extensions.Async.ObservableAsync;
using ExtensionsAsyncSubject = ReactiveUI.Extensions.Async.Subjects.SubjectAsync;
using PrimitivesAsyncSignal = ReactiveUI.Primitives.Async.SignalAsync;
using PrimitivesAsyncSignalFactory = ReactiveUI.Primitives.Async.Signals.Signal;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks the new async primitives API against ReactiveUI.Extensions 4.0.0.</summary>
[MemoryDiagnoser]
public class AsyncExtensionsComparisonBenchmarks
{
    /// <summary>The number of values produced by sequence-based benchmarks.</summary>
    private const int Count = 32;

    /// <summary>The number of observers attached by broadcast benchmarks.</summary>
    private const int SubscriberCount = 8;

    /// <summary>Maps, filters, and collects a primitive async sequence.</summary>
    /// <returns>The number of collected values.</returns>
    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesSequenceMapKeepToListAsync()
    {
        var values = await PrimitivesAsyncSignal.Sequence(0, Count)
            .Map(static value => value + 1)
            .Keep(static value => (value & 1) == 0)
            .ToListAsync()
            .ConfigureAwait(false);

        return values.Count;
    }

    /// <summary>Selects, filters, and collects a ReactiveUI.Extensions async range.</summary>
    /// <returns>The number of collected values.</returns>
    [Benchmark]
    public async Task<int> ExtensionsRangeSelectWhereToListAsync()
    {
        var values = await ExtensionsAsyncObservable.ToListAsync(
                ExtensionsAsyncObservable.Where(
                    ExtensionsAsyncObservable.Select(
                        ExtensionsAsyncObservable.Range(0, Count),
                        static value => value + 1),
                    static value => (value & 1) == 0))
            .ConfigureAwait(false);

        return values.Count;
    }

    /// <summary>Counts the values emitted by a primitive async sequence.</summary>
    /// <returns>The emitted value count.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSequenceCountAsync() =>
        await PrimitivesAsyncSignal.Sequence(0, Count)
            .CountAsync()
            .ConfigureAwait(false);

    /// <summary>Counts the values emitted by a ReactiveUI.Extensions async range.</summary>
    /// <returns>The emitted value count.</returns>
    [Benchmark]
    public async Task<int> ExtensionsRangeCountAsync() =>
        await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.Range(0, Count))
            .ConfigureAwait(false);

    /// <summary>Broadcasts primitive async signal values to multiple subscribers.</summary>
    /// <returns>The total value sum observed by all subscribers.</returns>
    [Benchmark]
    public async Task<int> PrimitivesSignalBroadcastAsync()
    {
        var signal = PrimitivesAsyncSignalFactory.Create<int>();
        var observers = new PrimitivesCountingWitness[SubscriberCount];
        var subscriptions = new IAsyncDisposable[SubscriberCount];

        try
        {
            for (var i = 0; i < SubscriberCount; i++)
            {
                var observer = new PrimitivesCountingWitness();
                observers[i] = observer;
                subscriptions[i] = await signal.SubscribeAsync(observer, CancellationToken.None)
                    .ConfigureAwait(false);
            }

            for (var i = 0; i < Count; i++)
            {
                await signal.OnNextAsync(i, CancellationToken.None).ConfigureAwait(false);
            }

            return Sum(observers);
        }
        finally
        {
            await DisposeAllAsync(subscriptions).ConfigureAwait(false);
            await signal.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Broadcasts ReactiveUI.Extensions async subject values to multiple subscribers.</summary>
    /// <returns>The total value sum observed by all subscribers.</returns>
    [Benchmark]
    public async Task<int> ExtensionsSubjectBroadcastAsync()
    {
        var subject = ExtensionsAsyncSubject.Create<int>();
        var observers = new ExtensionsCountingWitness[SubscriberCount];
        var subscriptions = new IAsyncDisposable[SubscriberCount];

        try
        {
            for (var i = 0; i < SubscriberCount; i++)
            {
                var observer = new ExtensionsCountingWitness();
                observers[i] = observer;
                subscriptions[i] = await subject.SubscribeAsync(observer, CancellationToken.None)
                    .ConfigureAwait(false);
            }

            for (var i = 0; i < Count; i++)
            {
                await subject.OnNextAsync(i, CancellationToken.None).ConfigureAwait(false);
            }

            return Sum(observers);
        }
        finally
        {
            await DisposeAllAsync(subscriptions).ConfigureAwait(false);
            await subject.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Sums the totals recorded by primitive async observers.</summary>
    /// <param name="observers">The observers to sum.</param>
    /// <returns>The combined observed total.</returns>
    private static int Sum(PrimitivesCountingWitness[] observers)
    {
        var total = 0;
        for (var i = 0; i < observers.Length; i++)
        {
            total += observers[i].Total;
        }

        return total;
    }

    /// <summary>Sums the totals recorded by ReactiveUI.Extensions async observers.</summary>
    /// <param name="observers">The observers to sum.</param>
    /// <returns>The combined observed total.</returns>
    private static int Sum(ExtensionsCountingWitness[] observers)
    {
        var total = 0;
        for (var i = 0; i < observers.Length; i++)
        {
            total += observers[i].Total;
        }

        return total;
    }

    /// <summary>Disposes every non-null async subscription in order.</summary>
    /// <param name="subscriptions">The subscriptions to dispose.</param>
    /// <returns>A task that represents the asynchronous dispose operation.</returns>
    private static async ValueTask DisposeAllAsync(IAsyncDisposable[] subscriptions)
    {
        for (var i = 0; i < subscriptions.Length; i++)
        {
            if (subscriptions[i] is not null)
            {
                await subscriptions[i].DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>Observer that accumulates primitive async signal values.</summary>
    private sealed class PrimitivesCountingWitness : Async.WitnessAsync<int>
    {
        /// <summary>Gets the accumulated value total.</summary>
        public int Total { get; private set; }

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(Async.Result result) => default;

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            default;

        /// <inheritdoc/>
        protected override ValueTask OnNextAsyncCore(int value, CancellationToken cancellationToken)
        {
            Total += value;
            return default;
        }
    }

    /// <summary>Observer that accumulates ReactiveUI.Extensions async subject values.</summary>
    private sealed class ExtensionsCountingWitness : ReactiveUI.Extensions.Async.ObserverAsync<int>
    {
        /// <summary>Gets the accumulated value total.</summary>
        public int Total { get; private set; }

        /// <inheritdoc/>
        protected override ValueTask OnCompletedAsyncCore(ReactiveUI.Extensions.Async.Result result) => default;

        /// <inheritdoc/>
        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            default;

        /// <inheritdoc/>
        protected override ValueTask OnNextAsyncCore(int value, CancellationToken cancellationToken)
        {
            Total += value;
            return default;
        }
    }
}
