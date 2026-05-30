// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using BenchmarkDotNet.Attributes;
using System.Threading;

using ExtensionsAsyncObservable = ReactiveUI.Extensions.Async.ObservableAsync;
using ExtensionsAsyncSubject = ReactiveUI.Extensions.Async.Subjects.SubjectAsync;
using PrimitivesAsyncSignal = ReactiveUI.Primitives.Async.SignalAsync;
using PrimitivesAsyncSignalFactory = ReactiveUI.Primitives.Async.Signals.Signal;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>
/// Benchmarks the new async primitives API against ReactiveUI.Extensions 4.0.0.
/// </summary>
[MemoryDiagnoser]
public class AsyncExtensionsComparisonBenchmarks
{
    private const int Count = 32;
    private const int SubscriberCount = 8;

    [Benchmark(Baseline = true)]
    public async Task<int> PrimitivesSequenceMapKeepToListAsync()
    {
        var values = await PrimitivesAsyncSignal.ToListAsync(
                PrimitivesAsyncSignal.Keep(
                    PrimitivesAsyncSignal.Map(
                        PrimitivesAsyncSignal.Sequence(0, Count),
                        static (int value) => value + 1),
                    static (int value) => (value & 1) == 0))
            .ConfigureAwait(false);

        return values.Count;
    }

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

    [Benchmark]
    public async Task<int> PrimitivesSequenceCountAsync()
    {
        return await PrimitivesAsyncSignal.CountAsync(
                PrimitivesAsyncSignal.Sequence(0, Count))
            .ConfigureAwait(false);
    }

    [Benchmark]
    public async Task<int> ExtensionsRangeCountAsync()
    {
        return await ExtensionsAsyncObservable.CountAsync(
                ExtensionsAsyncObservable.Range(0, Count))
            .ConfigureAwait(false);
    }

    [Benchmark]
    public async Task<int> PrimitivesSignalBroadcastAsync()
    {
        var signal = PrimitivesAsyncSignalFactory.Create<int>();
        var observers = new PrimitivesCountingObserver[SubscriberCount];
        var subscriptions = new IAsyncDisposable[SubscriberCount];

        try
        {
            for (var i = 0; i < SubscriberCount; i++)
            {
                var observer = new PrimitivesCountingObserver();
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

    [Benchmark]
    public async Task<int> ExtensionsSubjectBroadcastAsync()
    {
        var subject = ExtensionsAsyncSubject.Create<int>();
        var observers = new ExtensionsCountingObserver[SubscriberCount];
        var subscriptions = new IAsyncDisposable[SubscriberCount];

        try
        {
            for (var i = 0; i < SubscriberCount; i++)
            {
                var observer = new ExtensionsCountingObserver();
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

    private static int Sum(PrimitivesCountingObserver[] observers)
    {
        var total = 0;
        for (var i = 0; i < observers.Length; i++)
        {
            total += observers[i].Total;
        }

        return total;
    }

    private static int Sum(ExtensionsCountingObserver[] observers)
    {
        var total = 0;
        for (var i = 0; i < observers.Length; i++)
        {
            total += observers[i].Total;
        }

        return total;
    }

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

    private sealed class PrimitivesCountingObserver : ReactiveUI.Primitives.Async.ObserverAsync<int>
    {
        public int Total { get; private set; }

        protected override ValueTask OnCompletedAsyncCore(ReactiveUI.Primitives.Async.Result result) => default;

        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            default;

        protected override ValueTask OnNextAsyncCore(int value, CancellationToken cancellationToken)
        {
            Total += value;
            return default;
        }
    }

    private sealed class ExtensionsCountingObserver : ReactiveUI.Extensions.Async.ObserverAsync<int>
    {
        public int Total { get; private set; }

        protected override ValueTask OnCompletedAsyncCore(ReactiveUI.Extensions.Async.Result result) => default;

        protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            default;

        protected override ValueTask OnNextAsyncCore(int value, CancellationToken cancellationToken)
        {
            Total += value;
            return default;
        }
    }
}
