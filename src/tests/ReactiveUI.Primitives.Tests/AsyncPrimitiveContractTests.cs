// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#pragma warning disable SA1600, SA1611, SA1615, SA1618, S109, S1128, S6354, S6566, CA1861, IDE0300, RCS1196

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Concurrency;
using TUnit.Core;
using AsyncObs = ReactiveUI.Primitives.Async.SignalAsync;

namespace ReactiveUI.Primitives.Tests;

public sealed class AsyncPrimitiveContractTests
{
    [Test]
    public async Task PrimitivesFactoryAliasesMatchObservableAsyncSemantics()
    {
        var sequence = await AsyncObs.Sequence(3, 3).ToListAsync();
        var emitted = await AsyncObs.Emit(9).ToListAsync();
        var none = await AsyncObs.None<int>().ToListAsync();
        var enumerable = await new[] { 1, 2, 3 }.ToAsyncSignal().ToListAsync();

        Assert.Equal(new[] { 3, 4, 5 }, sequence);
        Assert.Equal(new[] { 9 }, emitted);
        Assert.Equal(Array.Empty<int>(), none);
        Assert.Equal(new[] { 1, 2, 3 }, enumerable);

        var error = new InvalidOperationException("failure");
        InvalidOperationException? observed = null;
        try
        {
            await AsyncObs.Fail<int>(error).ToListAsync();
        }
        catch (InvalidOperationException exception)
        {
            observed = exception;
        }

        Assert.Same(error, observed!);
    }

    [Test]
    public async Task PrimitivesTransformationAliasesComposeLikeCoreNaming()
    {
        var tapped = new List<int>();
        var values = await AsyncObs.Sequence(1, 6)
            .Map(value => value * 2)
            .Keep(value => value > 4)
            .Tap(tapped.Add)
            .Fold(0, (acc, value) => acc + value)
            .ToListAsync();

        Assert.Equal(new[] { 6, 8, 10, 12 }, tapped);
        Assert.Equal(new[] { 6, 14, 24, 36 }, values);

        var typed = await new object?[] { "one", 2, "three", null }
            .ToAsyncSignal()
            .KeepType<string>()
            .ToListAsync();
        Assert.Equal(new[] { "one", "three" }, typed);
    }

    [Test]
    public async Task PrimitivesCombinationAliasesForwardToAsyncOperators()
    {
        var chained = await AsyncObs.Chain(
            AsyncObs.Emit(1),
            AsyncObs.Sequence(2, 2))
            .ToListAsync();
        var paired = await AsyncObs.Emit(2).Pair(AsyncObs.Emit("a"), (left, right) => $"{left}{right}").ToListAsync();
        var latest = await AsyncObs.Emit(2).SyncLatest(AsyncObs.Emit(5), (left, right) => left + right).ToListAsync();
        var blended = await AsyncObs.Blend(AsyncObs.Emit(10), AsyncObs.Emit(20)).ToListAsync();

        Assert.Equal(new[] { 1, 2, 3 }, chained);
        Assert.Equal(new[] { "2a" }, paired);
        Assert.Equal(new[] { 7 }, latest);
        Assert.Equal(2, blended.Count);
        Assert.Contains(10, blended);
        Assert.Contains(20, blended);
    }

    [Test]
    public async Task PrimitivesErrorAndTerminalAliasesMatchExpectedBehavior()
    {
        var recovered = await AsyncObs.Fail<int>(new InvalidOperationException())
            .Recover(_ => AsyncObs.Emit(42))
            .ToListAsync();
        var resumed = await AsyncObs.Fail<int>(new InvalidOperationException())
            .Resume(AsyncObs.Emit(24))
            .ToListAsync();
        var attempt = 0;
        var reattempted = await AsyncObs.Defer(() =>
            ++attempt == 1 ? AsyncObs.Fail<int>(new InvalidOperationException()) : AsyncObs.Emit(7))
            .Reattempt(1)
            .ToListAsync();
        var collected = await AsyncObs.Sequence(1, 3).CollectArrayAsync();

        Assert.Equal(new[] { 42 }, recovered);
        Assert.Equal(new[] { 24 }, resumed);
        Assert.Equal(new[] { 7 }, reattempted);
        Assert.Equal((IEnumerable<int>)new[] { 1, 2, 3 }, collected);
    }

    [Test]
    public async Task UseDisposesAsyncResourceAfterCompletion()
    {
        var disposed = false;
        var values = await AsyncObs.Use(
            _ => new ValueTask<TestAsyncResource>(new TestAsyncResource(() => disposed = true)),
            _ => AsyncObs.Emit(5))
            .ToListAsync();

        Assert.Equal(new[] { 5 }, values);
        Assert.True(disposed);
    }

    [Test]
    public async Task ObserveOnSequencerSchedulesDirectWorkItems()
    {
        var sequencer = new QueuedSequencer();
        var task = AsyncObs.Emit(11)
            .ObserveOn(sequencer, forceYielding: true)
            .ToListAsync()
            .AsTask();

        var values = await DrainUntilComplete(task, sequencer);

        Assert.Equal(new[] { 11 }, values);
        Assert.True(sequencer.ScheduleCount > 0);
    }

    [Test]
    public async Task ShiftAndExpireAliasesUseTimeBasedOperators()
    {
        var shifted = await AsyncObs.Emit(3).Shift(TimeSpan.FromMilliseconds(1)).ToListAsync();
        Assert.Equal(new[] { 3 }, shifted);

        TimeoutException? timeout = null;
        try
        {
            await AsyncObs.Never<int>().Expire(TimeSpan.FromMilliseconds(1)).ToListAsync();
        }
        catch (TimeoutException exception)
        {
            timeout = exception;
        }

        Assert.NotNull(timeout);
    }

    private static async Task<T> DrainUntilComplete<T>(Task<T> task, QueuedSequencer sequencer)
    {
        for (var i = 0; i < 1_000; i++)
        {
            sequencer.DrainAll();
            if (task.IsCompleted)
            {
                return await task.ConfigureAwait(false);
            }

            await Task.Delay(1).ConfigureAwait(false);
        }

        return await task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
    }

    private sealed class QueuedSequencer : ISequencer
    {
        private readonly ConcurrentQueue<IWorkItem> _items = new();

        public DateTimeOffset Now => DateTimeOffset.UtcNow;

        public long Timestamp => DateTime.UtcNow.Ticks;

        public int ScheduleCount { get; private set; }

        public void Schedule(IWorkItem item)
        {
            ScheduleCount++;
            _items.Enqueue(item);
        }

        public void Schedule(IWorkItem item, long dueTimestamp) => Schedule(item);

        public void DrainAll()
        {
            while (_items.TryDequeue(out var item))
            {
                item.Execute();
            }
        }
    }

    private sealed class TestAsyncResource(Action onDispose) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            onDispose();
            return default;
        }
    }
}
