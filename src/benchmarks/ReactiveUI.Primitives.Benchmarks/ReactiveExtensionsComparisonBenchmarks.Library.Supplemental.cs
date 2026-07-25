// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Reactive.Concurrency;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;
using PackageExtensions = ReactiveUI.Extensions.ReactiveExtensions;
using PackageObservables = ReactiveUI.Extensions.Observables;
using PackageSubscriptionExtensions = ReactiveUI.Extensions.ObservableSubscriptionExtensions;
using PrimitivesExtensions = ReactiveUI.Primitives.Extensions.ReactiveExtensions;
using PrimitivesObservables = ReactiveUI.Primitives.Extensions.Observables;
using PrimitivesSubscriptionExtensions = ReactiveUI.Primitives.Extensions.ObservableSubscriptionExtensions;
using RxObservable = System.Reactive.Linq.Observable;
using RxUnit = System.Reactive.Unit;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks the complete synchronous ReactiveUI.Primitives.Extensions public helper surface.</summary>
public partial class ReactiveExtensionsComparisonBenchmarks
{
    /// <summary>Executes the <c>RunPartition</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunPartition</c> result.</returns>
    private static int RunPartition(ExtensionsLibrary library)
    {
        IntSignalWitness observer = new();
        if (library == ExtensionsLibrary.Primitives)
        {
            var (even, odd) = PrimitivesExtensions.Partition(ArraySource(library), static value => (value & 1) == 0);
            using var evenSubscription = even.Subscribe(observer);
            using var oddSubscription = odd.Subscribe(observer);
        }
        else
        {
            var (even, odd) = PackageExtensions.Partition(ArraySource(library), static value => (value & 1) == 0);
            using var evenSubscription = even.Subscribe(observer);
            using var oddSubscription = odd.Subscribe(observer);
        }

        return observer.Total;
    }

    /// <summary>Executes the <c>RunReplayLastOnSubscribe</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunReplayLastOnSubscribe</c> result.</returns>
    private static int RunReplayLastOnSubscribe(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ReplayLastOnSubscribe(ArraySource(library), Fallback)
            : PackageExtensions.ReplayLastOnSubscribe(ArraySource(library), Fallback));

    /// <summary>Executes the <c>RunRetryForeverWithDelay</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunRetryForeverWithDelay</c> result.</returns>
    private static int RunRetryForeverWithDelay(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.RetryForeverWithDelay(ArraySource(library), TimeSpan.Zero)
            : PackageExtensions.RetryForeverWithDelay(ArraySource(library), TimeSpan.Zero));

    /// <summary>Executes the <c>RunRetryWithBackoff</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunRetryWithBackoff</c> result.</returns>
    private static int RunRetryWithBackoff(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.RetryWithBackoff(
                ArraySource(library),
                1,
                TimeSpan.Zero,
                1.0,
                TimeSpan.Zero,
                Sequencer.Immediate)
            : PackageExtensions.RetryWithBackoff(
                ArraySource(library),
                1,
                TimeSpan.Zero,
                1.0,
                TimeSpan.Zero,
                ImmediateScheduler.Instance));

    /// <summary>Executes the <c>RunRetryWithDelay</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunRetryWithDelay</c> result.</returns>
    private static int RunRetryWithDelay(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.RetryWithDelay(ArraySource(library), 1, static _ => TimeSpan.Zero)
            : PackageExtensions.RetryWithDelay(ArraySource(library), 1, static _ => TimeSpan.Zero));

    /// <summary>Executes the <c>RunRetryWithFixedDelay</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunRetryWithFixedDelay</c> result.</returns>
    private static int RunRetryWithFixedDelay(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.RetryWithFixedDelay(ArraySource(library), 1, TimeSpan.Zero)
            : PackageExtensions.RetryWithFixedDelay(ArraySource(library), 1, TimeSpan.Zero));

    /// <summary>Executes the <c>RunReturn</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunReturn</c> result.</returns>
    private static int RunReturn(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesObservables.Return(Value)
            : PackageObservables.Return(Value));

    /// <summary>Executes the <c>RunRunAll</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunRunAll</c> result.</returns>
    private static int RunRunAll(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainPrimitiveUnit(PrimitivesExtensions.RunAll([PrimitivesObservables.Return(RxVoid.Default)]))
            : DrainPackageUnit(PackageExtensions.RunAll([PackageObservables.Return(RxUnit.Default)]));

    /// <summary>Executes the <c>RunSampleLatest</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunSampleLatest</c> result.</returns>
    private static int RunSampleLatest(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SampleLatest(
                ArraySource(library),
                PrimitivesExtensions.SelectConstant(ArraySource(library), new object()))
            : PackageExtensions.SampleLatest(
                ArraySource(library),
                PackageExtensions.SelectConstant(ArraySource(library), new object())));

    /// <summary>Executes the <c>RunScanWithInitial</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunScanWithInitial</c> result.</returns>
    private static int RunScanWithInitial(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ScanWithInitial(ArraySource(library), 0, static (acc, value) => acc + value)
            : PackageExtensions.ScanWithInitial(ArraySource(library), 0, static (acc, value) => acc + value));

    /// <summary>Executes the <c>RunSchedule</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunSchedule</c> result.</returns>
    private static int RunSchedule(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.Schedule(Value, Sequencer.Immediate, static value => value + 1)
            : PackageExtensions.Schedule(Value, ImmediateScheduler.Instance, static value => value + 1));

    /// <summary>Executes the <c>RunScheduleSafe</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunScheduleSafe</c> result.</returns>
    private static int RunScheduleSafe(ExtensionsLibrary library)
    {
        var count = 0;
        using var scheduled = library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ScheduleSafe(Sequencer.Immediate, () => count++)
            : PackageExtensions.ScheduleSafe(ImmediateScheduler.Instance, () => count++);
        return count;
    }

    /// <summary>Executes the <c>RunSelectAsyncScenario</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunSelectAsyncScenario</c> result.</returns>
    private static int RunSelectAsyncScenario(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SelectAsync(ArraySource(library), static value => Task.FromResult(value + 1))
            : PackageExtensions.SelectAsync(ArraySource(library), static value => Task.FromResult(value + 1)));

    /// <summary>Executes the <c>RunSelectAsyncConcurrent</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunSelectAsyncConcurrent</c> result.</returns>
    private static int RunSelectAsyncConcurrent(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SelectAsyncConcurrent(
                ArraySource(library),
                static value => Task.FromResult(value + 1),
                MaxConcurrency)
            : PackageExtensions.SelectAsyncConcurrent(
                ArraySource(library),
                static value => Task.FromResult(value + 1),
                MaxConcurrency));

    /// <summary>Executes the <c>RunSelectAsyncSequential</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunSelectAsyncSequential</c> result.</returns>
    private static int RunSelectAsyncSequential(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SelectAsyncSequential(
                ArraySource(library),
                static value => Task.FromResult(value + 1))
            : PackageExtensions.SelectAsyncSequential(
                ArraySource(library),
                static value => Task.FromResult(value + 1)));

    /// <summary>Executes the <c>RunSelectConstant</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunSelectConstant</c> result.</returns>
    private static int RunSelectConstant(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SelectConstant(ArraySource(library), Value)
            : PackageExtensions.SelectConstant(ArraySource(library), Value));

    /// <summary>Executes the <c>RunSelectLatestAsyncScenario</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunSelectLatestAsyncScenario</c> result.</returns>
    private static int RunSelectLatestAsyncScenario(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SelectLatestAsync(ArraySource(library), static value => Task.FromResult(value + 1))
            : PackageExtensions.SelectLatestAsync(ArraySource(library), static value => Task.FromResult(value + 1)));

    /// <summary>Executes the <c>RunSelectManyThen</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunSelectManyThen</c> result.</returns>
    private static int RunSelectManyThen(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SelectManyThen(
                PrimitivesObservables.Return(Value),
                static value => PrimitivesObservables.Return(value + 1),
                static value => PrimitivesObservables.Return(value + 1))
            : PackageExtensions.SelectManyThen(
                PackageObservables.Return(Value),
                static value => PackageObservables.Return(value + 1),
                static value => PackageObservables.Return(value + 1)));

    /// <summary>Executes the <c>RunShuffle</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunShuffle</c> result.</returns>
    private static int RunShuffle(ExtensionsLibrary library) =>
        DrainArray(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.Shuffle(PrimitivesObservables.Return(Values))
            : PackageExtensions.Shuffle(PackageObservables.Return(Values)));

    /// <summary>Executes the <c>RunSkipWhileNull</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunSkipWhileNull</c> result.</returns>
    private static int RunSkipWhileNull(ExtensionsLibrary library) =>
        DrainString(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SkipWhileNull(PrimitivesExtensions.FromArray(SkipStrings))
            : PackageExtensions.SkipWhileNull(PackageExtensions.FromArray(SkipStrings)));

    /// <summary>Executes the <c>RunStart</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunStart</c> result.</returns>
    private static int RunStart(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainPrimitiveUnit(PrimitivesExtensions.Start(static () => { }, Sequencer.Immediate))
            : DrainPackageUnit(PackageExtensions.Start(static () => { }, ImmediateScheduler.Instance));

    /// <summary>Executes the <c>RunSubscribeAndComplete</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunSubscribeAndComplete</c> result.</returns>
    private static int RunSubscribeAndComplete(ExtensionsLibrary library)
    {
        if (library == ExtensionsLibrary.Primitives)
        {
            PrimitivesSubscriptionExtensions.SubscribeAndComplete(PrimitivesObservables.Return(RxVoid.Default));
        }
        else
        {
            PackageSubscriptionExtensions.SubscribeAndComplete(PackageObservables.Return(RxUnit.Default));
        }

        return 1;
    }

    /// <summary>Executes the <c>RunSubscribeAsyncScenario</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunSubscribeAsyncScenario</c> result.</returns>
    private static int RunSubscribeAsyncScenario(ExtensionsLibrary library)
    {
        var total = 0;
        using var subscription = library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SubscribeAsync(ArraySource(library), value =>
            {
                total += value;
                return default;
            })
            : PackageExtensions.SubscribeAsync(ArraySource(library), value =>
            {
                total += value;
                return default;
            });
        return total;
    }

    /// <summary>Executes the <c>RunSubscribeGetError</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunSubscribeGetError</c> result.</returns>
    private static int RunSubscribeGetError(ExtensionsLibrary library)
    {
        var error = library == ExtensionsLibrary.Primitives
            ? PrimitivesSubscriptionExtensions.SubscribeGetError(ThrowInt(library))
            : PackageSubscriptionExtensions.SubscribeGetError(ThrowInt(library));

        return error is null ? 0 : 1;
    }

    /// <summary>Executes the <c>RunSubscribeGetValue</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunSubscribeGetValue</c> result.</returns>
    private static int RunSubscribeGetValue(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? PrimitivesSubscriptionExtensions.SubscribeGetValue(ArraySource(library))
            : PackageSubscriptionExtensions.SubscribeGetValue(ArraySource(library));

    /// <summary>Executes the <c>RunSubscribeSynchronous</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunSubscribeSynchronous</c> result.</returns>
    private static int RunSubscribeSynchronous(ExtensionsLibrary library)
    {
        var total = 0;
        using var subscription = library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SubscribeSynchronous(ArraySource(library), value =>
            {
                total += value;
                return default;
            })
            : PackageExtensions.SubscribeSynchronous(ArraySource(library), value =>
            {
                total += value;
                return default;
            });
        return total;
    }

    /// <summary>Executes the <c>RunSwitchIfEmpty</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunSwitchIfEmpty</c> result.</returns>
    private static int RunSwitchIfEmpty(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SwitchIfEmpty(Signal.None<int>(), PrimitivesObservables.Return(Value))
            : PackageExtensions.SwitchIfEmpty(RxObservable.Empty<int>(), PackageObservables.Return(Value)));

    /// <summary>Executes the <c>RunSyncTimer</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunSyncTimer</c> result.</returns>
    private static int RunSyncTimer(ExtensionsLibrary library)
    {
        if (library == ExtensionsLibrary.Primitives)
        {
            VirtualClock clock = new();
            CountingSignalWitness<DateTime> observer = new();
            using var subscription = PrimitivesExtensions.SyncTimer(Tick, clock).Subscribe(observer);
            clock.AdvanceBy(Tick);
            return observer.Count + observer.CompletionCount;
        }

        HistoricalScheduler scheduler = new();
        CountingSignalWitness<DateTime> packageObserver = new();
        using var packageSubscription = PackageExtensions.SyncTimer(Tick, scheduler).Subscribe(packageObserver);
        scheduler.AdvanceBy(Tick);
        return packageObserver.Count + packageObserver.CompletionCount;
    }

    /// <summary>Executes the <c>RunSynchronizeAsyncScenario</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunSynchronizeAsyncScenario</c> result.</returns>
    private static int RunSynchronizeAsyncScenario(ExtensionsLibrary library) =>
        DrainSyncTuple(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SynchronizeAsync(ArraySource(library))
            : PackageExtensions.SynchronizeAsync(ArraySource(library)));

    /// <summary>Executes the <c>RunSynchronizeSynchronous</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunSynchronizeSynchronous</c> result.</returns>
    private static int RunSynchronizeSynchronous(ExtensionsLibrary library) =>
        DrainSyncTuple(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SynchronizeSynchronous(ArraySource(library))
            : PackageExtensions.SynchronizeSynchronous(ArraySource(library)));

    /// <summary>Executes the <c>RunTakeUntil</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunTakeUntil</c> result.</returns>
    private static int RunTakeUntil(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.TakeUntil(ArraySource(library), static value => value == Match)
            : PackageExtensions.TakeUntil(ArraySource(library), static value => value == Match));

    /// <summary>Executes the <c>RunThrottleDistinct</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunThrottleDistinct</c> result.</returns>
    private static int RunThrottleDistinct(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ThrottleDistinct(ArraySource(library), TimeSpan.Zero, Sequencer.Immediate)
            : PackageExtensions.ThrottleDistinct(ArraySource(library), TimeSpan.Zero, ImmediateScheduler.Instance));

    /// <summary>Executes the <c>RunThrottleFirst</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunThrottleFirst</c> result.</returns>
    private static int RunThrottleFirst(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ThrottleFirst(ArraySource(library), TimeSpan.Zero, Sequencer.Immediate)
            : PackageExtensions.ThrottleFirst(ArraySource(library), TimeSpan.Zero, ImmediateScheduler.Instance));

    /// <summary>Executes the <c>RunThrottleOnScheduler</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunThrottleOnScheduler</c> result.</returns>
    private static int RunThrottleOnScheduler(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ThrottleOnScheduler(ArraySource(library), TimeSpan.Zero, Sequencer.Immediate)
            : PackageExtensions.ThrottleOnScheduler(ArraySource(library), TimeSpan.Zero, ImmediateScheduler.Instance));

    /// <summary>Executes the <c>RunThrottleUntilTrue</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunThrottleUntilTrue</c> result.</returns>
    private static int RunThrottleUntilTrue(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ThrottleUntilTrue(
                ArraySource(library),
                TimeSpan.Zero,
                static value => value >= Match)
            : PackageExtensions.ThrottleUntilTrue(
                ArraySource(library),
                TimeSpan.Zero,
                static value => value >= Match));

    /// <summary>Executes the <c>RunToHotTask</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunToHotTask</c> result.</returns>
    private static int RunToHotTask(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? GetCompletedResult(PrimitivesExtensions.ToHotTask(PrimitivesObservables.Return(Value)))
            : GetCompletedResult(PackageExtensions.ToHotTask(PackageObservables.Return(Value)));

    /// <summary>Executes the <c>RunToHotValueTask</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunToHotValueTask</c> result.</returns>
    private static int RunToHotValueTask(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? GetCompletedResult(PrimitivesExtensions.ToHotValueTask(PrimitivesObservables.Return(Value)))
            : GetCompletedResult(PackageExtensions.ToHotValueTask(PackageObservables.Return(Value)));

    /// <summary>Executes the <c>RunToPropertyObservable</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunToPropertyObservable</c> result.</returns>
    private static int RunToPropertyObservable(ExtensionsLibrary library)
    {
        PropertySource source = new();
        IntSignalWitness observer = new();
        using var subscription = (
                library == ExtensionsLibrary.Primitives
                    ? PrimitivesExtensions.ToPropertyObservable(source, static item => item.CurrentValue)
                    : PackageExtensions.ToPropertyObservable(source, static item => item.CurrentValue))
            .Subscribe(observer);
        source.CurrentValue = Value;
        return observer.Total;
    }

    /// <summary>Executes the <c>RunToReadOnlyBehavior</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunToReadOnlyBehavior</c> result.</returns>
    private static int RunToReadOnlyBehavior(ExtensionsLibrary library)
    {
        IntSignalWitness observer = new();
        if (library == ExtensionsLibrary.Primitives)
        {
            var (observable, sink) = PrimitivesExtensions.ToReadOnlyBehavior(Value);
            using var subscription = observable.Subscribe(observer);
            sink.OnNext(Value + 1);
        }
        else
        {
            var (observable, sink) = PackageExtensions.ToReadOnlyBehavior(Value);
            using var subscription = observable.Subscribe(observer);
            sink.OnNext(Value + 1);
        }

        return observer.Total;
    }

    /// <summary>Executes the <c>RunTrySelect</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunTrySelect</c> result.</returns>
    private static int RunTrySelect(ExtensionsLibrary library) =>
        DrainString(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.TrySelect(
                ArraySource(library),
                static value => value % EvenDivisor == 0 ? value.ToString(CultureInfo.InvariantCulture) : null)
            : PackageExtensions.TrySelect(
                ArraySource(library),
                static value => value % EvenDivisor == 0 ? value.ToString(CultureInfo.InvariantCulture) : null));

    /// <summary>Executes the <c>RunUsing</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunUsing</c> result.</returns>
    private static int RunUsing(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainPrimitiveUnit(PrimitivesExtensions.Using(new DummyResource(), static resource => resource.Touch()))
            : DrainPackageUnit(PackageExtensions.Using(new DummyResource(), static resource => resource.Touch()));

    /// <summary>Executes the <c>RunWaitForCompletion</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunWaitForCompletion</c> result.</returns>
    private static int RunWaitForCompletion(ExtensionsLibrary library)
    {
        if (library == ExtensionsLibrary.Primitives)
        {
            PrimitivesSubscriptionExtensions.WaitForCompletion(
                PrimitivesObservables.Return(RxVoid.Default),
                WaitTimeout);
        }
        else
        {
            PackageSubscriptionExtensions.WaitForCompletion(PackageObservables.Return(RxUnit.Default), WaitTimeout);
        }

        return 1;
    }

    /// <summary>Executes the <c>RunWaitForError</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunWaitForError</c> result.</returns>
    private static int RunWaitForError(ExtensionsLibrary library)
    {
        var error = library == ExtensionsLibrary.Primitives
            ? PrimitivesSubscriptionExtensions.WaitForError(ThrowInt(library), WaitTimeout)
            : PackageSubscriptionExtensions.WaitForError(ThrowInt(library), WaitTimeout);

        return error is null ? 0 : 1;
    }

    /// <summary>Executes the <c>RunWaitForValue</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunWaitForValue</c> result.</returns>
    private static int RunWaitForValue(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? PrimitivesSubscriptionExtensions.WaitForValue(ArraySource(library), WaitTimeout)
            : PackageSubscriptionExtensions.WaitForValue(ArraySource(library), WaitTimeout);

    /// <summary>Executes the <c>RunWaitUntil</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunWaitUntil</c> result.</returns>
    private static int RunWaitUntil(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.WaitUntil(ArraySource(library), static value => value == Match)
            : PackageExtensions.WaitUntil(ArraySource(library), static value => value == Match));

    /// <summary>Executes the <c>RunWhereFalse</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunWhereFalse</c> result.</returns>
    private static int RunWhereFalse(ExtensionsLibrary library) =>
        DrainBool(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.WhereFalse(BoolSource(library))
            : PackageExtensions.WhereFalse(BoolSource(library)));

    /// <summary>Executes the <c>RunWhereIsNotNull</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunWhereIsNotNull</c> result.</returns>
    private static int RunWhereIsNotNull(ExtensionsLibrary library) =>
        DrainString(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.WhereIsNotNull(PrimitivesExtensions.FromArray(NullableStrings))
            : PackageExtensions.WhereIsNotNull(PackageExtensions.FromArray(NullableStrings)));

    /// <summary>Executes the <c>RunWhereSelect</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunWhereSelect</c> result.</returns>
    private static int RunWhereSelect(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.WhereSelect(
                ArraySource(library),
                static value => (value & 1) == 0,
                static value => value * ResultMultiplier)
            : PackageExtensions.WhereSelect(
                ArraySource(library),
                static value => (value & 1) == 0,
                static value => value * ResultMultiplier));

    /// <summary>Executes the <c>RunWhereTrue</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunWhereTrue</c> result.</returns>
    private static int RunWhereTrue(ExtensionsLibrary library) =>
        DrainBool(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.WhereTrue(BoolSource(library))
            : PackageExtensions.WhereTrue(BoolSource(library)));

    /// <summary>Executes the <c>RunWhile</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunWhile</c> result.</returns>
    private static int RunWhile(ExtensionsLibrary library)
    {
        var remaining = Count;
        var total = 0;

        bool ShouldContinue()
        {
            var hasRemaining = remaining > 0;
            remaining--;
            return hasRemaining;
        }

        void RecordIteration() => total++;

        return library == ExtensionsLibrary.Primitives
            ? DrainPrimitiveUnit(PrimitivesExtensions.While(ShouldContinue, RecordIteration)) + total
            : DrainPackageUnit(PackageExtensions.While(ShouldContinue, RecordIteration)) + total;
    }

    /// <summary>Executes the <c>RunWithLimitedConcurrency</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunWithLimitedConcurrency</c> result.</returns>
    private static int RunWithLimitedConcurrency(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.WithLimitedConcurrency(CompletedTasks(), MaxConcurrency)
            : PackageExtensions.WithLimitedConcurrency(CompletedTasks(), MaxConcurrency));
}
