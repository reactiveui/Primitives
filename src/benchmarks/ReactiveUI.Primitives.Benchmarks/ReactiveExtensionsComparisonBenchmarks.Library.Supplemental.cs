// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Reactive.Concurrency;
using System.Runtime.CompilerServices;
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
    /// <summary>Splits the array source into even and odd halves and drains both.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
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

    /// <summary>Replays the last value of the array source to a late subscriber.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunReplayLastOnSubscribe(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ReplayLastOnSubscribe(ArraySource(library), Fallback)
            : PackageExtensions.ReplayLastOnSubscribe(ArraySource(library), Fallback));

    /// <summary>Retries the array source indefinitely with no delay between attempts.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunRetryForeverWithDelay(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.RetryForeverWithDelay(ArraySource(library), TimeSpan.Zero)
            : PackageExtensions.RetryForeverWithDelay(ArraySource(library), TimeSpan.Zero));

    /// <summary>Retries the array source once with a zero-length backoff.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

    /// <summary>Retries the array source once with a computed zero delay.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunRetryWithDelay(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.RetryWithDelay(ArraySource(library), 1, static _ => TimeSpan.Zero)
            : PackageExtensions.RetryWithDelay(ArraySource(library), 1, static _ => TimeSpan.Zero));

    /// <summary>Retries the array source once with a fixed zero delay.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunRetryWithFixedDelay(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.RetryWithFixedDelay(ArraySource(library), 1, TimeSpan.Zero)
            : PackageExtensions.RetryWithFixedDelay(ArraySource(library), 1, TimeSpan.Zero));

    /// <summary>Drains a single-value source.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunReturn(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesObservables.Return(Value)
            : PackageObservables.Return(Value));

    /// <summary>Runs a single unit source to completion through RunAll.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunRunAll(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainPrimitiveUnit(PrimitivesExtensions.RunAll([PrimitivesObservables.Return(RxVoid.Default)]))
            : DrainPackageUnit(PackageExtensions.RunAll([PackageObservables.Return(RxUnit.Default)]));

    /// <summary>Samples the latest array value on each sampler notification.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunSampleLatest(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SampleLatest(
                ArraySource(library),
                PrimitivesExtensions.SelectConstant(ArraySource(library), new object()))
            : PackageExtensions.SampleLatest(
                ArraySource(library),
                PackageExtensions.SelectConstant(ArraySource(library), new object())));

    /// <summary>Accumulates the array source from a seed value.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunScanWithInitial(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ScanWithInitial(ArraySource(library), 0, static (acc, value) => acc + value)
            : PackageExtensions.ScanWithInitial(ArraySource(library), 0, static (acc, value) => acc + value));

    /// <summary>Schedules a projection of one value on an immediate scheduler.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunSchedule(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.Schedule(Value, Sequencer.Immediate, static value => value + 1)
            : PackageExtensions.Schedule(Value, ImmediateScheduler.Instance, static value => value + 1));

    /// <summary>Schedules an error-isolated callback on an immediate scheduler.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunScheduleSafe(ExtensionsLibrary library)
    {
        var count = 0;
        using var scheduled = library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ScheduleSafe(Sequencer.Immediate, () => count++)
            : PackageExtensions.ScheduleSafe(ImmediateScheduler.Instance, () => count++);
        return count;
    }

    /// <summary>Projects each array value through a completed task.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunSelectAsyncScenario(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SelectAsync(ArraySource(library), static value => Task.FromResult(value + 1))
            : PackageExtensions.SelectAsync(ArraySource(library), static value => Task.FromResult(value + 1)));

    /// <summary>Projects each array value through a completed task under a concurrency cap.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

    /// <summary>Projects each array value through a completed task, one at a time.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunSelectAsyncSequential(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SelectAsyncSequential(
                ArraySource(library),
                static value => Task.FromResult(value + 1))
            : PackageExtensions.SelectAsyncSequential(
                ArraySource(library),
                static value => Task.FromResult(value + 1)));

    /// <summary>Replaces every array value with a constant.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunSelectConstant(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SelectConstant(ArraySource(library), Value)
            : PackageExtensions.SelectConstant(ArraySource(library), Value));

    /// <summary>Projects array values through a completed task, keeping only the latest.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunSelectLatestAsyncScenario(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SelectLatestAsync(ArraySource(library), static value => Task.FromResult(value + 1))
            : PackageExtensions.SelectLatestAsync(ArraySource(library), static value => Task.FromResult(value + 1)));

    /// <summary>Chains two sequential projections from a single value.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

    /// <summary>Shuffles a single emitted array.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunShuffle(ExtensionsLibrary library) =>
        DrainArray(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.Shuffle(PrimitivesObservables.Return(Values))
            : PackageExtensions.Shuffle(PackageObservables.Return(Values)));

    /// <summary>Skips the leading nulls of the string source.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunSkipWhileNull(ExtensionsLibrary library) =>
        DrainString(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SkipWhileNull(PrimitivesExtensions.FromArray(SkipStrings))
            : PackageExtensions.SkipWhileNull(PackageExtensions.FromArray(SkipStrings)));

    /// <summary>Starts a void action on an immediate scheduler and drains its unit notification.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunStart(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainPrimitiveUnit(PrimitivesExtensions.Start(static () => { }, Sequencer.Immediate))
            : DrainPackageUnit(PackageExtensions.Start(static () => { }, ImmediateScheduler.Instance));

    /// <summary>Subscribes to a single-value unit source and waits for completion.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
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

    /// <summary>Sums the array source through an asynchronous subscription callback.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
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

    /// <summary>Subscribes to a failing source and reports whether an error surfaced.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunSubscribeGetError(ExtensionsLibrary library)
    {
        var error = library == ExtensionsLibrary.Primitives
            ? PrimitivesSubscriptionExtensions.SubscribeGetError(ThrowInt(library))
            : PackageSubscriptionExtensions.SubscribeGetError(ThrowInt(library));

        return error is null ? 0 : 1;
    }

    /// <summary>Subscribes to the array source and reads the single returned value.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunSubscribeGetValue(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? PrimitivesSubscriptionExtensions.SubscribeGetValue(ArraySource(library))
            : PackageSubscriptionExtensions.SubscribeGetValue(ArraySource(library));

    /// <summary>Sums the array source through a synchronous subscription callback.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
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

    /// <summary>Substitutes a single-value source when the source completes empty.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunSwitchIfEmpty(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SwitchIfEmpty(Signal.None<int>(), PrimitivesObservables.Return(Value))
            : PackageExtensions.SwitchIfEmpty(RxObservable.Empty<int>(), PackageObservables.Return(Value)));

    /// <summary>Advances virtual time once over a wall-clock timer.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
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

    /// <summary>Pairs each array value with an asynchronously acquired synchronization handle.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunSynchronizeAsyncScenario(ExtensionsLibrary library) =>
        DrainSyncTuple(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SynchronizeAsync(ArraySource(library))
            : PackageExtensions.SynchronizeAsync(ArraySource(library)));

    /// <summary>Pairs each array value with a synchronously acquired synchronization handle.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunSynchronizeSynchronous(ExtensionsLibrary library) =>
        DrainSyncTuple(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.SynchronizeSynchronous(ArraySource(library))
            : PackageExtensions.SynchronizeSynchronous(ArraySource(library)));

    /// <summary>Truncates the array source at the match threshold.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunTakeUntil(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.TakeUntil(ArraySource(library), static value => value == Match)
            : PackageExtensions.TakeUntil(ArraySource(library), static value => value == Match));

    /// <summary>Throttles the array source, suppressing repeats inside the window.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunThrottleDistinct(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ThrottleDistinct(ArraySource(library), TimeSpan.Zero, Sequencer.Immediate)
            : PackageExtensions.ThrottleDistinct(ArraySource(library), TimeSpan.Zero, ImmediateScheduler.Instance));

    /// <summary>Throttles the array source, keeping the leading value of each window.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunThrottleFirst(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ThrottleFirst(ArraySource(library), TimeSpan.Zero, Sequencer.Immediate)
            : PackageExtensions.ThrottleFirst(ArraySource(library), TimeSpan.Zero, ImmediateScheduler.Instance));

    /// <summary>Throttles the array source on an immediate scheduler.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunThrottleOnScheduler(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ThrottleOnScheduler(ArraySource(library), TimeSpan.Zero, Sequencer.Immediate)
            : PackageExtensions.ThrottleOnScheduler(ArraySource(library), TimeSpan.Zero, ImmediateScheduler.Instance));

    /// <summary>Throttles the array source until a value clears the match threshold.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

    /// <summary>Converts a single-value source to an eagerly started task and waits for it.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunToHotTask(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? GetCompletedResult(PrimitivesExtensions.ToHotTask(PrimitivesObservables.Return(Value)))
            : GetCompletedResult(PackageExtensions.ToHotTask(PackageObservables.Return(Value)));

    /// <summary>Converts a single-value source to an eagerly started value task and waits for it.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunToHotValueTask(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? GetCompletedResult(PrimitivesExtensions.ToHotValueTask(PrimitivesObservables.Return(Value)))
            : GetCompletedResult(PackageExtensions.ToHotValueTask(PackageObservables.Return(Value)));

    /// <summary>Bridges a property-changed notification into an observable.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
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

    /// <summary>Pushes a value through a read-only behavior and its paired sink.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
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

    /// <summary>Projects even array values to strings and drops the rest.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunTrySelect(ExtensionsLibrary library) =>
        DrainString(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.TrySelect(
                ArraySource(library),
                static value => value % EvenDivisor == 0 ? value.ToString(CultureInfo.InvariantCulture) : null)
            : PackageExtensions.TrySelect(
                ArraySource(library),
                static value => value % EvenDivisor == 0 ? value.ToString(CultureInfo.InvariantCulture) : null));

    /// <summary>Scopes a disposable resource around a unit source.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunUsing(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainPrimitiveUnit(PrimitivesExtensions.Using(new DummyResource(), static resource => resource.Touch()))
            : DrainPackageUnit(PackageExtensions.Using(new DummyResource(), static resource => resource.Touch()));

    /// <summary>Blocks on a single-value unit source until it completes.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
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

    /// <summary>Blocks on a failing source and reports whether an error surfaced.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunWaitForError(ExtensionsLibrary library)
    {
        var error = library == ExtensionsLibrary.Primitives
            ? PrimitivesSubscriptionExtensions.WaitForError(ThrowInt(library), WaitTimeout)
            : PackageSubscriptionExtensions.WaitForError(ThrowInt(library), WaitTimeout);

        return error is null ? 0 : 1;
    }

    /// <summary>Blocks on the array source until its first value arrives.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunWaitForValue(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? PrimitivesSubscriptionExtensions.WaitForValue(ArraySource(library), WaitTimeout)
            : PackageSubscriptionExtensions.WaitForValue(ArraySource(library), WaitTimeout);

    /// <summary>Waits for the first array value that matches the threshold.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunWaitUntil(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.WaitUntil(ArraySource(library), static value => value == Match)
            : PackageExtensions.WaitUntil(ArraySource(library), static value => value == Match));

    /// <summary>Keeps only the false values of the boolean source.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunWhereFalse(ExtensionsLibrary library) =>
        DrainBool(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.WhereFalse(BoolSource(library))
            : PackageExtensions.WhereFalse(BoolSource(library)));

    /// <summary>Keeps only the non-null values of the string source.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunWhereIsNotNull(ExtensionsLibrary library) =>
        DrainString(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.WhereIsNotNull(PrimitivesExtensions.FromArray(NullableStrings))
            : PackageExtensions.WhereIsNotNull(PackageExtensions.FromArray(NullableStrings)));

    /// <summary>Filters the array source to even values and scales them.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

    /// <summary>Keeps only the true values of the boolean source.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunWhereTrue(ExtensionsLibrary library) =>
        DrainBool(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.WhereTrue(BoolSource(library))
            : PackageExtensions.WhereTrue(BoolSource(library)));

    /// <summary>Repeats a unit action while a counter remains positive.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
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

    /// <summary>Awaits pre-completed tasks under a concurrency cap.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunWithLimitedConcurrency(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.WithLimitedConcurrency(CompletedTasks(), MaxConcurrency)
            : PackageExtensions.WithLimitedConcurrency(CompletedTasks(), MaxConcurrency));
}
