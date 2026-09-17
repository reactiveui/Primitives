// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions;
using ReactiveUI.Primitives.Signals;
using PackageContinuation = ReactiveUI.Extensions.Continuation;
using PackageExtensions = ReactiveUI.Extensions.ReactiveExtensions;
using PackageObservables = ReactiveUI.Extensions.Observables;
using PackageObserverExtensions = ReactiveUI.Extensions.ObserverExtensions;
using PrimitivesContinuation = ReactiveUI.Primitives.Extensions.Continuation;
using PrimitivesExtensions = ReactiveUI.Primitives.Extensions.ReactiveExtensions;
using PrimitivesObservables = ReactiveUI.Primitives.Extensions.Observables;
using PrimitivesObserverExtensions = ReactiveUI.Primitives.Extensions.ObserverExtensions;
using RxObservable = System.Reactive.Linq.Observable;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks the complete synchronous ReactiveUI.Primitives.Extensions public helper surface.</summary>
public partial class ReactiveExtensionsComparisonBenchmarks
{
    /// <summary>Projects the selected library's range to unit values through AsSignal.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunAsSignal(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainPrimitiveUnit(PrimitivesExtensions.AsSignal(Range(library)))
            : DrainPackageUnit(PackageExtensions.AsSignal(Range(library)));

    /// <summary>Buffers the character source between bracket delimiters.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunBufferUntil(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainString(PrimitivesExtensions.BufferUntil(PrimitivesExtensions.FromArray(BufferCharacters), '[', ']'))
            : DrainString(PackageExtensions.BufferUntil(PackageExtensions.FromArray(BufferCharacters), '[', ']'));

    /// <summary>Buffers the array source until it falls idle, on an immediate scheduler.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunBufferUntilIdle(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainList(PrimitivesExtensions.BufferUntilIdle(ArraySource(library), TimeSpan.Zero, Sequencer.Immediate))
            : DrainList(PackageExtensions.BufferUntilIdle(
                ArraySource(library),
                TimeSpan.Zero,
                ImmediateScheduler.Instance));

    /// <summary>Buffers the array source until it falls inactive, on an immediate scheduler.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunBufferUntilInactive(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainList(
                PrimitivesExtensions.BufferUntilInactive(ArraySource(library), TimeSpan.Zero, Sequencer.Immediate))
            : DrainList(PackageExtensions.BufferUntilInactive(
                ArraySource(library),
                TimeSpan.Zero,
                ImmediateScheduler.Instance));

    /// <summary>Substitutes a computed fallback for a typed failure through CatchAndReturn.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunCatchAndReturn(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.CatchAndReturn<int, InvalidOperationException>(
                ThrowInt(library),
                static _ => Fallback)
            : PackageExtensions.CatchAndReturn<int, InvalidOperationException>(
                ThrowInt(library),
                static _ => Fallback));

    /// <summary>Swallows a typed failure through CatchIgnore.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunCatchIgnore(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.CatchIgnore<int, InvalidOperationException>(ThrowInt(library), static _ => { })
            : PackageExtensions.CatchIgnore<int, InvalidOperationException>(ThrowInt(library), static _ => { }));

    /// <summary>Substitutes a constant fallback for a failure through CatchReturn.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunCatchReturn(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.CatchReturn(ThrowInt(library), Fallback)
            : PackageExtensions.CatchReturn(ThrowInt(library), Fallback));

    /// <summary>Substitutes a unit value for a failing unit source.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunCatchReturnUnit(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainPrimitiveUnit(PrimitivesExtensions.CatchReturnUnit(ThrowPrimitiveUnit()))
            : DrainPackageUnit(PackageExtensions.CatchReturnUnit(ThrowPackageUnit()));

    /// <summary>Combines two false sources and tests that every value is false.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunCombineLatestValuesAreAllFalse(ExtensionsLibrary library) =>
        DrainBool(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.CombineLatestValuesAreAllFalse(BoolSources(library, false))
            : PackageExtensions.CombineLatestValuesAreAllFalse(BoolSources(library, false)));

    /// <summary>Combines two true sources and tests that every value is true.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunCombineLatestValuesAreAllTrue(ExtensionsLibrary library) =>
        DrainBool(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.CombineLatestValuesAreAllTrue(BoolSources(library, true))
            : PackageExtensions.CombineLatestValuesAreAllTrue(BoolSources(library, true)));

    /// <summary>Conflates the array source over a zero window on an immediate scheduler.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunConflate(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.Conflate(ArraySource(library), TimeSpan.Zero, Sequencer.Immediate)
            : PackageExtensions.Conflate(ArraySource(library), TimeSpan.Zero, ImmediateScheduler.Instance));

    /// <summary>Constructs and disposes a continuation, reading its completed phase count.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunContinuationDispose(ExtensionsLibrary library)
    {
        if (library == ExtensionsLibrary.Primitives)
        {
            using PrimitivesContinuation continuation = new();
            return (int)continuation.CompletedPhases;
        }

        using PackageContinuation packageContinuation = new();
        return (int)packageContinuation.CompletedPhases;
    }

    /// <summary>Acquires a continuation lock and observes the task-based handle.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunContinuationLock(ExtensionsLibrary library)
    {
        TupleWitness<int> observer = new();
        if (library == ExtensionsLibrary.Primitives)
        {
            PrimitivesContinuation continuation = new();
            var task = continuation.Lock(Value, observer);
            EnsureCompleted(task);
            return observer.ItemCount;
        }

        PackageContinuation packageContinuation = new();
        var packageTask = packageContinuation.Lock(Value, observer);
        EnsureCompleted(packageTask);
        return observer.ItemCount;
    }

    /// <summary>Acquires a continuation lock and observes the value-task handle.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunContinuationLockValueTask(ExtensionsLibrary library)
    {
        TupleWitness<int> observer = new();
        if (library == ExtensionsLibrary.Primitives)
        {
            PrimitivesContinuation continuation = new();
            var task = continuation.LockValueTask(Value, observer);
            EnsureCompleted(task);
            return observer.ItemCount;
        }

        PackageContinuation packageContinuation = new();
        var packageTask = packageContinuation.LockValueTask(Value, observer);
        EnsureCompleted(packageTask);
        return observer.ItemCount;
    }

    /// <summary>Debounces the array source, emitting the leading value of each window.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunDebounceImmediate(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.DebounceImmediate(ArraySource(library), TimeSpan.Zero, Sequencer.Immediate)
            : PackageExtensions.DebounceImmediate(ArraySource(library), TimeSpan.Zero, ImmediateScheduler.Instance));

    /// <summary>Debounces the array source until a value clears the match threshold.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunDebounceUntil(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.DebounceUntil(
                ArraySource(library),
                TimeSpan.Zero,
                static value => value >= Match,
                Sequencer.Immediate)
            : PackageExtensions.DebounceUntil(
                ArraySource(library),
                TimeSpan.Zero,
                static value => value >= Match,
                ImmediateScheduler.Instance));

    /// <summary>Advances virtual time over a silent source to emit a stale marker.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunDetectStale(ExtensionsLibrary library)
    {
        if (library == ExtensionsLibrary.Primitives)
        {
            VirtualClock clock = new();
            CountingSignalWitness<Stale<int>> observer = new();
            using var subscription =
                PrimitivesExtensions.DetectStale(Signal.Silent<int>(), Tick, clock).Subscribe(observer);
            clock.AdvanceBy(Tick);
            return observer.Count + observer.CompletionCount;
        }

        HistoricalScheduler scheduler = new();
        CountingSignalWitness<ReactiveUI.Extensions.Stale<int>> packageObserver = new();
        using var packageSubscription = PackageExtensions.DetectStale(RxObservable.Never<int>(), Tick, scheduler)
            .Subscribe(packageObserver);
        scheduler.AdvanceBy(Tick);
        return packageObserver.Count + packageObserver.CompletionCount;
    }

    /// <summary>Counts the dispose callback raised when the subscription is released.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunDoOnDispose(ExtensionsLibrary library)
    {
        var count = 0;
        using var subscription = (
                library == ExtensionsLibrary.Primitives
                    ? PrimitivesExtensions.DoOnDispose(ArraySource(library), () => count++)
                    : PackageExtensions.DoOnDispose(ArraySource(library), () => count++))
            .Subscribe(new IntSignalWitness());
        return count;
    }

    /// <summary>Counts the subscribe callback raised when the source is subscribed.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunDoOnSubscribe(ExtensionsLibrary library)
    {
        var count = 0;
        var total = DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.DoOnSubscribe(ArraySource(library), () => count++)
            : PackageExtensions.DoOnSubscribe(ArraySource(library), () => count++));
        return total + count;
    }

    /// <summary>Drops values from the array source while the handler is busy.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunDropIfBusy(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.DropIfBusy(ArraySource(library), static _ => default)
            : PackageExtensions.DropIfBusy(ArraySource(library), static _ => default));

    /// <summary>Pushes the shared array into an observer through FastForEach.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunFastForEach(ExtensionsLibrary library)
    {
        IntSignalWitness observer = new();
        if (library == ExtensionsLibrary.Primitives)
        {
            PrimitivesObserverExtensions.FastForEach(observer, Values);
        }
        else
        {
            PackageObserverExtensions.FastForEach(observer, Values);
        }

        return observer.Total;
    }

    /// <summary>Filters the string source by the even-digit regex.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunFilter(ExtensionsLibrary library) =>
        DrainString(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.Filter(PrimitivesExtensions.FromArray(StringValues), EvenRegex())
            : PackageExtensions.Filter(PackageExtensions.FromArray(StringValues), EvenRegex()));

    /// <summary>Probes candidate values and takes the first that clears the match threshold.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunFirstMatchFromCandidates(ExtensionsLibrary library)
    {
        var candidates = Values;
        return DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.FirstMatchFromCandidates(
                candidates,
                PrimitivesObservables.Return,
                static value => value * CandidateMultiplier,
                static value => value >= Match,
                Fallback)
            : PackageExtensions.FirstMatchFromCandidates(
                candidates,
                PackageObservables.Return,
                static value => value * CandidateMultiplier,
                static value => value >= Match,
                Fallback));
    }

    /// <summary>Flattens a single batch of values through ForEach.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunForEach(ExtensionsLibrary library)
    {
        int[][] batches = [Values];
        return DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ForEach(PrimitivesExtensions.FromArray<IEnumerable<int>>(batches), null)
            : PackageExtensions.ForEach(PackageExtensions.FromArray<IEnumerable<int>>(batches), null));
    }

    /// <summary>Drains the shared int array through the library's FromArray.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunFromArray(ExtensionsLibrary library) => DrainInt(ArraySource(library));

    /// <summary>Combines two scalar sources into their maximum.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunGetMax(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.GetMax(
                PrimitivesObservables.Return(FirstValue),
                PrimitivesObservables.Return(SecondValue))
            : PackageExtensions.GetMax(
                PackageObservables.Return(FirstValue),
                PackageObservables.Return(SecondValue)));

    /// <summary>Combines two scalar sources into their minimum.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunGetMin(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.GetMin(
                PrimitivesObservables.Return(FirstValue),
                PrimitivesObservables.Return(SecondValue))
            : PackageExtensions.GetMin(
                PackageObservables.Return(FirstValue),
                PackageObservables.Return(SecondValue)));

    /// <summary>Advances virtual time over a silent source to emit a heartbeat.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunHeartbeat(ExtensionsLibrary library)
    {
        if (library == ExtensionsLibrary.Primitives)
        {
            VirtualClock clock = new();
            CountingSignalWitness<Heartbeat<int>> observer = new();
            using var subscription =
                PrimitivesExtensions.Heartbeat(Signal.Silent<int>(), Tick, clock).Subscribe(observer);
            clock.AdvanceBy(Tick);
            return observer.Count + observer.CompletionCount;
        }

        HistoricalScheduler scheduler = new();
        CountingSignalWitness<ReactiveUI.Extensions.Heartbeat<int>> packageObserver = new();
        using var packageSubscription = PackageExtensions.Heartbeat(RxObservable.Never<int>(), Tick, scheduler)
            .Subscribe(packageObserver);
        scheduler.AdvanceBy(Tick);
        return packageObserver.Count + packageObserver.CompletionCount;
    }

    /// <summary>Reads the latest value of the array source, falling back when it has none.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunLatestOrDefault(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.LatestOrDefault(ArraySource(library), Fallback)
            : PackageExtensions.LatestOrDefault(ArraySource(library), Fallback));

    /// <summary>Wraps the array source in the error-logging operator and drains it.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunLogErrors(ExtensionsLibrary library)
    {
        var errors = 0;
        var total = DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.LogErrors(ArraySource(library), _ => errors++)
            : PackageExtensions.LogErrors(ArraySource(library), _ => errors++));
        return total + errors;
    }

    /// <summary>Negates each boolean emitted by the boolean source.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunNot(ExtensionsLibrary library) =>
        DrainBool(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.Not(BoolSource(library))
            : PackageExtensions.Not(BoolSource(library)));

    /// <summary>Conditionally reschedules the array source onto an immediate scheduler.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunObserveOnIf(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ObserveOnIf(ArraySource(library), true, Sequencer.Immediate)
            : PackageExtensions.ObserveOnIf(ArraySource(library), true, ImmediateScheduler.Instance));

    /// <summary>Reschedules the array source onto an immediate scheduler with error isolation.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunObserveOnSafe(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ObserveOnSafe(ArraySource(library), Sequencer.Immediate)
            : PackageExtensions.ObserveOnSafe(ArraySource(library), ImmediateScheduler.Instance));

    /// <summary>Retries the array source once on a typed failure with no delay.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RunOnErrorRetry(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.OnErrorRetry<int, InvalidOperationException>(
                ArraySource(library),
                static _ => { },
                1,
                TimeSpan.Zero,
                Sequencer.Immediate)
            : PackageExtensions.OnErrorRetry<int, InvalidOperationException>(
                ArraySource(library),
                static _ => { },
                1,
                TimeSpan.Zero,
                ImmediateScheduler.Instance));

    /// <summary>Pushes the shared array into an observer one value at a time.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunOnNext(ExtensionsLibrary library)
    {
        IntSignalWitness observer = new();
        if (library == ExtensionsLibrary.Primitives)
        {
            PrimitivesExtensions.OnNext(observer, Values);
        }
        else
        {
            PackageExtensions.OnNext(observer, Values);
        }

        return observer.Total;
    }

    /// <summary>Pairs each value of the array source with its predecessor.</summary>
    /// <param name="library">The library implementation to exercise.</param>
    /// <returns>The scenario checksum.</returns>
    private static int RunPairwise(ExtensionsLibrary library)
    {
        PairWitness observer = new();
        using var subscription = (
                library == ExtensionsLibrary.Primitives
                    ? PrimitivesExtensions.Pairwise(ArraySource(library))
                    : PackageExtensions.Pairwise(ArraySource(library)))
            .Subscribe(observer);
        return observer.Total;
    }
}
