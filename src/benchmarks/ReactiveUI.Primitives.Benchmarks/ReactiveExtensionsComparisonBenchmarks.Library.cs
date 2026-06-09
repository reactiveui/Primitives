// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;
using PackageContinuation = ReactiveUI.Extensions.Continuation;
using PackageExtensions = ReactiveUI.Extensions.ReactiveExtensions;
using PackageObservables = ReactiveUI.Extensions.Observables;
using PackageObserverExtensions = ReactiveUI.Extensions.ObserverExtensions;
using PackageSubscriptionExtensions = ReactiveUI.Extensions.ObservableSubscriptionExtensions;
using PrimitivesContinuation = ReactiveUI.Primitives.Extensions.Continuation;
using PrimitivesExtensions = ReactiveUI.Primitives.Extensions.ReactiveExtensions;
using PrimitivesObservables = ReactiveUI.Primitives.Extensions.Observables;
using PrimitivesObserverExtensions = ReactiveUI.Primitives.Extensions.ObserverExtensions;
using PrimitivesSubscriptionExtensions = ReactiveUI.Primitives.Extensions.ObservableSubscriptionExtensions;
using RxObservable = System.Reactive.Linq.Observable;
using RxUnit = System.Reactive.Unit;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Benchmarks the complete synchronous ReactiveUI.Primitives.Extensions public helper surface.</summary>
public partial class ReactiveExtensionsComparisonBenchmarks
{
    /// <summary>Executes the <c>RunAsSignal</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunAsSignal</c> result.</returns>
    private static int RunAsSignal(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainPrimitiveUnit(PrimitivesExtensions.AsSignal(Range(library)))
            : DrainPackageUnit(PackageExtensions.AsSignal(Range(library)));

    /// <summary>Executes the <c>RunBufferUntil</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunBufferUntil</c> result.</returns>
    private static int RunBufferUntil(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainString(PrimitivesExtensions.BufferUntil(PrimitivesExtensions.FromArray(BufferCharacters), '[', ']'))
            : DrainString(PackageExtensions.BufferUntil(PackageExtensions.FromArray(BufferCharacters), '[', ']'));

    /// <summary>Executes the <c>RunBufferUntilIdle</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunBufferUntilIdle</c> result.</returns>
    private static int RunBufferUntilIdle(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainList(PrimitivesExtensions.BufferUntilIdle(ArraySource(library), TimeSpan.Zero, Sequencer.Immediate))
            : DrainList(PackageExtensions.BufferUntilIdle(ArraySource(library), TimeSpan.Zero, ImmediateScheduler.Instance));

    /// <summary>Executes the <c>RunBufferUntilInactive</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunBufferUntilInactive</c> result.</returns>
    private static int RunBufferUntilInactive(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainList(PrimitivesExtensions.BufferUntilInactive(ArraySource(library), TimeSpan.Zero, Sequencer.Immediate))
            : DrainList(PackageExtensions.BufferUntilInactive(ArraySource(library), TimeSpan.Zero, ImmediateScheduler.Instance));

    /// <summary>Executes the <c>RunCatchAndReturn</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunCatchAndReturn</c> result.</returns>
    private static int RunCatchAndReturn(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.CatchAndReturn<int, InvalidOperationException>(ThrowInt(library), static _ => Fallback)
            : PackageExtensions.CatchAndReturn<int, InvalidOperationException>(ThrowInt(library), static _ => Fallback));

    /// <summary>Executes the <c>RunCatchIgnore</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunCatchIgnore</c> result.</returns>
    private static int RunCatchIgnore(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.CatchIgnore<int, InvalidOperationException>(ThrowInt(library), static _ => { })
            : PackageExtensions.CatchIgnore<int, InvalidOperationException>(ThrowInt(library), static _ => { }));

    /// <summary>Executes the <c>RunCatchReturn</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunCatchReturn</c> result.</returns>
    private static int RunCatchReturn(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.CatchReturn(ThrowInt(library), Fallback)
            : PackageExtensions.CatchReturn(ThrowInt(library), Fallback));

    /// <summary>Executes the <c>RunCatchReturnUnit</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunCatchReturnUnit</c> result.</returns>
    private static int RunCatchReturnUnit(ExtensionsLibrary library) =>
        library == ExtensionsLibrary.Primitives
            ? DrainPrimitiveUnit(PrimitivesExtensions.CatchReturnUnit(ThrowPrimitiveUnit()))
            : DrainPackageUnit(PackageExtensions.CatchReturnUnit(ThrowPackageUnit()));

    /// <summary>Executes the <c>RunCombineLatestValuesAreAllFalse</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunCombineLatestValuesAreAllFalse</c> result.</returns>
    private static int RunCombineLatestValuesAreAllFalse(ExtensionsLibrary library) =>
        DrainBool(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.CombineLatestValuesAreAllFalse(BoolSources(library, false))
            : PackageExtensions.CombineLatestValuesAreAllFalse(BoolSources(library, false)));

    /// <summary>Executes the <c>RunCombineLatestValuesAreAllTrue</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunCombineLatestValuesAreAllTrue</c> result.</returns>
    private static int RunCombineLatestValuesAreAllTrue(ExtensionsLibrary library) =>
        DrainBool(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.CombineLatestValuesAreAllTrue(BoolSources(library, true))
            : PackageExtensions.CombineLatestValuesAreAllTrue(BoolSources(library, true)));

    /// <summary>Executes the <c>RunConflate</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunConflate</c> result.</returns>
    private static int RunConflate(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.Conflate(ArraySource(library), TimeSpan.Zero, Sequencer.Immediate)
            : PackageExtensions.Conflate(ArraySource(library), TimeSpan.Zero, ImmediateScheduler.Instance));

    /// <summary>Executes the <c>RunContinuationDispose</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunContinuationDispose</c> result.</returns>
    private static int RunContinuationDispose(ExtensionsLibrary library)
    {
        if (library == ExtensionsLibrary.Primitives)
        {
            using var continuation = new PrimitivesContinuation();
            return (int)continuation.CompletedPhases;
        }

        using var packageContinuation = new PackageContinuation();
        return (int)packageContinuation.CompletedPhases;
    }

    /// <summary>Executes the <c>RunContinuationLock</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunContinuationLock</c> result.</returns>
    private static int RunContinuationLock(ExtensionsLibrary library)
    {
        var observer = new TupleWitness<int>();
        if (library == ExtensionsLibrary.Primitives)
        {
            var continuation = new PrimitivesContinuation();
            var task = continuation.Lock(Value, observer);
            EnsureCompleted(task);
            return observer.ItemCount;
        }

        var packageContinuation = new PackageContinuation();
        var packageTask = packageContinuation.Lock(Value, observer);
        EnsureCompleted(packageTask);
        return observer.ItemCount;
    }

    /// <summary>Executes the <c>RunContinuationLockValueTask</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunContinuationLockValueTask</c> result.</returns>
    private static int RunContinuationLockValueTask(ExtensionsLibrary library)
    {
        var observer = new TupleWitness<int>();
        if (library == ExtensionsLibrary.Primitives)
        {
            var continuation = new PrimitivesContinuation();
            var task = continuation.LockValueTask(Value, observer);
            EnsureCompleted(task);
            return observer.ItemCount;
        }

        var packageContinuation = new PackageContinuation();
        var packageTask = packageContinuation.LockValueTask(Value, observer);
        EnsureCompleted(packageTask);
        return observer.ItemCount;
    }

    /// <summary>Executes the <c>RunDebounceImmediate</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunDebounceImmediate</c> result.</returns>
    private static int RunDebounceImmediate(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.DebounceImmediate(ArraySource(library), TimeSpan.Zero, Sequencer.Immediate)
            : PackageExtensions.DebounceImmediate(ArraySource(library), TimeSpan.Zero, ImmediateScheduler.Instance));

    /// <summary>Executes the <c>RunDebounceUntil</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunDebounceUntil</c> result.</returns>
    private static int RunDebounceUntil(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.DebounceUntil(ArraySource(library), TimeSpan.Zero, static value => value >= Match, Sequencer.Immediate)
            : PackageExtensions.DebounceUntil(ArraySource(library), TimeSpan.Zero, static value => value >= Match, ImmediateScheduler.Instance));

    /// <summary>Executes the <c>RunDetectStale</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunDetectStale</c> result.</returns>
    private static int RunDetectStale(ExtensionsLibrary library)
    {
        if (library == ExtensionsLibrary.Primitives)
        {
            var clock = new TestClock();
            var observer = new CountingSignalWitness<Extensions.Stale<int>>();
            using var subscription = PrimitivesExtensions.DetectStale(Signal.Silent<int>(), Tick, clock).Subscribe(observer);
            clock.AdvanceBy(Tick);
            return observer.Count + observer.CompletionCount;
        }

        var scheduler = new HistoricalScheduler();
        var packageObserver = new CountingSignalWitness<ReactiveUI.Extensions.Stale<int>>();
        using var packageSubscription = PackageExtensions.DetectStale(RxObservable.Never<int>(), Tick, scheduler)
            .Subscribe(packageObserver);
        scheduler.AdvanceBy(Tick);
        return packageObserver.Count + packageObserver.CompletionCount;
    }

    /// <summary>Executes the <c>RunDoOnDispose</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunDoOnDispose</c> result.</returns>
    private static int RunDoOnDispose(ExtensionsLibrary library)
    {
        var count = 0;
        using var subscription = (library == ExtensionsLibrary.Primitives
                ? PrimitivesExtensions.DoOnDispose(ArraySource(library), () => count++)
                : PackageExtensions.DoOnDispose(ArraySource(library), () => count++))
            .Subscribe(new IntSignalWitness());
        return count;
    }

    /// <summary>Executes the <c>RunDoOnSubscribe</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunDoOnSubscribe</c> result.</returns>
    private static int RunDoOnSubscribe(ExtensionsLibrary library)
    {
        var count = 0;
        var total = DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.DoOnSubscribe(ArraySource(library), () => count++)
            : PackageExtensions.DoOnSubscribe(ArraySource(library), () => count++));
        return total + count;
    }

    /// <summary>Executes the <c>RunDropIfBusy</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunDropIfBusy</c> result.</returns>
    private static int RunDropIfBusy(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.DropIfBusy(ArraySource(library), static _ => default)
            : PackageExtensions.DropIfBusy(ArraySource(library), static _ => default));

    /// <summary>Executes the <c>RunFastForEach</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunFastForEach</c> result.</returns>
    private static int RunFastForEach(ExtensionsLibrary library)
    {
        var observer = new IntSignalWitness();
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

    /// <summary>Executes the <c>RunFilter</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunFilter</c> result.</returns>
    private static int RunFilter(ExtensionsLibrary library) =>
        DrainString(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.Filter(PrimitivesExtensions.FromArray(StringValues), EvenRegex())
            : PackageExtensions.Filter(PackageExtensions.FromArray(StringValues), EvenRegex()));

    /// <summary>Executes the <c>RunFirstMatchFromCandidates</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunFirstMatchFromCandidates</c> result.</returns>
    private static int RunFirstMatchFromCandidates(ExtensionsLibrary library)
    {
        var candidates = Values;
        return DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.FirstMatchFromCandidates(
                candidates,
                static value => PrimitivesObservables.Return(value),
                static value => value * CandidateMultiplier,
                static value => value >= Match,
                Fallback)
            : PackageExtensions.FirstMatchFromCandidates(
                candidates,
                static value => PackageObservables.Return(value),
                static value => value * CandidateMultiplier,
                static value => value >= Match,
                Fallback));
    }

    /// <summary>Executes the <c>RunForEach</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunForEach</c> result.</returns>
    private static int RunForEach(ExtensionsLibrary library)
    {
        var batches = new[] { Values };
        return DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ForEach(PrimitivesExtensions.FromArray<IEnumerable<int>>(batches), null)
            : PackageExtensions.ForEach(PackageExtensions.FromArray<IEnumerable<int>>(batches), null));
    }

    /// <summary>Executes the <c>RunFromArray</c> benchmark helper.</summary>
    /// <param name="library)">The <c>library)</c> value.</param>
    /// <returns>The <c>RunFromArray</c> result.</returns>
    private static int RunFromArray(ExtensionsLibrary library) => DrainInt(ArraySource(library));

    /// <summary>Executes the <c>RunGetMax</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunGetMax</c> result.</returns>
    private static int RunGetMax(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.GetMax(PrimitivesObservables.Return(FirstValue), PrimitivesObservables.Return(SecondValue))
            : PackageExtensions.GetMax(PackageObservables.Return(FirstValue), PackageObservables.Return(SecondValue)));

    /// <summary>Executes the <c>RunGetMin</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunGetMin</c> result.</returns>
    private static int RunGetMin(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.GetMin(PrimitivesObservables.Return(FirstValue), PrimitivesObservables.Return(SecondValue))
            : PackageExtensions.GetMin(PackageObservables.Return(FirstValue), PackageObservables.Return(SecondValue)));

    /// <summary>Executes the <c>RunHeartbeat</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunHeartbeat</c> result.</returns>
    private static int RunHeartbeat(ExtensionsLibrary library)
    {
        if (library == ExtensionsLibrary.Primitives)
        {
            var clock = new TestClock();
            var observer = new CountingSignalWitness<Extensions.Heartbeat<int>>();
            using var subscription = PrimitivesExtensions.Heartbeat(Signal.Silent<int>(), Tick, clock).Subscribe(observer);
            clock.AdvanceBy(Tick);
            return observer.Count + observer.CompletionCount;
        }

        var scheduler = new HistoricalScheduler();
        var packageObserver = new CountingSignalWitness<ReactiveUI.Extensions.Heartbeat<int>>();
        using var packageSubscription = PackageExtensions.Heartbeat(RxObservable.Never<int>(), Tick, scheduler)
            .Subscribe(packageObserver);
        scheduler.AdvanceBy(Tick);
        return packageObserver.Count + packageObserver.CompletionCount;
    }

    /// <summary>Executes the <c>RunLatestOrDefault</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunLatestOrDefault</c> result.</returns>
    private static int RunLatestOrDefault(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.LatestOrDefault(ArraySource(library), Fallback)
            : PackageExtensions.LatestOrDefault(ArraySource(library), Fallback));

    /// <summary>Executes the <c>RunLogErrors</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunLogErrors</c> result.</returns>
    private static int RunLogErrors(ExtensionsLibrary library)
    {
        var errors = 0;
        var total = DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.LogErrors(ArraySource(library), _ => errors++)
            : PackageExtensions.LogErrors(ArraySource(library), _ => errors++));
        return total + errors;
    }

    /// <summary>Executes the <c>RunNot</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunNot</c> result.</returns>
    private static int RunNot(ExtensionsLibrary library) =>
        DrainBool(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.Not(BoolSource(library))
            : PackageExtensions.Not(BoolSource(library)));

    /// <summary>Executes the <c>RunObserveOnIf</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunObserveOnIf</c> result.</returns>
    private static int RunObserveOnIf(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ObserveOnIf(ArraySource(library), true, Sequencer.Immediate)
            : PackageExtensions.ObserveOnIf(ArraySource(library), true, ImmediateScheduler.Instance));

    /// <summary>Executes the <c>RunObserveOnSafe</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunObserveOnSafe</c> result.</returns>
    private static int RunObserveOnSafe(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.ObserveOnSafe(ArraySource(library), Sequencer.Immediate)
            : PackageExtensions.ObserveOnSafe(ArraySource(library), ImmediateScheduler.Instance));

    /// <summary>Executes the <c>RunOnErrorRetry</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunOnErrorRetry</c> result.</returns>
    private static int RunOnErrorRetry(ExtensionsLibrary library) =>
        DrainInt(library == ExtensionsLibrary.Primitives
            ? PrimitivesExtensions.OnErrorRetry<int, InvalidOperationException>(ArraySource(library), static _ => { }, 1, TimeSpan.Zero, Sequencer.Immediate)
            : PackageExtensions.OnErrorRetry<int, InvalidOperationException>(ArraySource(library), static _ => { }, 1, TimeSpan.Zero, ImmediateScheduler.Instance));

    /// <summary>Executes the <c>RunOnNext</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunOnNext</c> result.</returns>
    private static int RunOnNext(ExtensionsLibrary library)
    {
        var observer = new IntSignalWitness();
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

    /// <summary>Executes the <c>RunPairwise</c> benchmark helper.</summary>
    /// <param name="library">The <c>library</c> value.</param>
    /// <returns>The <c>RunPairwise</c> result.</returns>
    private static int RunPairwise(ExtensionsLibrary library)
    {
        var observer = new PairWitness();
        using var subscription = (library == ExtensionsLibrary.Primitives
                ? PrimitivesExtensions.Pairwise(ArraySource(library))
                : PackageExtensions.Pairwise(ArraySource(library)))
            .Subscribe(observer);
        return observer.Total;
    }
}
