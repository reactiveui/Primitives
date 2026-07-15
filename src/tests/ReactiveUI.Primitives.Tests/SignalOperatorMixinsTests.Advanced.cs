// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies signal operator alias, guard, and coordinator contracts.</summary>
public partial class SignalOperatorMixinsTests
{
    /// <summary>The first terminal error message.</summary>
    private const string FirstErrorMessage = "first";

    /// <summary>The late terminal error message.</summary>
    private const string LateErrorMessage = "late";

    /// <summary>
    /// Map and Keep are pass-through wrappers: neither adds a thread affinity of its own, so each must report
    /// exactly the current-thread requirement of the source it wraps. Reporting <see langword="true"/> for a
    /// free-threaded source would needlessly pin subscription; reporting <see langword="false"/> for a pinned
    /// one would subscribe on the wrong thread.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task MapAndKeepSignalsInheritTheCurrentThreadRequirementOfTheirSource()
    {
        MapSignal<int, int> mappedFree = new(new OptionalCurrentThreadObservable<int>(false), static value => value);
        MapSignal<int, int> mappedPinned = new(new CurrentThreadObservable<int>(), static value => value);
        MapSignal<int, int> mappedPlain = new(new Signal<int>(), static value => value);

        KeepSignal<int> keptFree = new(new OptionalCurrentThreadObservable<int>(false), static _ => true);
        KeepSignal<int> keptPinned = new(new CurrentThreadObservable<int>(), static _ => true);
        KeepSignal<int> keptPlain = new(new Signal<int>(), static _ => true);

        await Assert.That(mappedFree.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(mappedPinned.IsRequiredSubscribeOnCurrentThread()).IsTrue();
        await Assert.That(mappedPlain.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(keptFree.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(keptPinned.IsRequiredSubscribeOnCurrentThread()).IsTrue();
        await Assert.That(keptPlain.IsRequiredSubscribeOnCurrentThread()).IsFalse();
    }

    /// <summary>
    /// A source that keeps notifying after it has completed must not get a second bite at a Keep subscriber. The
    /// filter must drop the late value before it even consults the predicate, and drop the late fault entirely.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task KeepSignalDropsSourceNotificationsThatArriveAfterItsTerminal()
    {
        var predicateCalls = 0;
        InvalidOperationException lateError = new(LateErrorMessage);
        KeepSignal<int> kept = new(
            new ScriptedObservable<int>(observer =>
            {
                observer.OnCompleted();
                observer.OnNext(One);
                observer.OnError(lateError);
            }),
            _ =>
            {
                predicateCalls++;
                return true;
            });

        RecordingWitness<int> observer = new();
        _ = kept.Subscribe(observer);

        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(predicateCalls).IsEqualTo(0);
    }

    /// <summary>Verifies advanced map-indexed and enumerable blend direct paths.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AdvancedSignalsReportThreadRequirementsAndBlendUnboundedSources()
    {
        MapIndexedSignal<int, int> mapped = new(Signal.Emit(One), static (value, index) => value + index);
        RecordingWitness<int> mappedValues = new();
        _ = mapped.Subscribe(mappedValues);
        MapIndexedSignal<int, int> currentThreadMapped = new(
            new CurrentThreadObservable<int>(),
            static (value, index) => value + index);
        MapIndexedSignal<int, int> optionalThreadMapped = new(
            new OptionalCurrentThreadObservable<int>(false),
            static (value, index) => value + index);

        List<int> blended = [];
        _ = ((IEnumerable<IObservable<int>>)[Signal.Emit(One), Signal.Emit(Two)])
            .Blend(int.MaxValue)
            .Subscribe(blended.Add);

        await Assert.That(mapped.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(currentThreadMapped.IsRequiredSubscribeOnCurrentThread()).IsTrue();
        await Assert.That(optionalThreadMapped.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(CurrentThreadRequirement.IsRequired(new ScriptedObservable<int>(static _ => { }))).IsFalse();
        await Assert.That(mappedValues.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(mappedValues.Completed).IsEqualTo(1);
        await Assert.That(blended.SequenceEqual(ExpectedOneTwo)).IsTrue();
    }

    /// <summary>A bounded enumerable blend keeps only the allowed number of sources active and admits the rest in turn.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EnumerableBlendWithAConcurrencyBoundAdmitsSourcesInTurn()
    {
        const int UnobservedValue = 7;
        Signal<int> first = new();
        Signal<int> second = new();
        IEnumerable<IObservable<int>> sources = [first, second];
        List<int> blended = [];
        var completions = 0;

        var bounded = sources.Blend(1);
        using var subscription = bounded.Subscribe(blended.Add, static ex => throw ex, () => completions++);

        // The bound is one, so the second source must not be subscribed while the first is still running.
        second.OnNext(UnobservedValue);
        first.OnNext(One);
        first.OnCompleted();

        // The first source completed, which frees the slot the second source was waiting for.
        second.OnNext(Two);
        second.OnCompleted();

        await Assert.That(blended.SequenceEqual(ExpectedOneTwo)).IsTrue();
        await Assert.That(completions).IsEqualTo(1);
        _ = Assert.Throws<ArgumentNullException>(() => bounded.Subscribe((IObserver<int>)null!));
    }

    /// <summary>Verifies bounded blend drains enumerable sources and suppresses late terminals.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task MaxConcurrentBlendCoordinatorDrainsAndSuppressesLateTerminals()
    {
        Signal<int> active = new();
        RecordingWitness<int> drained = new();
        var coordinator = new MaxConcurrentBlendCoordinator<int>(drained).Run([active], Two);

        active.OnNext(One);
        active.OnCompleted();
        coordinator.Dispose();
        coordinator.Dispose();

        await Assert.That(drained.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(drained.Completed).IsEqualTo(1);

        InvalidOperationException first = new(FirstErrorMessage);
        InvalidOperationException late = new(LateErrorMessage);
        RecordingWitness<int> failed = new();
        _ = new MaxConcurrentBlendCoordinator<int>(failed).Run(
            [
                new ScriptedObservable<int>(observer =>
                {
                    observer.OnError(first);
                    observer.OnError(late);
                    observer.OnCompleted();
                })
            ],
            One);

        await Assert.That(failed.Errors.Count).IsEqualTo(1);
        await Assert.That(failed.Errors[0]).IsSameReferenceAs(first);

        RecordingWitness<int> nullEnumerator = new();
        _ = new MaxConcurrentBlendCoordinator<int>(nullEnumerator).Run(new NullEnumeratorEnumerable<int>(true), One);

        await Assert.That(nullEnumerator.Completed).IsEqualTo(1);
    }

    /// <summary>Verifies task-chain coordinators suppress duplicate outer terminal notifications.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskChainCoordinatorSuppressesDuplicateOuterTerminalNotifications()
    {
        InvalidOperationException first = new(FirstErrorMessage);
        InvalidOperationException late = new(LateErrorMessage);
        RecordingWitness<int> failed = new();

        _ = new TaskChainCoordinator<int>(failed).Run(new ScriptedObservable<Task<int>>(observer =>
        {
            observer.OnError(first);
            observer.OnError(late);
            observer.OnCompleted();
        }));

        await Assert.That(failed.Errors.Count).IsEqualTo(1);
        await Assert.That(failed.Errors[0]).IsSameReferenceAs(first);
        await Assert.That(failed.Completed).IsEqualTo(0);

        var active = true;
        var done = false;
        TaskChainCoordinatorState.OnInnerCompleted(
            new(),
            ref done,
            ref active,
            new TaskChainCoordinator<int>(new RecordingWitness<int>()));
        await Assert.That(active).IsFalse();

        active = true;
        done = true;
        TaskChainCoordinatorState.OnInnerCompleted(
            new(),
            ref done,
            ref active,
            new TaskChainCoordinator<int>(new RecordingWitness<int>()));
        await Assert.That(active).IsTrue();
    }

    /// <summary>Verifies higher-order chain and blend operators reject null inner sources and suppress late errors.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HigherOrderChainAndBlendHandleNullSourcesAndDuplicateErrors()
    {
        RecordingWitness<int> chainDisposed = new();
        Signal.Silent<IObservable<int>>().Chain().Subscribe(chainDisposed).Dispose();

        RecordingWitness<int> chainNull = new();
        _ = new ScriptedObservable<IObservable<int>>(static observer => observer.OnNext(null!))
            .Chain()
            .Subscribe(chainNull);

        RecordingWitness<int> blendNull = new();
        _ = new ScriptedObservable<IObservable<int>>(static observer => observer.OnNext(null!))
            .Blend()
            .Subscribe(blendNull);

        InvalidOperationException first = new(FirstErrorMessage);
        InvalidOperationException late = new(LateErrorMessage);
        RecordingWitness<int> blendErrors = new();
        _ = new ScriptedObservable<IObservable<int>>(observer =>
        {
            observer.OnError(first);
            observer.OnError(late);
        }).Blend().Subscribe(blendErrors);

        await Assert.That(chainNull.Errors.Count).IsEqualTo(1);
        await Assert.That(chainNull.Errors[0]).IsTypeOf<InvalidOperationException>();
        await Assert.That(blendNull.Errors.Count).IsEqualTo(1);
        await Assert.That(blendNull.Errors[0]).IsTypeOf<InvalidOperationException>();
        await Assert.That(blendErrors.Errors.Count).IsEqualTo(1);
        await Assert.That(blendErrors.Errors[0]).IsSameReferenceAs(first);
    }

    /// <summary>Verifies latch and expire coordinators tolerate non-terminal right completion and duplicate timeouts.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LatchAndExpireCoordinatorTerminalGuardsAreDeterministic()
    {
        Signal<int> left = new();
        Signal<int> right = new();
        RecordingWitness<int> latched = new();
        using var latchedSubscription = left.Latch(right, static (leftValue, rightValue) => leftValue + rightValue)
            .Subscribe(latched);

        right.OnNext(Two);
        right.OnCompleted();
        left.OnNext(Three);
        left.OnCompleted();

        RecordingWitness<int> expired = new();
        _ = Signal.Silent<int>().Expire(TimeSpan.FromTicks(One), new DoubleFireSequencer()).Subscribe(expired);

        await Assert.That(latched.Values.SequenceEqual([Five])).IsTrue();
        await Assert.That(latched.Completed).IsEqualTo(1);
        await Assert.That(expired.Errors.Count).IsEqualTo(1);
        await Assert.That(expired.Errors[0]).IsTypeOf<TimeoutException>();
    }

    /// <summary>Verifies take-until terminal guards and distinct set creation branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TakeUntilAndDistinctTerminalBranchesAreCovered()
    {
        RecordingWitness<int> completed = new();
        _ = new ScriptedObservable<int>(static observer =>
        {
            observer.OnCompleted();
            observer.OnCompleted();
        }).TakeUntil(Signal.Silent<int>()).Subscribe(completed);

        InvalidOperationException first = new(FirstErrorMessage);
        InvalidOperationException late = new(LateErrorMessage);
        RecordingWitness<int> failed = new();
        _ = new ScriptedObservable<int>(observer =>
        {
            observer.OnError(first);
            observer.OnError(late);
        }).TakeUntil(Signal.Silent<int>()).Subscribe(failed);

        RecordingWitness<int> otherFailed = new();
        _ = Signal.Emit(One)
            .TakeUntil(new ScriptedObservable<int>(observer => observer.OnError(late)))
            .Subscribe(otherFailed);

        RecordingWitness<int> otherCompleted = new();
        _ = Signal.Emit(One)
            .TakeUntil(new ScriptedObservable<int>(static observer => observer.OnCompleted()))
            .Subscribe(otherCompleted);

        List<int> rangeDistinct = [];
        _ = Signal.Sequence(One, Three).Distinct().Subscribe(rangeDistinct.Add);

        List<int> comparerDistinct = [];
        _ = Signal.FromEnumerable([One, One, Two])
            .Distinct(EqualityComparer<int>.Default)
            .Subscribe(comparerDistinct.Add);

        List<int> defaultDistinct = [];
        _ = Signal.FromEnumerable([One, One, Two])
            .Distinct()
            .Subscribe(defaultDistinct.Add);

        await Assert.That(completed.Completed).IsEqualTo(1);
        await Assert.That(failed.Errors.Count).IsEqualTo(1);
        await Assert.That(failed.Errors[0]).IsSameReferenceAs(first);
        await Assert.That(otherFailed.Errors.Count).IsEqualTo(1);
        await Assert.That(otherFailed.Errors[0]).IsSameReferenceAs(late);
        await Assert.That(otherCompleted.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(otherCompleted.Completed).IsEqualTo(1);
        await Assert.That(rangeDistinct.SequenceEqual([One, Two, Three])).IsTrue();
        await Assert.That(comparerDistinct.SequenceEqual(ExpectedOneTwo)).IsTrue();
        await Assert.That(defaultDistinct.SequenceEqual(ExpectedOneTwo)).IsTrue();
    }

    /// <summary>A sequencer that invokes scheduled work twice before returning the disposable work item.</summary>
    private sealed class DoubleFireSequencer : ISequencer
    {
        /// <inheritdoc/>
        public DateTimeOffset Now => DateTimeOffset.UnixEpoch;

        /// <inheritdoc/>
        public long Timestamp => 0;

        /// <inheritdoc/>
        public void Schedule(IWorkItem item)
        {
            item.Execute();
            item.Execute();
        }

        /// <inheritdoc/>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Design",
            "SST2318:Members should not have identical bodies",
            Justification =
                "The relative and absolute Schedule overloads of this test-double sequencer intentionally behave the "
                + "same way; both are required by the ISequencer contract and, as distinct interface overloads, cannot "
                + "forward to one another.")]
        public void Schedule(IWorkItem item, long dueTimestamp)
        {
            item.Execute();
            item.Execute();
        }
    }

    /// <summary>An enumerable that returns a null enumerator for defensive coordinator branches.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class NullEnumeratorEnumerable<T> : IEnumerable<IObservable<T>>
    {
        /// <summary>The empty fallback source array.</summary>
        private static readonly IObservable<T>[] EmptySources = [];

        /// <summary>Initializes a new instance of the <see cref="NullEnumeratorEnumerable{T}"/> class.</summary>
        /// <param name="returnNullEnumerator">A value indicating whether the enumerable returns a null enumerator.</param>
        public NullEnumeratorEnumerable(bool returnNullEnumerator) => ReturnNullEnumerator = returnNullEnumerator;

        /// <summary>Gets a value indicating whether the enumerable returns a null enumerator.</summary>
        private bool ReturnNullEnumerator { get; }

        /// <inheritdoc/>
        public IEnumerator<IObservable<T>> GetEnumerator() =>
            ReturnNullEnumerator ? null! : ((IEnumerable<IObservable<T>>)EmptySources).GetEnumerator();

        /// <inheritdoc/>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>A source with configurable current-thread subscription requirements.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class OptionalCurrentThreadObservable<T> : IRequireCurrentThread<T>
    {
        /// <summary>Initializes a new instance of the <see cref="OptionalCurrentThreadObservable{T}"/> class.</summary>
        /// <param name="required">A value indicating whether current-thread subscription is required.</param>
        public OptionalCurrentThreadObservable(bool required) => Required = required;

        /// <summary>Gets a value indicating whether current-thread subscription is required.</summary>
        private bool Required { get; }

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() => Required;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer) => EmptyDisposable.Instance;
    }
}
