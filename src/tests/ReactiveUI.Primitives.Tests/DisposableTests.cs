// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies disposable primitive contracts.</summary>
public class DisposableTests
{
    /// <summary>The integer constant one.</summary>
    private const int One = 1;

    /// <summary>The integer constant two.</summary>
    private const int Two = 2;

    /// <summary>The integer constant three.</summary>
    private const int Three = 3;

    /// <summary>The integer constant four.</summary>
    private const int Four = 4;

    /// <summary>The integer constant seven.</summary>
    private const int Seven = 7;

    /// <summary>The integer constant eight.</summary>
    private const int Eight = 8;

    /// <summary>The number of disposables added to the multiple-disposable group.</summary>
    private const int Twenty = 20;

    /// <summary>Calendar year used by value-type timestamp coverage.</summary>
    private const int CalendarYear = 2024;

    /// <summary>The disposal count produced when a slot disposes twice.</summary>
    private const int DoubleDisposalCount = 2;

    /// <summary>The disposal count produced by the replaceable-slot replacement sequence.</summary>
    private const int ReplaceableDisposalCount = 5;

    /// <summary>A fixed deterministic timestamp used for absolute scheduling.</summary>
    private static readonly DateTimeOffset FixedTimestamp = new(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>Verifies a null disposable action uses the shared empty disposable.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ActionDisposableNullActionIsDisposedAfterDispose()
    {
        ActionDisposable disposable = new(null!);
        disposable.Dispose();
        await Assert.That(disposable.IsDisposed).IsTrue();
    }

    /// <summary>Verifies removing one disposable leaves the others attached until disposal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task MultipleDisposableRemoveDisposesOnlyTheRequestedItem()
    {
        var first = 0;
        var second = 0;
        ActionDisposable firstDisposable = new(() => first++);
        ActionDisposable secondDisposable = new(() => second++);
        MultipleDisposable pocket = new(firstDisposable, secondDisposable);
        await Assert.That(pocket.Remove(firstDisposable)).IsTrue();
        await Assert.That(first).IsEqualTo(1);
        await Assert.That(second).IsEqualTo(0);
        pocket.Dispose();
        await Assert.That(first).IsEqualTo(1);
        await Assert.That(second).IsEqualTo(1);
    }

    /// <summary>Verifies assigning a disposed single slot disposes the incoming disposable immediately.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SingleDisposableCreateAfterDisposeDisposesIncomingDisposableImmediately()
    {
        var disposed = 0;
        SingleDisposable slot = new();
        slot.Dispose();
        slot.Create(new ActionDisposable(() => disposed++));
        await Assert.That(slot.IsDisposed).IsTrue();
        await Assert.That(disposed).IsEqualTo(1);
    }

    /// <summary>Verifies a replaceable disposable invokes its disposal action only once.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SingleReplaceableDisposableRunsActionOnlyOnce()
    {
        var actionCount = 0;
        SingleReplaceableDisposable slot = new(() => actionCount++);
        slot.Dispose();
        slot.Dispose();
        await Assert.That(actionCount).IsEqualTo(1);
    }

    /// <summary>Covers disposable slot constructor, disposal, removal, and disposed-assignment branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposableSlotsCoverAssignmentReplacementAndRemovalBranches()
    {
        var disposedBeforeAssign = 0;
        SingleDisposable lateSingle = new(() => disposedBeforeAssign++);
        lateSingle.Dispose();
        lateSingle.Create(new ActionDisposable(() => disposedBeforeAssign++));
        await Assert.That(disposedBeforeAssign).IsEqualTo(1);
        _ = Assert.Throws<ArgumentNullException>(() => lateSingle.Create(null!));
        var replaced = 0;
        SingleReplaceableDisposable replaceable = new(
            new ActionDisposable(() => replaced++),
            () => replaced++);
        replaceable.Create(new ActionDisposable(() => replaced++));
        replaceable.Dispose();
        replaceable.Create(new ActionDisposable(() => replaced++));
        await Assert.That(replaced).IsEqualTo(ReplaceableDisposalCount);
        _ = Assert.Throws<ArgumentNullException>(() => replaceable.Create(null!));
        await AssertProtectedDisposePathRunsTheUnderlyingDisposableOnce();
        await AssertMultipleDisposableRemovesItemsAndDisposesTheRest();
        await AssertMultipleDisposableFactorySkipsNullsAndDisposesOnce();
        DisposeEveryConstructedSlotShape();
    }

    /// <summary>Verifies low-level disposables, collections, and schedulers cover deterministic edges.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task LowLevelDisposablesCollectionsAndSchedulersCoverDeterministicEdges()
    {
        MultipleDisposable multiple = [];
        for (var i = 0; i < Twenty; i++)
        {
            multiple.Add(EmptyDisposable.Instance);
        }

        await Assert.That(multiple.Remove(EmptyDisposable.Instance)).IsTrue();
        await Assert.That(multiple.Remove(new ActionDisposable(static () => { }))).IsFalse();
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            MultipleDisposable invalid = new((IDisposable[])null!);
            GC.KeepAlive(invalid);
        });
        _ = Assert.Throws<ArgumentNullException>(() => multiple.Add(null!));
        multiple.Dispose();
        multiple.Dispose();
        using CancellationTokenSource cts = new();
        CancellationDisposable cancellation = new(cts);
        cancellation.Dispose();
        cancellation.Dispose();
        await Assert.That(cts.IsCancellationRequested).IsTrue();
        var list = CopyOnWriteList<int>.Empty;
        await Assert.That(list.IndexOf(One)).IsEqualTo(-1);
        await Assert.That(list.Remove(One)).IsSameReferenceAs(list);
        var added = list.Add(One).Add(Two);
        await Assert.That(added.IndexOf(One)).IsEqualTo(0);
        await Assert.That(added.Remove(One).Remove(Two)).IsSameReferenceAs(CopyOnWriteList<int>.Empty);
        var observerList = CopyOnWriteList<IObserver<int>>.Empty.Add(new RecordingWitness<int>());
        ListWitness<int> witness = new(observerList);
        await Assert.That(witness.HasObservers).IsTrue();
        await Assert.That(witness.Add(new RecordingWitness<int>())).IsNotNull();
        PriorityQueue<int> queue = new();
        queue.Enqueue(One);
        queue.Enqueue(Two);
        await Assert.That(queue.Count > 0).IsTrue();
        EventPattern<EventArgs> eventPattern = new(null, EventArgs.Empty);
        EventPattern<EventArgs> samePattern = new(null, EventArgs.Empty);
        await Assert.That(eventPattern == samePattern).IsTrue();
        await Assert.That(eventPattern != samePattern).IsFalse();
        await Assert.That(eventPattern.Equals((object)samePattern)).IsTrue();
        await Assert.That(eventPattern.GetHashCode()).IsNotEqualTo(0);
        var current = Sequencer.CurrentThread;
        _ = Assert.Throws<ArgumentNullException>(() => current.Schedule((Action)null!));
        _ = Assert.Throws<ArgumentNullException>(() => current.Schedule(One, TimeSpan.Zero, null!));
        List<int> scheduled = [];
        current.Schedule(One, TimeSpan.FromMilliseconds(1), (_, state) =>
        {
            scheduled.Add(state);
            return EmptyDisposable.Instance;
        }).Dispose();
        current.Schedule(One, FixedTimestamp.AddMilliseconds(1), (_, state) =>
        {
            scheduled.Add(state + One);
            return EmptyDisposable.Instance;
        }).Dispose();
        await Assert.That(scheduled.Count).IsEqualTo(Two);
    }

    /// <summary>Exercises value types, disposables, and handle delegates.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CoreValueTypesDisposablesAndHandlesCoverEqualityAndLifecycleBranches()
    {
        var ignored = 0;
        InvalidOperationException thrown = new("throw-me");
        await AssertMomentIntervalAndVoidEqualityContracts();
        await InvokeInternalHandleMembers(thrown);
        _ = Handle.CatchIgnore<int>(new InvalidOperationException("ignored")).Subscribe(_ => ignored++);
        await Assert.That(ignored).IsEqualTo(0);
        await AssertDisposableLifecycleBranches();
    }

    /// <summary>Asserts the equality, hashing, and formatting contracts of the core value types.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertMomentIntervalAndVoidEqualityContracts()
    {
        Moment<int> moment = new(Seven, new(CalendarYear, 1, 1, 0, 0, 0, TimeSpan.Zero));
        Moment<int> sameMoment = new(Seven, moment.Timestamp);
        Moment<int> differentMoment = new(Eight, moment.Timestamp.AddTicks(1));
        TimeInterval<int> interval = new(Seven, TimeSpan.FromTicks(Three));
        TimeInterval<int> sameInterval = new(Seven, TimeSpan.FromTicks(Three));
        TimeInterval<int> differentInterval = new(Eight, TimeSpan.FromTicks(Four));
        var unit = default(RxVoid);
        await Assert.That(moment == sameMoment).IsTrue();
        await Assert.That(moment.Equals((object)sameMoment)).IsTrue();
        await Assert.That(moment == differentMoment).IsFalse();
        await Assert.That(moment != differentMoment).IsTrue();
        await Assert.That(moment.Equals("not a moment")).IsFalse();
        await Assert.That(sameMoment.GetHashCode()).IsEqualTo(moment.GetHashCode());
        await Assert.That(moment.ToString().Contains("7", StringComparison.Ordinal)).IsTrue();
        await Assert.That(interval == sameInterval).IsTrue();
        await Assert.That(interval.Equals((object)sameInterval)).IsTrue();
        await Assert.That(interval == differentInterval).IsFalse();
        await Assert.That(interval != differentInterval).IsTrue();
        await Assert.That(interval.Equals("not an interval")).IsFalse();
        await Assert.That(sameInterval.GetHashCode()).IsEqualTo(interval.GetHashCode());
        await Assert.That(interval.ToString().Contains("7", StringComparison.Ordinal)).IsTrue();
        await Assert.That(unit.Equals(default)).IsTrue();
        await Assert.That(unit.Equals((object)default(RxVoid))).IsTrue();
        await Assert.That(unit.GetHashCode()).IsEqualTo(0);
        await Assert.That(unit.ToString()).IsEqualTo("()");
    }

    /// <summary>Asserts the disposal lifecycle of the boolean, slot, pocket, single, and group disposables.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertDisposableLifecycleBranches()
    {
        BooleanDisposable boolean = new();
        await Assert.That(boolean.IsDisposed).IsFalse();
        boolean.Dispose();
        await Assert.That(boolean.IsDisposed).IsTrue();
        var slotDisposed = 0;
        var assignmentDisposed = 0;
        var pocketDisposed = 0;
        new Slot(new ActionDisposable(() => slotDisposed++), () => slotDisposed++).Dispose();
        new AssignmentSlot(new ActionDisposable(() => assignmentDisposed++), () => assignmentDisposed++).Dispose();
        new Pocket(new ActionDisposable(() => pocketDisposed++)).Dispose();
        await Assert.That(slotDisposed).IsEqualTo(Two);
        await Assert.That(assignmentDisposed).IsEqualTo(Two);
        await Assert.That(pocketDisposed).IsEqualTo(1);
        SingleDisposable single = new(new ActionDisposable(static () => { }), static () => { });
        _ = Assert.Throws<InvalidOperationException>(() => single.Create(EmptyDisposable.Instance));
        var replaceableFirst = 0;
        var replaceableSecond = 0;
        SingleReplaceableDisposable replaceable = new(new ActionDisposable(() => replaceableFirst++));
        replaceable.Create(new ActionDisposable(() => replaceableSecond++));
        replaceable.Dispose();
        await Assert.That(replaceableFirst).IsEqualTo(1);
        await Assert.That(replaceableSecond).IsEqualTo(1);
        MultipleDisposable multiple = [];
        _ = Assert.Throws<ArgumentNullException>(() => multiple.Remove(null));
        multiple.Dispose();
        var lateDisposed = 0;
        multiple.Add(new ActionDisposable(() => lateDisposed++));
        await Assert.That(lateDisposed).IsEqualTo(1);
        await Assert.That(multiple.IsDisposed).IsTrue();
    }

    /// <summary>Asserts the protected <c>Dispose(false)</c> path still disposes the underlying disposable exactly once.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertProtectedDisposePathRunsTheUnderlyingDisposableOnce()
    {
        var disposeFalse = 0;
        ExposedSingleDisposable single = new(new ActionDisposable(() => disposeFalse++));
        single.DisposeFalse();
        single.Dispose();
        await Assert.That(disposeFalse).IsEqualTo(1);
        var replaceableFalse = 0;
        ExposedSingleReplaceableDisposable exposedReplaceable = new(
            new ActionDisposable(() => replaceableFalse++));
        exposedReplaceable.DisposeFalse();
        exposedReplaceable.Dispose();
        await Assert.That(replaceableFalse).IsEqualTo(1);
        var multipleFalse = 0;
        ExposedMultipleDisposable exposedMultiple = new(new ActionDisposable(() => multipleFalse++));
        exposedMultiple.DisposeFalse();
        exposedMultiple.Dispose();
        await Assert.That(multipleFalse).IsEqualTo(1);
    }

    /// <summary>Asserts removal detaches an item from the group and disposal reaches only the items still in it.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertMultipleDisposableRemovesItemsAndDisposesTheRest()
    {
        var firstDisposed = 0;
        var secondDisposed = 0;
        var thirdDisposed = 0;
        var fourthDisposed = 0;
        var missing = EmptyDisposable.Instance;
        ActionDisposable first = new(() => firstDisposed++);
        ActionDisposable second = new(() => secondDisposed++);
        ActionDisposable third = new(() => thirdDisposed++);
        ActionDisposable fourth = new(() => fourthDisposed++);
        MultipleDisposable group = [first, second, third, fourth];
        await Assert.That(group.Remove(first)).IsTrue();
        await Assert.That(group.Remove(second)).IsTrue();
        await Assert.That(group.Remove(third)).IsTrue();
        await Assert.That(group.Remove(missing)).IsFalse();
        group.Dispose();
        await Assert.That(firstDisposed).IsEqualTo(1);
        await Assert.That(secondDisposed).IsEqualTo(1);
        await Assert.That(thirdDisposed).IsEqualTo(1);
        await Assert.That(fourthDisposed).IsEqualTo(1);
    }

    /// <summary>Asserts the group factory ignores a null entry, rejects a null array, and disposes its items once.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertMultipleDisposableFactorySkipsNullsAndDisposesOnce()
    {
        var factoryDisposed = 0;
        var factoryGroup = MultipleDisposable.Create(
            new ActionDisposable(() => factoryDisposed++),
            null!,
            new ActionDisposable(() => factoryDisposed++));
        factoryGroup.Dispose();
        factoryGroup.Dispose();
        await Assert.That(factoryDisposed).IsEqualTo(DoubleDisposalCount);
        _ = Assert.Throws<ArgumentNullException>(static () => MultipleDisposable.Create(null!));
    }

    /// <summary>Disposes one instance of every slot and pocket constructor overload.</summary>
    private static void DisposeEveryConstructedSlotShape()
    {
        IDisposable[] constructedDisposables =
        [
            new AssignmentSlot(),
            new AssignmentSlot(static () => { }),
            new AssignmentSlot(EmptyDisposable.Instance),
            new Slot(),
            new Slot(static () => { }),
            new Slot(EmptyDisposable.Instance),
            new Pocket(),
            new Pocket(EmptyDisposable.Instance, EmptyDisposable.Instance),
            new Pocket(EmptyDisposable.Instance, EmptyDisposable.Instance, EmptyDisposable.Instance)
        ];

        foreach (var disposable in constructedDisposables)
        {
            disposable.Dispose();
        }
    }

    /// <summary>Invokes the public handle members directly.</summary>
    /// <param name="exception">The exception expected from throwing delegates.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task InvokeInternalHandleMembers(Exception exception)
    {
        Handle.Nop();
        Handle<int>.Ignore(1);
        Handle<int, int>.Ignore(1, Two);
        Handle<int, int, int>.Ignore(1, Two, Three);
        await Assert.That(Handle<string>.Identity("x")).IsEqualTo("x");
        _ = Assert.Throws<InvalidOperationException>(() => Handle.Throw(exception));
        _ = Assert.Throws<InvalidOperationException>(() => Handle<int>.Throw(exception, 1));
        _ = Assert.Throws<InvalidOperationException>(() => Handle<int, int>.Throw(exception, 1, Two));
        _ = Assert.Throws<InvalidOperationException>(() => Handle<int, int, int>.Throw(exception, 1, Two, Three));
    }

    /// <summary>Exposes the protected dispose path for coverage.</summary>
    private sealed class ExposedSingleDisposable : SingleDisposable
    {
        /// <summary>Initializes a new instance of the <see cref="ExposedSingleDisposable"/> class.</summary>
        /// <param name="disposable">The disposable to assign.</param>
        public ExposedSingleDisposable(IDisposable disposable)
            : base(disposable)
        {
        }

        /// <summary>Invokes the protected dispose path with <see langword="false"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DisposeFalse() => Dispose(false);
    }

    /// <summary>Exposes the protected dispose path for coverage.</summary>
    private sealed class ExposedSingleReplaceableDisposable : SingleReplaceableDisposable
    {
        /// <summary>Initializes a new instance of the <see cref="ExposedSingleReplaceableDisposable"/> class.</summary>
        /// <param name="disposable">The disposable to assign.</param>
        public ExposedSingleReplaceableDisposable(IDisposable disposable)
            : base(disposable)
        {
        }

        /// <summary>Invokes the protected dispose path with <see langword="false"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DisposeFalse() => Dispose(false);
    }

    /// <summary>Exposes the protected dispose path for coverage.</summary>
    private sealed class ExposedMultipleDisposable : MultipleDisposable
    {
        /// <summary>Initializes a new instance of the <see cref="ExposedMultipleDisposable"/> class.</summary>
        /// <param name="disposable">The disposable to assign.</param>
        public ExposedMultipleDisposable(IDisposable disposable)
            : base(disposable, EmptyDisposable.Instance)
        {
        }

        /// <summary>Invokes the protected dispose path with <see langword="false"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void DisposeFalse() => Dispose(false);
    }
}
