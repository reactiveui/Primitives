// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for the signal type.</summary>
public class SignalTests
{
    /// <summary>The integer constant one.</summary>
    private const int One = 1;

    /// <summary>The integer constant two.</summary>
    private const int Two = 2;

    /// <summary>The integer constant three.</summary>
    private const int Three = 3;

    /// <summary>The integer constant four.</summary>
    private const int Four = 4;

    /// <summary>The integer constant five.</summary>
    private const int Five = 5;

    /// <summary>The integer constant seven.</summary>
    private const int Seven = 7;

    /// <summary>The integer constant nine.</summary>
    private const int Nine = 9;

    /// <summary>The integer constant forty-two.</summary>
    private const int FortyTwo = 42;

    /// <summary>Number of values expected in pair buffers.</summary>
    private const int PairCount = 2;

    /// <summary>Number of values skipped between non-overlapping buffers.</summary>
    private const int SkipCount = 2;

    /// <summary>Test value two.</summary>
    private const int ValueTwo = 2;

    /// <summary>Test value three.</summary>
    private const int ValueThree = 3;

    /// <summary>Test value four.</summary>
    private const int ValueFour = 4;

    /// <summary>Test value five.</summary>
    private const int ValueFive = 5;

    /// <summary>Test value six.</summary>
    private const int ValueSix = 6;

    /// <summary>Test value seven.</summary>
    private const int ValueSeven = 7;

    /// <summary>Test value eight.</summary>
    private const int ValueEight = 8;

    /// <summary>Divisor used by even-value filters.</summary>
    private const int EvenDivisor = 2;

    /// <summary>Multiplier used by select projection tests.</summary>
    private const int SelectMultiplier = 2;

    /// <summary>Expected immediate witness error messages.</summary>
    private static readonly string[] ExpectedImmediateWitness = ["immediate", "witness"];

    /// <summary>Expected first pair of buffered values.</summary>
    private static readonly int[] FirstPair = [1, ValueTwo];

    /// <summary>Expected second pair of buffered values.</summary>
    private static readonly int[] SecondPair = [ValueThree, ValueFour];

    /// <summary>Expected third pair of buffered values.</summary>
    private static readonly int[] ThirdPair = [ValueFive, ValueSix];

    /// <summary>Expected single RxVoid notification.</summary>
    private static readonly RxVoid[] SingleRxVoid = [RxVoid.Default];

    /// <summary>Expected pair of RxVoid notifications.</summary>
    private static readonly RxVoid[] DoubleRxVoid = [RxVoid.Default, RxVoid.Default];

    /// <summary>Called when [next].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnNext()
    {
        Signal<int> subject = new();
        var value = 0;
        var subscription = subject.Subscribe(i => value += i);
        subject.OnNext(1);
        await Assert.That(value).IsEqualTo(1);
        subject.OnNext(1);
        await Assert.That(value).IsEqualTo(PairCount);
        subscription.Dispose();
        subject.OnNext(1);
        await Assert.That(value).IsEqualTo(PairCount);
    }

    /// <summary>Called when [next disposed].</summary>
    [Test]
    public void OnNextDisposed()
    {
        Signal<int> subject = new();
        subject.Dispose();
        _ = Assert.Throws<ObjectDisposedException>(() => subject.OnNext(1));
    }

    /// <summary>Called when [next disposed subscriber].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnNextDisposedSubscriber()
    {
        Signal<int> subject = new();
        var value = 0;
        subject.Subscribe(i => value += i).Dispose();
        subject.OnNext(1);
        await Assert.That(value).IsEqualTo(0);
    }

    /// <summary>Called when [completed].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnCompleted()
    {
        Signal<int> subject = new();
        var completed = false;
        using var subscription = subject.Subscribe(
            static _ => { },
            () => completed = true);
        subject.OnCompleted();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Called when [completed no op].</summary>
    [Test]
    public void OnCompleted_NoErrors()
    {
        Signal<int> subject = new();
        using var subscription = subject.Subscribe(static _ => { });
        subject.OnCompleted();
    }

    /// <summary>Called when [completed once].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnCompletedOnce()
    {
        Signal<int> subject = new();
        var completed = 0;
        using var subscription = subject.Subscribe(
            static _ => { },
            () => completed++);
        subject.OnCompleted();
        await Assert.That(completed).IsEqualTo(1);
        subject.OnCompleted();
        await Assert.That(completed).IsEqualTo(1);
    }

    /// <summary>Called when [completed disposed].</summary>
    [Test]
    public void OnCompletedDisposed()
    {
        Signal<int> subject = new();
        subject.Dispose();
        _ = Assert.Throws<ObjectDisposedException>(subject.OnCompleted);
    }

    /// <summary>Called when [completed disposed subscriber].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnCompletedDisposedSubscriber()
    {
        Signal<int> subject = new();
        var completed = false;
        subject.Subscribe(
            static _ => { },
            () => completed = true).Dispose();
        subject.OnCompleted();
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Called when [error].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnError()
    {
        Signal<int> subject = new();
        var error = false;
        using var subscription = subject.Subscribe(
            static _ => { },
            _ => error = true);
        subject.OnError(new InvalidOperationException());
        await Assert.That(error).IsTrue();
    }

    /// <summary>Called when [error once].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnErrorOnce()
    {
        Signal<int> subject = new();
        var errors = 0;
        using var subscription = subject.Subscribe(
            static _ => { },
            _ => errors++);
        subject.OnError(new InvalidOperationException());
        await Assert.That(errors).IsEqualTo(1);
        subject.OnError(new InvalidOperationException());
        await Assert.That(errors).IsEqualTo(1);
    }

    /// <summary>Called when [error disposed].</summary>
    [Test]
    public void OnErrorDisposed()
    {
        Signal<int> subject = new();
        subject.Dispose();
        _ = Assert.Throws<ObjectDisposedException>(() => subject.OnError(new InvalidOperationException()));
    }

    /// <summary>Called when [error disposed subscriber].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnErrorDisposedSubscriber()
    {
        Signal<int> subject = new();
        var error = false;
        subject.Subscribe(
            static _ => { },
            _ => error = true).Dispose();
        subject.OnError(new InvalidOperationException());
        await Assert.That(error).IsFalse();
    }

    /// <summary>Verifies the single observer fast path emits, terminates, and detaches cleanly.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SingleObserverSubscriptionReceivesLifecycleAndDetaches()
    {
        Signal<int> subject = new();
        RecordingWitness observer = new();
        var subscription = subject.Subscribe(observer);
        subject.OnNext(1);
        subject.OnCompleted();
        subject.OnNext(ValueTwo);
        await Assert.That(observer.Total).IsEqualTo(1);
        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(observer.Errors).IsEqualTo(0);
        await Assert.That(subject.HasObservers).IsFalse();
        subscription.Dispose();
        Signal<int> faulted = new();
        RecordingWitness faultObserver = new();
        using var faultSubscription = faulted.Subscribe(faultObserver);
        faulted.OnError(new InvalidOperationException());
        await Assert.That(faultObserver.Errors).IsEqualTo(1);
        await Assert.That(faulted.HasObservers).IsFalse();
    }

    /// <summary>Called when [error rethrows by default].</summary>
    [Test]
    public void OnErrorRethrowsByDefault()
    {
        Signal<int> subject = new();
        using var subscription = subject.Subscribe(static _ => { });
        _ = Assert.Throws<ArgumentException>(() => subject.OnError(new ArgumentException("subject error")));
    }

    /// <summary>Called when [error null throws].</summary>
    [Test]
    public void OnErrorNullThrows() => Assert.Throws<ArgumentNullException>(static () => new Signal<int>().OnError(null!));

    /// <summary>Subscribes the null throws.</summary>
    [Test]
    public void SubscribeNullThrows() => Assert.Throws<ArgumentNullException>(static () => new Signal<int>().Subscribe(null!));

    /// <summary>Subscribes the disposed throws.</summary>
    [Test]
    public void SubscribeDisposedThrows()
    {
        Signal<int> subject = new();
        subject.Dispose();
        _ = Assert.Throws<ObjectDisposedException>(() => subject.Subscribe(static _ => { }));
    }

    /// <summary>Subscribes the on completed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeOnCompleted()
    {
        Signal<int> subject = new();
        subject.OnCompleted();
        var completed = false;
        subject.Subscribe(
            static _ => { },
            () => completed = true).Dispose();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Subscribes the on error.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeOnError()
    {
        Signal<int> subject = new();
        subject.OnError(new InvalidOperationException());
        var error = false;
        _ = subject.Subscribe(
            static _ => { },
            _ => error = true);
        await Assert.That(error).IsTrue();
    }

    /// <summary>Subscribes action observers, converts to multi-observer dispatch, and removes each observer independently.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeActionObservers_DisposeIndependently()
    {
        Signal<int> subject = new();
        var first = 0;
        var second = 0;
        var firstSubscription = subject.Subscribe(i => first += i);
        await Assert.That(subject.HasObservers).IsTrue();
        var secondSubscription = subject.Subscribe(i => second += i);
        subject.OnNext(1);
        await Assert.That(first).IsEqualTo(1);
        await Assert.That(second).IsEqualTo(1);
        firstSubscription.Dispose();
        subject.OnNext(ValueTwo);
        await Assert.That(first).IsEqualTo(1);
        await Assert.That(second).IsEqualTo(ValueThree);
        await Assert.That(subject.HasObservers).IsTrue();
        secondSubscription.Dispose();
        subject.OnNext(ValueFour);
        await Assert.That(first).IsEqualTo(1);
        await Assert.That(second).IsEqualTo(ValueThree);
        await Assert.That(subject.HasObservers).IsFalse();
    }

    /// <summary>Subjects the where.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresUnreferencedCode("Tests the action-based Subscribe overload that carries trimming annotations.")]
    public async Task SubjectWhere()
    {
        Signal<int> subject = new();
        List<int> values = [];
        _ = subject.Keep(static i => i % EvenDivisor == 0).Subscribe(values.Add);
        subject.OnNext(1);
        subject.OnNext(ValueTwo);
        subject.OnNext(ValueThree);
        subject.Dispose();
        await Assert.That(values).IsEquivalentTo([ValueTwo]);
    }

    /// <summary>Subjects the select.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [RequiresUnreferencedCode("Tests the action-based Subscribe overload that carries trimming annotations.")]
    public async Task SubjectSelect()
    {
        Signal<int> subject = new();
        List<int> values = [];
        _ = subject.Map(static i => i * SelectMultiplier).Subscribe(values.Add);
        subject.OnNext(ValueTwo);
        subject.Dispose();
        await Assert.That(values).IsEquivalentTo([ValueFour]);
    }

    /// <summary>Subjects the buffer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubjectBuffer()
    {
        Signal<int> subject = new();
        List<int> result = [];
        _ = subject.Buffer(PairCount).Subscribe(i => result = [.. i]);
        subject.OnNext(1);
        subject.OnNext(ValueTwo);
        await Assert.That(result.SequenceEqual(FirstPair)).IsTrue();
        subject.OnNext(ValueThree);
        subject.OnNext(ValueFour);
        await Assert.That(result.SequenceEqual(SecondPair)).IsTrue();
        subject.OnNext(ValueFive);
        subject.OnNext(ValueSix);
        await Assert.That(result.SequenceEqual(ThirdPair)).IsTrue();
        subject.Dispose();
    }

    /// <summary>Subjects the buffer skip2.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubjectBufferTake2Skip2()
    {
        Signal<int> subject = new();
        List<int> result = [];
        _ = subject.Buffer(PairCount, SkipCount).Subscribe(i => result = [.. i]);
        subject.OnNext(1);
        subject.OnNext(ValueTwo);
        await Assert.That(result.SequenceEqual(FirstPair)).IsTrue();
        subject.OnNext(ValueThree);
        subject.OnNext(ValueFour);
        await Assert.That(result.SequenceEqual(FirstPair)).IsTrue();
        subject.OnNext(ValueFive);
        subject.OnNext(ValueSix);
        await Assert.That(result.SequenceEqual(ThirdPair)).IsTrue();
        subject.OnNext(ValueSeven);
        subject.OnNext(ValueEight);
        await Assert.That(result.SequenceEqual(ThirdPair)).IsTrue();
        subject.Dispose();
    }

    /// <summary>Subjects the rx void.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubjectRxVoid()
    {
        Signal<RxVoid> subject = new();
        List<RxVoid> result = [];
        _ = subject.Subscribe(result.Add);
        subject.OnNext(RxVoid.Default);
        await Assert.That(result.SequenceEqual(SingleRxVoid)).IsTrue();
        subject.OnNext(RxVoid.Default);
        await Assert.That(result.SequenceEqual(DoubleRxVoid)).IsTrue();
        subject.Dispose();
    }

    /// <summary>Verifies immediate core signals, range, zip, repeat, and observer failures cover remainders.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateCoreSignalsRangeZipRepeatAndObserverFailuresCoverRemainders()
    {
        var completed = 0;
        _ = Signal.None<int>(Sequencer.Immediate).Subscribe(static _ => { }, static ex => throw ex, () => completed++);
        _ = Signal.None(0).Subscribe(static _ => { }, static ex => throw ex, () => completed++);
        await Assert.That(completed).IsEqualTo(Two);
        List<int> returnValues = [];
        _ = Signal.Emit(FortyTwo, Sequencer.Immediate).Subscribe(returnValues.Add);
        int[] expectedReturnValues = [FortyTwo];
        await Assert.That(returnValues.SequenceEqual(expectedReturnValues)).IsTrue();
        List<string> throwErrors = [];
        _ = Signal.Fail<int>(new InvalidOperationException("immediate"), Sequencer.Immediate)
            .Subscribe(static _ => { }, ex => throwErrors.Add(ex.Message));
        _ = Signal.Fail(new InvalidOperationException("witness"), Sequencer.Immediate, 0)
            .Subscribe(static _ => { }, ex => throwErrors.Add(ex.Message));
        await Assert.That(throwErrors.SequenceEqual(ExpectedImmediateWitness)).IsTrue();
        var never = Signal.Silent(0);
        await Assert.That(((IRequireCurrentThread<int>)never).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(((IRequireCurrentThread<RxVoid>)Signal.EmitRxVoid()).IsRequiredSubscribeOnCurrentThread())
            .IsFalse();
        RxVoid firstRxVoid = default;
        RxVoid secondRxVoid = default;
        await Assert.That(firstRxVoid == secondRxVoid).IsTrue();
        await Assert.That(firstRxVoid != secondRxVoid).IsFalse();
        await AssertRepeatRangeAndZipSignalsForwardTheirSequences();
        await AssertImmediateSignalsNeverRequireCurrentThreadSubscription();
        AssertObserverFailuresPropagateOutOfSubscribe();
    }

    /// <summary>Covers signal subject subscriber churn, late subscriptions, disposal, and terminal no-op branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubjectsCoverMultipleSubscriberChurnLateTerminalsAndDisposalBranches()
    {
        const int IgnoredAfterCompletion = 2;
        Signal<int> subject = new();
        RecordingWitness<int> first = new();
        RecordingWitness<int> second = new();
        RecordingWitness<int> third = new();
        RecordingWitness<int> fourth = new();
        List<int> actionValues = [];
        using var action = subject.Subscribe(actionValues.Add);
        using var firstSubscription = subject.Subscribe(first);
        using var secondSubscription = subject.Subscribe(second);
        using var thirdSubscription = subject.Subscribe(third);
        using var fourthSubscription = subject.Subscribe(fourth);
        secondSubscription.Dispose();
        subject.OnNext(1);
        action.Dispose();
        subject.OnCompleted();
        subject.OnCompleted();
        subject.OnNext(IgnoredAfterCompletion);
        RecordingWitness<int> lateCompleted = new();
        subject.Subscribe(lateCompleted).Dispose();
        await Assert.That(first.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(first.Completed).IsEqualTo(1);
        await Assert.That(second.Values.Count).IsEqualTo(0);
        await Assert.That(third.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(fourth.Values.SequenceEqual([1])).IsTrue();
        await Assert.That(actionValues.SequenceEqual([1])).IsTrue();
        await Assert.That(lateCompleted.Completed).IsEqualTo(1);
        Signal<int> faulted = new();
        RecordingWitness<int> faultObserver = new();
        var actionFaults = 0;
        using var faultSubscription = faulted.Subscribe(faultObserver);
        InvalidOperationException fault = new("fault");
        faulted.OnError(fault);
        faulted.OnError(new InvalidOperationException("late"));
        RecordingWitness<int> lateFault = new();
        faulted.Subscribe(lateFault).Dispose();
        await Assert.That(lateFault.Errors[0]).IsSameReferenceAs(fault);
        await Assert.That(actionFaults).IsEqualTo(0);
        _ = Assert.Throws<ArgumentNullException>(() => faulted.OnError(null!));
        Signal<int> actionFaulted = new();
        using var faultingAction = actionFaulted.Subscribe(_ => actionFaults++);
        _ = Assert.Throws<InvalidOperationException>(() =>
            actionFaulted.OnError(new InvalidOperationException("action-fault")));
        Signal<int> disposedSubject = new();
        disposedSubject.Dispose();
        disposedSubject.Dispose();
        _ = Assert.Throws<ObjectDisposedException>(() => disposedSubject.Subscribe(new RecordingWitness<int>()));
        _ = Assert.Throws<ObjectDisposedException>(() => disposedSubject.OnNext(1));
    }

    /// <summary>Asserts the repeat, range, and range-zip signals forward their sequences and validate their observers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertRepeatRangeAndZipSignalsForwardTheirSequences()
    {
        var completed = 0;
        RepeatSignal<int> repeat = new(Seven, Three);
        List<int> repeatValues = [];
        await Assert.That(repeat.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        repeat.Subscribe(new RecordingWitness<int>()).Dispose();
        repeat.Subscribe(repeatValues.Add, static ex => throw ex, () => completed++).Dispose();
        int[] expectedRepeatValues = [Seven, Seven, Seven];
        await Assert.That(repeatValues.SequenceEqual(expectedRepeatValues)).IsTrue();
        _ = Assert.Throws<ArgumentNullException>(() => repeat.Subscribe((IObserver<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => repeat.Subscribe(null!, static _ => { }, static () => { }));
        RangeSignal range = new(One, Three);
        List<int> rangeValues = [];
        await Assert.That(range.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        range.Subscribe(new RecordingWitness<int>()).Dispose();
        range.Subscribe(rangeValues.Add, static ex => throw ex, () => completed++).Dispose();
        int[] expectedRangeValues = [One, Two, Three];
        await Assert.That(rangeValues.SequenceEqual(expectedRangeValues)).IsTrue();
        _ = Assert.Throws<ArgumentNullException>(() => range.Subscribe((IObserver<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => range.Subscribe(null!, static _ => { }, static () => { }));
        RangeZipSignal<int> zip = new(new(One, Three), new(Four, Three), static (left, right) => left + right);
        List<int> zipValues = [];
        await Assert.That(zip.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        zip.Subscribe(new RecordingWitness<int>()).Dispose();
        zip.Subscribe(zipValues.Add, static ex => throw ex, () => completed++).Dispose();
        int[] expectedZipValues = [Five, Seven, Nine];
        await Assert.That(zipValues.SequenceEqual(expectedZipValues)).IsTrue();
        _ = Assert.Throws<ArgumentNullException>(() => zip.Subscribe((IObserver<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => zip.Subscribe(null!, static _ => { }, static () => { }));
        await Assert.That(completed).IsEqualTo(Three);
    }

    /// <summary>Asserts every immediate signal implementation reports that it does not need current-thread subscription.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertImmediateSignalsNeverRequireCurrentThreadSubscription()
    {
        await Assert.That(new ImmediateReturnSignal<int>(One).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(
                new ImmediateThrowSignal<int>(new InvalidOperationException("fast"))
                    .IsRequiredSubscribeOnCurrentThread())
            .IsFalse();
        await Assert.That(ImmutableEmptySignal<int>.Instance.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(ImmutableNeverSignal<int>.Instance.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(
            ((IRequireCurrentThread<int>)ImmutableReturnInt32Signal.GetInt32Signals(One))
            .IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(
            new RangeConcatSignal([new(One, Two), new(Three, Two)]).IsRequiredSubscribeOnCurrentThread()).IsFalse();
        await Assert.That(new SignalsBaseProbe<int>(false).IsRequiredSubscribeOnCurrentThread()).IsFalse();
    }

    /// <summary>Asserts an observer that throws from a notification surfaces the exception at the subscribe call.</summary>
    private static void AssertObserverFailuresPropagateOutOfSubscribe()
    {
        _ = Assert.Throws<InvalidOperationException>(static () => Signal.Emit(One, Sequencer.Immediate)
            .Subscribe(new ThrowingWitness<int>(true))
            .Dispose());
        _ = Assert.Throws<InvalidOperationException>(static () => Signal.None<int>(Sequencer.Immediate)
            .Subscribe(new ThrowingWitness<int>(throwOnCompleted: true))
            .Dispose());
        _ = Assert.Throws<InvalidOperationException>(static () => Signal
            .Fail<int>(new InvalidOperationException("observer"), Sequencer.Immediate)
            .Subscribe(new ThrowingWitness<int>(throwOnError: true)).Dispose());
        _ = Assert.Throws<ArgumentNullException>(static () =>
            new ImmediateThrowSignal<int>(new InvalidOperationException("null-observer"))
                .Subscribe((IObserver<int>)null!));
    }

    /// <summary>A minimal <see cref="IRequireCurrentThread{T}"/> probe used to exercise the subscription routing.</summary>
    /// <typeparam name="T">The type of the signal sequence elements.</typeparam>
    private sealed class SignalsBaseProbe<T> : IRequireCurrentThread<T>
    {
        /// <summary>Whether subscription must occur on the current thread.</summary>
        private readonly bool _currentThreadRequired;

        /// <summary>Initializes a new instance of the <see cref="SignalsBaseProbe{T}"/> class.</summary>
        /// <param name="required">Whether subscription must occur on the current thread.</param>
        public SignalsBaseProbe(bool required) => _currentThreadRequired = required;

        /// <summary>Returns whether subscription must occur on the current thread.</summary>
        /// <returns>The configured flag.</returns>
        public bool IsRequiredSubscribeOnCurrentThread() => _currentThreadRequired;

        /// <summary>Subscribes via the shared routing helper.</summary>
        /// <param name="observer">The observer to subscribe.</param>
        /// <returns>The subscription.</returns>
        public IDisposable Subscribe(IObserver<T> observer) =>
            SignalSubscription.Subscribe(observer, _currentThreadRequired, SubscribeCore);

        /// <summary>Performs the core subscription by returning an empty disposable.</summary>
        /// <param name="observer">The observer to subscribe.</param>
        /// <param name="cancel">The disposable used to cancel the subscription.</param>
        /// <returns>An empty disposable.</returns>
        [SuppressMessage("Maintainability", "SST1461:Remove unread private parameters", Justification = "The signature is fixed by the delegate SignalSubscription.Subscribe expects.")]
        private static IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel) =>
            EmptyDisposable.Instance;
    }

    /// <summary>Records integer observer lifecycle calls.</summary>
    private sealed class RecordingWitness : IObserver<int>
    {
        /// <summary>Gets the total of observed values.</summary>
        public int Total { get; private set; }

        /// <summary>Gets the number of completion calls.</summary>
        public int Completed { get; private set; }

        /// <summary>Gets the number of error calls.</summary>
        public int Errors { get; private set; }

        /// <summary>Receives the next value.</summary>
        /// <param name = "value">The value.</param>
        public void OnNext(int value) => Total += value;

        /// <summary>Receives an error.</summary>
        /// <param name = "error">The error.</param>
        public void OnError(Exception error) => Errors++;

        /// <summary>Receives completion.</summary>
        public void OnCompleted() => Completed++;
    }
}
