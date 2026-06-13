// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;
using ReactiveUI.Primitives.Extensions.Operators;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests for <see cref = "CurrentValueSubject{T}"/>, <see cref = "SingleValueObservable{T}"/>, and the <see cref = "CachedObservables"/> singletons.</summary>
public partial class CurrentValueSubjectTests
{
    /// <summary>Initial value used by subject tests so the replay value is unambiguous.</summary>
    private const int InitialValue = 42;

    /// <summary>Second value that overwrites the initial value via <c>OnNext</c>.</summary>
    private const int SecondValue = 7;

    /// <summary>Third value used to assert later observers see only the most recent emission.</summary>
    private const int ThirdValue = 99;

    /// <summary>Verifies the subject replays its initial value to a new subscriber.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenNewSubscriber_ThenReceivesInitialValueImmediately()
    {
        using CurrentValueSubject<int> subject = new(InitialValue);
        List<int> results = [];
        using var sub = subject.Subscribe(results.Add);
        await Assert.That(results).IsCollectionEqualTo([InitialValue]);
        await Assert.That(subject.Value).IsEqualTo(InitialValue);
    }

    /// <summary>Verifies that OnNext updates the cached value and broadcasts to a single observer.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnNextSingleObserver_ThenValueAndBroadcastUpdate()
    {
        using CurrentValueSubject<int> subject = new(InitialValue);
        List<int> results = [];
        using var sub = subject.Subscribe(results.Add);
        subject.OnNext(SecondValue);
        await Assert.That(results).IsCollectionEqualTo([InitialValue, SecondValue]);
        await Assert.That(subject.Value).IsEqualTo(SecondValue);
    }

    /// <summary>Verifies that two observers both receive the initial value and subsequent emissions.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTwoObservers_ThenBothReceiveReplayAndOnNext()
    {
        using CurrentValueSubject<int> subject = new(InitialValue);
        List<int> first = [];
        List<int> second = [];
        using var sub1 = subject.Subscribe(first.Add);
        using var sub2 = subject.Subscribe(second.Add);
        subject.OnNext(SecondValue);
        await Assert.That(first).IsCollectionEqualTo([InitialValue, SecondValue]);
        await Assert.That(second).IsCollectionEqualTo([InitialValue, SecondValue]);
    }

    /// <summary>Verifies that a late subscriber after multiple OnNext receives only the most recent value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLateSubscriber_ThenReceivesOnlyLatest()
    {
        using CurrentValueSubject<int> subject = new(InitialValue);
        subject.OnNext(SecondValue);
        subject.OnNext(ThirdValue);
        List<int> results = [];
        using var sub = subject.Subscribe(results.Add);
        await Assert.That(results).IsCollectionEqualTo([ThirdValue]);
    }

    /// <summary>Verifies that disposing the subscription stops future deliveries to the disposed observer.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscriptionDisposed_ThenNoFurtherDeliveries()
    {
        using CurrentValueSubject<int> subject = new(InitialValue);
        List<int> results = [];
        var sub = subject.Subscribe(results.Add);
        sub.Dispose();
        subject.OnNext(SecondValue);
        await Assert.That(results).IsCollectionEqualTo([InitialValue]);
    }

    /// <summary>Verifies that OnCompleted terminates active subscribers and that late subscribers also see completion.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnCompleted_ThenObserversReceiveCompletion()
    {
        using CurrentValueSubject<int> subject = new(InitialValue);
        var completedFirst = false;
        var completedLate = false;
        List<int> lateValues = [];
        using var subFirst = subject.Subscribe(
            static _ => { },
            () => completedFirst = true);
        subject.OnCompleted();
        using var subLate = subject.Subscribe(lateValues.Add, () => completedLate = true);
        await Assert.That(completedFirst).IsTrue();
        await Assert.That(completedLate).IsTrue();

        // Late subscriber still sees the replayed value before completion.
        await Assert.That(lateValues).IsCollectionEqualTo([InitialValue]);
    }

    /// <summary>Verifies that OnError terminates active subscribers and that late subscribers replay the error.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnError_ThenObserversReceiveError()
    {
        InvalidOperationException expected = new("boom");
        using CurrentValueSubject<int> subject = new(InitialValue);
        Exception? firstError = null;
        Exception? lateError = null;
        using var subFirst = subject.Subscribe(
            static _ => { },
            ex => firstError = ex);
        subject.OnError(expected);
        using var subLate = subject.Subscribe(
            static _ => { },
            ex => lateError = ex);
        await Assert.That(firstError).IsEqualTo(expected);
        await Assert.That(lateError).IsEqualTo(expected);
    }

    /// <summary>Verifies that OnNext after disposal is silently ignored.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposedThenOnNext_ThenIgnored()
    {
        CurrentValueSubject<int> subject = new(InitialValue);
        List<int> results = [];
        using var sub = subject.Subscribe(results.Add);
        subject.Dispose();
        subject.OnNext(SecondValue);
        await Assert.That(results).IsCollectionEqualTo([InitialValue]);
    }

    /// <summary>Verifies that subscribing after disposal immediately errors with <see cref = "ObjectDisposedException"/>.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAfterDispose_ThenObjectDisposedExceptionDelivered()
    {
        CurrentValueSubject<int> subject = new(InitialValue);
        subject.Dispose();
        Exception? error = null;
        using var sub = subject.Subscribe(
            static _ => { },
            ex => error = ex);
        await Assert.That(error).IsTypeOf<ObjectDisposedException>();
    }

    /// <summary>Verifies that AsObservable returns a hide-the-observer view that still delivers values.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsObservable_ThenDeliversValuesButHidesObserverApi()
    {
        using CurrentValueSubject<int> subject = new(InitialValue);
        var view = subject.AsObservable();
        List<int> results = [];
        using var sub = view.Subscribe(results.Add);
        subject.OnNext(SecondValue);
        await Assert.That(results).IsCollectionEqualTo([InitialValue, SecondValue]);
        await Assert.That(view).IsNotTypeOf<CurrentValueSubject<int>>();
    }

    /// <summary>Verifies that <see cref = "SingleValueObservable{T}"/> emits exactly one value and completes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleValueObservableSubscribed_ThenEmitsOnceAndCompletes()
    {
        SingleValueObservable<int> observable = new(SecondValue);
        List<int> results = [];
        var completed = false;
        using var sub = observable.Subscribe(results.Add, () => completed = true);
        await Assert.That(results).IsCollectionEqualTo([SecondValue]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <see cref = "CachedObservables.UnitDefault"/> emits a single <see cref = "RxVoid"/> and completes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCachedUnitDefaultSubscribed_ThenEmitsUnitAndCompletes()
    {
        var count = 0;
        var completed = false;
        var localCount = 0;
        using var sub = CachedObservables.UnitDefault.Subscribe(_ => localCount++, () => completed = true);
        count = localCount;
        await Assert.That(Volatile.Read(ref count)).IsEqualTo(1);
        await Assert.That(completed).IsTrue();

        // The same singleton instance should service repeated calls without re-allocating.
        await Assert.That(CachedObservables.UnitDefault).IsSameReferenceAs(CachedObservables.UnitDefault);
    }
}
