// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>SubjectTests.</summary>
public class SignalTests
{
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
        var subject = new Signal<int>();
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
        var subject = new Signal<int>();
        subject.Dispose();
        Assert.Throws<ObjectDisposedException>(() => subject.OnNext(1));
    }

    /// <summary>Called when [next disposed subscriber].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnNextDisposedSubscriber()
    {
        var subject = new Signal<int>();
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
        var subject = new Signal<int>();
        var completed = false;
        using var subscription = subject.Subscribe(
            _ =>
        {
        },
            () => completed = true);
        subject.OnCompleted();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Called when [completed no op].</summary>
    [Test]
    public void OnCompleted_NoErrors()
    {
        var subject = new Signal<int>();
        using var subscription = subject.Subscribe(_ =>
        {
        });
        subject.OnCompleted();
    }

    /// <summary>Called when [completed once].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnCompletedOnce()
    {
        var subject = new Signal<int>();
        var completed = 0;
        using var subscription = subject.Subscribe(
            _ =>
        {
        },
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
        var subject = new Signal<int>();
        subject.Dispose();
        Assert.Throws<ObjectDisposedException>(subject.OnCompleted);
    }

    /// <summary>Called when [completed disposed subscriber].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnCompletedDisposedSubscriber()
    {
        var subject = new Signal<int>();
        var completed = false;
        subject.Subscribe(
            _ =>
        {
        },
            () => completed = true).Dispose();
        subject.OnCompleted();
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Called when [error].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnError()
    {
        var subject = new Signal<int>();
        var error = false;
        using var subscription = subject.Subscribe(
            _ =>
        {
        },
            _ => error = true);
        subject.OnError(new InvalidOperationException());
        await Assert.That(error).IsTrue();
    }

    /// <summary>Called when [error once].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnErrorOnce()
    {
        var subject = new Signal<int>();
        var errors = 0;
        using var subscription = subject.Subscribe(
            _ =>
        {
        },
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
        var subject = new Signal<int>();
        subject.Dispose();
        Assert.Throws<ObjectDisposedException>(() => subject.OnError(new InvalidOperationException()));
    }

    /// <summary>Called when [error disposed subscriber].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnErrorDisposedSubscriber()
    {
        var subject = new Signal<int>();
        var error = false;
        subject.Subscribe(
            _ =>
        {
        },
            _ => error = true).Dispose();
        subject.OnError(new InvalidOperationException());
        await Assert.That(error).IsFalse();
    }

    /// <summary>Verifies the single observer fast path emits, terminates, and detaches cleanly.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SingleObserverSubscriptionReceivesLifecycleAndDetaches()
    {
        var subject = new Signal<int>();
        var observer = new RecordingWitness();
        var subscription = subject.Subscribe(observer);
        subject.OnNext(1);
        subject.OnCompleted();
        subject.OnNext(ValueTwo);
        await Assert.That(observer.Total).IsEqualTo(1);
        await Assert.That(observer.Completed).IsEqualTo(1);
        await Assert.That(observer.Errors).IsEqualTo(0);
        await Assert.That(subject.HasObservers).IsFalse();
        subscription.Dispose();
        var faulted = new Signal<int>();
        var faultObserver = new RecordingWitness();
        using var faultSubscription = faulted.Subscribe(faultObserver);
        faulted.OnError(new InvalidOperationException());
        await Assert.That(faultObserver.Errors).IsEqualTo(1);
        await Assert.That(faulted.HasObservers).IsFalse();
    }

    /// <summary>Called when [error rethrows by default].</summary>
    [Test]
    public void OnErrorRethrowsByDefault()
    {
        var subject = new Signal<int>();
        using var subscription = subject.Subscribe(_ =>
        {
        });
        Assert.Throws<ArgumentException>(() => subject.OnError(new ArgumentException("subject error")));
    }

    /// <summary>Called when [error null throws].</summary>
    [Test]
    public void OnErrorNullThrows() => Assert.Throws<ArgumentNullException>(() => new Signal<int>().OnError(null!));

    /// <summary>Subscribes the null throws.</summary>
    [Test]
    public void SubscribeNullThrows() => Assert.Throws<ArgumentNullException>(() => new Signal<int>().Subscribe(null!));

    /// <summary>Subscribes the disposed throws.</summary>
    [Test]
    public void SubscribeDisposedThrows()
    {
        var subject = new Signal<int>();
        subject.Dispose();
        Assert.Throws<ObjectDisposedException>(() => subject.Subscribe(_ =>
{
}));
    }

    /// <summary>Subscribes the on completed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeOnCompleted()
    {
        var subject = new Signal<int>();
        subject.OnCompleted();
        var completed = false;
        subject.Subscribe(
            _ =>
        {
        },
            () => completed = true).Dispose();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Subscribes the on error.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeOnError()
    {
        var subject = new Signal<int>();
        subject.OnError(new InvalidOperationException());
        var error = false;
        subject.Subscribe(
            _ =>
        {
        },
            _ => error = true);
        await Assert.That(error).IsTrue();
    }

    /// <summary>Subscribes action observers, converts to multi-observer dispatch, and removes each observer independently.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeActionObservers_DisposeIndependently()
    {
        var subject = new Signal<int>();
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
        var subject = new Signal<int>();
        var values = new List<int>();
        subject.Keep(i => i % EvenDivisor == 0).Subscribe(values.Add);
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
        var subject = new Signal<int>();
        var values = new List<int>();
        subject.Map(i => i * SelectMultiplier).Subscribe(values.Add);
        subject.OnNext(ValueTwo);
        subject.Dispose();
        await Assert.That(values).IsEquivalentTo([ValueFour]);
    }

    /// <summary>Subjects the buffer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubjectBuffer()
    {
        var subject = new Signal<int>();
        var result = new List<int>();
        subject.Buffer(PairCount).Subscribe(i => result = [.. i]);
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
        var subject = new Signal<int>();
        var result = new List<int>();
        subject.Buffer(PairCount, SkipCount).Subscribe(i => result = [.. i]);
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
        var subject = new Signal<RxVoid>();
        var result = new List<RxVoid>();
        subject.Subscribe(result.Add);
        subject.OnNext(RxVoid.Default);
        await Assert.That(result.SequenceEqual(SingleRxVoid)).IsTrue();
        subject.OnNext(RxVoid.Default);
        await Assert.That(result.SequenceEqual(DoubleRxVoid)).IsTrue();
        subject.Dispose();
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
