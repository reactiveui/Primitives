// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using ReactiveUI.Primitives.Signals;
using TUnit.Core;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// SubjectTests.
/// </summary>
public class SignalTests
{
    /// <summary>
    /// Number of values expected in pair buffers.
    /// </summary>
    private const int PairCount = 2;

    /// <summary>
    /// Number of values skipped between non-overlapping buffers.
    /// </summary>
    private const int SkipCount = 2;

    /// <summary>
    /// Test value two.
    /// </summary>
    private const int ValueTwo = 2;

    /// <summary>
    /// Test value three.
    /// </summary>
    private const int ValueThree = 3;

    /// <summary>
    /// Test value four.
    /// </summary>
    private const int ValueFour = 4;

    /// <summary>
    /// Test value five.
    /// </summary>
    private const int ValueFive = 5;

    /// <summary>
    /// Test value six.
    /// </summary>
    private const int ValueSix = 6;

    /// <summary>
    /// Test value seven.
    /// </summary>
    private const int ValueSeven = 7;

    /// <summary>
    /// Test value eight.
    /// </summary>
    private const int ValueEight = 8;

    /// <summary>
    /// Divisor used by even-value filters.
    /// </summary>
    private const int EvenDivisor = 2;

    /// <summary>
    /// Multiplier used by select projection tests.
    /// </summary>
    private const int SelectMultiplier = 2;

    /// <summary>
    /// Expected first pair of buffered values.
    /// </summary>
    private static readonly int[] FirstPair = [1, ValueTwo];

    /// <summary>
    /// Expected second pair of buffered values.
    /// </summary>
    private static readonly int[] SecondPair = [ValueThree, ValueFour];

    /// <summary>
    /// Expected third pair of buffered values.
    /// </summary>
    private static readonly int[] ThirdPair = [ValueFive, ValueSix];

    /// <summary>
    /// Expected single RxVoid notification.
    /// </summary>
    private static readonly RxVoid[] SingleRxVoid = [RxVoid.Default];

    /// <summary>
    /// Expected pair of RxVoid notifications.
    /// </summary>
    private static readonly RxVoid[] DoubleRxVoid = [RxVoid.Default, RxVoid.Default];

    /// <summary>
    /// Called when [next].
    /// </summary>
    [Test]
    public void OnNext()
    {
        var subject = new Signal<int>();
        var value = 0;

        var subscription = subject.Subscribe(i => value += i);

        subject.OnNext(1);
        Assert.Equal(1, value);

        subject.OnNext(1);
        Assert.Equal(PairCount, value);

        subscription.Dispose();

        subject.OnNext(1);
        Assert.Equal(PairCount, value);
    }

    /// <summary>
    /// Called when [next disposed].
    /// </summary>
    [Test]
    public void OnNextDisposed()
    {
        var subject = new Signal<int>();

        subject.Dispose();

        Assert.Throws<ObjectDisposedException>(() => subject.OnNext(1));
    }

    /// <summary>
    /// Called when [next disposed subscriber].
    /// </summary>
    [Test]
    public void OnNextDisposedSubscriber()
    {
        var subject = new Signal<int>();
        var value = 0;

        subject.Subscribe(i => value += i).Dispose();

        subject.OnNext(1);

        Assert.Equal(0, value);
    }

    /// <summary>
    /// Called when [completed].
    /// </summary>
    [Test]
    public void OnCompleted()
    {
        var subject = new Signal<int>();
        var completed = false;

        using var subscription = subject.Subscribe(_ => { }, () => completed = true);

        subject.OnCompleted();

        Assert.True(completed);
    }

    /// <summary>
    /// Called when [completed no op].
    /// </summary>
    [Test]
    public void OnCompleted_NoErrors()
    {
        var subject = new Signal<int>();

        using var subscription = subject.Subscribe(_ => { });

        subject.OnCompleted();
    }

    /// <summary>
    /// Called when [completed once].
    /// </summary>
    [Test]
    public void OnCompletedOnce()
    {
        var subject = new Signal<int>();
        var completed = 0;

        using var subscription = subject.Subscribe(_ => { }, () => completed++);

        subject.OnCompleted();

        Assert.Equal(1, completed);

        subject.OnCompleted();

        Assert.Equal(1, completed);
    }

    /// <summary>
    /// Called when [completed disposed].
    /// </summary>
    [Test]
    public void OnCompletedDisposed()
    {
        var subject = new Signal<int>();

        subject.Dispose();

        Assert.Throws<ObjectDisposedException>(subject.OnCompleted);
    }

    /// <summary>
    /// Called when [completed disposed subscriber].
    /// </summary>
    [Test]
    public void OnCompletedDisposedSubscriber()
    {
        var subject = new Signal<int>();
        var completed = false;

        subject.Subscribe(_ => { }, () => completed = true).Dispose();

        subject.OnCompleted();

        Assert.False(completed);
    }

    /// <summary>
    /// Called when [error].
    /// </summary>
    [Test]
    public void OnError()
    {
        var subject = new Signal<int>();
        var error = false;

        using var subscription = subject.Subscribe(_ => { }, _ => error = true);

        subject.OnError(new InvalidOperationException());

        Assert.True(error);
    }

    /// <summary>
    /// Called when [error once].
    /// </summary>
    [Test]
    public void OnErrorOnce()
    {
        var subject = new Signal<int>();
        var errors = 0;

        using var subscription = subject.Subscribe(_ => { }, _ => errors++);

        subject.OnError(new InvalidOperationException());

        Assert.Equal(1, errors);

        subject.OnError(new InvalidOperationException());

        Assert.Equal(1, errors);
    }

    /// <summary>
    /// Called when [error disposed].
    /// </summary>
    [Test]
    public void OnErrorDisposed()
    {
        var subject = new Signal<int>();

        subject.Dispose();

        Assert.Throws<ObjectDisposedException>(() => subject.OnError(new InvalidOperationException()));
    }

    /// <summary>
    /// Called when [error disposed subscriber].
    /// </summary>
    [Test]
    public void OnErrorDisposedSubscriber()
    {
        var subject = new Signal<int>();
        var error = false;

        subject.Subscribe(_ => { }, _ => error = true).Dispose();

        subject.OnError(new InvalidOperationException());

        Assert.False(error);
    }

    /// <summary>
    /// Called when [error rethrows by default].
    /// </summary>
    [Test]
    public void OnErrorRethrowsByDefault()
    {
        var subject = new Signal<int>();

        using var subscription = subject.Subscribe(_ => { });

        Assert.Throws<ArgumentException>(() => subject.OnError(new ArgumentException()));
    }

    /// <summary>
    /// Called when [error null throws].
    /// </summary>
    [Test]
    public void OnErrorNullThrows() =>
        Assert.Throws<ArgumentNullException>(() => new Signal<int>().OnError(null!));

    /// <summary>
    /// Subscribes the null throws.
    /// </summary>
    [Test]
    public void SubscribeNullThrows() =>
        Assert.Throws<ArgumentNullException>(() => new Signal<int>().Subscribe(null!));

    /// <summary>
    /// Subscribes the disposed throws.
    /// </summary>
    [Test]
    public void SubscribeDisposedThrows()
    {
        var subject = new Signal<int>();

        subject.Dispose();

        Assert.Throws<ObjectDisposedException>(() => subject.Subscribe(_ => { }));
    }

    /// <summary>
    /// Subscribes the on completed.
    /// </summary>
    [Test]
    public void SubscribeOnCompleted()
    {
        var subject = new Signal<int>();
        subject.OnCompleted();
        var completed = false;

        subject.Subscribe(_ => { }, () => completed = true).Dispose();

        Assert.True(completed);
    }

    /// <summary>
    /// Subscribes the on error.
    /// </summary>
    [Test]
    public void SubscribeOnError()
    {
        var subject = new Signal<int>();
        subject.OnError(new InvalidOperationException());
        var error = false;

        subject.Subscribe(_ => { }, _ => error = true);

        Assert.True(error);
    }

    /// <summary>
    /// Subscribes action observers, converts to multi-observer dispatch, and removes each observer independently.
    /// </summary>
    [Test]
    public void SubscribeActionObservers_DisposeIndependently()
    {
        var subject = new Signal<int>();
        var first = 0;
        var second = 0;

        var firstSubscription = subject.Subscribe(i => first += i);
        Assert.True(subject.HasObservers);

        var secondSubscription = subject.Subscribe(i => second += i);
        subject.OnNext(1);

        Assert.Equal(1, first);
        Assert.Equal(1, second);

        firstSubscription.Dispose();
        subject.OnNext(ValueTwo);

        Assert.Equal(1, first);
        Assert.Equal(ValueThree, second);
        Assert.True(subject.HasObservers);

        secondSubscription.Dispose();
        subject.OnNext(ValueFour);

        Assert.Equal(1, first);
        Assert.Equal(ValueThree, second);
        Assert.False(subject.HasObservers);
    }

    /// <summary>
    /// Subjects the where.
    /// </summary>
    [Test]
    public void SubjectWhere()
    {
        var subject = new Signal<int>();
        subject.Where(i => i % EvenDivisor == 0).Subscribe(i => Assert.Equal(ValueTwo, i));
        subject.OnNext(1);
        subject.OnNext(ValueTwo);
        subject.OnNext(ValueThree);
        subject.Dispose();
    }

    /// <summary>
    /// Subjects the select.
    /// </summary>
    [Test]
    public void SubjectSelect()
    {
        var subject = new Signal<int>();
        subject.Select(i => i * SelectMultiplier).Subscribe(i => Assert.Equal(ValueFour, i));
        subject.OnNext(ValueTwo);
        subject.Dispose();
    }

    /// <summary>
    /// Subjects the buffer.
    /// </summary>
    [Test]
    public void SubjectBuffer()
    {
        var subject = new Signal<int>();
        var result = new List<int>();
        subject.Buffer(PairCount).Subscribe(i => result = [.. i]);
        subject.OnNext(1);
        subject.OnNext(ValueTwo);
        Assert.Equal(FirstPair, result);
        subject.OnNext(ValueThree);
        subject.OnNext(ValueFour);
        Assert.Equal(SecondPair, result);
        subject.OnNext(ValueFive);
        subject.OnNext(ValueSix);
        Assert.Equal(ThirdPair, result);
        subject.Dispose();
    }

    /// <summary>
    /// Subjects the buffer skip2.
    /// </summary>
    [Test]
    public void SubjectBufferTake2Skip2()
    {
        var subject = new Signal<int>();
        var result = new List<int>();
        subject.Buffer(PairCount, SkipCount).Subscribe(i => result = [.. i]);
        subject.OnNext(1);
        subject.OnNext(ValueTwo);
        Assert.Equal(FirstPair, result);
        subject.OnNext(ValueThree);
        subject.OnNext(ValueFour);
        Assert.Equal(FirstPair, result);
        subject.OnNext(ValueFive);
        subject.OnNext(ValueSix);
        Assert.Equal(ThirdPair, result);
        subject.OnNext(ValueSeven);
        subject.OnNext(ValueEight);
        Assert.Equal(ThirdPair, result);
        subject.Dispose();
    }

    /// <summary>
    /// Subjects the rx void.
    /// </summary>
    [Test]
    public void SubjectRxVoid()
    {
        var subject = new Signal<RxVoid>();
        var result = new List<RxVoid>();
        subject.Subscribe(result.Add);
        subject.OnNext(RxVoid.Default);
        Assert.Equal(SingleRxVoid, result);
        subject.OnNext(RxVoid.Default);
        Assert.Equal(DoubleRxVoid, result);
        subject.Dispose();
    }
}
