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
        Assert.Equal(2, value);

        subscription.Dispose();

        subject.OnNext(1);
        Assert.Equal(2, value);
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

        var subscription = subject.Subscribe(_ => { }, () => completed = true);

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

        var subscription = subject.Subscribe(_ => { });

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

        var subscription = subject.Subscribe(_ => { }, () => completed++);

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

        var subscription = subject.Subscribe(_ => { }, _ => error = true);

        subject.OnError(new Exception());

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

        var subscription = subject.Subscribe(_ => { }, _ => errors++);

        subject.OnError(new Exception());

        Assert.Equal(1, errors);

        subject.OnError(new Exception());

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

        Assert.Throws<ObjectDisposedException>(() => subject.OnError(new Exception()));
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

        subject.OnError(new Exception());

        Assert.False(error);
    }

    /// <summary>
    /// Called when [error rethrows by default].
    /// </summary>
    [Test]
    public void OnErrorRethrowsByDefault()
    {
        var subject = new Signal<int>();

        var subs = subject.Subscribe(_ => { });

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
        subject.OnError(new Exception());
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
        subject.OnNext(2);

        Assert.Equal(1, first);
        Assert.Equal(3, second);
        Assert.True(subject.HasObservers);

        secondSubscription.Dispose();
        subject.OnNext(4);

        Assert.Equal(1, first);
        Assert.Equal(3, second);
        Assert.False(subject.HasObservers);
    }

    /// <summary>
    /// Subjects the where.
    /// </summary>
    [Test]
    public void SubjectWhere()
    {
        var subject = new Signal<int>();
        subject.Where(i => i % 2 == 0).Subscribe(i => Assert.Equal(2, i));
        subject.OnNext(1);
        subject.OnNext(2);
        subject.OnNext(3);
        subject.Dispose();
    }

    /// <summary>
    /// Subjects the select.
    /// </summary>
    [Test]
    public void SubjectSelect()
    {
        var subject = new Signal<int>();
        subject.Select(i => i * 2).Subscribe(i => Assert.Equal(4, i));
        subject.OnNext(2);
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
        subject.Buffer(2).Subscribe(i => result = [.. i]);
        subject.OnNext(1);
        subject.OnNext(2);
        Assert.Equal(new[] { 1, 2 }, result);
        subject.OnNext(3);
        subject.OnNext(4);
        Assert.Equal(new[] { 3, 4 }, result);
        subject.OnNext(5);
        subject.OnNext(6);
        Assert.Equal(new[] { 5, 6 }, result);
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
        subject.Buffer(2, 2).Subscribe(i => result = [.. i]);
        subject.OnNext(1);
        subject.OnNext(2);
        Assert.Equal(new[] { 1, 2 }, result);
        subject.OnNext(3);
        subject.OnNext(4);
        Assert.Equal(new[] { 1, 2 }, result);
        subject.OnNext(5);
        subject.OnNext(6);
        Assert.Equal(new[] { 5, 6 }, result);
        subject.OnNext(7);
        subject.OnNext(8);
        Assert.Equal(new[] { 5, 6 }, result);
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
        Assert.Equal(new[] { RxVoid.Default }, result);
        subject.OnNext(RxVoid.Default);
        Assert.Equal(new[] { RxVoid.Default, RxVoid.Default }, result);
        subject.Dispose();
    }
}
