// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;
using TUnit.Core;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// SignalsCreateTests.
/// </summary>
public class SignalCreateTests
{
    /// <summary>
    /// Creates the argument checking.
    /// </summary>
    [Test]
    public void Create_ArgumentChecking()
    {
        Assert.Throws<ArgumentNullException>(() => Signal.Create(default(Func<IObserver<int>, IDisposable>)));

        Assert.Throws<ArgumentNullException>(() => Signal.Create<int>(default).Subscribe(null));
    }

    /// <summary>
    /// Creates the null coalescing action.
    /// </summary>
    [Test]
    public void Create_NullCoalescingAction()
    {
        var xs = Signal.Create<int>(o =>
        {
            o.OnNext(42);
            return Disposable.Create(default!);
        });

        var lst = new List<int>();
        var d = xs.Subscribe(lst.Add);
        d.Dispose();

        Assert.True(lst.SequenceEqual([42]));
    }

    /// <summary>
    /// Creates the exception.
    /// </summary>
    [Test]
    public void Create_Exception() =>
        Assert.Throws<InvalidOperationException>(() =>
               Signal.Create(new Func<IObserver<int>, IDisposable>(_ => throw new InvalidOperationException())).Subscribe());

    /// <summary>
    /// Creates the observer throws.
    /// </summary>
    [Test]
    public void Create_ObserverThrows()
    {
        Assert.Throws<InvalidOperationException>(() =>
            Signal.Create<int>(o =>
            {
                o.OnNext(1);
                return Disposable.Empty;
            }).Subscribe(x => throw new InvalidOperationException()));
        Assert.Throws<InvalidOperationException>(() =>
            Signal.Create<int>(o =>
            {
                o.OnError(new Exception());
                return Disposable.Empty;
            }).Subscribe(x => { }, ex => throw new InvalidOperationException()));
        Assert.Throws<InvalidOperationException>(() =>
            Signal.Create<int>(o =>
            {
                o.OnCompleted();
                return Disposable.Empty;
            }).Subscribe(x => { }, ex => { }, () => throw new InvalidOperationException()));
    }

    /// <summary>
    /// Creates the with disposable argument checking.
    /// </summary>
    [Test]
    public void CreateWithDisposable_ArgumentChecking()
    {
        Assert.Throws<ArgumentNullException>(() => Signal.Create(default(Func<IObserver<int>, IDisposable>)));
        Assert.Throws<ArgumentNullException>(() => Signal.Create<int>(_ => DummyDisposable.Instance).Subscribe(null));
        Assert.Throws<ArgumentNullException>(() => Signal.Create<int>(o =>
        {
            o.OnError(null);
            return DummyDisposable.Instance;
        }).Subscribe(null));
    }

    /// <summary>
    /// Creates the with disposable null coalescing action.
    /// </summary>
    [Test]
    public void CreateWithDisposable_NullCoalescingAction()
    {
        var xs = Signal.Create<int>(o =>
        {
            o.OnNext(42);
            return default!;
        });

        var lst = new List<int>();
        var d = xs.Subscribe(lst.Add);
        d.Dispose();

        Assert.True(lst.SequenceEqual([42]));
    }

    /// <summary>
    /// Creates the with disposable exception.
    /// </summary>
    [Test]
    public void CreateWithDisposable_Exception() =>
        Assert.Throws<InvalidOperationException>(() =>
               Signal.Create(new Func<IObserver<int>, IDisposable>(_ => throw new InvalidOperationException())).Subscribe());
}
