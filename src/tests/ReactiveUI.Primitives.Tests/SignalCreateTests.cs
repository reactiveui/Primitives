// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>SignalsCreateTests.</summary>
public class SignalCreateTests
{
    /// <summary>Value emitted by create-signal tests.</summary>
    private const int CreatedValue = 42;

    /// <summary>Creates the argument checking.</summary>
    [Test]
    public void Create_ArgumentChecking()
    {
        Assert.Throws<ArgumentNullException>(() => Signal.Create(default(Func<IObserver<int>, IDisposable>)!));

        Assert.Throws<ArgumentNullException>(() => Signal.Create<int>(null!).Subscribe(null!));
    }

    /// <summary>Creates the null coalescing action.</summary>
    [Test]
    public void Create_NullCoalescingAction()
    {
        var xs = Signal.Create<int>(o =>
        {
            o.OnNext(CreatedValue);
            return new ActionDisposable(null!);
        });

        var lst = new List<int>();
        var d = xs.Subscribe(lst.Add);
        d.Dispose();

        Assert.True(lst.SequenceEqual([CreatedValue]));
    }

    /// <summary>Creates the exception.</summary>
    [Test]
    public void Create_Exception() =>
        Assert.Throws<InvalidOperationException>(() =>
               Signal.Create(new Func<IObserver<int>, IDisposable>(_ => throw new InvalidOperationException())).Subscribe());

    /// <summary>Creates the observer throws.</summary>
    [Test]
    public void Create_ObserverThrows()
    {
        Assert.Throws<InvalidOperationException>(() =>
            Signal.Create<int>(o =>
            {
                o.OnNext(1);
                return EmptyDisposable.Instance;
            }).Subscribe(x => throw new InvalidOperationException()));
        Assert.Throws<InvalidOperationException>(() =>
            Signal.Create<int>(o =>
            {
                o.OnError(new InvalidOperationException("source"));
                return EmptyDisposable.Instance;
            }).Subscribe(x => { }, ex => throw new InvalidOperationException()));
        Assert.Throws<InvalidOperationException>(() =>
            Signal.Create<int>(o =>
            {
                o.OnCompleted();
                return EmptyDisposable.Instance;
            }).Subscribe(x => { }, ex => { }, () => throw new InvalidOperationException()));
    }

    /// <summary>Creates the with disposable argument checking.</summary>
    [Test]
    public void CreateWithDisposable_ArgumentChecking()
    {
        Assert.Throws<ArgumentNullException>(() => Signal.Create(default(Func<IObserver<int>, IDisposable>)!));
        Assert.Throws<ArgumentNullException>(() => Signal.Create<int>(_ => DummyDisposable.Instance).Subscribe(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.Create<int>(o =>
        {
            o.OnError(null!);
            return DummyDisposable.Instance;
        }).Subscribe(null!));
    }

    /// <summary>Creates the with disposable null coalescing action.</summary>
    [Test]
    public void CreateWithDisposable_NullCoalescingAction()
    {
        var xs = Signal.Create<int>(o =>
        {
            o.OnNext(CreatedValue);
            return null!;
        });

        var lst = new List<int>();
        var d = xs.Subscribe(lst.Add);
        d.Dispose();

        Assert.True(lst.SequenceEqual([CreatedValue]));
    }

    /// <summary>Creates the with disposable exception.</summary>
    [Test]
    public void CreateWithDisposable_Exception() =>
        Assert.Throws<InvalidOperationException>(() =>
               Signal.Create(new Func<IObserver<int>, IDisposable>(_ => throw new InvalidOperationException())).Subscribe());

    /// <summary>Anonymous signals validate the subscribe delegate.</summary>
    [Test]
    public void AnonymousSignal_ArgumentChecking()
    {
        Assert.Throws<ArgumentNullException>(() => _ = new AnonymousSignal<int>(null!));

        var signal = new AnonymousSignal<int>(_ => EmptyDisposable.Instance);
        Assert.Throws<ArgumentNullException>(() => signal.Subscribe(null!));
    }

    /// <summary>Anonymous signals forward subscriptions to the supplied delegate.</summary>
    [Test]
    public void AnonymousSignal_ForwardsObserverAndDisposable()
    {
        var disposed = false;
        IObserver<int>? seenObserver = null;
        var expectedDisposable = new ActionDisposable(() => disposed = true);
        var signal = new AnonymousSignal<int>(observer =>
        {
            seenObserver = observer;
            observer.OnNext(CreatedValue);
            observer.OnCompleted();
            return expectedDisposable;
        });
        var observer = new Recorder<int>();

        var disposable = signal.Subscribe(observer);
        disposable.Dispose();

        Assert.True(ReferenceEquals(observer, seenObserver));
        Assert.Same(expectedDisposable, disposable);
        Assert.True(observer.Values.SequenceEqual([CreatedValue]));
        Assert.Equal(1, observer.Completed);
        Assert.True(disposed);
    }

    /// <summary>Anonymous signals use the empty disposable when a delegate returns null.</summary>
    [Test]
    public void AnonymousSignal_ReturnsEmptyDisposableForNullDelegateResult()
    {
        var signal = new AnonymousSignal<int>(_ => null!);

        var disposable = signal.Subscribe(new Recorder<int>());

        Assert.Same(EmptyDisposable.Instance, disposable);
    }

    /// <summary>Records observer notifications.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class Recorder<T> : IObserver<T>
    {
        /// <summary>Gets observed values.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets the number of completion notifications.</summary>
        public int Completed { get; private set; }

        /// <inheritdoc/>
        public void OnCompleted() => Completed++;

        /// <inheritdoc/>
        public void OnError(Exception error) => throw error;

        /// <inheritdoc/>
        public void OnNext(T value) => Values.Add(value);
    }
}
