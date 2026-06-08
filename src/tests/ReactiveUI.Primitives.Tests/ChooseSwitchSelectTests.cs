// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for the fused <see cref="LinqExtensions.Choose{TIn, TOut}"/> and <see cref="LinqExtensions.SwitchSelect{TSource, TResult}"/> projection operators.</summary>
public class ChooseSwitchSelectTests
{
    /// <summary>The value ten.</summary>
    private const int Ten = 10;

    /// <summary>The value eleven (stale inner value that must be ignored).</summary>
    private const int Eleven = 11;

    /// <summary>The value twenty.</summary>
    private const int Twenty = 20;

    /// <summary>The divisor used to select even values in the Choose test.</summary>
    private const int Two = 2;

    /// <summary>The expected single-occurrence count.</summary>
    private const int Once = 1;

    /// <summary>The first outer selector key.</summary>
    private const string KeyA = "a";

    /// <summary>The second outer selector key.</summary>
    private const string KeyB = "b";

    /// <summary>Shared error message.</summary>
    private const string Boom = "boom";

    /// <summary>Source values for the Choose test.</summary>
    private static readonly int[] _oneToFour = [1, 2, 3, 4];

    /// <summary>Expected even values chosen from <see cref="_oneToFour"/>.</summary>
    private static readonly int[] _evens = [2, 4];

    /// <summary>Expected forwarded values after switching inner sources.</summary>
    private static readonly int[] _tenThenTwenty = [Ten, Twenty];

    /// <summary>Expected single forwarded value before disposal.</summary>
    private static readonly int[] _tenOnly = [Ten];

    /// <summary>Verifies that Choose forwards only values whose chooser returns <c>HasValue = true</c>.</summary>
    [Test]
    public void ChooseForwardsOnlyChosenValues()
    {
        var values = new List<int>();

        Signal.FromEnumerable(_oneToFour)
            .Choose(x => (x % Two == 0, x))
            .Subscribe(values.Add);

        Assert.Equal(_evens, values);
    }

    /// <summary>Verifies that a chooser exception is forwarded as an error.</summary>
    [Test]
    public void ChooseForwardsChooserError()
    {
        Exception? error = null;

        Signal.FromEnumerable(_oneToFour)
            .Choose<int, int>(_ => throw new InvalidOperationException("boom"))
            .Subscribe(_ => { }, ex => error = ex, () => { });

        Assert.NotNull(error);
        Assert.True(error is InvalidOperationException);
    }

    /// <summary>
    /// Verifies that SwitchSelect skips null source values, mirrors the latest inner observable, and ignores
    /// values from a superseded inner observable.
    /// </summary>
    [Test]
    public void SwitchSelectFiltersNullSwitchesAndIgnoresStaleInner()
    {
        var outer = new Signal<string?>();
        var inner1 = new Signal<int>();
        var inner2 = new Signal<int>();
        var values = new List<int>();

        outer.SwitchSelect(key => key == KeyA ? inner1 : inner2)
            .Subscribe(values.Add);

        outer.OnNext(null); // skipped (null)
        outer.OnNext(KeyA); // subscribe inner1
        inner1.OnNext(Ten); // forwarded
        outer.OnNext(KeyB); // switch to inner2; inner1 superseded
        inner1.OnNext(Eleven); // stale -> ignored
        inner2.OnNext(Twenty); // forwarded

        Assert.Equal(_tenThenTwenty, values);
    }

    /// <summary>Verifies that SwitchSelect completes only once both the outer and the active inner have completed.</summary>
    [Test]
    public void SwitchSelectCompletesAfterOuterAndInner()
    {
        var outer = new Signal<string?>();
        var inner = new Signal<int>();
        var completed = 0;

        outer.SwitchSelect(_ => inner)
            .Subscribe(_ => { }, ex => throw ex, () => completed++);

        outer.OnNext(KeyA); // active inner
        outer.OnCompleted(); // outer done, inner still active -> not complete
        Assert.Equal(0, completed);
        inner.OnCompleted(); // now complete
        Assert.Equal(Once, completed);
    }

    /// <summary>Verifies that a source error is forwarded through Choose.</summary>
    [Test]
    public void ChooseForwardsSourceError()
    {
        var source = new Signal<int>();
        Exception? error = null;

        source.Choose(x => (true, x)).Subscribe(_ => { }, ex => error = ex, () => { });
        source.OnError(new InvalidOperationException(Boom));

        Assert.True(error is InvalidOperationException);
    }

    /// <summary>Verifies that a selector exception terminates SwitchSelect with an error.</summary>
    [Test]
    public void SwitchSelectForwardsSelectorError()
    {
        var outer = new Signal<string?>();
        Exception? error = null;

        outer.SwitchSelect<string, int>(_ => throw new InvalidOperationException(Boom))
            .Subscribe(_ => { }, ex => error = ex, () => { });
        outer.OnNext(KeyA);

        Assert.True(error is InvalidOperationException);
    }

    /// <summary>Verifies that an outer error terminates SwitchSelect.</summary>
    [Test]
    public void SwitchSelectForwardsOuterError()
    {
        var outer = new Signal<string?>();
        var inner = new Signal<int>();
        Exception? error = null;

        outer.SwitchSelect(_ => inner).Subscribe(_ => { }, ex => error = ex, () => { });
        outer.OnNext(KeyA);
        outer.OnError(new InvalidOperationException(Boom));

        Assert.True(error is InvalidOperationException);
    }

    /// <summary>Verifies that an inner error terminates SwitchSelect.</summary>
    [Test]
    public void SwitchSelectForwardsInnerError()
    {
        var outer = new Signal<string?>();
        var inner = new Signal<int>();
        Exception? error = null;

        outer.SwitchSelect(_ => inner).Subscribe(_ => { }, ex => error = ex, () => { });
        outer.OnNext(KeyA);
        inner.OnError(new InvalidOperationException(Boom));

        Assert.True(error is InvalidOperationException);
    }

    /// <summary>Verifies that disposing SwitchSelect tears down the outer and inner subscriptions.</summary>
    [Test]
    public void SwitchSelectDisposeUnsubscribes()
    {
        var outer = new Signal<string?>();
        var inner = new Signal<int>();
        var values = new List<int>();

        var subscription = outer.SwitchSelect(_ => inner).Subscribe(values.Add);
        outer.OnNext(KeyA);
        inner.OnNext(Ten);
        subscription.Dispose();
        inner.OnNext(Eleven); // disposed -> ignored
        outer.OnNext(KeyB); // disposed -> ignored

        Assert.Equal(_tenOnly, values);
    }

    /// <summary>Verifies completion when the inner completes before the outer.</summary>
    [Test]
    public void SwitchSelectCompletesWhenInnerThenOuterComplete()
    {
        var outer = new Signal<string?>();
        var inner = new Signal<int>();
        var completed = 0;

        outer.SwitchSelect(_ => inner).Subscribe(_ => { }, ex => throw ex, () => completed++);

        outer.OnNext(KeyA);
        inner.OnCompleted(); // inner done; outer still open -> not complete
        Assert.Equal(0, completed);
        outer.OnCompleted(); // outer done, no active inner -> complete
        Assert.Equal(Once, completed);
    }

    /// <summary>Verifies completion when the outer completes before any value is emitted.</summary>
    [Test]
    public void SwitchSelectCompletesWhenOuterCompletesWithNoValue()
    {
        var outer = new Signal<string?>();
        var completed = 0;

        outer.SwitchSelect(_ => new Signal<int>())
            .Subscribe(_ => { }, ex => throw ex, () => completed++);
        outer.OnCompleted();

        Assert.Equal(Once, completed);
    }

    /// <summary>
    /// Verifies the SwitchSelect race guards drop notifications from a superseded inner observable and
    /// from the outer/active-inner sources after disposal — the defensive early-returns that a
    /// well-behaved (unsubscribing) source would otherwise hide.
    /// </summary>
    [Test]
    public void SwitchSelectGuardsIgnoreStaleAndPostDisposeNotifications()
    {
        var outer = new ManualObservable<string?>();
        var inner1 = new ManualObservable<int>();
        var inner2 = new ManualObservable<int>();
        var values = new List<int>();
        Exception? error = null;
        var completed = 0;

        var subscription = outer
            .SwitchSelect(key => key == KeyA ? inner1 : inner2)
            .Subscribe(values.Add, ex => error = ex, () => completed++);

        outer.Next(KeyA); // inner1 active
        outer.Next(KeyB); // inner2 active; inner1 now superseded

        // Superseded inner1 (its id != the latest): every notification hits the stale guard.
        inner1.Next(Eleven);
        inner1.Error(new InvalidOperationException(Boom));
        inner1.Complete();

        subscription.Dispose();
        subscription.Dispose(); // idempotent: the second dispose hits the disposed guard

        // After disposal every outer and active-inner notification hits the disposed guard.
        outer.Next(KeyA);
        outer.Error(new InvalidOperationException(Boom));
        outer.Complete();
        inner2.Next(Twenty);
        inner2.Error(new InvalidOperationException(Boom));
        inner2.Complete();

        Assert.Equal(0, values.Count);
        Assert.True(error is null);
        Assert.Equal(0, completed);
    }

    /// <summary>Verifies argument validation for both operators and their subscriptions.</summary>
    [Test]
    public void NullArgumentsThrow()
    {
        Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.Choose<int, int>(x => (true, x)));
        Assert.Throws<ArgumentNullException>(() => Signal.FromEnumerable(_oneToFour).Choose<int, int>(null!));
        Assert.Throws<ArgumentNullException>(() => Signal.FromEnumerable(_oneToFour).Choose(x => (true, x)).Subscribe((IObserver<int>)null!));
        Assert.Throws<ArgumentNullException>(() => default(IObservable<string?>)!.SwitchSelect(_ => Signal.None<int>()));
        Assert.Throws<ArgumentNullException>(() => new Signal<string?>().SwitchSelect<string, int>(null!));
        Assert.Throws<ArgumentNullException>(() => new Signal<string?>().SwitchSelect(_ => Signal.None<int>()).Subscribe((IObserver<int>)null!));
    }

    /// <summary>
    /// An observable whose subscription deliberately ignores disposal, retaining its observer so a test
    /// can keep pushing notifications after the operator has switched away from it or disposed it. A
    /// well-behaved source unsubscribes on either event; this misbehaving source is what the operator's
    /// race guards exist to defend against.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    private sealed class ManualObservable<T> : IObservable<T>
    {
        /// <summary>The observer retained from the most recent subscription.</summary>
        private IObserver<T>? _observer;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            _observer = observer;
            return EmptyDisposable.Instance;
        }

        /// <summary>Pushes a value to the retained observer.</summary>
        /// <param name="value">The value to push.</param>
        public void Next(T value) => _observer?.OnNext(value);

        /// <summary>Pushes an error to the retained observer.</summary>
        /// <param name="exception">The error to push.</param>
        public void Error(Exception exception) => _observer?.OnError(exception);

        /// <summary>Pushes completion to the retained observer.</summary>
        public void Complete() => _observer?.OnCompleted();
    }
}
