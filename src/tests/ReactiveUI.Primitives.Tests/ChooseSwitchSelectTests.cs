// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for the fused <see cref = "LinqExtensions.Choose{TIn, TOut}"/> and <see cref = "LinqExtensions.SwitchSelect{TSource, TResult}"/> projection operators.</summary>
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

    /// <summary>Expected even values chosen from <see cref = "_oneToFour"/>.</summary>
    private static readonly int[] _evens = [2, 4];

    /// <summary>Expected forwarded values after switching inner sources.</summary>
    private static readonly int[] _tenThenTwenty = [Ten, Twenty];

    /// <summary>Expected single forwarded value before disposal.</summary>
    private static readonly int[] _tenOnly = [Ten];

    /// <summary>Verifies that Choose forwards only values whose chooser returns <c>HasValue = true</c>.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ChooseForwardsOnlyChosenValues()
    {
        List<int> values = [];
        _ = Signal.FromEnumerable(_oneToFour).Choose(static x => (x % Two == 0, x)).Subscribe(values.Add);
        await Assert.That(values.SequenceEqual(_evens)).IsTrue();
    }

    /// <summary>Verifies that a chooser exception is forwarded as an error.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ChooseForwardsChooserError()
    {
        Exception? error = null;
        _ = Signal.FromEnumerable(_oneToFour).Choose<int, int>(static _ => throw new InvalidOperationException("boom"))
            .Subscribe(
                static _ => { },
                ex => error = ex,
                static () => { });
        await Assert.That(error).IsNotNull();
        await Assert.That(error is InvalidOperationException).IsTrue();
    }

    /// <summary>
    /// Verifies that SwitchSelect skips null source values, mirrors the latest inner observable, and ignores
    /// values from a superseded inner observable.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchSelectFiltersNullSwitchesAndIgnoresStaleInner()
    {
        Signal<string?> outer = new();
        Signal<int> inner1 = new();
        Signal<int> inner2 = new();
        List<int> values = [];
        _ = outer.SwitchSelect(key => key == KeyA ? inner1 : inner2).Subscribe(values.Add);
        outer.OnNext(null); // skipped (null)
        outer.OnNext(KeyA); // subscribe inner1
        inner1.OnNext(Ten); // forwarded
        outer.OnNext(KeyB); // switch to inner2; inner1 superseded
        inner1.OnNext(Eleven); // stale -> ignored
        inner2.OnNext(Twenty); // forwarded
        await Assert.That(values.SequenceEqual(_tenThenTwenty)).IsTrue();
    }

    /// <summary>Verifies that SwitchSelect completes only once both the outer and the active inner have completed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchSelectCompletesAfterOuterAndInner()
    {
        Signal<string?> outer = new();
        Signal<int> inner = new();
        var completed = 0;
        _ = outer.SwitchSelect(_ => inner).Subscribe(
            static _ => { },
            static ex => throw ex,
            () => completed++);
        outer.OnNext(KeyA); // active inner
        outer.OnCompleted(); // outer done, inner still active -> not complete
        await Assert.That(completed).IsEqualTo(0);
        inner.OnCompleted(); // now complete
        await Assert.That(completed).IsEqualTo(Once);
    }

    /// <summary>Verifies that a source error is forwarded through Choose.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ChooseForwardsSourceError()
    {
        Signal<int> source = new();
        Exception? error = null;
        _ = source.Choose(static x => (true, x)).Subscribe(
            static _ => { },
            ex => error = ex,
            static () => { });
        source.OnError(new InvalidOperationException(Boom));
        await Assert.That(error is InvalidOperationException).IsTrue();
    }

    /// <summary>Verifies that a selector exception terminates SwitchSelect with an error.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchSelectForwardsSelectorError()
    {
        Signal<string?> outer = new();
        Exception? error = null;
        _ = outer.SwitchSelect<string, int>(static _ => throw new InvalidOperationException(Boom)).Subscribe(
            static _ => { },
            ex => error = ex,
            static () => { });
        outer.OnNext(KeyA);
        await Assert.That(error is InvalidOperationException).IsTrue();
    }

    /// <summary>Verifies that an outer error terminates SwitchSelect.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchSelectForwardsOuterError()
    {
        Signal<string?> outer = new();
        Signal<int> inner = new();
        Exception? error = null;
        _ = outer.SwitchSelect(_ => inner).Subscribe(
            static _ => { },
            ex => error = ex,
            static () => { });
        outer.OnNext(KeyA);
        outer.OnError(new InvalidOperationException(Boom));
        await Assert.That(error is InvalidOperationException).IsTrue();
    }

    /// <summary>Verifies that an inner error terminates SwitchSelect.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchSelectForwardsInnerError()
    {
        Signal<string?> outer = new();
        Signal<int> inner = new();
        Exception? error = null;
        _ = outer.SwitchSelect(_ => inner).Subscribe(
            static _ => { },
            ex => error = ex,
            static () => { });
        outer.OnNext(KeyA);
        inner.OnError(new InvalidOperationException(Boom));
        await Assert.That(error is InvalidOperationException).IsTrue();
    }

    /// <summary>Verifies that disposing SwitchSelect tears down the outer and inner subscriptions.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchSelectDisposeUnsubscribes()
    {
        Signal<string?> outer = new();
        Signal<int> inner = new();
        List<int> values = [];
        var subscription = outer.SwitchSelect(_ => inner).Subscribe(values.Add);
        outer.OnNext(KeyA);
        inner.OnNext(Ten);
        subscription.Dispose();
        inner.OnNext(Eleven); // disposed -> ignored
        outer.OnNext(KeyB); // disposed -> ignored
        await Assert.That(values.SequenceEqual(_tenOnly)).IsTrue();
    }

    /// <summary>Verifies completion when the inner completes before the outer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchSelectCompletesWhenInnerThenOuterComplete()
    {
        Signal<string?> outer = new();
        Signal<int> inner = new();
        var completed = 0;
        _ = outer.SwitchSelect(_ => inner).Subscribe(
            static _ => { },
            static ex => throw ex,
            () => completed++);
        outer.OnNext(KeyA);
        inner.OnCompleted(); // inner done; outer still open -> not complete
        await Assert.That(completed).IsEqualTo(0);
        outer.OnCompleted(); // outer done, no active inner -> complete
        await Assert.That(completed).IsEqualTo(Once);
    }

    /// <summary>Verifies completion when the outer completes before any value is emitted.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchSelectCompletesWhenOuterCompletesWithNoValue()
    {
        Signal<string?> outer = new();
        var completed = 0;
        _ = outer.SwitchSelect(static _ => new Signal<int>()).Subscribe(
            static _ => { },
            static ex => throw ex,
            () => completed++);
        outer.OnCompleted();
        await Assert.That(completed).IsEqualTo(Once);
    }

    /// <summary>
    /// Verifies the SwitchSelect race guards drop notifications from a superseded inner observable and
    /// from the outer/active-inner sources after disposal — the defensive early-returns that a
    /// well-behaved (unsubscribing) source would otherwise hide.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchSelectGuardsIgnoreStaleAndPostDisposeNotifications()
    {
        ManualObservable<string?> outer = new();
        ManualObservable<int> inner1 = new();
        ManualObservable<int> inner2 = new();
        List<int> values = [];
        Exception? error = null;
        var completed = 0;
        var subscription = outer.SwitchSelect(key => key == KeyA ? inner1 : inner2)
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
        await Assert.That(values.Count).IsEqualTo(0);
        await Assert.That(error is null).IsTrue();
        await Assert.That(completed).IsEqualTo(0);
    }

    /// <summary>Verifies argument validation for both operators and their subscriptions.</summary>
    [Test]
    public void NullArgumentsThrow()
    {
        _ = Assert.Throws<ArgumentNullException>(static () => default(IObservable<int>)!.Choose(static x => (true, x)));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.FromEnumerable(_oneToFour).Choose<int, int>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            Signal.FromEnumerable(_oneToFour).Choose(static x => (true, x)).Subscribe((IObserver<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            default(IObservable<string?>)!.SwitchSelect(static _ => Signal.None<int>()));
        _ = Assert.Throws<ArgumentNullException>(static () => new Signal<string?>().SwitchSelect<string, int>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            new Signal<string?>().SwitchSelect(static _ => Signal.None<int>()).Subscribe((IObserver<int>)null!));
    }

    /// <summary>
    /// An observable whose subscription deliberately ignores disposal, retaining its observer so a test
    /// can keep pushing notifications after the operator has switched away from it or disposed it. A
    /// well-behaved source unsubscribes on either event; this misbehaving source is what the operator's
    /// race guards exist to defend against.
    /// </summary>
    /// <typeparam name = "T">The element type.</typeparam>
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
        /// <param name = "value">The value to push.</param>
        public void Next(T value) => _observer?.OnNext(value);

        /// <summary>Pushes an error to the retained observer.</summary>
        /// <param name = "exception">The error to push.</param>
        public void Error(Exception exception) => _observer?.OnError(exception);

        /// <summary>Pushes completion to the retained observer.</summary>
        public void Complete() => _observer?.OnCompleted();
    }
}
