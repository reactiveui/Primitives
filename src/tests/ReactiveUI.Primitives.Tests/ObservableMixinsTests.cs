// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for internal observable helper operators.</summary>
public class ObservableMixinsTests
{
    /// <summary>First emitted source value.</summary>
    private const int FirstValue = 1;

    /// <summary>Second emitted source value.</summary>
    private const int SecondValue = 2;

    /// <summary>Stopper signal value.</summary>
    private const string StopValue = "stop";

    /// <summary>Verifies that <c>TakeUntil</c> validates a null source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSourceIsNull_ThenThrowsArgumentNull()
    {
        using var other = new Subject<string>();

        await Assert.That(() => LinqExtensions.TakeUntil<int, string>(null!, other)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies that <c>TakeUntil</c> validates a null other observable.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenOtherIsNull_ThenThrowsArgumentNull()
    {
        using var source = new Subject<int>();

        await Assert.That(() => LinqExtensions.TakeUntil<int, string>(source, null!)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies that <c>TakeUntil</c> validates a null downstream observer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenObserverIsNull_ThenThrowsArgumentNull()
    {
        using var source = new Subject<int>();
        using var other = new Subject<string>();
        var signal = LinqExtensions.TakeUntil(source, other);

        await Assert.That(() => signal.Subscribe((IObserver<int>)null!)).ThrowsExactly<ArgumentNullException>();
    }

    /// <summary>Verifies that <c>TakeUntil</c> completes when the other observable emits.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenOtherEmits_ThenCompletesAndStopsForwardingSource()
    {
        using Subject<int> source = new();
        using Subject<string> other = new();
        List<int> values = [];
        var completed = false;
        using var subscription =
            LinqExtensions.TakeUntil(source, other).Subscribe(values.Add, ThrowUnexpectedError, () => completed = true);
        source.OnNext(FirstValue);
        other.OnNext(StopValue);
        source.OnNext(SecondValue);
        await Assert.That(values.SequenceEqual([FirstValue])).IsTrue();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <c>TakeUntil</c> keeps the source alive when the other observable completes without a value.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenOtherCompletesWithoutValue_ThenSourceContinues()
    {
        using Subject<int> source = new();
        using Subject<string> other = new();
        List<int> values = [];
        var completed = false;
        using var subscription =
            LinqExtensions.TakeUntil(source, other).Subscribe(values.Add, ThrowUnexpectedError, () => completed = true);
        source.OnNext(FirstValue);
        other.OnCompleted();
        source.OnNext(SecondValue);
        source.OnCompleted();
        await Assert.That(values.SequenceEqual([FirstValue, SecondValue])).IsTrue();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <c>TakeUntil</c> forwards errors from the other observable.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenOtherErrors_ThenErrorIsForwardedAndSourceStops()
    {
        using Subject<int> source = new();
        using Subject<string> other = new();
        InvalidOperationException expected = new("expected");
        List<int> values = [];
        Exception? observed = null;
        var completed = false;
        using var subscription = LinqExtensions.TakeUntil(source, other)
            .Subscribe(values.Add, exception => observed = exception, () => completed = true);
        source.OnNext(FirstValue);
        other.OnError(expected);
        source.OnNext(SecondValue);
        await Assert.That(values.SequenceEqual([FirstValue])).IsTrue();
        await Assert.That(observed!).IsSameReferenceAs(expected);
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Verifies that synchronous other emission completes without subscribing to the source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenOtherEmitsSynchronously_ThenSourceIsNotSubscribed()
    {
        var source = new TrackingObservable<int>();
        var otherDisposable = new RecordingDisposable();
        var other = new ScriptedObservable<string>(
            observer => observer.OnNext(StopValue),
            otherDisposable);
        var completed = 0;

        using var subscription = LinqExtensions.TakeUntil(source, other).Subscribe(
            static _ => throw new InvalidOperationException("Source value should not be forwarded."),
            ThrowUnexpectedError,
            () => completed++);

        await Assert.That(source.SubscribeCount).IsEqualTo(0);
        await Assert.That(completed).IsEqualTo(1);
        await Assert.That(otherDisposable.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that disposing the outer subscription disposes both source subscriptions.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSubscriptionIsDisposed_ThenSourceAndOtherAreDisposed()
    {
        var sourceDisposable = new RecordingDisposable();
        var otherDisposable = new RecordingDisposable();
        var source = new TrackingObservable<int>(sourceDisposable);
        var other = new TrackingObservable<string>(otherDisposable);
        using var subscription = LinqExtensions.TakeUntil(source, other).Subscribe(static _ => { }, ThrowUnexpectedError, static () => { });

        subscription.Dispose();

        await Assert.That(source.SubscribeCount).IsEqualTo(1);
        await Assert.That(other.SubscribeCount).IsEqualTo(1);
        await Assert.That(sourceDisposable.DisposeCount).IsEqualTo(1);
        await Assert.That(otherDisposable.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Throws when an unexpected error arrives.</summary>
    /// <param name = "exception">The unexpected exception.</param>
    private static void ThrowUnexpectedError(Exception exception) =>
        throw new InvalidOperationException("Unexpected error.", exception);

    /// <summary>Observable that runs a subscription script synchronously and returns a supplied disposable.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class ScriptedObservable<T> : IObservable<T>
    {
        /// <summary>The script to execute during subscription.</summary>
        private readonly Action<IObserver<T>> _script;

        /// <summary>The disposable returned from subscription.</summary>
        private readonly IDisposable _disposable;

        /// <summary>Initializes a new instance of the <see cref="ScriptedObservable{T}"/> class.</summary>
        /// <param name="script">The subscription script.</param>
        /// <param name="disposable">The disposable returned from subscription.</param>
        internal ScriptedObservable(Action<IObserver<T>> script, IDisposable disposable)
        {
            _script = script;
            _disposable = disposable;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            _script(observer);
            return _disposable;
        }
    }

    /// <summary>Observable that records subscription count and returns a supplied disposable.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class TrackingObservable<T> : IObservable<T>
    {
        /// <summary>The disposable returned from subscription.</summary>
        private readonly IDisposable _disposable;

        /// <summary>Initializes a new instance of the <see cref="TrackingObservable{T}"/> class.</summary>
        internal TrackingObservable()
            : this(new RecordingDisposable())
        {
        }

        /// <summary>Initializes a new instance of the <see cref="TrackingObservable{T}"/> class.</summary>
        /// <param name="disposable">The disposable returned from subscription.</param>
        internal TrackingObservable(IDisposable disposable) => _disposable = disposable;

        /// <summary>Gets the number of subscriptions.</summary>
        public int SubscribeCount { get; private set; }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            SubscribeCount++;
            return _disposable;
        }
    }

    /// <summary>Disposable that records disposal calls.</summary>
    private sealed class RecordingDisposable : IDisposable
    {
        /// <summary>Gets the number of disposal calls.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => DisposeCount++;
    }
}
