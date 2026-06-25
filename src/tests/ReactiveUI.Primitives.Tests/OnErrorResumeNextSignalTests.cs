// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Focused coverage for the Rx-style on-error resume sequence coordinator.</summary>
public sealed class OnErrorResumeNextSignalTests
{
    /// <summary>The integer constant one.</summary>
    private const int One = 1;

    /// <summary>Verifies the coordinator advertises current-thread subscription requirements.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnErrorResumeNextRequiresCurrentThread()
    {
        OnErrorResumeNextSignal<int> signal = new([]);

        await Assert.That(signal.IsRequiredSubscribeOnCurrentThread()).IsTrue();
    }

    /// <summary>Verifies enumerable creation failures are reported to the observer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnErrorResumeNextReportsEnumeratorCreationFailure()
    {
        InvalidOperationException expected = new("enumerator");
        Exception? error = null;
        using var subscription = Signal.OnErrorResumeNext(new ThrowingEnumerable<int>(expected))
            .Subscribe(static _ => { }, captured => error = captured);

        await Assert.That(error).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies a null enumerator is treated as an empty source list.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnErrorResumeNextCompletesWhenEnumeratorIsNull()
    {
        var completed = 0;
        using var subscription = Signal.OnErrorResumeNext(new NullEnumeratorEnumerable<int>(returnsNull: true))
            .Subscribe(static _ => { }, error => throw error, () => completed++);

        await Assert.That(completed).IsEqualTo(One);
    }

    /// <summary>Verifies a null source inside the list is reported as an invalid sequence.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnErrorResumeNextReportsNullSourceEntry()
    {
        List<int> values = [];
        Exception? error = null;
        List<IObservable<int>> sources = [Signal.Emit(One), null!];
        using var subscription = Signal.OnErrorResumeNext(sources)
            .Subscribe(values.Add, captured => error = captured);

        await Assert.That(values.SequenceEqual([One])).IsTrue();
        await Assert.That(error).IsTypeOf<InvalidOperationException>();
    }

    /// <summary>Verifies move-next failures after an active source are reported once through the observer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnErrorResumeNextReportsMoveNextFailureAfterActiveSource()
    {
        InvalidOperationException expected = new("move-next");
        ManualSource<int> first = new();
        Exception? error = null;
        using var subscription = Signal.OnErrorResumeNext(new ThrowingAfterFirstEnumerable<int>(first, expected))
            .Subscribe(static _ => { }, captured => error = captured);

        first.Complete();

        await Assert.That(error).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies active-source completion releases the coordinator after the source list is exhausted.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnErrorResumeNextCompletesAfterActiveSourceCompletes()
    {
        ManualSource<int> first = new();
        var completed = 0;
        using var subscription = Signal.OnErrorResumeNext(first)
            .Subscribe(static _ => { }, error => throw error, () => completed++);

        first.Complete();

        await Assert.That(completed).IsEqualTo(One);
    }

    /// <summary>Verifies synchronous terminal sources advance through the current-thread trampoline.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnErrorResumeNextDoesNotReenterSynchronousSources()
    {
        SubscriptionDepth depth = new();
        List<IObservable<int>> sources =
        [
            new DepthTrackingSource<int>(depth),
            new DepthTrackingSource<int>(depth),
            new DepthTrackingSource<int>(depth),
            Signal.Emit(One)
        ];
        List<int> values = [];

        using var subscription = Signal.OnErrorResumeNext(sources)
            .Subscribe(values.Add);

        await Assert.That(depth.MaxDepth).IsEqualTo(One);
        await Assert.That(values.SequenceEqual([One])).IsTrue();
    }

    /// <summary>Verifies duplicate terminal callbacks cannot advance the chain after it has completed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnErrorResumeNextIgnoresDuplicateTerminalReschedules()
    {
        List<int> values = [];
        var completed = 0;

        using var subscription = Signal.OnErrorResumeNext(new DuplicateCompleteSource<int>(), Signal.Emit(One))
            .Subscribe(values.Add, error => throw error, () => completed++);

        await Assert.That(values.SequenceEqual([One])).IsTrue();
        await Assert.That(completed).IsEqualTo(One);
    }

    /// <summary>Verifies a completed source subscription is released once the chain advances to the next source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnErrorResumeNextDisposesCompletedSubscriptionWhenAdvancing()
    {
        CompletingSource<int> first = new();
        ManualSource<int> second = new();

        using var subscription = Signal.OnErrorResumeNext(first, second)
            .Subscribe(static _ => { });

        await Assert.That(first.Subscription?.IsDisposed).IsTrue();
        await Assert.That(second.Subscription?.IsDisposed).IsFalse();
    }

    /// <summary>Verifies late terminal callbacks from a disposed source do not reach the observer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task OnErrorResumeNextIgnoresTerminalAfterDispose()
    {
        ManualSource<int> first = new();
        List<int> values = [];
        Exception? error = null;
        var completed = 0;
        var subscription = Signal.OnErrorResumeNext(first, Signal.Emit(One))
            .Subscribe(values.Add, captured => error = captured, () => completed++);

        subscription.Dispose();
        first.Fail(new InvalidOperationException("late"));

        await Assert.That(values.Count).IsEqualTo(0);
        await Assert.That(error).IsNull();
        await Assert.That(completed).IsEqualTo(0);
    }

    /// <summary>Enumerable that throws when asked for an enumerator.</summary>
    /// <param name="error">The error to throw.</param>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class ThrowingEnumerable<T>(Exception error) : IEnumerable<IObservable<T>>
    {
        /// <inheritdoc/>
        public IEnumerator<IObservable<T>> GetEnumerator() => throw error;

        /// <inheritdoc/>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>Enumerable that returns a null enumerator to cover the defensive null path.</summary>
    /// <param name="returnsNull">Whether <see cref="IEnumerable{T}.GetEnumerator"/> returns null.</param>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class NullEnumeratorEnumerable<T>(bool returnsNull) : IEnumerable<IObservable<T>>
    {
        /// <inheritdoc/>
        public IEnumerator<IObservable<T>> GetEnumerator() => returnsNull ? null! : throw new InvalidOperationException();

        /// <inheritdoc/>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>Enumerable that returns one source and then throws when asked for the next source.</summary>
    /// <param name="first">The first source to return.</param>
    /// <param name="error">The error to throw on the second move.</param>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class ThrowingAfterFirstEnumerable<T>(IObservable<T> first, Exception error) : IEnumerable<IObservable<T>>
    {
        /// <inheritdoc/>
        public IEnumerator<IObservable<T>> GetEnumerator()
        {
            yield return first;
            throw error;
        }

        /// <inheritdoc/>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>Tracks nested synchronous subscriptions.</summary>
    private sealed class SubscriptionDepth
    {
        /// <summary>The current subscription depth.</summary>
        private int _depth;

        /// <summary>Gets the maximum observed subscription depth.</summary>
        internal int MaxDepth { get; private set; }

        /// <summary>Enters a source subscription.</summary>
        internal void Enter()
        {
            _depth++;
            MaxDepth = Math.Max(MaxDepth, _depth);
        }

        /// <summary>Exits a source subscription.</summary>
        internal void Exit() => _depth--;
    }

    /// <summary>Observable that completes synchronously while recording subscription nesting.</summary>
    /// <param name="depth">The shared depth tracker.</param>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class DepthTrackingSource<T>(SubscriptionDepth depth) : IObservable<T>
    {
        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            depth.Enter();
            try
            {
                observer.OnCompleted();
            }
            finally
            {
                depth.Exit();
            }

            return EmptyDisposable.Instance;
        }
    }

    /// <summary>Observable that raises completion twice from one subscription.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class DuplicateCompleteSource<T> : IObservable<T>
    {
        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            observer.OnCompleted();
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }
    }

    /// <summary>Observable that completes synchronously and exposes its returned subscription.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class CompletingSource<T> : IObservable<T>
    {
        /// <summary>Gets the subscription returned to the caller.</summary>
        internal BooleanDisposable? Subscription { get; private set; }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            Subscription = new();
            observer.OnCompleted();
            return Subscription;
        }
    }

    /// <summary>Observable that keeps the observer available for manual terminal callbacks.</summary>
    /// <typeparam name="T">The source value type.</typeparam>
    private sealed class ManualSource<T> : IObservable<T>
    {
        /// <summary>The subscribed observer.</summary>
        private IObserver<T>? _observer;

        /// <summary>Gets the subscription returned to the caller.</summary>
        internal BooleanDisposable? Subscription { get; private set; }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            _observer = observer;
            Subscription = new();
            return Subscription;
        }

        /// <summary>Manually raises an error.</summary>
        /// <param name="error">The error to raise.</param>
        internal void Fail(Exception error) => _observer?.OnError(error);

        /// <summary>Manually completes the source.</summary>
        internal void Complete() => _observer?.OnCompleted();
    }
}
