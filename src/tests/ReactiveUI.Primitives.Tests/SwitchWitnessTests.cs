// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="SwitchWitness{T}"/>.</summary>
public sealed class SwitchWitnessTests
{
    /// <summary>The integer constant one.</summary>
    private const int One = 1;

    /// <summary>The integer constant two.</summary>
    private const int Two = 2;

    /// <summary>Timeout used while waiting for background work in these tests.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies direct switch witness completion is emitted once.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessSuppressesDuplicateCompletionFromCurrentInner()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        CapturingObservable<int> inner = new();
        using var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnNext(inner);
        inner.Observer!.OnCompleted();
        outer.OnCompleted();
        inner.Observer.OnCompleted();

        await Assert.That(observer.Completed).IsEqualTo(One);
    }

    /// <summary>Verifies the public switch operator serializes consecutive inner values through the same observer gate.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchToDoesNotEnterObserverConcurrentlyWhenSwitchingInnerSources()
    {
        Signal<IObservable<int>> outer = new();
        CapturingObservable<int> first = new();
        CapturingObservable<int> second = new();
        using BlockingObserver observer = new();
        using var subscription = outer.SwitchTo().Subscribe(observer);

        outer.OnNext(first);
        var firstValueTask = Task.Run(() => first.Observer!.OnNext(One));
        await Assert.That(observer.OnNextEntered.Wait(WaitTimeout)).IsTrue();

        var switchTask = Task.Run(() => outer.OnNext(second));
        await Task.Delay(TimeSpan.FromMilliseconds(50)).ConfigureAwait(false);
        second.Observer?.OnNext(Two);

        await Assert.That(observer.ConcurrentOnNext).IsFalse();
        observer.ReleaseOnNext.Set();
        await firstValueTask.WaitAsync(WaitTimeout).ConfigureAwait(false);
        await switchTask.WaitAsync(WaitTimeout).ConfigureAwait(false);

        if (observer.Values == One)
        {
            second.Observer!.OnNext(Two);
        }

        await Assert.That(observer.ConcurrentOnNext).IsFalse();
        await Assert.That(observer.Values).IsEqualTo(Two);
    }

    /// <summary>Verifies switching inner sources forwards only the latest source's values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessForwardsOnlyLatestInnerValues()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        CapturingObservable<int> first = new();
        CapturingObservable<int> second = new();
        using var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnNext(first);
        first.Observer!.OnNext(One);

        outer.OnNext(second);

        // The stale first inner is now superseded; its values must be dropped.
        first.Observer.OnNext(One);
        second.Observer!.OnNext(Two);

        await Assert.That(observer.Values.Count).IsEqualTo(Two);
        await Assert.That(observer.Values[0]).IsEqualTo(One);
        await Assert.That(observer.Values[1]).IsEqualTo(Two);
    }

    /// <summary>Verifies a stale inner completion does not complete the witness.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessIgnoresStaleInnerCompletion()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        CapturingObservable<int> first = new();
        CapturingObservable<int> second = new();
        using var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnNext(first);
        var staleFirst = first.Observer!;
        outer.OnNext(second);
        outer.OnCompleted();

        // Stale inner completion (wrong version) must not complete the witness.
        staleFirst.OnCompleted();
        await Assert.That(observer.Completed).IsEqualTo(0);

        // Current inner completion plus outer-complete completes once.
        second.Observer!.OnCompleted();
        await Assert.That(observer.Completed).IsEqualTo(One);
    }

    /// <summary>Verifies outer completion before the inner finishes defers completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessDefersCompletionUntilActiveInnerCompletes()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        CapturingObservable<int> inner = new();
        using var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnNext(inner);
        outer.OnCompleted();

        // Outer done but inner still active: no completion yet.
        await Assert.That(observer.Completed).IsEqualTo(0);

        inner.Observer!.OnCompleted();
        await Assert.That(observer.Completed).IsEqualTo(One);
    }

    /// <summary>Verifies completion when the outer finishes with no inner ever active.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessCompletesWhenOuterCompletesWithNoInner()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        using var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnCompleted();

        await Assert.That(observer.Completed).IsEqualTo(One);
    }

    /// <summary>Verifies an outer error is forwarded once and gates later notifications.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessForwardsOuterErrorAndGatesAfterwards()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        CapturingObservable<int> inner = new();
        InvalidOperationException error = new("outer");
        using var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnNext(inner);
        outer.OnError(error);

        await Assert.That(observer.Errors).HasSingleItem();
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(error);

        // Everything is gated after the terminal error.
        inner.Observer!.OnNext(One);
        inner.Observer.OnCompleted();
        outer.OnNext(inner);
        outer.OnCompleted();

        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(observer.Errors).HasSingleItem();
    }

    /// <summary>Verifies an inner error is forwarded once and gates later notifications.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessForwardsInnerErrorAndGatesAfterwards()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        CapturingObservable<int> inner = new();
        InvalidOperationException error = new("inner");
        using var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnNext(inner);
        inner.Observer!.OnError(error);

        await Assert.That(observer.Errors).HasSingleItem();
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(error);

        // Subsequent inner/outer notifications are gated.
        inner.Observer.OnNext(One);
        outer.OnCompleted();

        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(observer.Errors).HasSingleItem();
    }

    /// <summary>Verifies a stale inner error (wrong version) is dropped.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessDropsStaleInnerError()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        CapturingObservable<int> first = new();
        CapturingObservable<int> second = new();
        using var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnNext(first);
        var staleFirst = first.Observer!;
        outer.OnNext(second);

        // Error from the superseded inner is dropped, not forwarded.
        staleFirst.OnError(new InvalidOperationException("stale"));

        await Assert.That(observer.Errors).IsEmpty();

        second.Observer!.OnNext(Two);
        await Assert.That(observer.Values).HasSingleItem();
        await Assert.That(observer.Values[0]).IsEqualTo(Two);
    }

    /// <summary>Verifies a new source arriving after a terminal is ignored.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessIgnoresSourceAfterTerminal()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        CapturingObservable<int> inner = new();
        using var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnCompleted();
        await Assert.That(observer.Completed).IsEqualTo(One);

        // A late source must be ignored; its observer is never captured.
        outer.OnNext(inner);
        await Assert.That(inner.Observer).IsNull();
    }

    /// <summary>Verifies disposal mid-switch disposes the active inner subscription.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessDisposesActiveInnerOnDispose()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        DisposalObservable<int> inner = new();
        var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnNext(inner);
        inner.Observer!.OnNext(One);
        await Assert.That(observer.Values).HasSingleItem();
        await Assert.That(observer.Values[0]).IsEqualTo(One);

        subscription.Dispose();

        // The inner subscription is disposed by the witness.
        await Assert.That(inner.Disposed).IsTrue();
    }

    /// <summary>An observable that captures its observer for manual notification.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class CapturingObservable<T> : IObservable<T>
    {
        /// <summary>Gets the captured observer.</summary>
        public IObserver<T>? Observer { get; private set; }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            Observer = observer;
            return EmptyDisposable.Instance;
        }
    }

    /// <summary>An observable that captures its observer and records subscription disposal.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class DisposalObservable<T> : IObservable<T>
    {
        /// <summary>Gets the captured observer.</summary>
        public IObserver<T>? Observer { get; private set; }

        /// <summary>Gets a value indicating whether the subscription was disposed.</summary>
        public bool Disposed { get; private set; }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            Observer = observer;
            return new ActionDisposable(() => Disposed = true);
        }
    }

    /// <summary>Observer that blocks the first value so concurrent re-entry can be detected.</summary>
    private sealed class BlockingObserver : IObserver<int>, IDisposable
    {
        /// <summary>Gets the event set when the first <see cref="OnNext"/> call is entered.</summary>
        public ManualResetEventSlim OnNextEntered { get; } = new();

        /// <summary>Gets the event released by the test to unblock the first <see cref="OnNext"/> call.</summary>
        public ManualResetEventSlim ReleaseOnNext { get; } = new();

        /// <summary>Gets the number of forwarded values.</summary>
        public int Values { get; private set; }

        /// <summary>Gets a value indicating whether <see cref="OnNext"/> was entered concurrently.</summary>
        public bool ConcurrentOnNext { get; private set; }

        /// <summary>Gets or sets a value indicating whether an <see cref="OnNext"/> call is active.</summary>
        private bool IsInOnNext { get; set; }

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc/>
        public void OnNext(int value)
        {
            if (IsInOnNext)
            {
                ConcurrentOnNext = true;
            }

            Values++;
            IsInOnNext = true;
            OnNextEntered.Set();
            if (value == One && !ReleaseOnNext.Wait(WaitTimeout))
            {
                throw new TimeoutException("Timed out waiting to release the blocked OnNext call.");
            }

            IsInOnNext = false;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            OnNextEntered.Dispose();
            ReleaseOnNext.Dispose();
        }
    }
}
