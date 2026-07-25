// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;
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

    /// <summary>Window used to confirm a gated operation stays blocked while a delivery is in flight.</summary>
    private const int GateProbeMilliseconds = 200;

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

    /// <summary>Verifies the public switch operator serializes a source switch against an in-flight inner delivery.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchToDoesNotEnterObserverConcurrentlyWhenSwitchingInnerSources()
    {
        Signal<IObservable<int>> outer = new();
        CapturingObservable<int> first = new();
        CapturingObservable<int> second = new();
        using GatedObserver observer = new();
        using var subscription = outer.SwitchTo().Subscribe(observer);

        outer.OnNext(first);

        // The first inner value enters the observer and blocks while the coordinator holds its gate. Run the
        // blocking delivery on a dedicated thread rather than the thread pool: on a saturated runner a Task.Run
        // body can sit unscheduled past the wait window, so FirstEntered would never be set in time even though
        // the delivery is ready to run. An OS thread is not subject to pool starvation.
        DeliveryThread firstDelivery = DeliveryThread.Start(() => first.Observer!.OnNext(One));
        await Assert.That(observer.FirstEntered.Wait(WaitTimeout)).IsTrue();

        // Switching sources is gated too: OnSource cannot subscribe the new inner until the delivery releases.
        DeliveryThread switchDelivery = DeliveryThread.Start(() => outer.OnNext(second));
        await Assert.That(SpinWait.SpinUntil(() => second.Observer is not null, GateProbeMilliseconds)).IsFalse();
        await Assert.That(observer.ConcurrentOnNext).IsFalse();

        observer.ReleaseFirst.Set();
        await Assert.That(firstDelivery.ReturnedWithin(WaitTimeout)).IsTrue();
        await Assert.That(switchDelivery.ReturnedWithin(WaitTimeout)).IsTrue();

        // Once the switch completes the new inner delivers serially behind the first value.
        second.Observer!.OnNext(Two);

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

    /// <summary>Verifies a terminal witness gates a later source switch and outer error while the outer source stays live.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessGatesLateNotificationsAfterInnerError()
    {
        RecordingWitness<int> observer = new();
        Signal<IObservable<int>> outer = new();
        CapturingObservable<int> first = new();
        CapturingObservable<int> late = new();
        using var subscription = new SwitchWitness<int>(observer).Run(outer);

        outer.OnNext(first);

        // An inner error makes the witness terminal without stopping the outer source.
        first.Observer!.OnError(new InvalidOperationException("boom"));
        await Assert.That(observer.Errors).HasSingleItem();

        // Subsequent source switches and outer errors are gated: nothing more is forwarded.
        outer.OnNext(late);
        outer.OnError(new InvalidOperationException("late"));

        await Assert.That(late.Observer).IsNull();
        await Assert.That(observer.Errors).HasSingleItem();
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

    /// <summary>Observer that blocks its first value so any concurrent re-entry is detected with thread-safe state.</summary>
    private sealed class GatedObserver : IObserver<int>, IDisposable
    {
        /// <summary>Tracks how many threads are currently inside <see cref="OnNext"/>.</summary>
        private int _inOnNext;

        /// <summary>Counts forwarded values.</summary>
        private int _values;

        /// <summary>Set when more than one thread is inside <see cref="OnNext"/> at once.</summary>
        private int _concurrent;

        /// <summary>Gets the event set when the first <see cref="OnNext"/> call is entered.</summary>
        public ManualResetEventSlim FirstEntered { get; } = new();

        /// <summary>Gets the event released by the test to unblock the first <see cref="OnNext"/> call.</summary>
        public ManualResetEventSlim ReleaseFirst { get; } = new();

        /// <summary>Gets the number of forwarded values.</summary>
        public int Values => Volatile.Read(ref _values);

        /// <summary>Gets a value indicating whether <see cref="OnNext"/> was entered concurrently.</summary>
        public bool ConcurrentOnNext => Volatile.Read(ref _concurrent) != 0;

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
            if (Interlocked.Increment(ref _inOnNext) != 1)
            {
                _ = Interlocked.Exchange(ref _concurrent, 1);
            }

            var index = Interlocked.Increment(ref _values);
            if (index == 1)
            {
                FirstEntered.Set();
                if (!ReleaseFirst.Wait(WaitTimeout))
                {
                    throw new TimeoutException("Timed out waiting to release the blocked OnNext call.");
                }
            }

            _ = Interlocked.Decrement(ref _inOnNext);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            FirstEntered.Dispose();
            ReleaseFirst.Dispose();
        }
    }

    /// <summary>Runs a blocking notification delivery on a background thread the test can join directly.</summary>
    /// <remarks>
    /// The deliveries these tests drive block inside the observer while the switch coordinator holds its gate.
    /// A <see cref="Task.Run(Action)"/> body would sit on the thread pool, so on a runner whose pool is saturated
    /// the delivery could be dequeued after the wait window had already closed — the gate signal it sets would be
    /// seen as never arriving even though the delivery was ready to run. <see cref="Thread.Join(TimeSpan)"/> is an
    /// OS-level wait no pool pressure can starve, so the test observes the delivery returning rather than the pool
    /// getting round to running it. A body that genuinely never returns still trips the timeout.
    /// </remarks>
    private sealed class DeliveryThread
    {
        /// <summary>The delivery body to run.</summary>
        private readonly Action _body;

        /// <summary>The background thread the body runs on.</summary>
        private readonly Thread _thread;

        /// <summary>The exception the body threw, if any.</summary>
        private ExceptionDispatchInfo? _failure;

        /// <summary>Initializes a new instance of the <see cref="DeliveryThread"/> class.</summary>
        /// <param name="body">The delivery body to run.</param>
        private DeliveryThread(Action body)
        {
            _body = body;
            _thread = new(Run) { IsBackground = true, };
        }

        /// <summary>Starts a delivery body on a background thread of its own.</summary>
        /// <param name="body">The delivery body to run.</param>
        /// <returns>The running delivery thread.</returns>
        internal static DeliveryThread Start(Action body)
        {
            DeliveryThread delivery = new(body);
            delivery._thread.Start();
            return delivery;
        }

        /// <summary>Waits for the delivery thread to return, rethrowing whatever the body threw.</summary>
        /// <param name="timeout">How long the thread is given to return.</param>
        /// <returns><see langword="true"/> when the thread returned within <paramref name="timeout"/>.</returns>
        internal bool ReturnedWithin(TimeSpan timeout)
        {
            if (!_thread.Join(timeout))
            {
                return false;
            }

            _failure?.Throw();
            return true;
        }

        /// <summary>Runs the delivery body, capturing a failure so it can be rethrown with its original stack.</summary>
        private void Run()
        {
            try
            {
                _body();
            }
            catch (Exception error)
            {
                _failure = ExceptionDispatchInfo.Capture(error);
            }
        }
    }
}
