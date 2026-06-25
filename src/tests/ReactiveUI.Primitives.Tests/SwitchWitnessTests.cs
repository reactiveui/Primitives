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
