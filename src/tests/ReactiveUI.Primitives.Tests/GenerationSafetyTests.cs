// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Tests that the switch and merge coordinators keep their generation bookkeeping correct when a source
/// behaves awkwardly: re-entering the outer sequence from a downstream handler, or signalling completion
/// more than once. Both shapes previously lost notifications or terminated early.
/// </summary>
public sealed class GenerationSafetyTests
{
    /// <summary>The value the first inner emits synchronously while it is being subscribed.</summary>
    private const int FirstValue = 1;

    /// <summary>The value the second inner emits after the re-entrant switch has settled.</summary>
    private const int SecondValue = 2;

    /// <summary>
    /// Subscribing an inner can push a value downstream synchronously, and a downstream handler may feed the
    /// outer sequence again. The newer generation must survive that re-entrancy: installing the older
    /// subscription afterwards would dispose the newer one and strand the sequence, because every surviving
    /// notification is then filtered out by version.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SwitchWitnessKeepsTheNewestGenerationWhenTheOuterIsReenteredWhileSubscribing()
    {
        Signal<IObservable<int>> outer = new();
        TrackedObservable<int> second = new();
        SynchronousObservable<int> first = new(FirstValue);
        ReenteringObserver<int> downstream = new(outer, second);

        using var subscription = new SwitchWitness<int>(downstream).Run(outer);

        // Pushing `first` subscribes it, which emits synchronously, which re-enters the outer with `second`.
        outer.OnNext(first);

        // `second` is the current generation, so it must still be live and still be heard.
        second.Observer?.OnNext(SecondValue);

        using (Assert.Multiple())
        {
            await Assert.That(second.DisposeCount).IsEqualTo(0);
            await Assert.That(downstream.Values).Contains(SecondValue);
        }
    }

    /// <summary>
    /// A source may signal completion more than once. A repeat must not decrement the merge's active count a
    /// second time, because that count belongs to a sibling that is still running - the merge would otherwise
    /// complete early and drop whatever the sibling had left.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task MergeCoordinatorIgnoresARepeatedInnerCompletionWhileASiblingIsStillRunning()
    {
        CapturingObservable<int> left = new();
        CapturingObservable<int> right = new();
        RecordingObserver<int> downstream = new();

        using var subscription = new MergeCoordinator<int>(downstream).Run([left, right]);

        left.Observer!.OnCompleted();
        left.Observer!.OnCompleted();

        // `right` has not finished, so the merge must still be open.
        await Assert.That(downstream.Completed).IsEqualTo(0);

        right.Observer!.OnCompleted();

        using (Assert.Multiple())
        {
            await Assert.That(downstream.Completed).IsEqualTo(1);
            await Assert.That(right.Observer).IsNotNull();
        }
    }

    /// <summary>An observable that emits one value while it is being subscribed, and counts its disposals.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="value">The value emitted during subscription.</param>
    private sealed class SynchronousObservable<T>(T value) : IObservable<T>
    {
        /// <summary>Gets the number of times the subscription was disposed.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            observer.OnNext(value);
            return new Tracker(() => DisposeCount++);
        }
    }

    /// <summary>An observable that captures its observer and counts its disposals.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class TrackedObservable<T> : IObservable<T>
    {
        /// <summary>Gets the captured observer.</summary>
        public IObserver<T>? Observer { get; private set; }

        /// <summary>Gets the number of times the subscription was disposed.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            Observer = observer;
            return new Tracker(() => DisposeCount++);
        }
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
            return new Tracker(static () => { });
        }
    }

    /// <summary>Records values and terminal signals.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private class RecordingObserver<T> : IObserver<T>
    {
        /// <summary>Gets the observed values.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets the completion count.</summary>
        public int Completed { get; private set; }

        /// <inheritdoc/>
        public void OnCompleted() => Completed++;

        /// <inheritdoc/>
        public void OnError(Exception error) => throw error;

        /// <inheritdoc/>
        public virtual void OnNext(T value) => Values.Add(value);
    }

    /// <summary>Feeds a replacement inner into the outer sequence the first time it observes a value.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    /// <param name="outer">The outer sequence to re-enter.</param>
    /// <param name="replacement">The inner pushed on the first observed value.</param>
    private sealed class ReenteringObserver<T>(Signal<IObservable<T>> outer, IObservable<T> replacement) : RecordingObserver<T>
    {
        /// <summary>Whether the re-entrant push has already happened.</summary>
        private int _pushed;

        /// <inheritdoc/>
        public override void OnNext(T value)
        {
            base.OnNext(value);
            if (Interlocked.Exchange(ref _pushed, 1) != 0)
            {
                return;
            }

            outer.OnNext(replacement);
        }
    }

    /// <summary>Runs an action when disposed.</summary>
    /// <param name="onDispose">The action to run on disposal.</param>
    private sealed class Tracker(Action onDispose) : IDisposable
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => onDispose();
    }
}
