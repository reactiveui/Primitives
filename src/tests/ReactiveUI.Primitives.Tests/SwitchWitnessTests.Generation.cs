// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests switch generation tracking during reentrant subscription.</summary>
public sealed partial class SwitchWitnessTests
{
    /// <summary>The value the first inner emits synchronously while it is being subscribed.</summary>
    private const int FirstValue = 1;

    /// <summary>The value the second inner emits after the re-entrant switch has settled.</summary>
    private const int SecondValue = 2;

    /// <summary>Reentrant replacement during subscription keeps the newest inner active.</summary>
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

        // The replacement remains subscribed after the first subscription returns.
        second.Observer?.OnNext(SecondValue);

        using (Assert.Multiple())
        {
            await Assert.That(second.DisposeCount).IsEqualTo(0);
            await Assert.That(downstream.Values).Contains(SecondValue);
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
