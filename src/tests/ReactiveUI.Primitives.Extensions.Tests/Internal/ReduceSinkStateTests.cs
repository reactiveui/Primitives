// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Tests.Internal;

/// <summary>Tests for <see cref="ReduceSinkState{TIn, TOut}"/>, the shared state object used by the synchronous combine-then-reduce operator family.</summary>
public class ReduceSinkStateTests
{
    /// <summary>Verifies that a freshly-constructed state reports zero values seen and is not terminal.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFreshState_ThenZeroValuesAndNotTerminal()
    {
        const int SourceCount = 3;
        var observer = new CaptureObserver<int>();
        var state = new ReduceSinkState<int, int>(observer, SourceCount);

        await Assert.That(state.HasValueCount).IsEqualTo(0);
        await Assert.That(state.CompletedCount).IsEqualTo(0);
        await Assert.That(state.IsDone).IsFalse();
        await Assert.That(state.AllValuesPresent).IsFalse();
        await Assert.That(state.Values).Count().IsEqualTo(SourceCount);
        await Assert.That(state.Completed).Count().IsEqualTo(SourceCount);
    }

    /// <summary>Verifies that <c>AllValuesPresent</c> flips once every slot has a value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenEverySlotPopulated_ThenAllValuesPresent()
    {
        const int FirstSeed = 1;
        const int SecondSeed = 2;
        var observer = new CaptureObserver<int>();
        var state = new ReduceSinkState<int, int>(observer, count: 2);

        state.Values[0] = FirstSeed;
        state.HasValueCount++;
        await Assert.That(state.AllValuesPresent).IsFalse();

        state.Values[1] = SecondSeed;
        state.HasValueCount++;
        await Assert.That(state.AllValuesPresent).IsTrue();
    }

    /// <summary>Verifies that <c>HandleError</c> forwards once, marks terminal, and is idempotent.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHandleError_ThenForwardsAndIsTerminal()
    {
        var observer = new CaptureObserver<int>();
        var state = new ReduceSinkState<int, int>(observer, count: 2);
        var error = new InvalidOperationException("boom");

        state.HandleError(error);
        state.HandleError(new InvalidOperationException("second")); // should be no-op

        await Assert.That(state.IsDone).IsTrue();
        await Assert.That(observer.Errors).Count().IsEqualTo(1);
        await Assert.That(observer.Errors[0]).IsEqualTo(error);
    }

    /// <summary>Verifies that <c>HandleCompleted</c> completes once every source has completed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAllSourcesComplete_ThenDownstreamCompletes()
    {
        const int SeedValue1 = 1;
        const int SeedValue2 = 2;
        const int SeededValueCount = 2;
        var observer = new CaptureObserver<int>();
        var state = new ReduceSinkState<int, int>(observer, count: 2);

        // Seed both values so completion-without-value path isn't triggered.
        state.Values[0] = SeedValue1;
        state.Values[1] = SeedValue2;
        state.HasValueCount = SeededValueCount;

        state.HandleCompleted(0);
        await Assert.That(state.IsDone).IsFalse();
        await Assert.That(observer.Completed).IsFalse();

        state.HandleCompleted(1);
        await Assert.That(state.IsDone).IsTrue();
        await Assert.That(observer.Completed).IsTrue();
    }

    /// <summary>Verifies that a source completing without ever emitting closes the combined sequence.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourceCompletesWithoutValue_ThenDownstreamCompletesEarly()
    {
        const int FirstSeed = 1;
        var observer = new CaptureObserver<int>();
        var state = new ReduceSinkState<int, int>(observer, count: 2);

        // Source 0 emitted; source 1 completes without a value.
        state.Values[0] = FirstSeed;
        state.HasValueCount = 1;

        state.HandleCompleted(1);

        await Assert.That(state.IsDone).IsTrue();
        await Assert.That(observer.Completed).IsTrue();
    }

    /// <summary>Verifies that <c>HandleCompleted</c> is idempotent per-source-index.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSameSourceCompletesTwice_ThenSecondCallIgnored()
    {
        var observer = new CaptureObserver<int>();
        var state = new ReduceSinkState<int, int>(observer, count: 2);

        state.HandleCompleted(0);
        state.HandleCompleted(0); // same index — should be no-op

        await Assert.That(state.CompletedCount).IsEqualTo(1);
    }

    /// <summary>Tiny capture observer used by the helper tests.</summary>
    /// <typeparam name="T">Captured element type.</typeparam>
    private sealed class CaptureObserver<T> : IObserver<T>
    {
        /// <summary>Gets the captured OnNext values in order.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets the captured OnError exceptions in order.</summary>
        public List<Exception> Errors { get; } = [];

        /// <summary>Gets a value indicating whether OnCompleted was observed.</summary>
        public bool Completed { get; private set; }

        /// <inheritdoc/>
        public void OnNext(T value) => Values.Add(value);

        /// <inheritdoc/>
        public void OnError(Exception error) => Errors.Add(error);

        /// <inheritdoc/>
        public void OnCompleted() => Completed = true;
    }
}
