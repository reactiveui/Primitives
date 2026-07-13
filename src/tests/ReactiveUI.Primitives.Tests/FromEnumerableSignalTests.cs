// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="FromEnumerableSignal{T}"/> synchronous fast-path contracts.</summary>
public class FromEnumerableSignalTests
{
    /// <summary>A reusable value for one.</summary>
    private const int One = 1;

    /// <summary>A reusable value for two.</summary>
    private const int Two = 2;

    /// <summary>A reusable value for three.</summary>
    private const int Three = 3;

    /// <summary>A reusable value for four.</summary>
    private const int Four = 4;

    /// <summary>A reusable value for five.</summary>
    private const int Five = 5;

    /// <summary>A reusable value for six.</summary>
    private const int Six = 6;

    /// <summary>Expected one-two value sequence.</summary>
    private static readonly int[] ExpectedOneTwo = [One, Two];

    /// <summary>Expected three-four value sequence.</summary>
    private static readonly int[] ExpectedThreeFour = [Three, Four];

    /// <summary>Expected five-six value sequence.</summary>
    private static readonly int[] ExpectedFiveSix = [Five, Six];

    /// <summary>Covers enumerable signal fast paths for arrays, read-only lists, iterators, and delegate subscriptions.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromEnumerableSignalCoversAllSynchronousFastPaths()
    {
        RecordingWitness<int> arrayObserver = new();
        FromEnumerableSignal<int> arraySignal = new([One, Two]);
        await Assert.That(arraySignal.IsRequiredSubscribeOnCurrentThread()).IsFalse();
        arraySignal.Subscribe(arrayObserver).Dispose();
        await Assert.That(arrayObserver.Values.SequenceEqual(ExpectedOneTwo)).IsTrue();
        await Assert.That(arrayObserver.Completed).IsEqualTo(1);
        List<int> listValues = [];
        var listCompleted = 0;
        new FromEnumerableSignal<int>([Three, Four])
            .Subscribe(listValues.Add, static ex => throw ex, () => listCompleted++).Dispose();
        await Assert.That(listValues.SequenceEqual(ExpectedThreeFour)).IsTrue();
        await Assert.That(listCompleted).IsEqualTo(1);
        RecordingWitness<int> iteratorObserver = new();
        new FromEnumerableSignal<int>(YieldValues()).Subscribe(iteratorObserver).Dispose();
        await Assert.That(iteratorObserver.Values.SequenceEqual(ExpectedFiveSix)).IsTrue();
        await Assert.That(iteratorObserver.Completed).IsEqualTo(1);
        List<int> iteratorValues = [];
        var iteratorCompleted = 0;
        new FromEnumerableSignal<int>(YieldValues())
            .Subscribe(iteratorValues.Add, static ex => throw ex, () => iteratorCompleted++).Dispose();
        await Assert.That(iteratorValues.SequenceEqual(ExpectedFiveSix)).IsTrue();
        await Assert.That(iteratorCompleted).IsEqualTo(1);
        _ = Assert.Throws<ArgumentNullException>(() => arraySignal.Subscribe((IObserver<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => arraySignal.Subscribe(null!, static ex => { }, static () => { }));
        _ = Assert.Throws<ArgumentNullException>(() => arraySignal.Subscribe(static _ => { }, static ex => { }, null!));
    }

    /// <summary>Creates an iterator-backed enumerable for the non-indexable enumerable path.</summary>
    /// <returns>The yielded values.</returns>
    private static IEnumerable<int> YieldValues()
    {
        yield return Five;
        yield return Six;
    }

    /// <summary>Records observer values and terminal signals.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class RecordingWitness<T> : IObserver<T>
    {
        /// <summary>Gets observed values.</summary>
        public List<T> Values { get; } = [];

        /// <summary>Gets the completion count.</summary>
        public int Completed { get; private set; }

        /// <summary>Gets the observed errors.</summary>
        public List<Exception> Errors { get; } = [];

        /// <inheritdoc/>
        public void OnCompleted() => Completed++;

        /// <inheritdoc/>
        public void OnError(Exception error) => Errors.Add(error);

        /// <inheritdoc/>
        public void OnNext(T value) => Values.Add(value);
    }
}
