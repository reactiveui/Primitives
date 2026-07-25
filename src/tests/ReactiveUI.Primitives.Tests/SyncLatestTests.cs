// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Verifies the multi-source <c>SyncLatest</c> combinators for three through nine sources: a result is
/// withheld until every source has produced a value, each source lands in its own selector parameter, and a
/// later value from any source replaces only that source's contribution.
/// </summary>
public partial class SyncLatestTests
{
    /// <summary>The number of sources combined by the three-source overload.</summary>
    private const int ThreeSources = 3;

    /// <summary>The number of sources combined by the four-source overload.</summary>
    private const int FourSources = 4;

    /// <summary>The number of sources combined by the five-source overload.</summary>
    private const int FiveSources = 5;

    /// <summary>The number of sources combined by the six-source overload.</summary>
    private const int SixSources = 6;

    /// <summary>The number of sources combined by the seven-source overload.</summary>
    private const int SevenSources = 7;

    /// <summary>The number of sources combined by the eight-source overload.</summary>
    private const int EightSources = 8;

    /// <summary>The number of sources combined by the nine-source overload.</summary>
    private const int NineSources = 9;

    /// <summary>The replacement value pushed into the first source once every source has a value.</summary>
    private const int ReplacementValue = 100;

    /// <summary>The value delivered by the first source.</summary>
    private const int FirstValue = 1;

    /// <summary>The value delivered by the second source.</summary>
    private const int SecondValue = 2;

    /// <summary>The value delivered by the third source.</summary>
    private const int ThirdValue = 3;

    /// <summary>The number of results the combined sequence has produced once the first source emits again.</summary>
    private const int ResultCountAfterReplacement = 2;

    /// <summary>Verifies the three-source overload combines the latest value of every source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SyncLatestOverThreeSourcesCombinesTheLatestValueOfEverySource()
    {
        var sources = CreateSources(ThreeSources);
        List<int[]> results = [];
        using var subscription = sources[0]
            .SyncLatest(sources[1], sources[2], static (a, b, c) => new[] { a, b, c })
            .Subscribe(results.Add);
        await AssertCombinesLatestOfEverySource(sources, results).ConfigureAwait(false);
    }

    /// <summary>Verifies the four-source overload combines the latest value of every source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SyncLatestOverFourSourcesCombinesTheLatestValueOfEverySource()
    {
        var sources = CreateSources(FourSources);
        List<int[]> results = [];
        using var subscription = sources[0]
            .SyncLatest(sources[1], sources[2], sources[3], static (a, b, c, d) => new[] { a, b, c, d })
            .Subscribe(results.Add);
        await AssertCombinesLatestOfEverySource(sources, results).ConfigureAwait(false);
    }

    /// <summary>Verifies the five-source overload combines the latest value of every source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SyncLatestOverFiveSourcesCombinesTheLatestValueOfEverySource()
    {
        var sources = CreateSources(FiveSources);
        List<int[]> results = [];
        using var subscription = sources[0]
            .SyncLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                static (a, b, c, d, e) => new[] { a, b, c, d, e })
            .Subscribe(results.Add);
        await AssertCombinesLatestOfEverySource(sources, results).ConfigureAwait(false);
    }

    /// <summary>Verifies the six-source overload combines the latest value of every source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SyncLatestOverSixSourcesCombinesTheLatestValueOfEverySource()
    {
        var sources = CreateSources(SixSources);
        List<int[]> results = [];
        using var subscription = sources[0]
            .SyncLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                static (a, b, c, d, e, f) => new[] { a, b, c, d, e, f })
            .Subscribe(results.Add);
        await AssertCombinesLatestOfEverySource(sources, results).ConfigureAwait(false);
    }

    /// <summary>Verifies the seven-source overload combines the latest value of every source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SyncLatestOverSevenSourcesCombinesTheLatestValueOfEverySource()
    {
        var sources = CreateSources(SevenSources);
        List<int[]> results = [];
        using var subscription = sources[0]
            .SyncLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                static (a, b, c, d, e, f, g) => new[] { a, b, c, d, e, f, g })
            .Subscribe(results.Add);
        await AssertCombinesLatestOfEverySource(sources, results).ConfigureAwait(false);
    }

    /// <summary>Verifies the eight-source overload combines the latest value of every source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SyncLatestOverEightSourcesCombinesTheLatestValueOfEverySource()
    {
        var sources = CreateSources(EightSources);
        List<int[]> results = [];
        using var subscription = sources[0]
            .SyncLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                static (a, b, c, d, e, f, g, h) => new[] { a, b, c, d, e, f, g, h })
            .Subscribe(results.Add);
        await AssertCombinesLatestOfEverySource(sources, results).ConfigureAwait(false);
    }

    /// <summary>Verifies the nine-source overload combines the latest value of every source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SyncLatestOverNineSourcesCombinesTheLatestValueOfEverySource()
    {
        var sources = CreateSources(NineSources);
        List<int[]> results = [];
        using var subscription = sources[0]
            .SyncLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                static (a, b, c, d, e, f, g, h, i) => new[] { a, b, c, d, e, f, g, h, i })
            .Subscribe(results.Add);
        await AssertCombinesLatestOfEverySource(sources, results).ConfigureAwait(false);
    }

    /// <summary>Verifies a null source or selector is rejected before any subscription is made.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SyncLatestRejectsNullSourcesAndSelectors()
    {
        var sources = CreateSources(ThreeSources);
        _ = Assert.Throws<ArgumentNullException>(() =>
            ((IObservable<int>)null!).SyncLatest(sources[1], sources[2], static (a, b, c) => a + b + c));
        _ = Assert.Throws<ArgumentNullException>(() =>
            sources[0].SyncLatest<int, int, int, int>(null!, sources[2], static (a, b, c) => a + b + c));
        _ = Assert.Throws<ArgumentNullException>(() =>
            sources[0].SyncLatest<int, int, int, int>(sources[1], null!, static (a, b, c) => a + b + c));
        _ = Assert.Throws<ArgumentNullException>(() =>
            sources[0].SyncLatest<int, int, int, int>(sources[1], sources[2], null!));
        await Assert.That(sources[0].HasObservers).IsFalse();
    }

    /// <summary>
    /// Verifies the combined sequence ignores every notification a source delivers after the sequence has
    /// terminated, and ignores a duplicate completion from a single source.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SyncLatestIgnoresSourceNotificationsDeliveredAfterTheCombinedSequenceTerminates()
    {
        IObserver<int>? first = null;
        IObserver<int>? second = null;
        IObserver<int>? third = null;
        RecordingWitness<int[]> witness = new();
        using var subscription = new ScriptedObservable<int>(observer => first = observer)
            .SyncLatest(
                new ScriptedObservable<int>(observer => second = observer),
                new ScriptedObservable<int>(observer => third = observer),
                static (a, b, c) => new[] { a, b, c })
            .Subscribe(witness);
        first!.OnNext(FirstValue);
        second!.OnNext(SecondValue);
        third!.OnNext(ThirdValue);
        first.OnCompleted();
        first.OnCompleted();
        second.OnCompleted();
        third.OnCompleted();
        first.OnNext(ReplacementValue);
        first.OnCompleted();
        first.OnError(new InvalidOperationException("late"));
        await Assert.That(witness.Values.Count).IsEqualTo(1);
        await Assert.That(witness.Values[0].SequenceEqual([FirstValue, SecondValue, ThirdValue])).IsTrue();
        await Assert.That(witness.Completed).IsEqualTo(1);
        await Assert.That(witness.Errors.Count).IsEqualTo(0);
    }

    /// <summary>
    /// Verifies a source that throws while being subscribed tears down the sources already subscribed and
    /// surfaces the original failure to the caller.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SyncLatestUnsubscribesEarlierSourcesWhenALaterSourceThrowsOnSubscribe()
    {
        Signal<int> first = new();
        Signal<int> third = new();
        InvalidOperationException expected = new("subscribe-fault");
        var combined = first.SyncLatest(
            new ScriptedObservable<int>(_ => throw expected),
            third,
            static (a, b, c) => a + b + c);
        var thrown = Assert.Throws<InvalidOperationException>(() => _ = combined.Subscribe(static _ => { }));
        await Assert.That(thrown).IsSameReferenceAs(expected);
        await Assert.That(first.HasObservers).IsFalse();
        await Assert.That(third.HasObservers).IsFalse();
    }

    /// <summary>Creates the requested number of independent subjects.</summary>
    /// <param name="count">The number of subjects to create.</param>
    /// <returns>The created subjects.</returns>
    private static Signal<int>[] CreateSources(int count)
    {
        var sources = new Signal<int>[count];
        for (var i = 0; i < sources.Length; i++)
        {
            sources[i] = new();
        }

        return sources;
    }

    /// <summary>
    /// Drives every source and asserts the combined sequence: nothing is emitted until the final source
    /// produces its first value, the first result carries each source's value in its own selector position,
    /// and a later value on the first source replaces only that source's contribution.
    /// </summary>
    /// <param name="sources">The subjects feeding the combinator, in selector-parameter order.</param>
    /// <param name="results">The results recorded from the combined sequence.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertCombinesLatestOfEverySource(Signal<int>[] sources, List<int[]> results)
    {
        var count = sources.Length;
        for (var i = 0; i < count - 1; i++)
        {
            sources[i].OnNext(i + 1);
        }

        await Assert.That(results.Count).IsEqualTo(0);
        sources[count - 1].OnNext(count);
        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].SequenceEqual(Enumerable.Range(1, count))).IsTrue();
        sources[0].OnNext(ReplacementValue);
        await Assert.That(results.Count).IsEqualTo(ResultCountAfterReplacement);
        await Assert.That(results[1][0]).IsEqualTo(ReplacementValue);
        await Assert.That(results[1].Skip(1).SequenceEqual(Enumerable.Range(SecondValue, count - 1))).IsTrue();
    }
}
