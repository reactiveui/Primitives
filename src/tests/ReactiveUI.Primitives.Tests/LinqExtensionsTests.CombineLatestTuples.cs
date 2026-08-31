// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests System.Reactive-named tuple-returning <c>CombineLatest</c> overloads.</summary>
public partial class LinqExtensionsTests
{
    /// <summary>The first source value.</summary>
    private const int One = 1;

    /// <summary>The second source value.</summary>
    private const int Two = 2;

    /// <summary>The third source value.</summary>
    private const int Three = 3;

    /// <summary>The widest supported source count.</summary>
    private const int Sixteen = 16;

    /// <summary>The replacement value for the final source.</summary>
    private const int OneHundred = 100;

    /// <summary>The tuple-returning CombineLatest builders, ordered by arity from 2 through 16.</summary>
    private static readonly Func<TupleSources, IObservable<int>>[] _tupleCombineLatestBuilders =
    [
        TupleCombineLatestOfSecond,
        TupleCombineLatestOfThird,
        TupleCombineLatestOfFourth,
        TupleCombineLatestOfFifth,
        TupleCombineLatestOfSixth,
        TupleCombineLatestOfSeventh,
        TupleCombineLatestOfEighth,
        TupleCombineLatestOfNinth,
        TupleCombineLatestOfTenth,
        TupleCombineLatestOfEleventh,
        TupleCombineLatestOfTwelfth,
        TupleCombineLatestOfThirteenth,
        TupleCombineLatestOfFourteenth,
        TupleCombineLatestOfFifteenth,
        TupleCombineLatestOfSixteenth,
    ];

    /// <summary>Provides tuple-returning CombineLatest arities.</summary>
    /// <returns>The tuple CombineLatest arities from 2 through 16.</returns>
    public static IEnumerable<int> TupleCombineLatestArities()
    {
        for (var arity = Two; arity <= Sixteen; arity++)
        {
            yield return arity;
        }
    }

    /// <summary>Verifies the binary tuple overload emits latest-value pairs.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CombineLatestTuplePairUsesLatestValues()
    {
        Signal<int> source = new();
        Signal<string> source2 = new();
        List<(int First, string Second)> values = [];
        using var subscription = source.CombineLatest(source2).Subscribe(values.Add);

        source.OnNext(One);
        await Assert.That(values.Count).IsEqualTo(0);

        source2.OnNext("two");
        source.OnNext(Two);

        await Assert.That(values.Count).IsEqualTo(Two);
        await Assert.That(values[0].First).IsEqualTo(One);
        await Assert.That(values[0].Second).IsEqualTo("two");
        await Assert.That(values[1].First).IsEqualTo(Two);
        await Assert.That(values[1].Second).IsEqualTo("two");
    }

    /// <summary>Verifies the three-source tuple overload emits each source in the expected tuple slot.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CombineLatestTupleThreeSourcesPreservesSourceOrdering()
    {
        Signal<int> source = new();
        Signal<string> source2 = new();
        Signal<bool> source3 = new();
        List<(int First, string Second, bool Third)> values = [];
        using var subscription = source.CombineLatest(source2, source3).Subscribe(values.Add);

        source.OnNext(One);
        source2.OnNext("two");
        await Assert.That(values.Count).IsEqualTo(0);

        source3.OnNext(true);

        await Assert.That(values.Count).IsEqualTo(1);
        await Assert.That(values[0].First).IsEqualTo(One);
        await Assert.That(values[0].Second).IsEqualTo("two");
        await Assert.That(values[0].Third).IsTrue();
    }

    /// <summary>Verifies the widest tuple overload emits all sixteen source values in order.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CombineLatestTupleSixteenSourcesPreservesSourceOrdering()
    {
        using TupleSources signals = new();
        var sources = signals.InOrder;
        List<(
            int First,
            int Second,
            int Third,
            int Fourth,
            int Fifth,
            int Sixth,
            int Seventh,
            int Eighth,
            int Ninth,
            int Tenth,
            int Eleventh,
            int Twelfth,
            int Thirteenth,
            int Fourteenth,
            int Fifteenth,
            int Sixteenth)> values = [];
        using var subscription = signals.First
            .CombineLatest(
                signals.Second,
                signals.Third,
                signals.Fourth,
                signals.Fifth,
                signals.Sixth,
                signals.Seventh,
                signals.Eighth,
                signals.Ninth,
                signals.Tenth,
                signals.Eleventh,
                signals.Twelfth,
                signals.Thirteenth,
                signals.Fourteenth,
                signals.Fifteenth,
                signals.Sixteenth)
            .Subscribe(values.Add);

        for (var i = 0; i < sources.Length; i++)
        {
            sources[i].OnNext(i + One);
        }

        await Assert.That(values.Count).IsEqualTo(1);
        await Assert.That(values[0].First).IsEqualTo(One);
        await Assert.That(values[0].Second).IsEqualTo(Two);
        await Assert.That(values[0].Third).IsEqualTo(Three);
        await Assert.That(values[0].Sixteenth).IsEqualTo(Sixteen);

        signals.Sixteenth.OnNext(OneHundred);

        await Assert.That(values.Count).IsEqualTo(Two);
        await Assert.That(values[1].First).IsEqualTo(One);
        await Assert.That(values[1].Sixteenth).IsEqualTo(OneHundred);
    }

    /// <summary>Verifies every tuple-returning CombineLatest arity uses latest-value semantics.</summary>
    /// <param name="arity">The overload arity under test.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [MethodDataSource(nameof(TupleCombineLatestArities))]
    public async Task CombineLatestTupleAritiesUseLatestValues(int arity)
    {
        using TupleSources signals = new();
        var sources = signals.InOrder;
        List<int> values = [];
        var expectedInitial = Enumerable.Range(One, arity).Sum();
        using var subscription = CreateTupleCombineLatest(arity, signals).Subscribe(values.Add);

        for (var i = 0; i < arity; i++)
        {
            sources[i].OnNext(i + One);
        }

        await Assert.That(values.SequenceEqual([expectedInitial])).IsTrue();

        sources[arity - One].OnNext(OneHundred);

        await Assert.That(values.SequenceEqual([expectedInitial, expectedInitial - arity + OneHundred])).IsTrue();
    }

    /// <summary>Verifies tuple-returning CombineLatest can be invoked through the static extension method form.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CombineLatestTuplePairSupportsStaticExtensionInvocation()
    {
        Signal<int> source = new();
        Signal<int> source2 = new();
        List<(int First, int Second)> values = [];
        using var subscription = LinqExtensions.CombineLatest(source, source2).Subscribe(values.Add);

        source.OnNext(One);
        source2.OnNext(Two);

        await Assert.That(values.Count).IsEqualTo(1);
        await Assert.That(values[0].First).IsEqualTo(One);
        await Assert.That(values[0].Second).IsEqualTo(Two);
    }

    /// <summary>Verifies tuple-returning CombineLatest completes only after every source completes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CombineLatestTuplePairCompletesAfterBothSourcesComplete()
    {
        Signal<int> source = new();
        Signal<int> source2 = new();
        var completed = 0;
        using var subscription = source.CombineLatest(source2).Subscribe(static _ => { }, static _ => { }, () => completed++);

        source.OnNext(One);
        source2.OnNext(Two);
        source.OnCompleted();

        await Assert.That(completed).IsEqualTo(0);

        source2.OnCompleted();

        await Assert.That(completed).IsEqualTo(1);
    }

    /// <summary>Verifies disposing a tuple-returning CombineLatest subscription unsubscribes its sources.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CombineLatestTuplePairDisposeUnsubscribesSources()
    {
        Signal<int> source = new();
        Signal<int> source2 = new();
        var subscription = source.CombineLatest(source2).Subscribe(static _ => { });

        await Assert.That(source.HasObservers).IsTrue();
        await Assert.That(source2.HasObservers).IsTrue();

        subscription.Dispose();

        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(source2.HasObservers).IsFalse();
    }

    /// <summary>Verifies tuple-returning CombineLatest overloads reject null sources before subscribing.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CombineLatestTupleOverloadsRejectNullSources()
    {
        Signal<int> source = new();
        Signal<int> source2 = new();

        _ = Assert.Throws<ArgumentNullException>(() => default(IObservable<int>)!.CombineLatest(source2));
        _ = Assert.Throws<ArgumentNullException>(() => source.CombineLatest((IObservable<int>)null!));
        _ = Assert.Throws<ArgumentNullException>(() => source.CombineLatest(source2, (IObservable<int>)null!));

        await Assert.That(source.HasObservers).IsFalse();
        await Assert.That(source2.HasObservers).IsFalse();
    }

    /// <summary>Creates a tuple-returning CombineLatest overload and maps its tuple to a sum.</summary>
    /// <param name="arity">The overload arity to create.</param>
    /// <param name="sources">The named source signals.</param>
    /// <returns>The summed latest-value tuple observable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservable<int> CreateTupleCombineLatest(int arity, TupleSources sources) =>
        _tupleCombineLatestBuilders[arity - Two](sources);

    /// <summary>Builds the 2-source tuple CombineLatest overload.</summary>
    /// <param name="sources">The named source signals.</param>
    /// <returns>The summed tuple observable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservable<int> TupleCombineLatestOfSecond(TupleSources sources) =>
        sources.First.CombineLatest(sources.Second)
        .Select(static values => values.First + values.Second);

    /// <summary>Builds the 3-source tuple CombineLatest overload.</summary>
    /// <param name="sources">The named source signals.</param>
    /// <returns>The summed tuple observable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservable<int> TupleCombineLatestOfThird(TupleSources sources) =>
        sources.First.CombineLatest(
            sources.Second,
            sources.Third)
        .Select(static values => values.First + values.Second + values.Third);

    /// <summary>Builds the 4-source tuple CombineLatest overload.</summary>
    /// <param name="sources">The named source signals.</param>
    /// <returns>The summed tuple observable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservable<int> TupleCombineLatestOfFourth(TupleSources sources) =>
        sources.First.CombineLatest(
            sources.Second,
            sources.Third,
            sources.Fourth)
        .Select(static values => values.First + values.Second + values.Third + values.Fourth);

    /// <summary>Builds the 5-source tuple CombineLatest overload.</summary>
    /// <param name="sources">The named source signals.</param>
    /// <returns>The summed tuple observable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservable<int> TupleCombineLatestOfFifth(TupleSources sources) =>
        sources.First.CombineLatest(
            sources.Second,
            sources.Third,
            sources.Fourth,
            sources.Fifth)
        .Select(static values => values.First + values.Second + values.Third + values.Fourth
            + values.Fifth);

    /// <summary>Builds the 6-source tuple CombineLatest overload.</summary>
    /// <param name="sources">The named source signals.</param>
    /// <returns>The summed tuple observable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservable<int> TupleCombineLatestOfSixth(TupleSources sources) =>
        sources.First.CombineLatest(
            sources.Second,
            sources.Third,
            sources.Fourth,
            sources.Fifth,
            sources.Sixth)
        .Select(static values => values.First + values.Second + values.Third + values.Fourth
            + values.Fifth + values.Sixth);

    /// <summary>Builds the 7-source tuple CombineLatest overload.</summary>
    /// <param name="sources">The named source signals.</param>
    /// <returns>The summed tuple observable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservable<int> TupleCombineLatestOfSeventh(TupleSources sources) =>
        sources.First.CombineLatest(
            sources.Second,
            sources.Third,
            sources.Fourth,
            sources.Fifth,
            sources.Sixth,
            sources.Seventh)
        .Select(static values => values.First + values.Second + values.Third + values.Fourth
            + values.Fifth + values.Sixth + values.Seventh);

    /// <summary>Builds the 8-source tuple CombineLatest overload.</summary>
    /// <param name="sources">The named source signals.</param>
    /// <returns>The summed tuple observable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservable<int> TupleCombineLatestOfEighth(TupleSources sources) =>
        sources.First.CombineLatest(
            sources.Second,
            sources.Third,
            sources.Fourth,
            sources.Fifth,
            sources.Sixth,
            sources.Seventh,
            sources.Eighth)
        .Select(static values => values.First + values.Second + values.Third + values.Fourth
            + values.Fifth + values.Sixth + values.Seventh + values.Eighth);

    /// <summary>Builds the 9-source tuple CombineLatest overload.</summary>
    /// <param name="sources">The named source signals.</param>
    /// <returns>The summed tuple observable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservable<int> TupleCombineLatestOfNinth(TupleSources sources) =>
        sources.First.CombineLatest(
            sources.Second,
            sources.Third,
            sources.Fourth,
            sources.Fifth,
            sources.Sixth,
            sources.Seventh,
            sources.Eighth,
            sources.Ninth)
        .Select(static values => values.First + values.Second + values.Third + values.Fourth
            + values.Fifth + values.Sixth + values.Seventh + values.Eighth
            + values.Ninth);

    /// <summary>Builds the 10-source tuple CombineLatest overload.</summary>
    /// <param name="sources">The named source signals.</param>
    /// <returns>The summed tuple observable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservable<int> TupleCombineLatestOfTenth(TupleSources sources) =>
        sources.First.CombineLatest(
            sources.Second,
            sources.Third,
            sources.Fourth,
            sources.Fifth,
            sources.Sixth,
            sources.Seventh,
            sources.Eighth,
            sources.Ninth,
            sources.Tenth)
        .Select(static values => values.First + values.Second + values.Third + values.Fourth
            + values.Fifth + values.Sixth + values.Seventh + values.Eighth
            + values.Ninth + values.Tenth);

    /// <summary>Builds the 11-source tuple CombineLatest overload.</summary>
    /// <param name="sources">The named source signals.</param>
    /// <returns>The summed tuple observable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservable<int> TupleCombineLatestOfEleventh(TupleSources sources) =>
        sources.First.CombineLatest(
            sources.Second,
            sources.Third,
            sources.Fourth,
            sources.Fifth,
            sources.Sixth,
            sources.Seventh,
            sources.Eighth,
            sources.Ninth,
            sources.Tenth,
            sources.Eleventh)
        .Select(static values => values.First + values.Second + values.Third + values.Fourth
            + values.Fifth + values.Sixth + values.Seventh + values.Eighth
            + values.Ninth + values.Tenth + values.Eleventh);

    /// <summary>Builds the 12-source tuple CombineLatest overload.</summary>
    /// <param name="sources">The named source signals.</param>
    /// <returns>The summed tuple observable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservable<int> TupleCombineLatestOfTwelfth(TupleSources sources) =>
        sources.First.CombineLatest(
            sources.Second,
            sources.Third,
            sources.Fourth,
            sources.Fifth,
            sources.Sixth,
            sources.Seventh,
            sources.Eighth,
            sources.Ninth,
            sources.Tenth,
            sources.Eleventh,
            sources.Twelfth)
        .Select(static values => values.First + values.Second + values.Third + values.Fourth
            + values.Fifth + values.Sixth + values.Seventh + values.Eighth
            + values.Ninth + values.Tenth + values.Eleventh + values.Twelfth);

    /// <summary>Builds the 13-source tuple CombineLatest overload.</summary>
    /// <param name="sources">The named source signals.</param>
    /// <returns>The summed tuple observable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservable<int> TupleCombineLatestOfThirteenth(TupleSources sources) =>
        sources.First.CombineLatest(
            sources.Second,
            sources.Third,
            sources.Fourth,
            sources.Fifth,
            sources.Sixth,
            sources.Seventh,
            sources.Eighth,
            sources.Ninth,
            sources.Tenth,
            sources.Eleventh,
            sources.Twelfth,
            sources.Thirteenth)
        .Select(static values => values.First + values.Second + values.Third + values.Fourth
            + values.Fifth + values.Sixth + values.Seventh + values.Eighth
            + values.Ninth + values.Tenth + values.Eleventh + values.Twelfth
            + values.Thirteenth);

    /// <summary>Builds the 14-source tuple CombineLatest overload.</summary>
    /// <param name="sources">The named source signals.</param>
    /// <returns>The summed tuple observable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservable<int> TupleCombineLatestOfFourteenth(TupleSources sources) =>
        sources.First.CombineLatest(
            sources.Second,
            sources.Third,
            sources.Fourth,
            sources.Fifth,
            sources.Sixth,
            sources.Seventh,
            sources.Eighth,
            sources.Ninth,
            sources.Tenth,
            sources.Eleventh,
            sources.Twelfth,
            sources.Thirteenth,
            sources.Fourteenth)
        .Select(static values => values.First + values.Second + values.Third + values.Fourth
            + values.Fifth + values.Sixth + values.Seventh + values.Eighth
            + values.Ninth + values.Tenth + values.Eleventh + values.Twelfth
            + values.Thirteenth + values.Fourteenth);

    /// <summary>Builds the 15-source tuple CombineLatest overload.</summary>
    /// <param name="sources">The named source signals.</param>
    /// <returns>The summed tuple observable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservable<int> TupleCombineLatestOfFifteenth(TupleSources sources) =>
        sources.First.CombineLatest(
            sources.Second,
            sources.Third,
            sources.Fourth,
            sources.Fifth,
            sources.Sixth,
            sources.Seventh,
            sources.Eighth,
            sources.Ninth,
            sources.Tenth,
            sources.Eleventh,
            sources.Twelfth,
            sources.Thirteenth,
            sources.Fourteenth,
            sources.Fifteenth)
        .Select(static values => values.First + values.Second + values.Third + values.Fourth
            + values.Fifth + values.Sixth + values.Seventh + values.Eighth
            + values.Ninth + values.Tenth + values.Eleventh + values.Twelfth
            + values.Thirteenth + values.Fourteenth + values.Fifteenth);

    /// <summary>Builds the 16-source tuple CombineLatest overload.</summary>
    /// <param name="sources">The named source signals.</param>
    /// <returns>The summed tuple observable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static IObservable<int> TupleCombineLatestOfSixteenth(TupleSources sources) =>
        sources.First.CombineLatest(
            sources.Second,
            sources.Third,
            sources.Fourth,
            sources.Fifth,
            sources.Sixth,
            sources.Seventh,
            sources.Eighth,
            sources.Ninth,
            sources.Tenth,
            sources.Eleventh,
            sources.Twelfth,
            sources.Thirteenth,
            sources.Fourteenth,
            sources.Fifteenth,
            sources.Sixteenth)
        .Select(static values => values.First + values.Second + values.Third + values.Fourth
            + values.Fifth + values.Sixth + values.Seventh + values.Eighth
            + values.Ninth + values.Tenth + values.Eleventh + values.Twelfth
            + values.Thirteenth + values.Fourteenth + values.Fifteenth + values.Sixteenth);

    /// <summary>Owns the typed sources used by tuple combination tests.</summary>
    private sealed class TupleSources : IDisposable
    {
        /// <summary>Initializes a new instance of the <see cref="TupleSources"/> class.</summary>
        public TupleSources() =>
            InOrder =
            [
                First,
                Second,
                Third,
                Fourth,
                Fifth,
                Sixth,
                Seventh,
                Eighth,
                Ninth,
                Tenth,
                Eleventh,
                Twelfth,
                Thirteenth,
                Fourteenth,
                Fifteenth,
                Sixteenth,
            ];

        /// <summary>Gets every source in argument order.</summary>
        public Signal<int>[] InOrder { get; }

        /// <summary>Gets the source in the first argument position.</summary>
        public Signal<int> First { get; } = new();

        /// <summary>Gets the source in the second argument position.</summary>
        public Signal<int> Second { get; } = new();

        /// <summary>Gets the source in the third argument position.</summary>
        public Signal<int> Third { get; } = new();

        /// <summary>Gets the source in the fourth argument position.</summary>
        public Signal<int> Fourth { get; } = new();

        /// <summary>Gets the source in the fifth argument position.</summary>
        public Signal<int> Fifth { get; } = new();

        /// <summary>Gets the source in the sixth argument position.</summary>
        public Signal<int> Sixth { get; } = new();

        /// <summary>Gets the source in the seventh argument position.</summary>
        public Signal<int> Seventh { get; } = new();

        /// <summary>Gets the source in the eighth argument position.</summary>
        public Signal<int> Eighth { get; } = new();

        /// <summary>Gets the source in the ninth argument position.</summary>
        public Signal<int> Ninth { get; } = new();

        /// <summary>Gets the source in the tenth argument position.</summary>
        public Signal<int> Tenth { get; } = new();

        /// <summary>Gets the source in the eleventh argument position.</summary>
        public Signal<int> Eleventh { get; } = new();

        /// <summary>Gets the source in the twelfth argument position.</summary>
        public Signal<int> Twelfth { get; } = new();

        /// <summary>Gets the source in the thirteenth argument position.</summary>
        public Signal<int> Thirteenth { get; } = new();

        /// <summary>Gets the source in the fourteenth argument position.</summary>
        public Signal<int> Fourteenth { get; } = new();

        /// <summary>Gets the source in the fifteenth argument position.</summary>
        public Signal<int> Fifteenth { get; } = new();

        /// <summary>Gets the source in the sixteenth argument position.</summary>
        public Signal<int> Sixteenth { get; } = new();

        /// <inheritdoc/>
        public void Dispose()
        {
            foreach (var source in InOrder)
            {
                source.Dispose();
            }
        }
    }
}
