// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the System.Reactive-named <c>CombineLatest</c> overloads over a collection of same-typed sources.</summary>
public partial class LinqExtensionsTests
{
    /// <summary>Verifies the collection overload waits for every source before emitting a latest-value list.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CombineLatestCollectionEmitsLatestValuesOfEverySource()
    {
        using Signal<int> first = new();
        using Signal<int> second = new();
        List<IList<int>> values = [];
        using var subscription = new[] { (IObservable<int>)first, second }.CombineLatest().Subscribe(values.Add);

        first.OnNext(One);
        await Assert.That(values).IsEmpty();

        second.OnNext(Two);
        first.OnNext(Three);

        await Assert.That(values.Count).IsEqualTo(Two);
        await Assert.That(values[0].SequenceEqual([One, Two])).IsTrue();
        await Assert.That(values[1].SequenceEqual([Three, Two])).IsTrue();
    }

    /// <summary>Verifies every notification hands the subscriber its own list rather than a reused buffer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CombineLatestCollectionGivesEachNotificationItsOwnList()
    {
        using Signal<int> first = new();
        using Signal<int> second = new();
        List<IList<int>> values = [];
        using var subscription = new[] { (IObservable<int>)first, second }.CombineLatest().Subscribe(values.Add);

        first.OnNext(One);
        second.OnNext(Two);
        first.OnNext(Three);

        await Assert.That(values.Count).IsEqualTo(Two);
        await Assert.That(ReferenceEquals(values[0], values[1])).IsFalse();
        await Assert.That(values[0].SequenceEqual([One, Two])).IsTrue();
    }

    /// <summary>Verifies the selector overload projects the latest-value list.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CombineLatestCollectionSelectorProjectsTheLatestValues()
    {
        using Signal<int> first = new();
        using Signal<int> second = new();
        List<int> totals = [];
        using var subscription = new[] { (IObservable<int>)first, second }
            .CombineLatest(static values => values[0] + values[1])
            .Subscribe(totals.Add);

        first.OnNext(One);
        second.OnNext(Two);
        first.OnNext(Three);

        await Assert.That(totals.SequenceEqual([One + Two, Three + Two])).IsTrue();
    }

    /// <summary>Verifies the params overload combines the same way as the collection overload.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CombineLatestParamsCombinesEverySource()
    {
        using Signal<int> first = new();
        using Signal<int> second = new();
        using Signal<int> third = new();
        List<IList<int>> values = [];
        using var subscription = LinqExtensions.CombineLatest<int>(first, second, third).Subscribe(values.Add);

        first.OnNext(One);
        second.OnNext(Two);
        await Assert.That(values).IsEmpty();

        third.OnNext(Three);

        await Assert.That(values.Count).IsEqualTo(1);
        await Assert.That(values[0].SequenceEqual([One, Two, Three])).IsTrue();
    }

    /// <summary>Verifies the source collection is enumerated once, when the operator is called.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CombineLatestCollectionEnumeratesTheSourcesOnce()
    {
        using Signal<int> first = new();
        var enumerations = 0;
        var combined = CountingSources(first, () => enumerations++).CombineLatest();

        await Assert.That(enumerations).IsEqualTo(1);

        using var firstSubscription = combined.Subscribe(static _ => { });
        using var secondSubscription = combined.Subscribe(static _ => { });

        await Assert.That(enumerations).IsEqualTo(1);
    }

    /// <summary>Verifies an empty source collection produces an empty sequence rather than one that never ends.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CombineLatestOfNoSourcesCompletesImmediately()
    {
        RecordingWitness<IList<int>> listWitness = new();
        RecordingWitness<int> selectorWitness = new();
        using var listSubscription = Array.Empty<IObservable<int>>().CombineLatest().Subscribe(listWitness);
        using var selectorSubscription = Array.Empty<IObservable<int>>()
            .CombineLatest(static values => values.Count)
            .Subscribe(selectorWitness);

        await Assert.That(listWitness.Values).IsEmpty();
        await Assert.That(listWitness.Completed).IsEqualTo(1);
        await Assert.That(selectorWitness.Values).IsEmpty();
        await Assert.That(selectorWitness.Completed).IsEqualTo(1);
    }

    /// <summary>Verifies the collection overloads reject a null collection, a null element, and a null selector.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CombineLatestCollectionOverloadsRejectNullArguments()
    {
        using Signal<int> first = new();
        IObservable<int>[] withNullElement = [first, null!];

        _ = Assert.Throws<ArgumentNullException>(static () => ((IEnumerable<IObservable<int>>)null!).CombineLatest());
        _ = Assert.Throws<ArgumentNullException>(
            static () => ((IEnumerable<IObservable<int>>)null!).CombineLatest(static values => values.Count));
        _ = Assert.Throws<ArgumentNullException>(static () => LinqExtensions.CombineLatest<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(() => withNullElement.CombineLatest());
        _ = Assert.Throws<ArgumentNullException>(
            () => new[] { (IObservable<int>)first }.CombineLatest((Func<IList<int>, int>)null!));

        await Assert.That(first.HasObservers).IsFalse();
    }

    /// <summary>Yields the supplied sources, counting how many times the collection is enumerated.</summary>
    /// <param name="source">The single source to yield.</param>
    /// <param name="onEnumerated">Invoked once per enumeration.</param>
    /// <returns>The source collection.</returns>
    [SuppressMessage(
        "Design",
        "SST2306:Iterator arguments should be validated eagerly",
        Justification = "The deferred enumeration is what this test double exists to observe.")]
    private static IEnumerable<IObservable<int>> CountingSources(IObservable<int> source, Action onEnumerated)
    {
        onEnumerated();
        yield return source;
    }
}
