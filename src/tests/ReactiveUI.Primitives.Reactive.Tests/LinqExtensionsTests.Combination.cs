// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using Microsoft.Reactive.Testing;
using ReactiveLinqExtensions = ReactiveUI.Primitives.Reactive.LinqExtensions;

namespace ReactiveUI.Primitives.Reactive.Tests;

/// <summary>Tests shared combination and timing operators in the Reactive package.</summary>
public partial class LinqExtensionsTests
{
    /// <summary>Verifies tuple combination works with System.Reactive subjects.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CombineLatestTuplesAcceptSystemReactiveSources()
    {
        const string SecondValue = "second";
        using Subject<int> first = new();
        using Subject<string> second = new();
        List<(int First, string Second)> values = [];
        using var subscription = ReactiveLinqExtensions.CombineLatest(first, second).Subscribe(values.Add);

        first.OnNext(One);
        await Assert.That(values).IsEmpty();
        second.OnNext(SecondValue);
        first.OnNext(Two);

        await Assert.That(values.SequenceEqual([(One, SecondValue), (Two, SecondValue)])).IsTrue();
    }

    /// <summary>Verifies list combination works with System.Reactive subjects.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CombineLatestListsAcceptSystemReactiveSources()
    {
        using Subject<int> first = new();
        using Subject<int> second = new();
        List<IList<int>> values = [];
        using var subscription = ReactiveLinqExtensions.CombineLatest<int>(first, second).Subscribe(values.Add);

        first.OnNext(One);
        await Assert.That(values).IsEmpty();
        second.OnNext(Two);

        await Assert.That(values.Count).IsEqualTo(1);
        await Assert.That(values[0].SequenceEqual([One, Two])).IsTrue();
    }

    /// <summary>Verifies the Reactive throttle honors the latest value's quiet period.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ThrottleRestartsTheQuietPeriodOnSystemReactiveScheduler()
    {
        const int QuietTicks = 10;
        const int HalfQuietTicks = 5;
        TestScheduler scheduler = new();
        using Subject<int> source = new();
        List<int> values = [];
        using var subscription = ReactiveLinqExtensions.Throttle(source, TimeSpan.FromTicks(QuietTicks), scheduler)
            .Subscribe(values.Add);

        source.OnNext(One);
        scheduler.AdvanceBy(HalfQuietTicks);
        source.OnNext(Two);
        scheduler.AdvanceBy(HalfQuietTicks);
        await Assert.That(values).IsEmpty();

        scheduler.AdvanceBy(HalfQuietTicks);
        await Assert.That(values.SequenceEqual([Two])).IsTrue();
    }
}
