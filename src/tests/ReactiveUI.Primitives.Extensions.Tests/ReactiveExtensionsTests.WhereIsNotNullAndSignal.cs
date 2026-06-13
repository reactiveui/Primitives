// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests for ReactiveExtensionsTests.</summary>
public partial class ReactiveExtensionsTests
{
    /// <summary>Alternating true/false source pattern (T,F,T,F,T) used by predicate tests.</summary>
    private static readonly bool[] WhereIsNotNullSignalAlternatingTrueFalse = [true, false, true, false, true];

    /// <summary>Expected sequence of first/second/third strings (nullable element type to match WhereIsNotNull source signature).</summary>
    private static readonly string?[] ExpectedFirstSecondThirdNullable = ["first", "second", "third"];

    /// <summary>Tests the WhereIsNotNull extension.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task GivenNull_WhenWhereIsNotNull_ThenNoNotification()
    {
        // Given, When
        bool? result = null;
        using var disposable = Observable.Return<bool?>(null).WhereIsNotNull().Subscribe(x => result = x);

        // Then
        await Assert.That(result).IsNull();
    }

    /// <summary>Tests the WhereIsNotNull extension.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task GivenValue_WhenWhereIsNotNull_ThenNotification()
    {
        // Given, When
        bool? result = null;
        using var disposable = Observable.Return<bool?>(false).WhereIsNotNull().Subscribe(x => result = x);

        // Then
        await Assert.That(result).IsFalse();
    }

    /// <summary>Tests the AsSignal extension.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task GivenObservable_WhenAsSignal_ThenNotifiesUnit()
    {
        // Given, When
        RxVoid? result = null;
        using var disposable = Observable.Return<bool?>(false).AsSignal().Subscribe(x => result = x);

        // Then
        await Assert.That(result).IsEqualTo(RxVoid.Default);
    }

    /// <summary>Tests Not inverts boolean.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Not_InvertsBoolean()
    {
        BehaviorSubject<bool> subject = new(true);
        bool? result = null;
        using var sub = subject.Not().Subscribe(x => result = x);
        await Assert.That(result).IsFalse();
    }

    /// <summary>Tests WhereTrue filters to true values.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhereTrue_FiltersTrueValues()
    {
        var source = WhereIsNotNullSignalAlternatingTrueFalse.ToObservable();
        List<bool> results = [];
        using var sub = source.WhereTrue().Subscribe(results.Add);
        await Assert.That(results).IsCollectionEqualTo([true, true, true]);
    }

    /// <summary>Tests WhereFalse filters to false values.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhereFalse_FiltersFalseValues()
    {
        var source = WhereIsNotNullSignalAlternatingTrueFalse.ToObservable();
        List<bool> results = [];
        using var sub = source.WhereFalse().Subscribe(results.Add);
        await Assert.That(results).IsCollectionEqualTo([false, false]);
    }

    /// <summary>Tests WhereIsNotNull filters null values.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhereIsNotNull_FiltersNullValues()
    {
        var source = new[] { "a", null, "b", null, "c" }.ToObservable();
        List<string> results = [];
        using var sub = source.WhereIsNotNull().Subscribe(x => results.Add(x!));
        await Assert.That(results).IsCollectionEqualTo(["a", "b", "c"]);
    }

    /// <summary>Tests AsSignal converts to RxVoid.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task AsSignal_ConvertsToUnit()
    {
        var source = Observable.Range(1, 3);
        List<RxVoid> results = [];
        using var sub = source.AsSignal().Subscribe(results.Add);
        await Assert.That(results).Count().IsEqualTo(SampleValue3);
    }

    /// <summary>Tests WhereIsNotNull filtering nulls over time.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhereIsNotNull_FiltersNullsOverTime()
    {
        Subject<string?> subject = new();
        List<string?> results = [];
        subject.WhereIsNotNull().Subscribe(results.Add);
        subject.OnNext("first");
        subject.OnNext(null);
        subject.OnNext("second");
        subject.OnNext(null);
        subject.OnNext("third");

        // Only non-null values collected
        await Assert.That(results).IsCollectionEqualTo(ExpectedFirstSecondThirdNullable);
    }

    /// <summary>Tests Not operator inverting boolean values over time.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Not_InvertsBooleanValuesOverTime()
    {
        Subject<bool> subject = new();
        List<bool> results = [];
        subject.Not().Subscribe(results.Add);
        subject.OnNext(true);
        subject.OnNext(false);
        subject.OnNext(true);
        subject.OnNext(false);
        await Assert.That(results).IsCollectionEqualTo([false, true, false, true]);
    }

    /// <summary>Tests AsSignal converting values to RxVoid over time.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task AsSignal_ConvertsToUnitOverTime()
    {
        Subject<int> subject = new();
        List<RxVoid> results = [];
        subject.AsSignal().Subscribe(results.Add);
        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        subject.OnNext(SampleValue3);

        // All values converted to RxVoid.Default
        await Assert.That(results).Count().IsEqualTo(SampleValue3);
        await Assert.That(results).All(x => x == RxVoid.Default);
    }
}
