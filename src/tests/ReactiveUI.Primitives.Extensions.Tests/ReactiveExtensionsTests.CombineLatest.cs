// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests for ReactiveExtensionsTests.</summary>
public partial class ReactiveExtensionsTests
{
    /// <summary>Exercises the <c>BooleanReduceObservable</c> ctor's <c>sources is null</c>
    /// branch — both the cast-to-IReadOnlyList and the <c>sources?.ToList()</c> shortcut produce
    /// null, so <c>InvalidOperationExceptionHelper.Check</c> throws.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestValuesAreAllFalseNullSources_ThenThrowsInvalidOperation()
    {
        Action call = static () => _ = ((IEnumerable<IObservable<bool>>)null!).CombineLatestValuesAreAllFalse();
        var ex = Assert.Throws<InvalidOperationException>(call);
        await Assert.That(ex).IsNotNull();
    }

    /// <summary>Exercises the <c>BooleanReduceObservable</c> ctor's fallback when the supplied
    /// <see cref = "IEnumerable{T}"/> is not also an <see cref = "IReadOnlyList{T}"/> — the cast
    /// fails, the null-coalescing operator falls through to <c>sources.ToList()</c>.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestValuesAreAllFalseNonListEnumerable_ThenMaterializedToList()
    {
        BehaviorSubject<bool> subject1 = new(false);
        BehaviorSubject<bool> subject2 = new(false);
        var sources = new[] { subject1.AsObservable(), subject2.AsObservable() }.Where(static _ => true);
        bool? result = null;
        using var sub = sources.CombineLatestValuesAreAllFalse().Subscribe(x => result = x);
        await Assert.That(result).IsTrue();
    }

    /// <summary>Tests CombineLatestValuesAreAllFalse.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task CombineLatestValuesAreAllFalse_WhenAllFalse_ReturnsTrue()
    {
        BehaviorSubject<bool> subject1 = new(false);
        BehaviorSubject<bool> subject2 = new(false);
        IObservable<bool>[] sources = [subject1.AsObservable(), subject2.AsObservable()];
        bool? result = null;
        using var sub = sources.CombineLatestValuesAreAllFalse().Subscribe(x => result = x);
        await Assert.That(result).IsTrue();
    }

    /// <summary>Tests CombineLatestValuesAreAllTrue.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task CombineLatestValuesAreAllTrue_WhenAllTrue_ReturnsTrue()
    {
        BehaviorSubject<bool> subject1 = new(true);
        BehaviorSubject<bool> subject2 = new(true);
        IObservable<bool>[] sources = [subject1.AsObservable(), subject2.AsObservable()];
        bool? result = null;
        using var sub = sources.CombineLatestValuesAreAllTrue().Subscribe(x => result = x);
        await Assert.That(result).IsTrue();
    }

    /// <summary>Tests GetMax returns maximum value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task GetMax_WithMultipleSources_ReturnsMaximum()
    {
        BehaviorSubject<int> subject1 = new(SampleValue5);
        BehaviorSubject<int> subject2 = new(SampleValue10);
        BehaviorSubject<int> subject3 = new(SampleValue3);
        int? result = null;
        using var sub = subject1.GetMax(subject2, subject3).Subscribe(x => result = x);
        await Assert.That(result).IsEqualTo(SampleValue10);
    }

    /// <summary>Tests GetMin returns minimum value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task GetMin_WithMultipleSources_ReturnsMinimum()
    {
        BehaviorSubject<int> subject1 = new(SampleValue5);
        BehaviorSubject<int> subject2 = new(SampleValue10);
        BehaviorSubject<int> subject3 = new(SampleValue3);
        int? result = null;
        using var sub = subject1.GetMin(subject2, subject3).Subscribe(x => result = x);
        await Assert.That(result).IsEqualTo(SampleValue3);
    }

    /// <summary>Tests GetMin tracking minimum values as sources change over time.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task GetMin_TracksMinimumOverTime()
    {
        BehaviorSubject<int> subject1 = new(SampleValue5);
        BehaviorSubject<int> subject2 = new(SampleValue10);
        BehaviorSubject<int> subject3 = new(SampleValue3);
        List<int> results = [];
        using var sub = subject1.GetMin(subject2, subject3).Subscribe(results.Add);

        // Initial minimum is 3
        await Assert.That(results).IsCollectionEqualTo([SampleValue3]);

        // Change minimum to 1
        subject3.OnNext(1);
        await Assert.That(results).IsCollectionEqualTo([SampleValue3, 1]);

        // Change minimum to 0
        subject1.OnNext(0);
        await Assert.That(results).IsCollectionEqualTo([SampleValue3, 1, 0]);
    }

    /// <summary>Tests GetMax tracking maximum values as sources change over time.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task GetMax_TracksMaximumOverTime()
    {
        BehaviorSubject<int> subject1 = new(SampleValue5);
        BehaviorSubject<int> subject2 = new(SampleValue10);
        BehaviorSubject<int> subject3 = new(SampleValue3);
        List<int> results = [];
        using var sub = subject1.GetMax(subject2, subject3).Subscribe(results.Add);

        // Initial maximum is 10
        await Assert.That(results).IsCollectionEqualTo([SampleValue10]);

        // Change maximum to 15
        subject2.OnNext(SampleValue15);
        await Assert.That(results).IsCollectionEqualTo([SampleValue10, SampleValue15]);

        // Change maximum to 20
        subject1.OnNext(SampleValue20);
        await Assert.That(results).IsCollectionEqualTo([SampleValue10, SampleValue15, SampleValue20]);
    }

    /// <summary>Tests CombineLatestValuesAreAllTrue tracking state changes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task CombineLatestValuesAreAllTrue_TracksStateChanges()
    {
        BehaviorSubject<bool> subject1 = new(false);
        BehaviorSubject<bool> subject2 = new(false);
        BehaviorSubject<bool> subject3 = new(false);
        List<bool> results = [];
        using var sub = new[] { subject1, subject2, subject3 }.CombineLatestValuesAreAllTrue().Subscribe(results.Add);

        // Initially all false
        await Assert.That(results).IsCollectionEqualTo([false]);

        // One true, still false
        subject1.OnNext(true);
        await Assert.That(results).IsCollectionEqualTo([false, false]);

        // Two true, still false
        subject2.OnNext(true);
        await Assert.That(results).IsCollectionEqualTo([false, false, false]);

        // All true
        subject3.OnNext(true);
        await Assert.That(results).IsCollectionEqualTo([false, false, false, true]);

        // Back to false
        subject1.OnNext(false);
        await Assert.That(results).IsCollectionEqualTo([false, false, false, true, false]);
    }

    /// <summary>Tests CombineLatestValuesAreAllFalse tracking state changes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task CombineLatestValuesAreAllFalse_TracksStateChanges()
    {
        BehaviorSubject<bool> subject1 = new(false);
        BehaviorSubject<bool> subject2 = new(false);
        BehaviorSubject<bool> subject3 = new(false);
        List<bool> results = [];
        using var sub = new[] { subject1, subject2, subject3 }.CombineLatestValuesAreAllFalse().Subscribe(results.Add);

        // Initially all false - result is true
        await Assert.That(results).IsCollectionEqualTo([true]);

        // One becomes true - result becomes false
        subject1.OnNext(true);
        await Assert.That(results).IsCollectionEqualTo([true, false]);

        // Back to false - result becomes true
        subject1.OnNext(false);
        await Assert.That(results).IsCollectionEqualTo([true, false, true]);

        // Another becomes true - result becomes false
        subject2.OnNext(true);
        await Assert.That(results).IsCollectionEqualTo([true, false, true, false]);
    }
}
