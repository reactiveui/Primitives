// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for the fused <see cref = "LinqExtensions.BlendUnique{T}(IObservable{T}[])"/> operator (merge + distinct-until-changed in a single sink).</summary>
public class BlendUniqueTests
{
    /// <summary>The value one.</summary>
    private const int One = 1;

    /// <summary>The value two.</summary>
    private const int Two = 2;

    /// <summary>The value three.</summary>
    private const int Three = 3;

    /// <summary>The expected single-occurrence count.</summary>
    private const int Once = 1;

    /// <summary>First source: a value, an immediate duplicate, then a second value.</summary>
    private static readonly int[] _firstDuplicatedThenSecond = [One, One, Two];

    /// <summary>Second source: a value equal to the first source's last, then a value duplicated.</summary>
    private static readonly int[] _secondThenThirdDuplicated = [Two, Three, Three];

    /// <summary>Expected distinct-until-changed merge of the two sources.</summary>
    private static readonly int[] _distinctMerged = [One, Two, Three];

    /// <summary>A single-value source.</summary>
    private static readonly int[] _single = [One];

    /// <summary>Case-varied values for the comparer test.</summary>
    private static readonly string[] _caseVariants = ["a", "A", "B"];

    /// <summary>Expected case-insensitive distinct result.</summary>
    private static readonly string[] _distinctCaseInsensitive = ["a", "B"];

    /// <summary>
    /// Verifies that the merged stream forwards only values that differ from the previously forwarded one
    /// and completes once every source has completed.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task MergesSourcesAndSuppressesConsecutiveDuplicates()
    {
        List<int> values = [];
        var completed = 0;

        // source0 emits 1,1,2 (-> 1,2) then source1 emits 2,3,3 (2 == last, dropped -> 3).
        _ = LinqExtensions
            .BlendUnique(
            Signal.FromEnumerable(_firstDuplicatedThenSecond),
            Signal.FromEnumerable(_secondThenThirdDuplicated)).Subscribe(values.Add, ex => throw ex, () => completed++);
        await Assert.That(values.SequenceEqual(_distinctMerged)).IsTrue();
        await Assert.That(completed).IsEqualTo(Once);
    }

    /// <summary>Verifies that an empty source set completes immediately with no values.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task EmptySourcesCompletesImmediately()
    {
        List<int> values = [];
        var completed = 0;
        _ = LinqExtensions.BlendUnique<int>().Subscribe(values.Add, ex => throw ex, () => completed++);
        await Assert.That(values.Count).IsEqualTo(0);
        await Assert.That(completed).IsEqualTo(Once);
    }

    /// <summary>Verifies that a custom comparer is used to suppress duplicates.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UsesSuppliedComparer()
    {
        List<string> values = [];

        // Case-insensitive: "a","A" collapse; "B" forwarded.
        _ = LinqExtensions.BlendUnique([Signal.FromEnumerable(_caseVariants)], StringComparer.OrdinalIgnoreCase)
            .Subscribe(values.Add);
        await Assert.That(values.SequenceEqual(_distinctCaseInsensitive)).IsTrue();
    }

    /// <summary>Verifies that the first source error terminates the merged stream.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ForwardsFirstSourceError()
    {
        List<int> values = [];
        Exception? error = null;
        _ = LinqExtensions
            .BlendUnique(Signal.FromEnumerable(_single), Signal.Fail<int>(new InvalidOperationException("boom")))
            .Subscribe(values.Add, ex => error = ex, () => { });
        await Assert.That(values.SequenceEqual(_single)).IsTrue();
        await Assert.That(error).IsNotNull();
        await Assert.That(error is InvalidOperationException).IsTrue();
    }

    /// <summary>Verifies that disposing the subscription tears down every source subscription.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposeUnsubscribesFromSources()
    {
        Signal<int> source = new();
        List<int> values = [];
        var subscription = LinqExtensions.BlendUnique(source).Subscribe(values.Add);
        source.OnNext(One);
        subscription.Dispose();
        source.OnNext(Two); // no longer subscribed -> ignored
        await Assert.That(values.SequenceEqual(_single)).IsTrue();
    }

    /// <summary>Verifies that values, completion, and further errors are suppressed after the first terminal error.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SuppressesNotificationsAfterTerminalError()
    {
        Signal<int> first = new();
        Signal<int> second = new();
        Signal<int> third = new();
        List<int> values = [];
        Exception? error = null;
        var completed = 0;
        _ = LinqExtensions.BlendUnique(first, second, third).Subscribe(values.Add, ex => error = ex, () => completed++);
        first.OnNext(One); // forwarded
        second.OnError(new InvalidOperationException("boom")); // terminal
        first.OnNext(Two); // value suppressed (done)
        first.OnCompleted(); // completion suppressed (done)
        third.OnError(new InvalidOperationException("again")); // error suppressed (done)
        await Assert.That(values.SequenceEqual(_single)).IsTrue();
        await Assert.That(error).IsNotNull();
        await Assert.That(completed).IsEqualTo(0);
    }

    /// <summary>Verifies argument validation for the sources array, a null source element, and the observer.</summary>
    [Test]
    public void NullArgumentsThrow()
    {
        _ = Assert.Throws<ArgumentNullException>(() => LinqExtensions.BlendUnique<int>(null!, comparer: null));
        _ = Assert.Throws<ArgumentNullException>(() => LinqExtensions.BlendUnique(Signal.FromEnumerable(_single), null!));
        _ = Assert.Throws<ArgumentNullException>(() =>
            LinqExtensions.BlendUnique(Signal.FromEnumerable(_single)).Subscribe(null!));
    }
}
