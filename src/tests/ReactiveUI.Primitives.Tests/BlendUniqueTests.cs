// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Tests for the fused <see cref="LinqMixins.BlendUnique{T}(System.IObservable{T}[])"/> operator
/// (merge + distinct-until-changed in a single sink).
/// </summary>
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
    [Test]
    public void MergesSourcesAndSuppressesConsecutiveDuplicates()
    {
        var values = new List<int>();
        var completed = 0;

        // source0 emits 1,1,2 (-> 1,2) then source1 emits 2,3,3 (2 == last, dropped -> 3).
        LinqMixins.BlendUnique(
                Signal.FromEnumerable(_firstDuplicatedThenSecond),
                Signal.FromEnumerable(_secondThenThirdDuplicated))
            .Subscribe(values.Add, ex => throw ex, () => completed++);

        Assert.Equal(_distinctMerged, values);
        Assert.Equal(Once, completed);
    }

    /// <summary>Verifies that an empty source set completes immediately with no values.</summary>
    [Test]
    public void EmptySourcesCompletesImmediately()
    {
        var values = new List<int>();
        var completed = 0;

        LinqMixins.BlendUnique<int>()
            .Subscribe(values.Add, ex => throw ex, () => completed++);

        Assert.Equal(0, values.Count);
        Assert.Equal(Once, completed);
    }

    /// <summary>Verifies that a custom comparer is used to suppress duplicates.</summary>
    [Test]
    public void UsesSuppliedComparer()
    {
        var values = new List<string>();

        // Case-insensitive: "a","A" collapse; "B" forwarded.
        LinqMixins.BlendUnique(
                [Signal.FromEnumerable(_caseVariants)],
                StringComparer.OrdinalIgnoreCase)
            .Subscribe(values.Add);

        Assert.Equal(_distinctCaseInsensitive, values);
    }

    /// <summary>Verifies that the first source error terminates the merged stream.</summary>
    [Test]
    public void ForwardsFirstSourceError()
    {
        var values = new List<int>();
        Exception? error = null;

        LinqMixins.BlendUnique(
                Signal.FromEnumerable(_single),
                Signal.Fail<int>(new InvalidOperationException("boom")))
            .Subscribe(values.Add, ex => error = ex, () => { });

        Assert.Equal(_single, values);
        Assert.NotNull(error);
        Assert.True(error is InvalidOperationException);
    }

    /// <summary>Verifies argument validation for the sources array and the observer.</summary>
    [Test]
    public void NullArgumentsThrow()
    {
        Assert.Throws<ArgumentNullException>(() => LinqMixins.BlendUnique<int>(null!, comparer: null));
        Assert.Throws<ArgumentNullException>(() => LinqMixins.BlendUnique(Signal.FromEnumerable(_single)).Subscribe(null!));
    }
}
