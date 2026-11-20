// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="SkipSignal{T}"/> and <c>Skip</c> reductions.</summary>
public sealed class SkipSignalTests
{
    /// <summary>First skip stage count.</summary>
    private const int FirstSkip = 2;

    /// <summary>Second skip stage count.</summary>
    private const int SecondSkip = 3;

    /// <summary>Total emitted elements.</summary>
    private const int TotalElements = 7;

    /// <summary>First expected value after skipping five items.</summary>
    private const int SixthElement = 6;

    /// <summary>Skip(0) returns the original source instance without wrapping.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Skip_ZeroCount_ReturnsSourceUnchanged()
    {
        using Signal<int> subject = new();
        await Assert.That(subject.Skip(0)).IsSameReferenceAs(subject);
    }

    /// <summary>Chained Skip stages sum their skip counts into a single stage.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Skip_ChainedStages_SumsCounts()
    {
        using Signal<int> subject = new();
        List<int> values = [];
        using var sub = subject.Skip(FirstSkip).Skip(SecondSkip).Subscribe(values.Add);

        for (var i = 1; i <= TotalElements; i++)
        {
            subject.OnNext(i);
        }

        await Assert.That(values.SequenceEqual([SixthElement, TotalElements])).IsTrue();
    }
}
