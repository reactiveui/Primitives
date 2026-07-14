// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Verifies the range-backed fast paths of the aggregate operators: a sequence built from
/// <see cref="Signal.Sequence(int, int)"/> is answered from its bounds instead of by enumerating it, and a
/// predicate that throws while the bounds are being walked is still reported as an error to the observer.
/// </summary>
public partial class SignalOperatorParityMixinsTests
{
    /// <summary>Verifies the aggregate operators answer a range source directly from its bounds.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [SuppressMessage(
        "Major Code Smell",
        "S6966:Awaitable method should be used",
        Justification =
            "This test deliberately exercises the synchronous IObservable operator overloads, not their awaitable terminal counterparts.")]
    [Test]
    public async Task RangeBackedAggregatesAreAnsweredFromTheRangeBounds()
    {
        List<int> counted = [];
        List<int> countedMatches = [];
        List<int> countedNoMatches = [];
        List<bool> any = [];
        var completions = 0;
        _ = Signal.Sequence(First, Fourth).Count().Subscribe(counted.Add, static _ => { }, () => completions++);
        _ = Signal.Sequence(First, Fourth).Count(static value => value % Second == 0).Subscribe(countedMatches.Add);
        _ = Signal.Sequence(First, Fourth).Count(static value => value == MissingRangeValue)
            .Subscribe(countedNoMatches.Add);
        _ = Signal.Sequence(First, Fourth).Any().Subscribe(any.Add);
        await Assert.That(counted.SequenceEqual([Fourth])).IsTrue();
        await Assert.That(completions).IsEqualTo(1);
        await Assert.That(countedMatches.SequenceEqual([Second])).IsTrue();
        await Assert.That(countedNoMatches.SequenceEqual([0])).IsTrue();
        await Assert.That(any.SequenceEqual([true])).IsTrue();
    }

    /// <summary>Verifies a predicate failure on a range source is reported as an error rather than thrown.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [SuppressMessage(
        "Major Code Smell",
        "S6966:Awaitable method should be used",
        Justification =
            "This test deliberately exercises the synchronous IObservable operator overloads, not their awaitable terminal counterparts.")]
    [Test]
    public async Task RangeBackedAggregatesReportAPredicateFailureAsAnError()
    {
        InvalidOperationException predicateFault = new("range-predicate-fault");
        List<Exception> countErrors = [];
        List<Exception> longCountErrors = [];
        List<Exception> anyErrors = [];
        List<int> countValues = [];
        _ = Signal.Sequence(First, Fourth).Count(_ => throw predicateFault)
            .Subscribe(countValues.Add, countErrors.Add);
        _ = Signal.Sequence(First, Fourth).LongCount(_ => throw predicateFault)
            .Subscribe(static _ => { }, longCountErrors.Add);
        _ = Signal.Sequence(First, Fourth).Any(_ => throw predicateFault)
            .Subscribe(static _ => { }, anyErrors.Add);
        await Assert.That(countValues.Count).IsEqualTo(0);
        await Assert.That(countErrors.Count).IsEqualTo(1);
        await Assert.That(countErrors[0]).IsSameReferenceAs(predicateFault);
        await Assert.That(longCountErrors.Count).IsEqualTo(1);
        await Assert.That(longCountErrors[0]).IsSameReferenceAs(predicateFault);
        await Assert.That(anyErrors.Count).IsEqualTo(1);
        await Assert.That(anyErrors[0]).IsSameReferenceAs(predicateFault);
    }
}
