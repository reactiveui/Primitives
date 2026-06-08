// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Direct coverage for the cancellation-token / comparer "shortcut" overloads on the
/// terminal async operators (<c>CountAsync</c>, <c>LongCountAsync</c>, <c>FirstOrDefaultAsync</c>,
/// <c>LastOrDefaultAsync</c>, <c>SingleOrDefaultAsync</c>, <c>ContainsAsync</c>). Each shortcut
/// forwards to the full overload with a defaulted optional argument and was previously uncovered.</summary>
public partial class TerminalOperatorTests
{
    /// <summary>Exercises the <c>CountAsync(cancellationToken)</c> overload — the no-predicate
    /// shortcut that forwards to the full overload with a null predicate.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCountAsyncWithCancellationTokenOverload_ThenReturnsCount()
    {
        const int ExpectedCount = 3;
        var result = await SignalAsync.Range(1, 3).CountAsync(CancellationToken.None);
        await Assert.That(result).IsEqualTo(ExpectedCount);
    }

    /// <summary>Exercises the <c>LongCountAsync(cancellationToken)</c> overload — the no-predicate
    /// shortcut that forwards to the full overload with a null predicate.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLongCountAsyncWithCancellationTokenOverload_ThenReturnsCount()
    {
        const long ExpectedCount = 3L;
        var result = await SignalAsync.Range(1, 3).LongCountAsync(CancellationToken.None);
        await Assert.That(result).IsEqualTo(ExpectedCount);
    }

    /// <summary>Exercises the <c>FirstOrDefaultAsync(cancellationToken)</c> overload — the no-default-no-predicate shortcut.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFirstOrDefaultAsyncWithCancellationTokenOverload_ThenReturnsFirst()
    {
        const int ExpectedFirst = 7;
        var result = await SignalAsync.Range(ExpectedFirst, 3).FirstOrDefaultAsync(CancellationToken.None);
        await Assert.That(result).IsEqualTo(ExpectedFirst);
    }

    /// <summary>Exercises the <c>LastOrDefaultAsync(cancellationToken)</c> overload — the no-default-no-predicate shortcut.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLastOrDefaultAsyncWithCancellationTokenOverload_ThenReturnsLast()
    {
        const int RangeStart = 7;
        const int ExpectedLast = 9;
        var result = await SignalAsync.Range(RangeStart, 3).LastOrDefaultAsync(CancellationToken.None);
        await Assert.That(result).IsEqualTo(ExpectedLast);
    }

    /// <summary>Exercises the <c>SingleOrDefaultAsync(cancellationToken)</c> overload — the no-default-no-predicate shortcut.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleOrDefaultAsyncWithCancellationTokenOverload_ThenReturnsValue()
    {
        const int ExpectedSingle = 11;
        var result = await SignalAsync.Return(ExpectedSingle).SingleOrDefaultAsync(CancellationToken.None);
        await Assert.That(result).IsEqualTo(ExpectedSingle);
    }

    /// <summary>Exercises the <c>ContainsAsync(value, comparer)</c> overload — the no-cancellation
    /// shortcut that forwards to the full overload with <see cref="CancellationToken.None"/>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenContainsAsyncWithComparerOverload_ThenForwardsResult()
    {
        const int Match = 2;
        var result = await SignalAsync.Range(1, 3).ContainsAsync(Match, EqualityComparer<int>.Default);
        await Assert.That(result).IsTrue();
    }

    /// <summary>Exercises the <c>ContainsAsync(value, cancellationToken)</c> overload — the
    /// no-comparer shortcut that forwards to the full overload with a null comparer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenContainsAsyncWithCancellationTokenOverload_ThenForwardsResult()
    {
        const int Match = 2;
        var result = await SignalAsync.Range(1, 3).ContainsAsync(Match, CancellationToken.None);
        await Assert.That(result).IsTrue();
    }
}
