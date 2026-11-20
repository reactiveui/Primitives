// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests cancellation-token and comparer overloads of terminal async operators.</summary>
public partial class TerminalOperatorTests
{
    /// <summary>Exercises the <c>CountAsync(cancellationToken)</c> overload - the no-predicate
    /// shortcut that forwards to the full overload with a null predicate.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCountAsyncWithCancellationTokenOverload_ThenReturnsCount()
    {
        const int ExpectedCount = 3;
        var result = await SignalAsync.Range(1, ShortSourceValueCount).CountAsync(CancellationToken.None);
        await Assert.That(result).IsEqualTo(ExpectedCount);
    }

    /// <summary>Exercises the <c>LongCountAsync(cancellationToken)</c> overload - the no-predicate
    /// shortcut that forwards to the full overload with a null predicate.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLongCountAsyncWithCancellationTokenOverload_ThenReturnsCount()
    {
        const long ExpectedCount = 3L;
        var result = await SignalAsync.Range(1, ShortSourceValueCount).LongCountAsync(CancellationToken.None);
        await Assert.That(result).IsEqualTo(ExpectedCount);
    }

    /// <summary>Exercises the <c>FirstOrDefaultAsync(cancellationToken)</c> overload - the no-default-no-predicate shortcut.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFirstOrDefaultAsyncWithCancellationTokenOverload_ThenReturnsFirst()
    {
        const int ExpectedFirst = 7;
        var result = await SignalAsync.Range(ExpectedFirst, ShortSourceValueCount).FirstOrDefaultAsync(CancellationToken.None);
        await Assert.That(result).IsEqualTo(ExpectedFirst);
    }

    /// <summary>Exercises the <c>LastOrDefaultAsync(cancellationToken)</c> overload - the no-default-no-predicate shortcut.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLastOrDefaultAsyncWithCancellationTokenOverload_ThenReturnsLast()
    {
        const int RangeStart = 7;
        const int ExpectedLast = 9;
        var result = await SignalAsync.Range(RangeStart, ShortSourceValueCount).LastOrDefaultAsync(CancellationToken.None);
        await Assert.That(result).IsEqualTo(ExpectedLast);
    }

    /// <summary>Exercises the <c>SingleOrDefaultAsync(cancellationToken)</c> overload - the no-default-no-predicate shortcut.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleOrDefaultAsyncWithCancellationTokenOverload_ThenReturnsValue()
    {
        const int ExpectedSingle = 11;
        var result = await SignalAsync.Return(ExpectedSingle).SingleOrDefaultAsync(CancellationToken.None);
        await Assert.That(result).IsEqualTo(ExpectedSingle);
    }

    /// <summary>Exercises the <c>ContainsAsync(value, comparer)</c> overload - the no-cancellation
    /// shortcut that forwards to the full overload with <see cref="CancellationToken.None"/>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenContainsAsyncWithComparerOverload_ThenForwardsResult()
    {
        const int Match = 2;
        var result = await SignalAsync.Range(1, ShortSourceValueCount).ContainsAsync(Match, EqualityComparer<int>.Default);
        await Assert.That(result).IsTrue();
    }

    /// <summary>Exercises the <c>ContainsAsync(value, cancellationToken)</c> overload - the
    /// no-comparer shortcut that forwards to the full overload with a null comparer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenContainsAsyncWithCancellationTokenOverload_ThenForwardsResult()
    {
        const int Match = 2;
        var result = await SignalAsync.Range(1, ShortSourceValueCount).ContainsAsync(Match, CancellationToken.None);
        await Assert.That(result).IsTrue();
    }

    /// <summary>Exercises the remaining terminal operator forwarding overloads with predicates, defaults, comparers, and tokens.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRemainingTerminalOverloadsInvoked_ThenForwardExpectedResults()
    {
        const int Match = 2;
        const int MissingThreshold = 10;
        const int FallbackValue = -1;
        var range = SignalAsync.Range(1, ShortSourceValueCount);

        await Assert.That(await range.AnyAsync(CancellationToken.None)).IsTrue();
        await Assert.That(await range.AnyAsync(static x => x == Match)).IsTrue();
        await Assert.That(await range.AllAsync(static x => x > 0, CancellationToken.None)).IsTrue();
        await Assert.That(await range.ContainsAsync(Match, EqualityComparer<int>.Default, CancellationToken.None)).IsTrue();
        await Assert.That(await range.CountAsync(static x => x >= Match, CancellationToken.None)).IsEqualTo(Match);
        await Assert.That(await range.LongCountAsync(static x => x >= Match, CancellationToken.None)).IsEqualTo(Match);
        await Assert.That(await range.FirstAsync(static x => x == Match, CancellationToken.None)).IsEqualTo(Match);
        await Assert.That(await range.FirstOrDefaultAsync(static x => x > MissingThreshold, FallbackValue, CancellationToken.None))
            .IsEqualTo(FallbackValue);
        await Assert.That(await SignalAsync.Empty<int>().FirstOrDefaultAsync(FallbackValue, CancellationToken.None))
            .IsEqualTo(FallbackValue);

        List<int> visited = [];
        await range.ForEachAsync(visited.Add, CancellationToken.None);
        await Assert.That(visited).Count().IsEqualTo(ShortSourceValueCount);

        await Assert.That(await range.LastAsync(static x => x <= Match, CancellationToken.None)).IsEqualTo(Match);
        await Assert.That(await range.LastAsync(CancellationToken.None)).IsEqualTo(ShortSourceValueCount);
        await Assert.That(await range.LastOrDefaultAsync(static x => x > MissingThreshold, FallbackValue, CancellationToken.None))
            .IsEqualTo(FallbackValue);
        await Assert.That(await SignalAsync.Empty<int>().LastOrDefaultAsync(FallbackValue, CancellationToken.None))
            .IsEqualTo(FallbackValue);

        await Assert.That(await range.SingleAsync(static x => x == Match, CancellationToken.None)).IsEqualTo(Match);
        await Assert.That(await SignalAsync.Return(Match).SingleAsync(CancellationToken.None)).IsEqualTo(Match);
        await Assert.That(await range.SingleOrDefaultAsync(static x => x > MissingThreshold, FallbackValue, CancellationToken.None))
            .IsEqualTo(FallbackValue);
        await Assert.That(await SignalAsync.Empty<int>().SingleOrDefaultAsync(FallbackValue, CancellationToken.None))
            .IsEqualTo(FallbackValue);

        List<int> enumerated = [];
        await foreach (var item in range.ToAsyncEnumerable(
            static () => System.Threading.Channels.Channel.CreateUnbounded<int>(),
            static (_, _) => default))
        {
            enumerated.Add(item);
        }

        await Assert.That(enumerated).Count().IsEqualTo(ShortSourceValueCount);

        var dictionary = await range.ToDictionaryAsync(static x => x, EqualityComparer<int>.Default, CancellationToken.None);
        await Assert.That(dictionary.Count).IsEqualTo(ShortSourceValueCount);

        var list = await range.ToListAsync(CancellationToken.None);
        await Assert.That(list).Count().IsEqualTo(ShortSourceValueCount);

        await range.WaitCompletionAsync(CancellationToken.None);
    }
}
