// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Threading.Channels;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Tests for terminal operators: FirstAsync, LastAsync, SingleAsync, CountAsync, AnyAsync, AllAsync,
/// ContainsAsync, AggregateAsync, ToListAsync, ToDictionaryAsync, ForEachAsync, WaitCompletionAsync, ToAsyncEnumerable.
/// </summary>
public partial class TerminalOperatorTests
{
    /// <summary>String literal "resume error" used by multiple tests.</summary>
    private const string ResumeErrorMessage = "resume error";

    /// <summary>Expected exception text when a single/first observer terminates on an empty source without a predicate.</summary>
    private const string NoElementsMessage = "Sequence contains no elements.";

    /// <summary>Expected exception text when a single/first observer terminates with a predicate that never matches.</summary>
    private const string NoMatchingElementsMessage = "Sequence contains no matching elements.";

    /// <summary>Expected exception text when SingleAsync sees more than one element with no predicate.</summary>
    private const string MoreThanOneElementMessage = "Sequence contains more than one element.";

    /// <summary>Expected exception text when SingleAsync's predicate matches more than one element.</summary>
    private const string MoreThanOneMatchingElementMessage = "Sequence contains more than one matching element.";

    /// <summary>String literal "source failed" used by multiple tests.</summary>
    private const string SourceFailedMessage = "source failed";

    /// <summary>Number of values in the five-element source shared by the terminal tests.</summary>
    private const int SourceValueCount = 5;

    /// <summary>Number of values in the three-element source shared by the terminal tests.</summary>
    private const int ShortSourceValueCount = 3;

    /// <summary>Number of values in the four-element source used by the aggregate tests.</summary>
    private const int AggregateSourceCount = 4;

    /// <summary>Predicate threshold that only the values above the third clear.</summary>
    private const int MatchThreshold = 3;

    /// <summary>Predicate bound below which only the first three values fall.</summary>
    private const int UpperMatchBound = 4;

    /// <summary>Predicate threshold that several values clear, so a single match is impossible.</summary>
    private const int MultiMatchThreshold = 2;

    /// <summary>Predicate bound that no value of any test source reaches.</summary>
    private const int UnmatchableThreshold = 100;

    /// <summary>Value present in the sources used by the contains tests.</summary>
    private const int PresentValue = 3;

    /// <summary>Value absent from the sources used by the contains tests.</summary>
    private const int AbsentValue = 99;

    /// <summary>Value emitted by the single-value sources.</summary>
    private const int SentinelValue = 42;

    /// <summary>Divisor of the even-number predicate.</summary>
    private const int EvenDivisor = 2;

    /// <summary>Hoisted source array used by tests (was inline literal).</summary>
    private static readonly string[] SequenceABbCcc = ["a", "bb", "ccc"];

    /// <summary>Hoisted source array used by tests (was inline literal).</summary>
    private static readonly string[] SequenceHelloWorld = ["Hello", "World"];

    /// <summary>Tests FirstAsync returns first element.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFirstAsync_ThenReturnsFirstElement()
    {
        const int ExpectedFirst = 10;
        var result = await SignalAsync.Range(ExpectedFirst, ShortSourceValueCount).FirstAsync();
        await Assert.That(result).IsEqualTo(ExpectedFirst);
    }

    /// <summary>Tests FirstAsync with predicate returns first match.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFirstAsyncWithPredicate_ThenReturnsFirstMatch()
    {
        const int ExpectedFirstMatch = 4;
        var result = await SignalAsync.Range(1, SourceValueCount).FirstAsync(static x => x > MatchThreshold);
        await Assert.That(result).IsEqualTo(ExpectedFirstMatch);
    }

    /// <summary>Tests FirstAsync on empty throws InvalidOperationException with the no-elements
    /// message — exercises the predicate-null branch of <c>FirstTaskObserver.OnCompletedAsyncCore</c>.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFirstAsyncOnEmpty_ThenThrowsInvalidOperation()
    {
        var ex = await Assert.That(static async () => await SignalAsync.Empty<int>().FirstAsync())
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(NoElementsMessage);
    }

    /// <summary>Tests FirstAsync with predicate when no elements match throws InvalidOperationException with matching message.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFirstAsyncWithPredicateNoMatch_ThenThrowsInvalidOperationWithMatchingMessage()
    {
        var ex = await Assert.That(
            static async () => await SignalAsync.Range(1, SourceValueCount)
                .FirstAsync(static x => x > UnmatchableThreshold))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(NoMatchingElementsMessage);
    }

    /// <summary>Tests FirstAsync propagates error from OnErrorResumeAsync.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFirstAsyncSourceEmitsErrorResume_ThenThrowsSourceException()
    {
        InvalidOperationException expectedError = new(ResumeErrorMessage);
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(expectedError, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.FirstAsync()).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(ResumeErrorMessage);
    }

    /// <summary>Tests FirstAsync propagates error when source completes with failure result.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFirstAsyncSourceCompletesWithFailure_ThenThrowsSourceException()
    {
        InvalidOperationException expectedError = new(SourceFailedMessage);
        var source = SignalAsync.Create<int>(async (observer, _) =>
        {
            await observer.OnCompletedAsync(new(expectedError));
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.FirstAsync()).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(SourceFailedMessage);
    }

    /// <summary>Tests FirstOrDefault on empty returns default.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFirstOrDefaultOnEmpty_ThenReturnsDefault()
    {
        var result = await SignalAsync.Empty<int>().FirstOrDefaultAsync();
        await Assert.That(result).IsEqualTo(0);
    }

    /// <summary>Tests FirstOrDefault with predicate match returns first.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFirstOrDefaultWithMatch_ThenReturnsFirst()
    {
        const int ExpectedFirstMatch = 4;
        var result = await SignalAsync.Range(1, SourceValueCount).Where(static x => x > MatchThreshold).FirstOrDefaultAsync(0);
        await Assert.That(result).IsEqualTo(ExpectedFirstMatch);
    }

    /// <summary>Tests FirstOrDefaultAsync with predicate returns first matching element.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFirstOrDefaultAsyncWithPredicate_ThenReturnsFirstMatch()
    {
        const int ExpectedFirstMatch = 4;
        var result = await SignalAsync.Range(1, SourceValueCount).FirstOrDefaultAsync(static x => x > MatchThreshold, -1);
        await Assert.That(result).IsEqualTo(ExpectedFirstMatch);
    }

    /// <summary>Tests FirstOrDefaultAsync with predicate and no match returns specified default value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFirstOrDefaultAsyncWithPredicateNoMatch_ThenReturnsDefaultValue()
    {
        var result = await SignalAsync.Range(1, SourceValueCount).FirstOrDefaultAsync(static x => x > UnmatchableThreshold, -1);
        await Assert.That(result).IsEqualTo(-1);
    }

    /// <summary>Tests FirstOrDefaultAsync with predicate propagates error from OnErrorResumeAsync.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFirstOrDefaultAsyncWithPredicateSourceEmitsErrorResume_ThenThrows()
    {
        InvalidOperationException expectedError = new(ResumeErrorMessage);
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(expectedError, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.FirstOrDefaultAsync(static x => x > 0, -1))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(ResumeErrorMessage);
    }

    /// <summary>Tests FirstOrDefaultAsync propagates error from OnErrorResumeAsync.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFirstOrDefaultAsyncSourceEmitsErrorResume_ThenThrows()
    {
        InvalidOperationException expectedError = new(ResumeErrorMessage);
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(expectedError, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.FirstOrDefaultAsync())
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(ResumeErrorMessage);
    }

    /// <summary>Tests LastAsync returns last element.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLastAsync_ThenReturnsLastElement()
    {
        const int ExpectedLast = 5;
        var result = await SignalAsync.Range(1, SourceValueCount).LastAsync();
        await Assert.That(result).IsEqualTo(ExpectedLast);
    }

    /// <summary>Tests LastAsync with predicate returns last match.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLastAsyncWithPredicate_ThenReturnsLastMatch()
    {
        const int ExpectedLastMatch = 3;
        var result = await SignalAsync.Range(1, SourceValueCount).LastAsync(static x => x < UpperMatchBound);
        await Assert.That(result).IsEqualTo(ExpectedLastMatch);
    }

    /// <summary>Tests LastAsync on empty throws.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenLastAsyncOnEmpty_ThenThrowsInvalidOperation() => await Assert
        .That(static async () => await SignalAsync.Empty<int>().LastAsync())
        .ThrowsExactly<InvalidOperationException>();

    /// <summary>Tests LastAsync with predicate and no match throws with matching-elements message.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLastAsyncWithPredicateNoMatch_ThenThrowsWithMatchingMessage()
    {
        var ex = await Assert.That(
            static async () => await SignalAsync.Range(1, SourceValueCount)
                .LastAsync(static x => x > UnmatchableThreshold))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(NoMatchingElementsMessage);
    }

    /// <summary>Tests LastAsync on empty throws with no-elements message.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLastAsyncOnEmpty_ThenThrowsWithNoElementsMessage()
    {
        var ex = await Assert.That(static async () => await SignalAsync.Empty<int>().LastAsync())
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(NoElementsMessage);
    }

    /// <summary>Tests LastAsync propagates error from OnErrorResumeAsync.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLastAsyncSourceEmitsErrorResume_ThenThrowsSourceException()
    {
        InvalidOperationException expectedError = new(ResumeErrorMessage);
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(expectedError, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.LastAsync()).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(ResumeErrorMessage);
    }

    /// <summary>Tests LastAsync propagates error when source completes with failure result.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLastAsyncSourceCompletesWithFailure_ThenThrowsSourceException()
    {
        InvalidOperationException expectedError = new(SourceFailedMessage);
        var source = SignalAsync.Create<int>(async (observer, _) =>
        {
            await observer.OnCompletedAsync(new(expectedError));
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.LastAsync()).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(SourceFailedMessage);
    }

    /// <summary>Tests LastOrDefault on empty returns default.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLastOrDefaultOnEmpty_ThenReturnsDefault()
    {
        var result = await SignalAsync.Empty<int>().LastOrDefaultAsync();
        await Assert.That(result).IsEqualTo(0);
    }

    /// <summary>Tests LastOrDefaultAsync with predicate returns the last matching element.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLastOrDefaultAsyncWithPredicate_ThenReturnsLastMatch()
    {
        const int ExpectedLastMatch = 3;
        var result = await SignalAsync.Range(1, SourceValueCount).LastOrDefaultAsync(static x => x < UpperMatchBound, -1);
        await Assert.That(result).IsEqualTo(ExpectedLastMatch);
    }

    /// <summary>Tests LastOrDefaultAsync with predicate returns default when no elements match.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLastOrDefaultAsyncWithPredicateNoMatch_ThenReturnsDefaultValue()
    {
        var result = await SignalAsync.Range(1, SourceValueCount).LastOrDefaultAsync(static x => x > UnmatchableThreshold, -1);
        await Assert.That(result).IsEqualTo(-1);
    }

    /// <summary>Tests LastOrDefaultAsync with predicate on empty returns default value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLastOrDefaultAsyncWithPredicateOnEmpty_ThenReturnsDefaultValue()
    {
        const int DefaultValue = 42;
        var result = await SignalAsync.Empty<int>().LastOrDefaultAsync(static _ => true, DefaultValue);
        await Assert.That(result).IsEqualTo(DefaultValue);
    }

    /// <summary>Tests LastOrDefaultAsync with custom default value on empty returns that default.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLastOrDefaultAsyncWithDefaultValueOnEmpty_ThenReturnsCustomDefault()
    {
        const int CustomDefault = 99;
        var result = await SignalAsync.Empty<int>().LastOrDefaultAsync(CustomDefault);
        await Assert.That(result).IsEqualTo(CustomDefault);
    }

    /// <summary>Tests LastOrDefaultAsync propagates error from OnErrorResumeAsync.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLastOrDefaultAsyncSourceEmitsErrorResume_ThenThrows()
    {
        InvalidOperationException expectedError = new(ResumeErrorMessage);
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(expectedError, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.LastOrDefaultAsync())
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(ResumeErrorMessage);
    }

    /// <summary>Tests LastOrDefaultAsync propagates error when source completes with failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLastOrDefaultAsyncSourceCompletesWithFailure_ThenThrows()
    {
        InvalidOperationException expectedError = new(SourceFailedMessage);
        var source = SignalAsync.Create<int>(async (observer, _) =>
        {
            await observer.OnCompletedAsync(new(expectedError));
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.LastOrDefaultAsync())
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(SourceFailedMessage);
    }

    /// <summary>Tests SingleAsync returns single element.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAsync_ThenReturnsSingleElement()
    {
        const int SingleValue = 42;
        var result = await SignalAsync.Return(SingleValue).SingleAsync();
        await Assert.That(result).IsEqualTo(SingleValue);
    }

    /// <summary>Tests SingleAsync multiple elements throws.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSingleAsyncMultipleElements_ThenThrowsInvalidOperation()
    {
        const int MultipleElementCount = 3;
        await Assert.That(static async () => await SignalAsync.Range(1, MultipleElementCount).SingleAsync())
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Tests SingleAsync on empty throws — exercises the predicate-null branch of
    /// <c>SingleElementObserver.OnCompletedAsyncCore</c>'s message construction.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAsyncOnEmpty_ThenThrowsInvalidOperation()
    {
        var ex = await Assert.That(static async () => await SignalAsync.Empty<int>().SingleAsync())
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(NoElementsMessage);
    }

    /// <summary>Tests SingleAsync with predicate on no-match throws — exercises the
    /// predicate-non-null branch of <c>SingleElementObserver.OnCompletedAsyncCore</c>.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAsyncWithPredicateNoMatch_ThenThrowsInvalidOperationWithMatchingMessage()
    {
        var ex = await Assert.That(
            static async () => await SignalAsync.Range(1, SourceValueCount)
                .SingleAsync(static x => x > UnmatchableThreshold))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(NoMatchingElementsMessage);
    }

    /// <summary>Tests SingleOrDefault on empty returns default.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleOrDefaultOnEmpty_ThenReturnsDefault()
    {
        var result = await SignalAsync.Empty<int>().SingleOrDefaultAsync();
        await Assert.That(result).IsEqualTo(0);
    }

    /// <summary>Tests SingleOrDefaultAsync with predicate returns matching element.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleOrDefaultAsyncWithPredicate_ThenReturnsMatchingElement()
    {
        const int ExpectedMatch = 3;
        var result = await SignalAsync.Range(1, SourceValueCount).SingleOrDefaultAsync(static x => x == ExpectedMatch, -1);
        await Assert.That(result).IsEqualTo(ExpectedMatch);
    }

    /// <summary>Tests SingleOrDefaultAsync with predicate and no match returns default value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleOrDefaultAsyncWithPredicateNoMatch_ThenReturnsDefaultValue()
    {
        var result = await SignalAsync.Range(1, SourceValueCount).SingleOrDefaultAsync(static x => x > UnmatchableThreshold, -1);
        await Assert.That(result).IsEqualTo(-1);
    }

    /// <summary>Tests SingleOrDefaultAsync with predicate matching multiple elements throws.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSingleOrDefaultAsyncWithPredicateMultipleMatches_ThenThrowsInvalidOperation()
    {
        const int SourceCount = 5;
        const int Threshold = 2;
        await Assert
            .That(static async () =>
                await SignalAsync.Range(1, SourceCount).SingleOrDefaultAsync(static x => x > Threshold, -1))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Tests SingleOrDefaultAsync with no predicate and multiple elements throws.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSingleOrDefaultAsyncMultipleElements_ThenThrowsInvalidOperation()
    {
        const int MultipleElementCount = 3;
        await Assert.That(
            static async () => await SignalAsync.Range(1, MultipleElementCount).SingleOrDefaultAsync(0))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Tests SingleOrDefaultAsync with custom default value on empty returns that default.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleOrDefaultAsyncWithDefaultValueOnEmpty_ThenReturnsCustomDefault()
    {
        const int CustomDefault = 99;
        var result = await SignalAsync.Empty<int>().SingleOrDefaultAsync(CustomDefault);
        await Assert.That(result).IsEqualTo(CustomDefault);
    }

    /// <summary>Tests SingleOrDefaultAsync propagates error from OnErrorResumeAsync.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSingleOrDefaultAsyncSourceEmitsErrorResume_ThenThrows()
    {
        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException("resume"), ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        await Assert.That(async () => await source.SingleOrDefaultAsync()).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Tests SingleOrDefaultAsync propagates error from source completing with failure.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenSingleOrDefaultAsyncSourceCompletesWithError_ThenThrows()
    {
        var source = SignalAsync.Create<int>(static async (observer, _) =>
        {
            await observer.OnCompletedAsync(Result.Failure(new InvalidOperationException("fail")));
            return DisposableAsync.Empty;
        });
        await Assert.That(async () => await source.SingleOrDefaultAsync()).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Tests SingleOrDefaultAsync without predicate reports correct message when multiple elements exist.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleOrDefaultAsyncMultipleElementsNoPredicate_ThenMessageReportsMoreThanOneElement()
    {
        var ex = await Assert.That(
            static async () => await SignalAsync.Range(1, ShortSourceValueCount).SingleOrDefaultAsync(0))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(MoreThanOneElementMessage);
    }

    /// <summary>Tests SingleOrDefaultAsync with predicate reports correct message when multiple elements match.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task
        WhenSingleOrDefaultAsyncMultipleMatchesWithPredicate_ThenMessageReportsMoreThanOneMatchingElement()
    {
        var ex = await Assert.That(
            static async () => await SignalAsync.Range(1, SourceValueCount)
                .SingleOrDefaultAsync(static x => x > MultiMatchThreshold, -1))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(MoreThanOneMatchingElementMessage);
    }

    /// <summary>Tests SingleOrDefaultAsync propagates error from OnErrorResumeAsync with the defaultValue overload.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleOrDefaultAsyncWithDefaultValueSourceEmitsErrorResume_ThenThrowsWithCorrectMessage()
    {
        InvalidOperationException expectedError = new("resume error detail");
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(expectedError, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.SingleOrDefaultAsync(0))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo("resume error detail");
    }

    /// <summary>Tests SingleOrDefaultAsync propagates failure result from OnCompletedAsync with the defaultValue overload.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleOrDefaultAsyncWithDefaultValueSourceCompletesWithFailure_ThenThrowsWithCorrectMessage()
    {
        InvalidOperationException expectedError = new("completion failure detail");
        var source = SignalAsync.Create<int>(async (observer, _) =>
        {
            await observer.OnCompletedAsync(Result.Failure(expectedError));
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.SingleOrDefaultAsync(0))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo("completion failure detail");
    }

    /// <summary>Tests CountAsync returns element count.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCountAsync_ThenReturnsElementCount()
    {
        const int ExpectedCount = 5;
        var result = await SignalAsync.Range(1, SourceValueCount).CountAsync();
        await Assert.That(result).IsEqualTo(ExpectedCount);
    }

    /// <summary>Tests CountAsync on empty returns zero.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCountAsyncOnEmpty_ThenReturnsZero()
    {
        var result = await SignalAsync.Empty<int>().CountAsync();
        await Assert.That(result).IsEqualTo(0);
    }

    /// <summary>Tests CountAsync propagates error from OnErrorResumeAsync through the OnErrorResumeAsyncCore path.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCountAsyncSourceEmitsErrorResume_ThenThrowsSourceException()
    {
        InvalidOperationException expectedError = new(ResumeErrorMessage);
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(expectedError, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.CountAsync()).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(ResumeErrorMessage);
    }

    /// <summary>Tests LongCountAsync returns element count.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLongCountAsync_ThenReturnsElementCount()
    {
        const long ExpectedLongCount = 3L;
        var result = await SignalAsync.Range(1, ShortSourceValueCount).LongCountAsync();
        await Assert.That(result).IsEqualTo(ExpectedLongCount);
    }

    /// <summary>Tests AnyAsync on non-empty returns true.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAnyAsyncOnNonEmpty_ThenReturnsTrue()
    {
        var result = await SignalAsync.Return(1).AnyAsync();
        await Assert.That(result).IsTrue();
    }

    /// <summary>Tests AnyAsync on empty returns false.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAnyAsyncOnEmpty_ThenReturnsFalse()
    {
        var result = await SignalAsync.Empty<int>().AnyAsync();
        await Assert.That(result).IsFalse();
    }

    /// <summary>Tests AnyAsync with predicate checks condition.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAnyAsyncWithPredicateMatch_ThenReturnsTrue()
    {
        var hasEven = await SignalAsync.Range(1, SourceValueCount).AnyAsync(static x => x % EvenDivisor == 0, CancellationToken.None);
        await Assert.That(hasEven).IsTrue();
    }

    /// <summary>Tests AnyAsync with predicate no match returns false.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAnyAsyncWithPredicateNoMatch_ThenReturnsFalse()
    {
        var hasNeg = await SignalAsync.Range(1, SourceValueCount).AnyAsync(static x => x < 0, CancellationToken.None);
        await Assert.That(hasNeg).IsFalse();
    }

    /// <summary>Tests AllAsync checks all elements.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAllAsyncAllMatch_ThenReturnsTrue()
    {
        var allPositive = await SignalAsync.Range(1, SourceValueCount).AllAsync(static x => x > 0);
        await Assert.That(allPositive).IsTrue();
    }

    /// <summary>Tests AllAsync with partial match returns false.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAllAsyncPartialMatch_ThenReturnsFalse()
    {
        var allGreaterThan3 = await SignalAsync.Range(1, SourceValueCount)
            .AllAsync(static x => x > MatchThreshold);
        await Assert.That(allGreaterThan3).IsFalse();
    }

    /// <summary>Tests AllAsync on empty returns true.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAllAsyncOnEmpty_ThenReturnsTrue()
    {
        var result = await SignalAsync.Empty<int>().AllAsync(static _ => false);
        await Assert.That(result).IsTrue();
    }

    /// <summary>Tests AnyAsync propagates error from OnErrorResumeAsync.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAnyAsyncSourceEmitsErrorResume_ThenThrowsSourceException()
    {
        InvalidOperationException expectedError = new("any resume error");
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(expectedError, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.AnyAsync()).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo("any resume error");
    }

    /// <summary>Tests AnyAsync with predicate propagates error from OnErrorResumeAsync.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAnyAsyncWithPredicateSourceEmitsErrorResume_ThenThrowsSourceException()
    {
        InvalidOperationException expectedError = new("any predicate resume error");
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(expectedError, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.AnyAsync(static x => x > 0, CancellationToken.None))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo("any predicate resume error");
    }

    /// <summary>Tests AllAsync propagates error from OnErrorResumeAsync.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAllAsyncSourceEmitsErrorResume_ThenThrowsSourceException()
    {
        InvalidOperationException expectedError = new("all resume error");
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(expectedError, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.AllAsync(static x => x > 0))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo("all resume error");
    }

    /// <summary>Tests ContainsAsync with match returns true.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenContainsAsyncWithMatch_ThenReturnsTrue()
    {
        var result = await SignalAsync.Range(1, SourceValueCount).ContainsAsync(PresentValue);
        await Assert.That(result).IsTrue();
    }

    /// <summary>Tests ContainsAsync with no match returns false.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenContainsAsyncWithNoMatch_ThenReturnsFalse()
    {
        var result = await SignalAsync.Range(1, SourceValueCount).ContainsAsync(AbsentValue);
        await Assert.That(result).IsFalse();
    }

    /// <summary>Tests ContainsAsync propagates error from OnErrorResumeAsync.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenContainsAsyncSourceEmitsErrorResume_ThenThrowsSourceException()
    {
        InvalidOperationException expectedError = new(ResumeErrorMessage);
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(expectedError, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.ContainsAsync(SentinelValue))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(ResumeErrorMessage);
    }

    /// <summary>Tests sync AggregateAsync computes final value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAggregateAsyncSync_ThenComputesFinalValue()
    {
        const int ExpectedSum = 10;
        var result = await SignalAsync.Range(1, AggregateSourceCount)
            .AggregateAsync(0, static (acc, x) => acc + x);
        await Assert.That(result).IsEqualTo(ExpectedSum);
    }

    /// <summary>Tests async AggregateAsync computes final value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAggregateAsyncAsync_ThenComputesFinalValue()
    {
        var result = await SignalAsync.Range(1, ShortSourceValueCount).AggregateAsync(
            string.Empty,
            static async (acc, x, _) =>
            {
                await Task.Yield();
                return acc + x;
            });
        await Assert.That(result).IsEqualTo("123");
    }

    /// <summary>Tests AggregateAsync with result selector transforms final value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAggregateAsyncWithResultSelector_ThenTransformsFinalValue()
    {
        var result = await SignalAsync.Range(1, AggregateSourceCount)
            .AggregateAsync(0, static (acc, x) => acc + x, static acc => $"Sum={acc}");
        await Assert.That(result).IsEqualTo("Sum=10");
    }

    /// <summary>Tests AggregateAsync propagates error from OnErrorResumeAsync.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAggregateAsyncSourceEmitsErrorResume_ThenThrowsSourceException()
    {
        InvalidOperationException expectedError = new("aggregate resume error");
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(expectedError, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.AggregateAsync(0, static (acc, x) => acc + x))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo("aggregate resume error");
    }

    /// <summary>Tests AggregateAsync null accumulator throws.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenAggregateAsyncNullAccumulator_ThenThrowsArgumentNull() => await Assert
        .That(static async () => await SignalAsync.Return(1).AggregateAsync(0, (Func<int, int, int>)null!))
        .ThrowsExactly<ArgumentNullException>();

    /// <summary>Tests ToListAsync collects all elements.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenToListAsync_ThenCollectsAllElements()
    {
        const int Second = 2;
        const int Third = 3;
        const int Fourth = 4;
        var result = await SignalAsync.Range(1, AggregateSourceCount).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([1, Second, Third, Fourth]);
    }

    /// <summary>Tests ToListAsync propagates error when source emits OnErrorResumeAsync.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenToListAsyncSourceEmitsErrorResume_ThenThrowsSourceException()
    {
        InvalidOperationException expectedError = new(ResumeErrorMessage);
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(expectedError, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.ToListAsync()).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(ResumeErrorMessage);
    }

    /// <summary>Tests ToDictionaryAsync creates correct dictionary.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenToDictionaryAsync_ThenCreatesCorrectDictionary()
    {
        const int ExpectedDictionaryCount = 3;
        var source = SequenceABbCcc.ToAsyncSignal();
        var result = await source.ToDictionaryAsync(static s => s.Length);
        await Assert.That(result).Count().IsEqualTo(ExpectedDictionaryCount);
        await Assert.That(result[1]).IsEqualTo("a");
    }

    /// <summary>Tests ToDictionaryAsync with key and element selectors creates correct dictionary.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenToDictionaryAsyncWithElementSelector_ThenCreatesCorrectDictionary()
    {
        const int ExpectedDictionaryCount = 3;
        const int LengthTwoKey = 2;
        const int LengthThreeKey = 3;
        var source = SequenceABbCcc.ToAsyncSignal();
        var result = await source.ToDictionaryAsync(static s => s.Length, static s => s.ToUpperInvariant());
        await Assert.That(result).Count().IsEqualTo(ExpectedDictionaryCount);
        await Assert.That(result[1]).IsEqualTo("A");
        await Assert.That(result[LengthTwoKey]).IsEqualTo("BB");
        await Assert.That(result[LengthThreeKey]).IsEqualTo("CCC");
    }

    /// <summary>Tests ToDictionaryAsync with key and element selectors and custom comparer uses the comparer.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenToDictionaryAsyncWithElementSelectorAndComparer_ThenUsesComparer()
    {
        const int ExpectedDictionaryCount = 2;
        const int ExpectedWordLength = 5;
        var source = SequenceHelloWorld.ToAsyncSignal();
        var result = await source.ToDictionaryAsync(
            static s => s,
            static s => s.Length,
            StringComparer.OrdinalIgnoreCase,
            CancellationToken.None);
        await Assert.That(result).Count().IsEqualTo(ExpectedDictionaryCount);
        await Assert.That(result["hello"]).IsEqualTo(ExpectedWordLength);
        await Assert.That(result["WORLD"]).IsEqualTo(ExpectedWordLength);
    }

    /// <summary>Tests ToDictionaryAsync with element selector throws ArgumentNullException when keySelector is null.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenToDictionaryAsyncWithElementSelectorNullKeySelector_ThenThrowsArgumentNull() => await Assert
        .That(static async () =>
            await SignalAsync.Return("a").ToDictionaryAsync((Func<string, string>)null!, static s => s.Length))
        .ThrowsExactly<ArgumentNullException>();

    /// <summary>Tests ToDictionaryAsync with element selector throws ArgumentNullException when elementSelector is null.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenToDictionaryAsyncWithNullElementSelector_ThenThrowsArgumentNull() => await Assert
        .That(static async () =>
            await SignalAsync.Return("a").ToDictionaryAsync(static s => s.Length, (Func<string, int>)null!))
        .ThrowsExactly<ArgumentNullException>();

    /// <summary>Tests ToDictionaryAsync propagates error from OnErrorResumeAsync.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenToDictionaryAsyncSourceEmitsErrorResume_ThenThrows()
    {
        InvalidOperationException expectedError = new(ResumeErrorMessage);
        var source = SignalAsync.Create<string>(async (observer, ct) =>
        {
            await observer.OnNextAsync("a", ct);
            await observer.OnErrorResumeAsync(expectedError, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.ToDictionaryAsync(static s => s))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(ResumeErrorMessage);
    }

    /// <summary>Tests ToDictionaryAsync propagates error when source completes with failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenToDictionaryAsyncSourceCompletesWithFailure_ThenThrows()
    {
        InvalidOperationException expectedError = new(SourceFailedMessage);
        var source = SignalAsync.Create<string>(async (observer, _) =>
        {
            await observer.OnCompletedAsync(Result.Failure(expectedError));
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.ToDictionaryAsync(static s => s))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(SourceFailedMessage);
    }

    /// <summary>Tests ForEachAsync processes all elements.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForEachAsync_ThenProcessesAllElements()
    {
        const int SourceCount = 3;
        const int Second = 2;
        const int Third = 3;
        List<int> items = [];
        await SignalAsync.Range(1, SourceCount).ForEachAsync(items.Add);
        await Assert.That(items).IsCollectionEqualTo([1, Second, Third]);
    }

    /// <summary>Tests WaitCompletionAsync waits for completion.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitCompletionAsync_ThenWaitsForCompletion()
    {
        const int SourceCount = 3;
        await SignalAsync.Range(1, SourceCount).WaitCompletionAsync();
    }

    /// <summary>Tests WaitCompletionAsync on error throws.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenWaitCompletionAsyncOnError_ThenThrows() => await Assert
        .That(static async () =>
            await SignalAsync.Throw<int>(new InvalidOperationException("err")).WaitCompletionAsync())
        .ThrowsExactly<InvalidOperationException>();

    /// <summary>Tests WaitCompletionAsync propagates error from OnErrorResumeAsync.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitCompletionAsyncSourceEmitsErrorResume_ThenThrowsSourceException()
    {
        InvalidOperationException expectedError = new(ResumeErrorMessage);
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(expectedError, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.WaitCompletionAsync())
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(ResumeErrorMessage);
    }

    /// <summary>Tests ToAsyncEnumerable can be enumerated.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenToAsyncEnumerable_ThenCanBeEnumerated()
    {
        const int SourceCount = 3;
        const int Second = 2;
        const int Third = 3;
        List<int> items = [];
        await foreach (var item in SignalAsync.Range(1, SourceCount).ToAsyncEnumerable(Channel.CreateUnbounded<int>))
        {
            items.Add(item);
        }

        await Assert.That(items).IsCollectionEqualTo([1, Second, Third]);
    }

    /// <summary>Tests that ToAsyncEnumerable yields all elements from the source when completed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenToAsyncEnumerableCompletes_ThenAllElementsYielded()
    {
        const int SourceCount = 3;
        const int ExpectedItemCount = 3;
        const int SecondIndex = 1;
        const int ThirdIndex = 2;
        const int ExpectedSecond = 2;
        const int ExpectedThird = 3;
        List<int> items = [];
        await foreach (var item in SignalAsync.Range(1, SourceCount).ToAsyncEnumerable(Channel.CreateUnbounded<int>))
        {
            items.Add(item);
        }

        await Assert.That(items).Count().IsEqualTo(ExpectedItemCount);
        await Assert.That(items[0]).IsEqualTo(1);
        await Assert.That(items[SecondIndex]).IsEqualTo(ExpectedSecond);
        await Assert.That(items[ThirdIndex]).IsEqualTo(ExpectedThird);
    }

    /// <summary>Tests SingleAsync with predicate returns the single matching element.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAsyncWithPredicate_ThenReturnsSingleMatch()
    {
        const int ExpectedMatch = 3;
        var result = await SignalAsync.Range(1, SourceValueCount).SingleAsync(static x => x == ExpectedMatch);
        await Assert.That(result).IsEqualTo(ExpectedMatch);
    }

    /// <summary>Tests SingleAsync with predicate throws when multiple elements match.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAsyncWithPredicateMultipleMatches_ThenThrowsInvalidOperation()
    {
        var ex = await Assert.That(
            static async () => await SignalAsync.Range(1, SourceValueCount)
                .SingleAsync(static x => x > MultiMatchThreshold))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(MoreThanOneMatchingElementMessage);
    }

    /// <summary>Tests SingleAsync propagates error from OnErrorResumeAsync.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAsyncSourceEmitsErrorResume_ThenThrowsSourceException()
    {
        InvalidOperationException expectedError = new(ResumeErrorMessage);
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(expectedError, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.SingleAsync()).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(ResumeErrorMessage);
    }

    /// <summary>Tests SingleAsync propagates error when source completes with failure result.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAsyncSourceCompletesWithFailure_ThenThrowsSourceException()
    {
        InvalidOperationException expectedError = new(SourceFailedMessage);
        var source = SignalAsync.Create<int>(async (observer, _) =>
        {
            await observer.OnCompletedAsync(new(expectedError));
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.SingleAsync()).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(SourceFailedMessage);
    }

    /// <summary>Tests that ForEachAsync captures an exception sent via OnErrorResumeAsync.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForEachAsyncSourceEmitsErrorResume_ThenExceptionCaptured()
    {
        InvalidOperationException expectedError = new(ResumeErrorMessage);
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(expectedError, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        List<int> items = [];
        var caughtException = await Assert.That(async () => await source.ForEachAsync(items.Add))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(caughtException!.Message).IsEqualTo(ResumeErrorMessage);
        await Assert.That(items).IsCollectionEqualTo([1]);
    }

    /// <summary>Tests that the async ForEachAsync overload throws ArgumentNullException when onNextAsync is null.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenAsyncForEachAsyncNullCallback_ThenThrowsArgumentNull() => await Assert
        .That(static async () => await SignalAsync.Return(1).ForEachAsync(null!))
        .ThrowsExactly<ArgumentNullException>();

    /// <summary>Tests that the synchronous ForEachAsync overload throws ArgumentNullException when onNext action is null.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncForEachAsyncNullAction_ThenThrowsArgumentNull()
    {
        var ex = await Assert.That(static async () => await SignalAsync.Return(1).ForEachAsync((Action<int>)null!))
            .ThrowsExactly<ArgumentNullException>();
        await Assert.That(ex!.ParamName).IsEqualTo("onNext");
    }

    /// <summary>Tests that the async ForEachAsync overload processes all elements.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncForEachAsync_ThenProcessesAllElements()
    {
        const int SourceCount = 3;
        const int Second = 2;
        const int Third = 3;
        List<int> items = [];
        await SignalAsync.Range(1, SourceCount).ForEachAsync(async (x, _) =>
        {
            await Task.Yield();
            items.Add(x);
        });
        await Assert.That(items).IsCollectionEqualTo([1, Second, Third]);
    }

    /// <summary>Tests that the async ForEachAsync overload propagates errors from OnErrorResumeAsync.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncForEachAsyncSourceEmitsErrorResume_ThenThrows()
    {
        InvalidOperationException expectedError = new("async resume error");
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(expectedError, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        List<int> items = [];
        var ex = await Assert.That(async () => await source.ForEachAsync(async (x, _) =>
        {
            await Task.Yield();
            items.Add(x);
        })).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo("async resume error");
        await Assert.That(items).IsCollectionEqualTo([1]);
    }

    /// <summary>Tests that the async ForEachAsync overload propagates errors when source completes with failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncForEachAsyncSourceCompletesWithFailure_ThenThrows()
    {
        InvalidOperationException expectedError = new(SourceFailedMessage);
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnCompletedAsync(new(expectedError));
            return DisposableAsync.Empty;
        });
        List<int> items = [];
        var ex = await Assert.That(async () => await source.ForEachAsync(async (x, _) =>
        {
            await Task.Yield();
            items.Add(x);
        })).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(SourceFailedMessage);
        await Assert.That(items).IsCollectionEqualTo([1]);
    }

    /// <summary>Tests LongCountAsync propagates error from OnErrorResumeAsync through the observer.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLongCountAsyncSourceEmitsErrorResume_ThenThrowsSourceException()
    {
        InvalidOperationException expectedError = new(ResumeErrorMessage);
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(expectedError, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        var ex = await Assert.That(async () => await source.LongCountAsync(null))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(ResumeErrorMessage);
    }

    /// <summary>
    /// Tests that ToAsyncEnumerable uses the default error handler when onErrorResume is null,
    /// completing the channel with the exception so that enumeration throws.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenToAsyncEnumerableSourceErrorsWithoutErrorHandler_ThenEnumerationThrows()
    {
        InvalidOperationException expectedError = new("source error");
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(expectedError, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        List<int> items = [];
        var ex = await Assert.That(async () =>
        {
            await foreach (var item in source.ToAsyncEnumerable(static () => Channel.CreateUnbounded<int>()))
            {
                items.Add(item);
            }
        }).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo("source error");
        await Assert.That(items).IsCollectionEqualTo([1]);
    }

    /// <summary>
    /// Tests that ToAsyncEnumerable yields each element from a multi-item source
    /// and completes enumeration when the source completes successfully.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenToAsyncEnumerableMultipleItems_ThenYieldsAllAndCompletes()
    {
        const int First = 10;
        const int Second = 20;
        const int Third = 30;
        const int Fourth = 40;
        const int Fifth = 50;
        var source = new[] { First, Second, Third, Fourth, Fifth }.ToAsyncSignal();
        List<int> items = [];
        await foreach (var item in source.ToAsyncEnumerable(Channel.CreateUnbounded<int>))
        {
            items.Add(item);
        }

        await Assert.That(items).IsCollectionEqualTo([First, Second, Third, Fourth, Fifth]);
    }

    /// <summary>Tests AggregateAsync propagates source failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAggregateAsyncSourceFails_ThenThrows()
    {
        InvalidOperationException error = new("test");
        await Assert.That(async () => await SignalAsync.Throw<int>(error).AggregateAsync(0, static (acc, x) => acc + x))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Tests AnyAsync propagates source failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAnyAsyncSourceFails_ThenThrows() => await Assert
        .That(static async () => await SignalAsync.Throw<int>(new InvalidOperationException("fail")).AnyAsync())
        .ThrowsExactly<InvalidOperationException>();

    /// <summary>Tests AllAsync propagates source failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAllAsyncSourceFails_ThenThrows() => await Assert
        .That(static async () => await SignalAsync.Throw<int>(new InvalidOperationException("fail")).AllAsync(static _ => true))
        .ThrowsExactly<InvalidOperationException>();

    /// <summary>Tests ContainsAsync propagates source failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenContainsAsyncSourceFails_ThenThrows() => await Assert
        .That(static async () => await SignalAsync.Throw<int>(new InvalidOperationException("fail")).ContainsAsync(1))
        .ThrowsExactly<InvalidOperationException>();

    /// <summary>Tests CountAsync propagates source failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCountAsyncSourceFails_ThenThrows() => await Assert
        .That(static async () => await SignalAsync.Throw<int>(new InvalidOperationException("fail")).CountAsync())
        .ThrowsExactly<InvalidOperationException>();

    /// <summary>Tests LongCountAsync propagates source failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLongCountAsyncSourceFails_ThenThrows() => await Assert
        .That(static async () => await SignalAsync.Throw<int>(new InvalidOperationException("fail")).LongCountAsync())
        .ThrowsExactly<InvalidOperationException>();

    /// <summary>Tests SingleAsync propagates source failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAsyncSourceFails_ThenThrows() => await Assert
        .That(static async () => await SignalAsync.Throw<int>(new InvalidOperationException("fail")).SingleAsync())
        .ThrowsExactly<InvalidOperationException>();

    /// <summary>Tests FirstAsync propagates source failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFirstAsyncSourceFails_ThenThrows() => await Assert
        .That(static async () => await SignalAsync.Throw<int>(new InvalidOperationException("fail")).FirstAsync())
        .ThrowsExactly<InvalidOperationException>();

    /// <summary>Tests FirstOrDefaultAsync propagates source failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFirstOrDefaultAsyncSourceFails_ThenThrows() => await Assert
        .That(static async () => await SignalAsync.Throw<int>(new InvalidOperationException("fail")).FirstOrDefaultAsync())
        .ThrowsExactly<InvalidOperationException>();

    /// <summary>Tests WaitCompletionAsync propagates source failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitCompletionAsyncSourceFails_ThenThrows() => await Assert
        .That(static async () => await SignalAsync.Throw<int>(new InvalidOperationException("fail")).WaitCompletionAsync())
        .ThrowsExactly<InvalidOperationException>();

    /// <summary>Tests ContainsAsync when value is not found returns false.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenContainsAsyncValueNotFound_ThenReturnsFalse()
    {
        var result = await SignalAsync.Range(1, ShortSourceValueCount).ContainsAsync(AbsentValue);
        await Assert.That(result).IsFalse();
    }

    /// <summary>Tests CountAsync with predicate that filters some elements.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCountAsyncWithPredicate_ThenCountsMatchesOnly()
    {
        const int ExpectedMatchCount = 2;
        var result = await SignalAsync.Range(1, SourceValueCount).CountAsync(static x => x > MatchThreshold);
        await Assert.That(result).IsEqualTo(ExpectedMatchCount);
    }

    /// <summary>Tests LongCountAsync with predicate that filters some elements.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLongCountAsyncWithPredicate_ThenCountsMatchesOnly()
    {
        const long ExpectedMatchCount = 2L;
        var result = await SignalAsync.Range(1, SourceValueCount)
            .LongCountAsync(static x => x > MatchThreshold);
        await Assert.That(result).IsEqualTo(ExpectedMatchCount);
    }

    /// <summary>Tests WaitCompletionAsync on successful sequence completes without error.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitCompletionAsyncOnSuccess_ThenCompletes()
    {
        const int SourceValue = 42;
        await SignalAsync.Return(SourceValue).WaitCompletionAsync();
    }
}
