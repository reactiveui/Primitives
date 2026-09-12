// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests first, last and single-element terminal selection.</summary>
public partial class TerminalOperatorTests
{
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

    /// <summary>Tests FirstAsync reports an empty source with the no-elements message.</summary>
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

    /// <summary>Tests SingleAsync reports an empty source with the no-elements message.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSingleAsyncOnEmpty_ThenThrowsInvalidOperation()
    {
        var ex = await Assert.That(static async () => await SignalAsync.Empty<int>().SingleAsync())
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex!.Message).IsEqualTo(NoElementsMessage);
    }

    /// <summary>Tests SingleAsync reports an unmatched predicate with the no-matching-elements message.</summary>
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
}
