// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for the factory overload of <c>CatchAndReturn</c>
/// backed by <c>CatchAndReturnWithFactoryObservable&lt;T, TException&gt;</c> —
/// matching exception path, non-matching exception passthrough, and
/// factory-error propagation.</summary>
public class CatchAndReturnWithFactoryObservableTests
{
    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "matching";

    /// <summary>Synthetic error message attached to mismatched source errors.</summary>
    private const string MismatchedErrorMessage = "different type";

    /// <summary>Synthetic error message attached to factory failures.</summary>
    private const string FactoryFailedMessage = "factory failed";

    /// <summary>Length used to build a fallback from the captured exception message.</summary>
    private const int FallbackBaseValue = 100;

    /// <summary>Verifies that a matching exception emits a fallback from the factory and completes.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchAndReturnMatchingException_ThenEmitsFactoryFallbackAndCompletes()
    {
        var subject = new Subject<int>();
        var results = new List<int>();
        var completed = false;
        var expected = new InvalidOperationException(SourceErrorMessage);

        using var sub = subject.CatchAndReturn<int, InvalidOperationException>(
                ex => FallbackBaseValue + ex.Message.Length)
            .Subscribe(results.Add, () => completed = true);

        subject.OnError(expected);

        await Assert.That(results).IsCollectionEqualTo([FallbackBaseValue + SourceErrorMessage.Length]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that a non-matching exception passes through to <c>OnError</c>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchAndReturnNonMatchingException_ThenForwardsError()
    {
        var subject = new Subject<int>();
        Exception? caught = null;
        var expected = new ArgumentException(MismatchedErrorMessage);

        using var sub = subject.CatchAndReturn<int, InvalidOperationException>(
                static _ => FallbackBaseValue)
            .Subscribe(static _ => { }, ex => caught = ex);

        subject.OnError(expected);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that an exception thrown by the fallback factory replaces the original error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchAndReturnFactoryThrows_ThenForwardsFactoryError()
    {
        var subject = new Subject<int>();
        Exception? caught = null;
        var sourceError = new InvalidOperationException(SourceErrorMessage);
        var factoryError = new InvalidOperationException(FactoryFailedMessage);

        using var sub = subject.CatchAndReturn<int, InvalidOperationException>(
                _ => throw factoryError)
            .Subscribe(static _ => { }, ex => caught = ex);

        subject.OnError(sourceError);

        await Assert.That(caught).IsSameReferenceAs(factoryError);
    }

    /// <summary>Verifies that <c>OnNext</c> values pass through unchanged when no error occurs.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchAndReturnNoError_ThenPassesValuesThrough()
    {
        const int First = 1;
        const int Second = 2;
        var subject = new Subject<int>();
        var results = new List<int>();
        var completed = false;

        using var sub = subject.CatchAndReturn<int, InvalidOperationException>(
                static _ => FallbackBaseValue)
            .Subscribe(results.Add, () => completed = true);

        subject.OnNext(First);
        subject.OnNext(Second);
        subject.OnCompleted();

        await Assert.That(results).IsCollectionEqualTo([First, Second]);
        await Assert.That(completed).IsTrue();
    }
}
