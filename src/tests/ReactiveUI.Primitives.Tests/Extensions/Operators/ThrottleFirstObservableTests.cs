// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions;
using ReactiveUI.Primitives.Extensions.Internal;
using ReactiveUI.Primitives.Extensions.Operators;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using System.IO;
using ReactiveUI.Primitives.Extensions.Tests;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for <c>ThrottleFirst</c> backed by
/// <c>ThrottleFirstObservable&lt;T&gt;</c> — error/completion forwarding and
/// post-terminal behaviour not exercised by the happy-path window test.</summary>
public class ThrottleFirstObservableTests
{
    /// <summary>Message attached to synthetic source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Throttle window for the tests.</summary>
    private static readonly TimeSpan ThrottleFirstWindow = TimeSpan.FromMilliseconds(50);

    /// <summary>Verifies that <c>ThrottleFirst</c> forwards source errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleFirstSourceErrors_ThenForwardsError()
    {
        var subject = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);

        using var sub = subject.ThrottleFirst(ThrottleFirstWindow)
            .Subscribe(static _ => { }, ex => caught = ex);

        subject.OnError(expected);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>ThrottleFirst</c> forwards completion and ignores post-completion emissions.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleFirstSourceCompletes_ThenForwardsCompletionAndIgnoresLater()
    {
        const int Initial = 1;
        const int IgnoredAfterCompletion = 2;
        var subject = new Subject<int>();
        var results = new List<int>();
        var completed = false;

        using var sub = subject.ThrottleFirst(ThrottleFirstWindow)
            .Subscribe(results.Add, () => completed = true);

        subject.OnNext(Initial);
        subject.OnCompleted();
        subject.OnNext(IgnoredAfterCompletion);

        await Assert.That(completed).IsTrue();
        await Assert.That(results).IsCollectionEqualTo([Initial]);
    }

    /// <summary>Verifies that values arriving after an error are not delivered downstream.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleFirstValueAfterError_ThenIgnored()
    {
        const int IgnoredAfterError = 5;
        var subject = new Subject<int>();
        var results = new List<int>();
        var expected = new InvalidOperationException(SourceErrorMessage);

        using var sub = subject.ThrottleFirst(ThrottleFirstWindow)
            .Subscribe(results.Add, static _ => { });

        subject.OnError(expected);
        subject.OnNext(IgnoredAfterError);

        await Assert.That(results).IsEmpty();
    }

    /// <summary>Verifies that <c>OnNext</c>, <c>OnError</c> and a duplicate <c>OnCompleted</c>
    /// arriving after the source has already completed are silently dropped.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenEventsAfterCompleted_ThenDropped()
    {
        var source = new SyncDirectSource<int>();
        var values = new List<int>();
        Exception? caught = null;
        var completedCount = 0;

        using var sub = source.ThrottleFirst(ThrottleFirstWindow)
            .Subscribe(values.Add, ex => caught = ex, () => completedCount++);

        source.Observer.OnCompleted();
        source.Observer.OnNext(1);
        source.Observer.OnError(new InvalidOperationException("late"));
        source.Observer.OnCompleted();

        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsEmpty();
        await Assert.That(caught).IsNull();
    }
}
