// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.SystemReactiveBridge;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Async.Subjects;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Tests for SubjectAsync factory, all subject variants, and SubjectMixins.
/// </summary>
public partial class SubjectTests
{
#if NET9_0_OR_GREATER
    /// <summary>Synchronization gate used by tests.</summary>
    private readonly Lock _gate = new();
#else
    /// <summary>Synchronization gate used by tests.</summary>
    private readonly object _gate = new();
#endif

    /// <summary>Tests serial subject pushes values to all observers in order.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSerialSubjectPushValues_ThenAllObserversReceiveInOrder()
    {
        var subject = SubjectAsync.Create<int>();
        var items = new List<int>();
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null,
            _ =>
            {
                completed.TrySetResult();
                return default;
            });

        const int FirstValue = 1;
        const int SecondValue = 2;
        const int ThirdValue = 3;

        await subject.OnNextAsync(FirstValue, CancellationToken.None);
        await subject.OnNextAsync(SecondValue, CancellationToken.None);
        await subject.OnNextAsync(ThirdValue, CancellationToken.None);
        await subject.OnCompletedAsync(Result.Success);

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).IsEquivalentTo([FirstValue, SecondValue, ThirdValue]);
    }

    /// <summary>Tests concurrent subject pushes values to all observers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentSubjectPushValues_ThenAllObserversReceive()
    {
        var options = new SubjectCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent, IsStateless = false
        };
        var subject = SubjectAsync.Create<int>(options);
        var items = new List<int>();
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items.Add(x);
                }

                return default;
            },
            null,
            _ =>
            {
                completed.TrySetResult();
                return default;
            });

        const int FirstValue = 10;
        const int SecondValue = 20;
        const int ExpectedCount = 2;

        await subject.OnNextAsync(FirstValue, CancellationToken.None);
        await subject.OnNextAsync(SecondValue, CancellationToken.None);
        await subject.OnCompletedAsync(Result.Success);

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).Count().IsEqualTo(ExpectedCount);
    }

    /// <summary>Tests serial stateless subject pushes values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSerialStatelessSubjectPushValues_ThenObserversReceive()
    {
        var options = new SubjectCreationOptions { PublishingOption = PublishingOption.Serial, IsStateless = true };
        var subject = SubjectAsync.Create<string>(options);
        var items = new List<string>();
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null,
            _ =>
            {
                completed.TrySetResult();
                return default;
            });

        await subject.OnNextAsync("a", CancellationToken.None);
        await subject.OnNextAsync("b", CancellationToken.None);
        await subject.OnCompletedAsync(Result.Success);

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).IsEquivalentTo(["a", "b"]);
    }

    /// <summary>Tests concurrent stateless subject pushes values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentStatelessSubjectPushValues_ThenObserversReceive()
    {
        var options = new SubjectCreationOptions { PublishingOption = PublishingOption.Concurrent, IsStateless = true };
        var subject = SubjectAsync.Create<int>(options);
        var items = new List<int>();
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items.Add(x);
                }

                return default;
            },
            null,
            _ =>
            {
                completed.TrySetResult();
                return default;
            });

        const int PushedValue = 5;

        await subject.OnNextAsync(PushedValue, CancellationToken.None);
        await subject.OnCompletedAsync(Result.Success);

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).IsEquivalentTo([PushedValue]);
    }

    /// <summary>Tests subject OnErrorResume delivers error to observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubjectOnErrorResume_ThenObserverReceivesError()
    {
        var subject = SubjectAsync.Create<int>();
        var errors = new List<Exception>();
        var errorReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await subject.Values.SubscribeAsync(
            (_, _) => default,
            (ex, _) =>
            {
                errors.Add(ex);
                errorReceived.TrySetResult();
                return default;
            });

        await subject.OnErrorResumeAsync(new InvalidOperationException("test"), CancellationToken.None);
        await errorReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        const int ExpectedErrorCount = 1;
        await Assert.That(errors).Count().IsEqualTo(ExpectedErrorCount);
        await Assert.That(errors[0].Message).IsEqualTo("test");
    }

    /// <summary>Tests subject OnCompleted delivers completion to observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubjectOnCompleted_ThenObserverReceivesCompletion()
    {
        var subject = SubjectAsync.Create<int>();
        Result? completionResult = null;
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await subject.Values.SubscribeAsync(
            (_, _) => default,
            null,
            result =>
            {
                completionResult = result;
                completed.TrySetResult();
                return default;
            });

        await subject.OnCompletedAsync(Result.Success);
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Tests subject OnCompleted with failure delivers failure to observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubjectOnCompletedWithFailure_ThenObserverReceivesFailure()
    {
        var subject = SubjectAsync.Create<int>();
        Result? completionResult = null;
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await subject.Values.SubscribeAsync(
            (_, _) => default,
            null,
            result =>
            {
                completionResult = result;
                completed.TrySetResult();
                return default;
            });

        await subject.OnCompletedAsync(Result.Failure(new InvalidOperationException("fatal")));
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Tests multiple observers all receive values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMultipleObservers_ThenAllReceiveValues()
    {
        var subject = SubjectAsync.Create<int>();
        var items1 = new List<int>();
        var items2 = new List<int>();
        var completed1 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var completed2 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub1 = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                items1.Add(x);
                return default;
            },
            null,
            _ =>
            {
                completed1.TrySetResult();
                return default;
            });

        await using var sub2 = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                items2.Add(x);
                return default;
            },
            null,
            _ =>
            {
                completed2.TrySetResult();
                return default;
            });

        const int FirstValue = 1;
        const int SecondValue = 2;

        await subject.OnNextAsync(FirstValue, CancellationToken.None);
        await subject.OnNextAsync(SecondValue, CancellationToken.None);
        await subject.OnCompletedAsync(Result.Success);

        await Task.WhenAll(completed1.Task, completed2.Task).WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items1).IsEquivalentTo([FirstValue, SecondValue]);
        await Assert.That(items2).IsEquivalentTo([FirstValue, SecondValue]);
    }

    /// <summary>Tests AsObserverAsync forwards to subject.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsObserverAsync_ThenForwardsToSubject()
    {
        var subject = SubjectAsync.Create<int>();
        var observer = subject.AsObserverAsync();
        var items = new List<int>();
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null,
            _ =>
            {
                completed.TrySetResult();
                return default;
            });

        const int FirstValue = 1;
        const int SecondValue = 2;

        await observer.OnNextAsync(FirstValue, CancellationToken.None);
        await observer.OnNextAsync(SecondValue, CancellationToken.None);
        await observer.OnCompletedAsync(Result.Success);

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).IsEquivalentTo([FirstValue, SecondValue]);
    }

    /// <summary>Tests default SubjectCreationOptions is serial and stateful.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDefaultSubjectCreationOptions_ThenSerialAndStateful()
    {
        var options = SubjectCreationOptions.Default;

        await Assert.That(options.PublishingOption).IsEqualTo(PublishingOption.Serial);
        await Assert.That(options.IsStateless).IsFalse();
    }

    /// <summary>Tests default BehaviorSubjectCreationOptions is serial and stateful.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDefaultBehaviorSubjectCreationOptions_ThenSerialAndStateful()
    {
        var options = BehaviorSubjectCreationOptions.Default;

        await Assert.That(options.PublishingOption).IsEqualTo(PublishingOption.Serial);
        await Assert.That(options.IsStateless).IsFalse();
    }

    /// <summary>Tests default ReplayLatestSubjectCreationOptions is serial and stateful.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDefaultReplayLatestSubjectCreationOptions_ThenSerialAndStateful()
    {
        var options = ReplayLatestSubjectCreationOptions.Default;

        await Assert.That(options.PublishingOption).IsEqualTo(PublishingOption.Serial);
        await Assert.That(options.IsStateless).IsFalse();
    }
}
