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
/// BehaviorSubject and ReplayLatest tests for <see cref="SubjectTests"/>.
/// </summary>
public partial class SubjectTests
{
    /// <summary>Tests behavior subject with start value emits latest first to new subscriber.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBehaviorSubjectWithStartValue_ThenNewSubscriberReceivesLatestFirst()
    {
        const int StartValue = 42;
        var subject = SubjectAsync.CreateBehavior(StartValue);
        var items = new List<int>();
        var firstReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                firstReceived.TrySetResult();
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo(StartValue);
    }

    /// <summary>Tests concurrent behavior subject emits latest to new subscriber.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBehaviorSubjectConcurrent_ThenNewSubscriberReceivesLatest()
    {
        var options = new BehaviorSubjectCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent, IsStateless = false
        };
        const int StartValue = 100;
        var subject = SubjectAsync.CreateBehavior(StartValue, options);
        var items = new List<int>();
        var firstReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items.Add(x);
                }

                firstReceived.TrySetResult();
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo(StartValue);
    }

    /// <summary>Tests replay latest subject replays last value to late subscriber.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestSubject_ThenLateSubscriberGetsLatestValue()
    {
        const int FirstValue = 10;
        const int LatestValue = 20;
        var subject = SubjectAsync.CreateReplayLatest<int>();

        await subject.OnNextAsync(FirstValue, CancellationToken.None);
        await subject.OnNextAsync(LatestValue, CancellationToken.None);

        var items = new List<int>();
        var firstReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                firstReceived.TrySetResult();
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo(LatestValue);
    }

    /// <summary>Tests concurrent replay latest subject replays latest to new subscriber.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestSubjectConcurrent_ThenLateSubscriberGetsLatest()
    {
        var options = new ReplayLatestSubjectCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent, IsStateless = false
        };
        var subject = SubjectAsync.CreateReplayLatest<int>(options);

        const int PushedValue = 5;
        await subject.OnNextAsync(PushedValue, CancellationToken.None);

        var items = new List<int>();
        var firstReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items.Add(x);
                }

                firstReceived.TrySetResult();
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo(PushedValue);
    }

    /// <summary>Tests behavior subject stateless emits start value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBehaviorSubjectStateless_ThenEmitsStartValueToNewSubscriber()
    {
        var options = new BehaviorSubjectCreationOptions
        {
            PublishingOption = PublishingOption.Serial, IsStateless = true
        };
        var subject = SubjectAsync.CreateBehavior("initial", options);
        var items = new List<string>();
        var firstReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                firstReceived.TrySetResult();
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo("initial");
    }

    /// <summary>Tests replay latest stateless emits latest to new subscriber.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestStateless_ThenEmitsLatestToNewSubscriber()
    {
        var options = new ReplayLatestSubjectCreationOptions
        {
            PublishingOption = PublishingOption.Serial, IsStateless = true
        };
        var subject = SubjectAsync.CreateReplayLatest<int>(options);

        const int PushedValue = 7;
        await subject.OnNextAsync(PushedValue, CancellationToken.None);

        var items = new List<int>();
        var firstReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                firstReceived.TrySetResult();
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo(PushedValue);
    }

    /// <summary>Tests concurrent stateless replay latest emits latest.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentStatelessReplayLatest_ThenEmitsLatest()
    {
        var options = new ReplayLatestSubjectCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent, IsStateless = true
        };
        var subject = SubjectAsync.CreateReplayLatest<int>(options);

        const int PushedValue = 77;
        await subject.OnNextAsync(PushedValue, CancellationToken.None);

        var items = new List<int>();
        var firstReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items.Add(x);
                }

                firstReceived.TrySetResult();
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo(PushedValue);
    }

    /// <summary>Tests that OnNextAsync on a serial stateless replay-last subject replays the value to a late subscriber.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatelessReplayLastOnNext_ThenLateSubscriberReceivesReplayedValue()
    {
        var options = new ReplayLatestSubjectCreationOptions
        {
            PublishingOption = PublishingOption.Serial, IsStateless = true
        };
        var subject = SubjectAsync.CreateReplayLatest<int>(options);

        const int FirstValue = 42;
        const int SecondValue = 99;

        await subject.OnNextAsync(FirstValue, CancellationToken.None);

        var items = new List<int>();
        await using var sub = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        await subject.OnNextAsync(SecondValue, CancellationToken.None);

        await Assert.That(items).IsEquivalentTo([FirstValue, SecondValue]);
    }

    /// <summary>Tests that OnErrorResumeAsync on a serial stateless replay-last subject delivers the error to observers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatelessReplayLastOnErrorResume_ThenObserverReceivesError()
    {
        var options = new ReplayLatestSubjectCreationOptions
        {
            PublishingOption = PublishingOption.Serial, IsStateless = true
        };
        var subject = SubjectAsync.CreateReplayLatest<int>(options);
        var errorTcs = new TaskCompletionSource<Exception>();

        await using var sub = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                errorTcs.TrySetResult(ex);
                return default;
            });

        var expected = new InvalidOperationException("stateless-error");
        await subject.OnErrorResumeAsync(expected, CancellationToken.None);

        var received = await errorTcs.Task;
        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that OnErrorResumeAsync on a concurrent stateless replay-last subject delivers the error to observers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentStatelessReplayLastOnErrorResume_ThenObserverReceivesError()
    {
        var options = new ReplayLatestSubjectCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent, IsStateless = true
        };
        var subject = SubjectAsync.CreateReplayLatest<int>(options);
        var errorTcs = new TaskCompletionSource<Exception>();

        await using var sub = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                errorTcs.TrySetResult(ex);
                return default;
            });

        var expected = new InvalidOperationException("concurrent-stateless-error");
        await subject.OnErrorResumeAsync(expected, CancellationToken.None);

        var received = await errorTcs.Task;
        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that OnCompletedAsync on a serial stateless replay-last subject delivers completion and resets state.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatelessReplayLastOnCompleted_ThenObserverReceivesCompletionAndStateResets()
    {
        var options = new ReplayLatestSubjectCreationOptions
        {
            PublishingOption = PublishingOption.Serial, IsStateless = true
        };
        var subject = SubjectAsync.CreateReplayLatest<int>(options);
        var resultTcs = new TaskCompletionSource<Result>();

        await using var sub = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            result =>
            {
                resultTcs.TrySetResult(result);
                return default;
            });

        const int PushedValue = 10;
        await subject.OnNextAsync(PushedValue, CancellationToken.None);
        await subject.OnCompletedAsync(Result.Success);

        var received = await resultTcs.Task;
        await Assert.That(received.IsSuccess).IsTrue();

        // After completion, a new subscriber should NOT receive a replayed value since state was reset.
        var lateItems = new List<int>();
        await using var lateSub = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                lateItems.Add(x);
                return default;
            },
            null);

        await Assert.That(lateItems).Count().IsEqualTo(0);
    }

    /// <summary>Tests that OnCompletedAsync on a concurrent stateless replay-last subject delivers completion to observers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentStatelessReplayLastOnCompleted_ThenObserverReceivesCompletion()
    {
        var options = new ReplayLatestSubjectCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent, IsStateless = true
        };
        var subject = SubjectAsync.CreateReplayLatest<int>(options);
        var resultTcs = new TaskCompletionSource<Result>();

        await using var sub = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            result =>
            {
                resultTcs.TrySetResult(result);
                return default;
            });

        await subject.OnCompletedAsync(Result.Success);

        var received = await resultTcs.Task;
        await Assert.That(received.IsSuccess).IsTrue();
    }

    /// <summary>Tests that DisposeAsync on a serial stateless replay-last subject completes without error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatelessReplayLastDispose_ThenCompletesSuccessfully()
    {
        var options = new ReplayLatestSubjectCreationOptions
        {
            PublishingOption = PublishingOption.Serial, IsStateless = true
        };
        var subject = SubjectAsync.CreateReplayLatest<int>(options);

        await subject.OnNextAsync(1, CancellationToken.None);
        await subject.DisposeAsync();
    }

    /// <summary>Tests that DisposeAsync on a concurrent stateless replay-last subject completes without error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentStatelessReplayLastDispose_ThenCompletesSuccessfully()
    {
        var options = new ReplayLatestSubjectCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent, IsStateless = true
        };
        var subject = SubjectAsync.CreateReplayLatest<int>(options);

        await subject.OnNextAsync(1, CancellationToken.None);
        await subject.DisposeAsync();
    }

    /// <summary>Tests concurrent stateless behavior emits start value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentStatelessBehavior_ThenEmitsStartValue()
    {
        var options = new BehaviorSubjectCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent, IsStateless = true
        };
        const int StartValue = 55;
        var subject = SubjectAsync.CreateBehavior(StartValue, options);
        var items = new List<int>();
        var firstReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items.Add(x);
                }

                firstReceived.TrySetResult();
                return default;
            },
            null);

        await firstReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(items[0]).IsEqualTo(StartValue);
    }

    /// <summary>Tests that OnNextAsync on a replay-latest subject is ignored after the subject has completed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestOnNextAfterCompleted_ThenValueIsIgnored()
    {
        var subject = SubjectAsync.CreateReplayLatest<int>();
        var items = new List<int>();
        var completionTcs = new TaskCompletionSource();

        await using var sub = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null,
            _ =>
            {
                completionTcs.TrySetResult();
                return default;
            });

        const int PostCompletionValue = 2;

        await subject.OnNextAsync(1, CancellationToken.None);
        await subject.OnCompletedAsync(Result.Success);
        await completionTcs.Task;

        await subject.OnNextAsync(PostCompletionValue, CancellationToken.None);

        await Assert.That(items).IsEquivalentTo([1]);
    }

    /// <summary>Tests that OnErrorResumeAsync on a replay-latest subject delivers the error to observers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestOnErrorResume_ThenObserverReceivesError()
    {
        var subject = SubjectAsync.CreateReplayLatest<int>();
        var errorTcs = new TaskCompletionSource<Exception>();

        await using var sub = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                errorTcs.TrySetResult(ex);
                return default;
            });

        var expected = new InvalidOperationException("replay-error");
        await subject.OnErrorResumeAsync(expected, CancellationToken.None);

        var received = await errorTcs.Task;
        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that OnErrorResumeAsync on a replay-latest subject is ignored after completion.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestOnErrorResumeAfterCompleted_ThenErrorIsIgnored()
    {
        var subject = SubjectAsync.CreateReplayLatest<int>();
        var errors = new List<Exception>();
        var completionTcs = new TaskCompletionSource();

        await using var sub = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                errors.Add(ex);
                return default;
            },
            _ =>
            {
                completionTcs.TrySetResult();
                return default;
            });

        await subject.OnCompletedAsync(Result.Success);
        await completionTcs.Task;

        await subject.OnErrorResumeAsync(new InvalidOperationException("ignored"), CancellationToken.None);

        await Assert.That(errors).IsEmpty();
    }

    /// <summary>Tests that OnCompletedAsync on a replay-latest subject delivers completion to observers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestOnCompleted_ThenObserverReceivesCompletion()
    {
        var subject = SubjectAsync.CreateReplayLatest<int>();
        var completionTcs = new TaskCompletionSource<Result>();

        await using var sub = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            result =>
            {
                completionTcs.TrySetResult(result);
                return default;
            });

        await subject.OnCompletedAsync(Result.Success);

        var completionResult = await completionTcs.Task;
        await Assert.That(completionResult.IsSuccess).IsTrue();
    }

    /// <summary>Tests that calling OnCompletedAsync twice on a replay-latest subject ignores the second call.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestOnCompletedCalledTwice_ThenSecondCallIsIgnored()
    {
        var subject = SubjectAsync.CreateReplayLatest<int>();
        var completionCount = 0;
        var completionTcs = new TaskCompletionSource();

        await using var sub = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            _ =>
            {
                Interlocked.Increment(ref completionCount);
                completionTcs.TrySetResult();
                return default;
            });

        await subject.OnCompletedAsync(Result.Success);
        await completionTcs.Task;

        await subject.OnCompletedAsync(Result.Failure(new InvalidOperationException("second")));

        await Assert.That(completionCount).IsEqualTo(1);
    }

    /// <summary>Tests that DisposeAsync on a replay-latest subject completes without error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestDisposeAsync_ThenCompletesSuccessfully()
    {
        const int PushedValue = 42;
        var subject = SubjectAsync.CreateReplayLatest<int>();
        await subject.OnNextAsync(PushedValue, CancellationToken.None);

        await subject.DisposeAsync();
    }

    /// <summary>Tests that OnErrorResumeAsync on a concurrent stateful replay-latest subject delivers the error to observers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentReplayLatestOnErrorResume_ThenObserverReceivesError()
    {
        var options = new ReplayLatestSubjectCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent, IsStateless = false
        };
        var subject = SubjectAsync.CreateReplayLatest<int>(options);
        var errorTcs = new TaskCompletionSource<Exception>();

        await using var sub = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                errorTcs.TrySetResult(ex);
                return default;
            });

        var expected = new InvalidOperationException("concurrent-stateful-error");
        await subject.OnErrorResumeAsync(expected, CancellationToken.None);

        var received = await errorTcs.Task;
        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that OnCompletedAsync on a concurrent stateful replay-latest subject delivers completion to observers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentReplayLatestOnCompleted_ThenObserverReceivesCompletion()
    {
        var options = new ReplayLatestSubjectCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent, IsStateless = false
        };
        var subject = SubjectAsync.CreateReplayLatest<int>(options);
        var resultTcs = new TaskCompletionSource<Result>();

        await using var sub = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            result =>
            {
                resultTcs.TrySetResult(result);
                return default;
            });

        await subject.OnCompletedAsync(Result.Success);

        var received = await resultTcs.Task;
        await Assert.That(received.IsSuccess).IsTrue();
    }

    /// <summary>Tests that subscribing to an already-completed replay-latest subject immediately delivers completion.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeToCompletedReplayLatest_ThenObserverReceivesImmediateCompletion()
    {
        var subject = SubjectAsync.CreateReplayLatest<int>();
        var firstCompletionTcs = new TaskCompletionSource();

        await using var firstSub = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            _ =>
            {
                firstCompletionTcs.TrySetResult();
                return default;
            });

        const int PushedValue = 99;
        await subject.OnNextAsync(PushedValue, CancellationToken.None);
        await subject.OnCompletedAsync(Result.Success);
        await firstCompletionTcs.Task;

        Result? lateResult = null;
        var lateTcs = new TaskCompletionSource();

        await using var lateSub = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            result =>
            {
                lateResult = result;
                lateTcs.TrySetResult();
                return default;
            });

        await lateTcs.Task;

        await Assert.That(lateResult).IsNotNull();
        await Assert.That(lateResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Verifies that the replay-latest subject's <c>OnNextAsync</c> with a caller-supplied
    /// cancellation token takes the linked-CTS slow path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestOnNextWithCustomToken_ThenForwardsValue()
    {
        var subject = SubjectAsync.CreateReplayLatest<int>(new ReplayLatestSubjectCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent,
            IsStateless = false,
        });
        var tcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await subject.Values.SubscribeAsync(
            (v, _) =>
            {
                tcs.TrySetResult(v);
                return default;
            });

        using var cts = new CancellationTokenSource();
        const int LinkedCtsValue = 11;
        await subject.OnNextAsync(LinkedCtsValue, cts.Token);

        var received = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(received).IsEqualTo(LinkedCtsValue);
    }

    /// <summary>Verifies that the replay-latest subject's <c>OnErrorResumeAsync</c> with a
    /// caller-supplied cancellation token takes the linked-CTS slow path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestOnErrorResumeWithCustomToken_ThenForwardsError()
    {
        var subject = SubjectAsync.CreateReplayLatest<int>(new ReplayLatestSubjectCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent,
            IsStateless = false,
        });
        var tcs = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                tcs.TrySetResult(ex);
                return default;
            });

        var expected = new InvalidOperationException("linked-cts");
        using var cts = new CancellationTokenSource();
        await subject.OnErrorResumeAsync(expected, cts.Token);

        var received = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(received).IsSameReferenceAs(expected);
    }

    /// <summary>Exercises the <c>_isDisposed</c> idempotency guard on
    /// <c>BaseReplayLatestSubjectAsync.DisposeAsync</c> — a second dispose is a no-op.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReplayLatestSubjectDisposedTwice_ThenIdempotent()
    {
        var subject = SubjectAsync.CreateReplayLatest<int>();

        await subject.DisposeAsync();
        await subject.DisposeAsync();

        await Assert.That(subject).IsNotNull();
    }

    /// <summary>Exercises the <c>_isDisposed</c> idempotency guard on
    /// <c>BaseStatelessReplayLastSubjectAsync.DisposeAsync</c>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStatelessReplayLastSubjectDisposedTwice_ThenIdempotent()
    {
        var subject = SubjectAsync.CreateReplayLatest<int>(new ReplayLatestSubjectCreationOptions
        {
            PublishingOption = PublishingOption.Serial,
            IsStateless = true,
        });

        await subject.DisposeAsync();
        await subject.DisposeAsync();

        await Assert.That(subject).IsNotNull();
    }
}
