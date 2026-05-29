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
using System.Collections.Immutable;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Async.Subjects;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Mapped subject, mixins, concurrent multi-observer, and edge-case tests for <see cref="SubjectTests"/>.
/// </summary>
public partial class SubjectTests
{
    /// <summary>Tests MapValues transforms observable.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMapValues_ThenTransformsObservable()
    {
        const int Multiplier = 10;
        const int FirstInput = 1;
        const int SecondInput = 2;
        const int FirstMapped = 10;
        const int SecondMapped = 20;

        var subject = SubjectAsync.Create<int>();
        var mapped = subject.MapValues(values => values.Select(x => x * Multiplier));

        var items = new List<int>();
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await mapped.Values.SubscribeAsync(
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

        await mapped.OnNextAsync(FirstInput, CancellationToken.None);
        await mapped.OnNextAsync(SecondInput, CancellationToken.None);
        await mapped.OnCompletedAsync(Result.Success);

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).IsEquivalentTo([FirstMapped, SecondMapped]);
    }

    /// <summary>Tests that OnErrorResumeAsync on a serial stateless subject delivers the error to the observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSerialStatelessSubjectOnErrorResume_ThenObserverReceivesError()
    {
        var options = new SubjectCreationOptions { PublishingOption = PublishingOption.Serial, IsStateless = true };
        var subject = SubjectAsync.Create<int>(options);
        var errorTcs = new TaskCompletionSource<Exception>();

        await using var sub = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                errorTcs.TrySetResult(ex);
                return default;
            });

        var expected = new InvalidOperationException("serial-stateless-error");
        await subject.OnErrorResumeAsync(expected, CancellationToken.None);

        var received = await errorTcs.Task;
        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that OnErrorResumeAsync on a concurrent stateless subject delivers the error to the observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentStatelessSubjectOnErrorResume_ThenObserverReceivesError()
    {
        var options = new SubjectCreationOptions { PublishingOption = PublishingOption.Concurrent, IsStateless = true };
        var subject = SubjectAsync.Create<int>(options);
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

    /// <summary>Tests that DisposeAsync on a serial stateless subject clears observers and completes without error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSerialStatelessSubjectDispose_ThenCompletesAndClearsObservers()
    {
        var options = new SubjectCreationOptions { PublishingOption = PublishingOption.Serial, IsStateless = true };
        var subject = SubjectAsync.Create<int>(options);
        var items = new List<int>();

        await using var sub = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        const int PostDisposeValue = 2;

        await subject.OnNextAsync(1, CancellationToken.None);
        await subject.DisposeAsync();

        // After dispose, observers are cleared so no further values should be delivered.
        await subject.OnNextAsync(PostDisposeValue, CancellationToken.None);

        await Assert.That(items).IsEquivalentTo([1]);
    }

    /// <summary>Tests that DisposeAsync on a concurrent stateless subject clears observers and completes without error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentStatelessSubjectDispose_ThenCompletesAndClearsObservers()
    {
        var options = new SubjectCreationOptions { PublishingOption = PublishingOption.Concurrent, IsStateless = true };
        var subject = SubjectAsync.Create<int>(options);
        var items = new List<int>();

        await using var sub = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items.Add(x);
                }

                return default;
            },
            null);

        const int PostDisposeValue = 2;

        await subject.OnNextAsync(1, CancellationToken.None);
        await subject.DisposeAsync();

        // After dispose, observers are cleared so no further values should be delivered.
        await subject.OnNextAsync(PostDisposeValue, CancellationToken.None);

        await Assert.That(items).IsEquivalentTo([1]);
    }

    /// <summary>Tests that OnErrorResumeAsync on a concurrent stateful subject delivers the error to observers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentSubjectOnErrorResume_ThenObserverReceivesError()
    {
        var options = new SubjectCreationOptions
        {
            PublishingOption = PublishingOption.Concurrent, IsStateless = false
        };
        var subject = SubjectAsync.Create<int>(options);
        var errorTcs = new TaskCompletionSource<Exception>();

        await using var sub = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                errorTcs.TrySetResult(ex);
                return default;
            });

        var expected = new InvalidOperationException("concurrent-stateful");
        await subject.OnErrorResumeAsync(expected, CancellationToken.None);

        var received = await errorTcs.Task;
        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that OnErrorResumeAsync is ignored after the subject has already completed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorResumeAsyncCalledAfterCompletion_ThenIsIgnored()
    {
        var subject = SubjectAsync.Create<int>();
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

        await subject.OnErrorResumeAsync(new InvalidOperationException("should be ignored"), CancellationToken.None);

        await Assert.That(errors).IsEmpty();
    }

    /// <summary>Tests that OnCompletedAsync is ignored on the second call after the subject has already completed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnCompletedAsyncCalledTwice_ThenSecondCallIsIgnored()
    {
        var subject = SubjectAsync.Create<int>();
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

    /// <summary>Tests that subscribing to an already-completed subject immediately delivers the completion result.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribingToAlreadyCompletedSubject_ThenObserverReceivesCompletionImmediately()
    {
        var subject = SubjectAsync.Create<int>();
        var firstCompletionTcs = new TaskCompletionSource();

        await using var firstSub = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            _ =>
            {
                firstCompletionTcs.TrySetResult();
                return default;
            });

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

    /// <summary>Tests that OnNextAsync is ignored after the subject has already completed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnNextAsyncCalledAfterCompletion_ThenIsIgnored()
    {
        var subject = SubjectAsync.Create<int>();
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

    /// <summary>Tests that OnErrorResumeAsync forwards the error to observers when the subject has not completed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorResumeAsyncCalledBeforeCompletion_ThenErrorIsForwarded()
    {
        var subject = SubjectAsync.Create<int>();
        var errorTcs = new TaskCompletionSource<Exception>();

        await using var sub = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                errorTcs.TrySetResult(ex);
                return default;
            });

        var expected = new InvalidOperationException("forwarded");
        await subject.OnErrorResumeAsync(expected, CancellationToken.None);

        var received = await errorTcs.Task;

        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that OnCompletedAsync forwards the result to observers and clears the observer list.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnCompletedAsyncCalled_ThenResultIsForwardedToObservers()
    {
        var subject = SubjectAsync.Create<int>();
        var resultTcs = new TaskCompletionSource<Result>();

        await using var sub = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            result =>
            {
                resultTcs.TrySetResult(result);
                return default;
            });

        var failure = Result.Failure(new InvalidOperationException("done"));
        await subject.OnCompletedAsync(failure);

        var received = await resultTcs.Task;

        await Assert.That(received.IsFailure).IsTrue();
    }

    /// <summary>Tests that OnErrorResumeAsync on a serial stateless subject delivers the error to multiple observers sequentially.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSerialStatelessSubjectOnErrorResumeWithMultipleObservers_ThenAllObserversReceiveError()
    {
        var options = new SubjectCreationOptions { PublishingOption = PublishingOption.Serial, IsStateless = true };
        var subject = SubjectAsync.Create<int>(options);
        var errors1 = new List<Exception>();
        var errors2 = new List<Exception>();

        await using var sub1 = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                errors1.Add(ex);
                return default;
            });

        await using var sub2 = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                errors2.Add(ex);
                return default;
            });

        var expected = new InvalidOperationException("multi-observer-error");
        await subject.OnErrorResumeAsync(expected, CancellationToken.None);
        await subject.OnCompletedAsync(Result.Success);

        await Assert.That(errors1).Count().IsEqualTo(1);
        await Assert.That(errors1[0]).IsEqualTo(expected);
        await Assert.That(errors2).Count().IsEqualTo(1);
        await Assert.That(errors2[0]).IsEqualTo(expected);
    }

    /// <summary>Tests that SubjectAsync.Create throws ArgumentOutOfRangeException for an invalid options combination.</summary>
    [Test]
    public void WhenCreateWithInvalidOptions_ThenThrowsArgumentOutOfRangeException() =>
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            SubjectAsync.Create<int>(null!));

    /// <summary>Tests that SubjectAsync.CreateBehavior throws ArgumentOutOfRangeException for an invalid options combination.</summary>
    [Test]
    public void WhenCreateBehaviorWithInvalidOptions_ThenThrowsArgumentOutOfRangeException() =>
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            SubjectAsync.CreateBehavior(0, null!));

    /// <summary>Tests that SubjectAsync.CreateReplayLatest throws ArgumentOutOfRangeException for an invalid options combination.</summary>
    [Test]
    public void WhenCreateReplayLatestWithInvalidOptions_ThenThrowsArgumentOutOfRangeException() =>
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            SubjectAsync.CreateReplayLatest<int>(null!));

    /// <summary>Tests that MappedSubject.SubscribeAsync subscribes an observer through the mapped values observable.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMappedSubjectSubscribeAsync_ThenObserverReceivesMappedValues()
    {
        const int InputValue = 10;
        const int Increment = 1;
        const int MappedValue = 11;

        var subject = SubjectAsync.Create<int>();
        var mapped = subject.MapValues(values => values.Select(x => x + Increment));

        var collector = SubjectAsync.Create<int>();
        var items = new List<int>();
        await using var collectorSub = await collector.Values.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        var observer = collector.AsObserverAsync();
        await using var sub = await mapped.SubscribeAsync(observer, CancellationToken.None);

        await mapped.OnNextAsync(InputValue, CancellationToken.None);
        await mapped.OnCompletedAsync(Result.Success);

        await Assert.That(items).IsEquivalentTo([MappedValue]);
    }

    /// <summary>Tests that MappedSubject.OnErrorResumeAsync forwards the error to the original subject.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMappedSubjectOnErrorResumeAsync_ThenErrorIsForwardedToOriginal()
    {
        var subject = SubjectAsync.Create<int>();
        var mapped = subject.MapValues(values => values);
        var errorTcs = new TaskCompletionSource<Exception>();

        await using var sub = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                errorTcs.TrySetResult(ex);
                return default;
            });

        var expected = new InvalidOperationException("mapped-error");
        await mapped.OnErrorResumeAsync(expected, CancellationToken.None);

        var received = await errorTcs.Task;
        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that MappedSubject.DisposeAsync disposes the original subject.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMappedSubjectDisposeAsync_ThenOriginalSubjectIsDisposed()
    {
        var subject = SubjectAsync.Create<int>();
        var mapped = subject.MapValues(values => values);

        // DisposeAsync on the mapped subject should dispose the underlying subject.
        await mapped.DisposeAsync();

        // Verify double-dispose is safe.
        await mapped.DisposeAsync();
    }

    /// <summary>Tests that AsObserverAsync forwards OnErrorResumeAsync to the underlying subject.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsObserverAsyncOnErrorResume_ThenErrorIsForwardedToSubject()
    {
        var subject = SubjectAsync.Create<int>();
        var observer = subject.AsObserverAsync();
        var errorTcs = new TaskCompletionSource<Exception>();

        await using var sub = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                errorTcs.TrySetResult(ex);
                return default;
            });

        var expected = new InvalidOperationException("observer-error");
        await observer.OnErrorResumeAsync(expected, CancellationToken.None);

        var received = await errorTcs.Task;
        await Assert.That(received).IsEqualTo(expected);
    }

    /// <summary>Tests that AsObserverAsync forwards OnCompletedAsync to the underlying subject.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsObserverAsyncOnCompleted_ThenCompletionIsForwardedToSubject()
    {
        var subject = SubjectAsync.Create<int>();
        var observer = subject.AsObserverAsync();
        var resultTcs = new TaskCompletionSource<Result>();

        await using var sub = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            result =>
            {
                resultTcs.TrySetResult(result);
                return default;
            });

        await observer.OnCompletedAsync(Result.Success);

        var received = await resultTcs.Task;
        await Assert.That(received.IsSuccess).IsTrue();
    }

    /// <summary>Tests that ForwardOnErrorResumeConcurrently with an empty observer list completes immediately.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForwardOnErrorResumeConcurrentlyWithEmptyObservers_ThenCompletesImmediately()
    {
        var emptyObservers = ImmutableArray<IObserverAsync<int>>.Empty;

        var task = Concurrent.ForwardOnErrorResumeConcurrently(
            emptyObservers,
            new InvalidOperationException("unused"),
            CancellationToken.None);

        await Assert.That(task.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>Tests that ForwardOnCompletedConcurrently with an empty observer list completes immediately.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForwardOnCompletedConcurrentlyWithEmptyObservers_ThenCompletesImmediately()
    {
        var emptyObservers = ImmutableArray<IObserverAsync<int>>.Empty;

        var task = Concurrent.ForwardOnCompletedConcurrently<int>(emptyObservers, Result.Success);

        await Assert.That(task.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>Tests that concurrent subject forwards OnNext to multiple observers concurrently.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentSubjectWithMultipleObservers_ThenAllReceiveOnNext()
    {
        var subject = SubjectAsync.Create<int>(new()
        {
            PublishingOption = PublishingOption.Concurrent, IsStateless = false
        });
        var items1 = new List<int>();
        var items2 = new List<int>();

        await using var sub1 = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items1.Add(x);
                }

                return default;
            },
            null);
        await using var sub2 = await subject.Values.SubscribeAsync(
            (x, _) =>
            {
                lock (_gate)
                {
                    items2.Add(x);
                }

                return default;
            },
            null);

        const int PushedValue = 42;

        await subject.OnNextAsync(PushedValue, CancellationToken.None);
        await subject.OnCompletedAsync(Result.Success);

        await Assert.That(items1).IsEquivalentTo([PushedValue]);
        await Assert.That(items2).IsEquivalentTo([PushedValue]);
    }

    /// <summary>Tests that concurrent subject forwards OnErrorResume to multiple observers concurrently.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentSubjectWithMultipleObservers_ThenAllReceiveOnErrorResume()
    {
        var subject = SubjectAsync.Create<int>(new()
        {
            PublishingOption = PublishingOption.Concurrent, IsStateless = false
        });
        var errors1 = new List<Exception>();
        var errors2 = new List<Exception>();
        var error = new InvalidOperationException("test");

        await using var sub1 = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                lock (_gate)
                {
                    errors1.Add(ex);
                }

                return default;
            });
        await using var sub2 = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                lock (_gate)
                {
                    errors2.Add(ex);
                }

                return default;
            });

        await subject.OnErrorResumeAsync(error, CancellationToken.None);
        await subject.OnCompletedAsync(Result.Success);

        await Assert.That(errors1).Count().IsEqualTo(1);
        await Assert.That(errors2).Count().IsEqualTo(1);
    }

    /// <summary>Tests that concurrent subject forwards OnCompleted to multiple observers concurrently.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenConcurrentSubjectWithMultipleObservers_ThenAllReceiveOnCompleted()
    {
        var subject = SubjectAsync.Create<int>(new()
        {
            PublishingOption = PublishingOption.Concurrent, IsStateless = false
        });
        var completed1 = new TaskCompletionSource();
        var completed2 = new TaskCompletionSource();

        await using var sub1 = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            _ =>
            {
                completed1.TrySetResult();
                return default;
            });
        await using var sub2 = await subject.Values.SubscribeAsync(
            static (_, _) => default,
            null,
            _ =>
            {
                completed2.TrySetResult();
                return default;
            });

        await subject.OnCompletedAsync(Result.Success);

        await completed1.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await completed2.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }
}
