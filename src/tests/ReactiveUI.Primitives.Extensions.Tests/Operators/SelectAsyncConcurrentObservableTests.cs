// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive;
using System.Reactive.Subjects;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions.Operators;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests concurrent projection completion, errors, and disposal during projection.</summary>
public class SelectAsyncConcurrentObservableTests
{
    /// <summary>Synthetic error message attached to a failing selector.</summary>
    private const string SelectorErrorMessage = "selector failed";

    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Max concurrency used for two-in-flight tests.</summary>
    private const int MaxConcurrencyTwo = 2;

    /// <summary>Max concurrency used for four-in-flight tests.</summary>
    private const int MaxConcurrencyFour = 4;

    /// <summary>Verifies that <c>SelectAsyncConcurrent</c> forwards selector exceptions.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncConcurrentSelectorThrows_ThenForwardsError()
    {
        const int TriggerValue = 1;
        Subject<int> subject = new();
        TaskCompletionSource<Exception> faulted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        InvalidOperationException expected = new(SelectorErrorMessage);
        using var sub = subject.SelectAsyncConcurrent(_ => Task.FromException<int>(expected), MaxConcurrencyTwo).Subscribe(
            static _ => { },
            ex => faulted.TrySetResult(ex));
        subject.OnNext(TriggerValue);
        var caught = await faulted.Task;
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>SelectAsyncConcurrent</c> forwards source errors.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncConcurrentSourceErrors_ThenForwardsError()
    {
        Subject<int> subject = new();
        Exception? caught = null;
        InvalidOperationException expected = new(SourceErrorMessage);
        using var sub = subject.SelectAsyncConcurrent(Task.FromResult, MaxConcurrencyTwo).Subscribe(
            static _ => { },
            ex => caught = ex);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that disposing the subscription mid-flight suppresses further emissions and completion.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncConcurrentDisposedMidFlight_ThenSuppressesEmissionAndCompletion()
    {
        const int TriggerValue = 1;
        TaskCompletionSource<int> gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> results = [];
        var completed = false;
        SelectAsyncConcurrentObservable<int, int>.SelectAsyncConcurrentSink sink = new(Observer.Create<int>(results.Add, () => completed = true), _ => gate.Task, MaxConcurrencyTwo);
        var processing = sink.OnNextAsync(TriggerValue);
        sink.Dispose();
        gate.SetResult(TriggerValue);
        await processing;
        sink.OnCompleted();
        await Assert.That(results).IsEmpty();
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Verifies completion waits for every active projection.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncConcurrentCompletesWithInFlight_ThenDeferredCompletion()
    {
        const int First = 1;
        const int Second = 2;
        Subject<int> subject = new();
        TaskCompletionSource<bool> gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> results = [];
        TaskCompletionSource<bool> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = subject.SelectAsyncConcurrent(
            async x =>
            {
                await gate.Task.ConfigureAwait(false);
                return x;
            },
            MaxConcurrencyFour).Subscribe(results.Add, () => completed.TrySetResult(true));
        subject.OnNext(First);
        subject.OnNext(Second);
        subject.OnCompleted();
        await Assert.That(completed.Task.IsCompleted).IsFalse();
        await Assert.That(results).IsEmpty();
        gate.SetResult(true);
        var done = await completed.Task;
        await Assert.That(done).IsTrue();

        // Delivery is serialized, but concurrent projections may finish in either order.
        int[] sorted = [.. results];
        Array.Sort(sorted);
        await Assert.That(sorted).IsCollectionEqualTo([First, Second]);
    }

    /// <summary>Verifies that <c>OnNext</c>, <c>OnError</c> and a duplicate <c>OnCompleted</c>
    /// arriving after the source has already completed are silently dropped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenEventsAfterCompleted_ThenDropped()
    {
        SyncDirectSource<int> source = new();
        List<int> values = [];
        Exception? caught = null;
        var completedCount = 0;
        using var sub = source.SelectAsyncConcurrent(Task.FromResult, 1).Subscribe(values.Add, ex => caught = ex, () => completedCount++);
        source.Observer.OnCompleted();
        source.Observer.OnNext(1);
        source.Observer.OnError(new InvalidOperationException("late"));
        source.Observer.OnCompleted();
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsEmpty();
        await Assert.That(caught).IsNull();
    }

    /// <summary>Verifies an observer that marshals to another thread which completes the source does not deadlock the projection delivery.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task WhenObserverMarshalsCompletionDuringProjectionDelivery_ThenNoDeadlock() =>
        SerializedDeliveryAssertions.ObserverMarshallingCompletionDoesNotDeadlock<int>(
            static (source, observer) => source.SelectAsyncConcurrent(Task.FromResult, MaxConcurrencyTwo).Subscribe(observer),
            static observer => observer.OnNext(1));
}
